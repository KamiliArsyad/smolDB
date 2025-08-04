#include "wal_mgr.h"

#include <chrono>
#include <cstring>
#include <vector>

#include "bfrpl.h"

using namespace smoldb;

WAL_mgr::WAL_mgr(const std::filesystem::path& path,
                 boost::asio::any_io_executor executor)
    : path_(path), executor_(std::move(executor))
{
  std::scoped_lock lock(general_mtx_);

  LSN max_lsn = 0;
  std::ifstream in(path_, std::ios::binary);
  if (in.is_open())
  {
    std::streamoff curr_offset = 0;
    while (in.peek() != EOF)
    {
      curr_offset = in.tellg();
      LogRecordHeader hdr{};
      in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
      if (in.gcount() != sizeof(hdr)) break;
      if (hdr.lsn == 0) continue;
      lsn_to_offset_idx_[hdr.lsn] = curr_offset;
      max_lsn = std::max(max_lsn, hdr.lsn);
      if (hdr.lr_length > sizeof(LogRecordHeader))
      {
        in.seekg(hdr.lr_length - sizeof(LogRecordHeader), std::ios::cur);
      }
    }
    in.close();
  }

  next_lsn_.store(max_lsn + 1);
  flushed_lsn_.store(max_lsn);

  wal_stream_.open(path_, std::ios::binary | std::ios::app);
  writer_thread_ = std::thread(&WAL_mgr::writer_thread_main, this);
}

WAL_mgr::~WAL_mgr()
{
  if (writer_thread_.joinable())
  {
    stop_writer_.store(true);
    cv_.notify_one();
    writer_thread_.join();
  }
}

void WAL_mgr::writer_thread_main()
{
  std::list<std::unique_ptr<LogRecordBatch>> local_queue;
  LSN max_lsn_in_batch = 0;

  while (true)
  {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      cv_.wait(lock,
               [&] { return !write_queue_.empty() || stop_writer_.load(); });

      if (stop_writer_.load() && write_queue_.empty())
      {
        return;
      }

      local_queue.splice(local_queue.end(), write_queue_);
    }

    if (!local_queue.empty())
    {
      max_lsn_in_batch = 0;
      {
        std::scoped_lock lock(general_mtx_);
        for (const auto& batch : local_queue)
        {
          std::streamoff offset = wal_stream_.tellp();
          const auto* hdr =
              reinterpret_cast<const LogRecordHeader*>(batch->data.data());
          lsn_to_offset_idx_[hdr->lsn] = offset;
          max_lsn_in_batch = std::max(max_lsn_in_batch, hdr->lsn);
          wal_stream_.write(
              reinterpret_cast<const std::ostream::char_type*>(hdr),
              batch->data.size());
        }
        wal_stream_.flush();
      }

      flushed_lsn_.store(max_lsn_in_batch, std::memory_order_release);

      std::vector<std::function<void()>> handlers_to_run;
      {
        std::lock_guard lock(waiters_mutex_);
        auto range_end = waiters_.upper_bound(max_lsn_in_batch);
        for (auto it = waiters_.begin(); it != range_end; ++it)
        {
          handlers_to_run.push_back(std::move(it->second));
        }
        waiters_.erase(waiters_.begin(), range_end);
      }

      for (auto& handler : handlers_to_run)
      {
        boost::asio::post(executor_, std::move(handler));
      }

      {
        std::lock_guard lock(flush_mutex_);
        flush_cv_.notify_all();
      }

      local_queue.clear();
    }
  }
}

LSN WAL_mgr::append_record_async(std::unique_ptr<LogRecordBatch>&& batch)
{
  auto* hdr = reinterpret_cast<LogRecordHeader*>(batch->data.data());
  LSN reserved_lsn = next_lsn_.fetch_add(1);
  hdr->lsn = reserved_lsn;

  {
    std::scoped_lock<std::mutex> lock(mtx_);
    write_queue_.push_back(std::move(batch));
  }

  cv_.notify_one();

  return reserved_lsn;
}

LSN WAL_mgr::append_record(LogRecordHeader& hdr, const void* payload)
{
  auto batch = std::make_unique<LogRecordBatch>();
  const std::size_t payload_sz =
      hdr.lr_length - sizeof(LogRecordHeader);  // may be 0
  batch->data.resize(sizeof(LogRecordHeader) + payload_sz);

  // copy header; further mutation happens inside append_record_async
  std::memcpy(batch->data.data(), &hdr, sizeof(LogRecordHeader));

  if (payload_sz && payload)
    std::memcpy(batch->data.data() + sizeof(LogRecordHeader), payload,
                payload_sz);

  LSN lsn = append_record_async(std::move(batch));

  std::unique_lock lk(flush_mutex_);
  flush_cv_.wait(lk, [&] { return is_lsn_flushed(lsn); });

  hdr.lsn = lsn;
  return lsn;
}

void WAL_mgr::read_all_records_for_txn(
    uint64_t target_txn_id,
    std::vector<std::pair<LogRecordHeader, std::vector<char>>>& out)
{
  std::scoped_lock lock(general_mtx_);
  out.clear();

  wal_stream_.flush();
  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return;
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    in.read(buf.data(), payload_len);

    if (hdr.txn_id == target_txn_id)
    {
      out.emplace_back(hdr, std::move(buf));
    }
  }

  in.close();
}

void WAL_mgr::recover(BufferPool& bfr_manager,
                      const std::filesystem::path& path)
{
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open())
  {
    throw std::runtime_error("Unable to open WAL file: " + path.string());
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    in.read(buf.data(), payload_len);
    if (static_cast<uint32_t>(in.gcount()) != payload_len)
    {
      throw std::runtime_error("Corrupt WAL: unexpected payload size");
    }

    switch (hdr.type)
    {
      case UPDATE:
      {
        auto* upd = reinterpret_cast<const UpdatePagePayload*>(buf.data());
        auto page = bfr_manager.fetch_page(upd->page_id);
        auto pw = page.write();
        std::memcpy(pw->data() + upd->offset, upd->aft(), upd->length);
        pw->hdr.page_lsn = hdr.lsn;
        page.mark_dirty();
        break;
      }
      case COMMIT:
      case ABORT:
        break;
      default:
        break;
    }
  }

  in.close();
}

std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>>
WAL_mgr::read_all_records()
{
  std::scoped_lock lock(general_mtx_);
  std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>> records;

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return records;
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;
    if (hdr.lr_length < sizeof(LogRecordHeader)) break;

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    if (payload_len > 0)
    {
      in.read(buf.data(), payload_len);
      if ((uint32_t)in.gcount() != payload_len) break;
    }
    records[hdr.lsn] = {hdr, std::move(buf)};
  }
  return records;
}

bool WAL_mgr::get_record(LSN lsn, LogRecordHeader& header_out,
                         std::vector<char>& payload_out)
{
  std::scoped_lock lock(general_mtx_);
  if (!lsn_to_offset_idx_.contains(lsn)) return false;
  auto offset = lsn_to_offset_idx_[lsn];

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return false;
  }

  in.seekg(offset);
  if (!in) return false;

  in.read(reinterpret_cast<char*>(&header_out), sizeof(LogRecordHeader));
  if (in.gcount() != sizeof(LogRecordHeader)) return false;

  if (header_out.lsn != lsn)
  {
    throw std::runtime_error("LSN mismatch: index corruption.");
  }

  uint32_t payload_size = header_out.lr_length - sizeof(LogRecordHeader);
  if (payload_size > 0)
  {
    payload_out.resize(payload_size);
    in.read(payload_out.data(), payload_size);
    if (static_cast<uint32_t>(in.gcount()) != payload_size) return false;
  }
  else
  {
    payload_out.clear();
  }

  return true;
}

WAL_mgr::BatchBuilder WAL_mgr::make_batch(LR_TYPE type, uint64_t txn_id,
                                          LSN prev_lsn, std::size_t payload_sz,
                                          std::size_t extra_sz)
{
  auto batch = std::make_unique<LogRecordBatch>();
  batch->data.resize(sizeof(LogRecordHeader) + payload_sz + extra_sz);

  auto* hdr = reinterpret_cast<LogRecordHeader*>(batch->data.data());
  hdr->lsn = 0;  // filled when appended
  hdr->type = type;
  hdr->txn_id = txn_id;
  hdr->prev_lsn = prev_lsn;
  hdr->lr_length = static_cast<uint32_t>(batch->data.size());

  return BatchBuilder(std::move(batch), hdr);
}
