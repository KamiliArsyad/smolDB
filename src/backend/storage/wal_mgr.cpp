#include "wal_mgr.h"

#include <chrono>
#include <cstring>
#include <vector>

#include "bfrpl.h"

using namespace smoldb;

WAL_mgr::WAL_mgr(const std::filesystem::path& path) : path_(path)
{
  // To be safe, we're locking the WAL.
  std::scoped_lock lock(general_mtx_);

  // Before starting the writer thread or opening the stream for appending,
  // we must first scan the existing WAL to find the correct starting LSN.
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

      // Seek past the payload to get to the next record quickly.
      if (hdr.lr_length > sizeof(LogRecordHeader))
      {
        in.seekg(hdr.lr_length - sizeof(LogRecordHeader), std::ios::cur);
      }
    }
    in.close();
  }

  // Initialize our atomic counter to the next available LSN.
  next_lsn_.store(max_lsn + 1);
  flushed_lsn_.store(max_lsn);

  // Now, open the stream in append mode for runtime writes.
  wal_stream_.open(path_, std::ios::binary | std::ios::app);

  // The writer thread starts on construction.
  writer_thread_ = std::thread(&WAL_mgr::writer_thread_main, this);
}

WAL_mgr::~WAL_mgr()
{
  // The writer thread is stopped and joined on destruction.
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
      /**
       * We can be assured that this won't cause a deadlock as the only other
       * possible holder of `general_mtx_` is `read_all_records_for_txn` which
       * does not wait for any other mutex.
       */
      std::scoped_lock lock(general_mtx_);  // Lock before writing to the file
      for (const auto& batch : local_queue)
      {
        std::streamoff offset = wal_stream_.tellp();

        const auto* hdr =
            reinterpret_cast<const LogRecordHeader*>(batch->data.data());
        lsn_to_offset_idx_[hdr->lsn] = offset;

        wal_stream_.write(reinterpret_cast<const std::ostream::char_type*>(hdr),
                          batch->data.size());
      }
      wal_stream_.flush();

      for (auto& batch : local_queue)
      {
        batch->flushed.set_value();
      }
      local_queue.clear();
    }
  }
}

LSN WAL_mgr::append_record(LogRecordHeader& hdr, const void* payload)
{
  hdr.lsn = next_lsn_.fetch_add(1);

  auto batch = std::make_unique<LogRecordBatch>();
  size_t payload_size = hdr.lr_length - sizeof(hdr);
  batch->data.resize(sizeof(hdr) + payload_size);

  std::memcpy(batch->data.data(), &hdr, sizeof(hdr));
  if (payload)
  {
    std::memcpy(batch->data.data() + sizeof(hdr), payload, payload_size);
  }

  auto future = batch->flushed.get_future();

  {
    std::scoped_lock<std::mutex> lock(mtx_);
    write_queue_.push_back(std::move(batch));
  }

  cv_.notify_one();

  // Wait for the writer thread to flush this record to disk
  future.wait();
  flushed_lsn_.store(hdr.lsn);

  return hdr.lsn;
}

void WAL_mgr::read_all_records_for_txn(
    uint64_t target_txn_id,
    std::vector<std::pair<LogRecordHeader, std::vector<char>>>& out)
{
  std::scoped_lock lock(general_mtx_);
  out.clear();

  // Ensure any buffered writes are on disk before reading
  wal_stream_.flush();

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    // This can happen if no WAL records were ever written. It's not an error.
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
    if (in.gcount() != sizeof(hdr)) break;  // truncated header

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
        // reinterpret the payload buffer as our struct
        auto* upd = reinterpret_cast<const UpdatePagePayload*>(buf.data());

        // 1) pin the page
        auto page = bfr_manager.fetch_page(upd->page_id);
        auto pw = page.write();

        // 2) apply the "after" image
        std::memcpy(pw->data() + upd->offset, upd->aft(), upd->length);

        // 3) bump page’s LSN so we don't reapply older records
        pw->hdr.page_lsn = hdr.lsn;

        // 4) unpin as dirty so it'll be flushed later
        page.mark_dirty();
        break;
      }

      case COMMIT:
      case ABORT:
        // nothing to do for now
        break;
      default:
        // skip unknown record types
        break;
    }
  }

  in.close();
}

std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>>
WAL_mgr::read_all_records()
{
  std::scoped_lock lock(general_mtx_);
  std::unordered_map<LSN, std::pair<LogRecordHeader, std::vector<char>>>
      records;

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return {records.begin(), records.end()};
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
  return {records.begin(), records.end()};
}

bool WAL_mgr::get_record(LSN lsn, LogRecordHeader& header_out,
                         std::vector<char>& payload_out)
{
  std::scoped_lock lock(general_mtx_);
  if (!lsn_to_offset_idx_.contains(lsn)) return false;
  auto offset = lsn_to_offset_idx_[lsn];

  // Open a separate ifstream for reading. This is critical to avoid interfering
  // with the state of the append-only wal_stream_.
  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return false;  // Should not happen if WAL exists.
  }

  in.seekg(offset);
  if (!in) return false;

  in.read(reinterpret_cast<char*>(&header_out), sizeof(LogRecordHeader));
  if (in.gcount() != sizeof(LogRecordHeader)) return false;

  // Sanity check that the record on disk has the LSN we expect.
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
