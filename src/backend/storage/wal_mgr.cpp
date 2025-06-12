#include "wal_mgr.h"

#include <chrono>
#include <cstring>
#include <vector>

#include "bfrpl.h"

WAL_mgr::WAL_mgr(const std::filesystem::path& path)
    : path_(path), wal_stream_(path, std::ios::binary | std::ios::app)
{
  writer_thread_ = std::thread(&WAL_mgr::writer_thread_main, this);
}

WAL_mgr::~WAL_mgr()
{
  stop_writer_.store(true);
  cv_.notify_one();
  if (writer_thread_.joinable())
  {
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
      for (const auto& batch : local_queue)
      {
        wal_stream_.write(batch->data.data(), batch->data.size());
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
  size_t payload_size = (payload) ? hdr.lr_length - sizeof(hdr) : 0;
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
        PageGuard page = bfr_manager.fetch_page(upd->page_id);

        // 2) apply the "after" image
        std::memcpy(page->data() + upd->offset, upd->aft(), upd->length);

        // 3) bump page’s LSN so we don't reapply older records
        page->hdr.page_lsn = hdr.lsn;

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
