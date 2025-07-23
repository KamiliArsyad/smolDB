#ifndef WAL_MGR_H
#define WAL_MGR_H
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "storage.h"

namespace smoldb
{

class BufferPool;

/* --------- WAL-related ---------------*/
enum LR_TYPE
{
  BEGIN,
  UPDATE,
  COMMIT,
  ABORT,
  CLR
};

#pragma pack(push, 1)
struct LogRecordHeader
{
  LSN lsn;
  LSN prev_lsn;
  uint64_t txn_id;
  LR_TYPE type;
  uint32_t lr_length;  // Record length including the header
};
#pragma pack(pop)

struct UpdatePagePayload
{
  PageID page_id;
  uint16_t offset;
  uint16_t length;
  std::byte data[];

  static UpdatePagePayload* create(PageID pid, uint16_t off, uint16_t len)
  {
    size_t total =
        sizeof(UpdatePagePayload) + size_t(len) * 2 * sizeof(std::byte);
    void* mem = operator new(total);
    return new (mem) UpdatePagePayload{pid, off, len};
  }

  const std::byte* bef() const { return data; }
  const std::byte* aft() const { return data + length; }
};

struct CLR_Payload
{
  PageID page_id;
  uint16_t offset;
  uint16_t length;
  LSN undoNextLSN;
  std::byte data[];

  static CLR_Payload* create(PageID pid, uint16_t off, uint16_t len,
                             LSN undo_next)
  {
    size_t total = sizeof(CLR_Payload) + size_t(len) * sizeof(std::byte);
    void* mem = operator new(total);
    auto* clr = new (mem) CLR_Payload{pid, off, len, undo_next};
    return clr;
  }

  const std::byte* compensation_data() const { return data; }
};

// A self-contained unit of work for the logger thread.
struct LogRecordBatch
{
  std::vector<char> data;
};

class WAL_mgr
{
 public:
  explicit WAL_mgr(const std::filesystem::path& path,
                   boost::asio::any_io_executor executor);
  ~WAL_mgr();

  // No copy/move
  WAL_mgr(const WAL_mgr&) = delete;
  WAL_mgr& operator=(const WAL_mgr&) = delete;

  // DEPRECATED: Will be removed in Phase 2.
  LSN append_record(LogRecordHeader& hdr, const void* payload = nullptr);

  // ASYNC API: Appends a record to the WAL buffer. Does NOT block.
  LSN append_record_async(std::unique_ptr<LogRecordBatch>&& batch);

  // ASYNC API: Asynchronously waits for an LSN to be durable.
  template <typename CompletionToken>
  auto async_wait_for_flush(LSN lsn, CompletionToken&& token)
  {
    auto initiation = [this](auto&& completion_handler, LSN lsn_to_wait)
    {
      // Acquire lock before checking the condition.
      std::lock_guard lock(waiters_mutex_);

      // Re-check the condition while holding the lock.
      if (is_lsn_flushed(lsn_to_wait))
      {
        // Post completion to the executor to avoid re-entrancy.
        boost::asio::post(executor_,
                          [handler = std::move(completion_handler)]() mutable
                          { handler(boost::system::error_code{}); });
        return;
      }

      // If not yet flushed, it's now safe to emplace the handler.
      auto shared_handler =
          std::make_shared<std::decay_t<decltype(completion_handler)>>(
              std::move(completion_handler));

      waiters_.emplace(lsn_to_wait, [shared_handler]() mutable
                       { (*shared_handler)(boost::system::error_code{}); });
    };

    return boost::asio::async_initiate<CompletionToken,
                                       void(boost::system::error_code)>(
        initiation, token, lsn);
  }

  // Checks if a given LSN is durable (i.e., has been fsync'd to disk).
  bool is_lsn_flushed(LSN lsn) const
  {
    return lsn <= flushed_lsn_.load(std::memory_order_acquire);
  }

  void recover(BufferPool& bfr_manager, const std::filesystem::path& path);

  void read_all_records_for_txn(
      uint64_t txn_id,
      std::vector<std::pair<LogRecordHeader, std::vector<char>>>& out);

  std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>>
  read_all_records();

  bool get_record(LSN lsn, LogRecordHeader& header_out,
                  std::vector<char>& payload_out);

  void flush_to_lsn(LSN target)
  {
    if (is_lsn_flushed(target)) return;
    std::unique_lock lk(flush_mutex_);
    flush_cv_.wait(lk, [&] { return is_lsn_flushed(target); });
  }

  class BatchBuilder
  {
    friend class WAL_mgr;
    std::unique_ptr<LogRecordBatch> batch_;
    LogRecordHeader* hdr_;  // points inside batch_->data

    BatchBuilder(std::unique_ptr<LogRecordBatch>&& b, LogRecordHeader* h)
        : batch_(std::move(b)), hdr_(h)
    {
    }

   public:
    template <typename T>
    T* payload()
    {
      return reinterpret_cast<T*>(batch_->data.data() +
                                  sizeof(LogRecordHeader));
    }

    std::byte* extra(std::size_t offset)
    {
      return reinterpret_cast<std::byte*>(batch_->data.data() +
                                          sizeof(LogRecordHeader) + offset);
    }

    std::unique_ptr<LogRecordBatch>&& done() { return std::move(batch_); }
  };

  // generic factory to make batch
  static BatchBuilder make_batch(LR_TYPE type, uint64_t txn_id, LSN prev_lsn,
                                 std::size_t payload_bytes,
                                 std::size_t extra_bytes = 0);

 private:
  void writer_thread_main();

  std::filesystem::path path_;
  std::ofstream wal_stream_;
  std::atomic<LSN> next_lsn_ = 1;
  std::atomic<LSN> flushed_lsn_ = 0;

  boost::asio::any_io_executor executor_;

  std::mutex general_mtx_;
  std::mutex mtx_;
  std::condition_variable cv_;
  std::list<std::unique_ptr<LogRecordBatch>> write_queue_;

  std::map<LSN, std::streamoff> lsn_to_offset_idx_;

  std::thread writer_thread_;
  std::atomic<bool> stop_writer_ = false;

  // --- State for async waiting ---
  std::mutex waiters_mutex_;
  std::multimap<LSN, std::function<void()>> waiters_;

  // --- State for blocking append_record ---
  std::mutex flush_mutex_;
  std::condition_variable flush_cv_;
};

}  // namespace smoldb
#endif  // WAL_MGR_H