#ifndef WAL_MGR_H
#define WAL_MGR_H
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "storage.h"

class BufferPool;

/* --------- WAL-related ---------------*/
enum LR_TYPE
{
  BEGIN,
  UPDATE,
  COMMIT,
  ABORT,
};

#pragma pack(push, 1)
struct LogRecordHeader
{
  LSN lsn;
  LSN prev_lsn;
  uint64_t txn_id;
  LR_TYPE type;
  uint32_t lr_length;  // Record length including the header
  // Todo: add checksum
};
#pragma pack(pop)

struct UpdatePagePayload
{
  uint32_t page_id;
  uint16_t offset;
  uint16_t length;
  std::byte data[];

  /**
   * @brief Allocate header + 2*length bytes in one go
   * @param pid {in} the page id of the updated record.
   * @param off {in} the offset within page of the updated record
   * @param len {in} the length of the block updated
   * @return A struct
   */
  static UpdatePagePayload* create(uint32_t pid, uint16_t off, uint16_t len)
  {
    // sizeof(header) + payload for bef+aft
    size_t total =
        sizeof(UpdatePagePayload) + size_t(len) * 2 * sizeof(std::byte);
    void* mem = operator new(total);
    return new (mem) UpdatePagePayload{pid, off, len};
  }

  // convenience accessors
  const std::byte* bef() const { return data; }
  const std::byte* aft() const { return data + length; }

  // intrusively serialize header + payload as raw bytes
  template <class Archive>
  void serialize(Archive& ar, unsigned /*ver*/)
  {
    ar & page_id & offset & length;
    ar& boost::serialization::make_binary_object(data, length * 2);
  }
};

// A self-contained unit of work for the logger thread.
struct LogRecordBatch
{
  std::vector<char> data;
  std::promise<void> flushed;
};

class WAL_mgr
{
 public:
  explicit WAL_mgr(const std::filesystem::path& path);
  ~WAL_mgr();

  // No copy/move
  WAL_mgr(const WAL_mgr&) = delete;
  WAL_mgr& operator=(const WAL_mgr&) = delete;

  /**
   * @brief Appends a log record to the WAL file. This is thread-safe.
   * @param hdr {in} The header of the log record.
   * @param payload {in} The log record payload.
   * @return The LSN of the log.
   */
  LSN append_record(LogRecordHeader& hdr, const void* payload = nullptr);

  void recover(BufferPool& bfr_manager, const std::filesystem::path& path);

  void read_all_records_for_txn(
      uint64_t txn_id,
      std::vector<std::pair<LogRecordHeader, std::vector<char>>>& out);

  // This method may be called externally and needs a lock
  void flush_to_lsn(LSN target)
  {
    std::scoped_lock lock(general_mtx_);
    if (target > flushed_lsn_) wal_stream_.flush();
  }

 private:
  void writer_thread_main();

  std::filesystem::path path_;
  std::ofstream wal_stream_;
  std::atomic<LSN> next_lsn_ = 1;
  std::atomic<LSN> flushed_lsn_ = 0;

  // Mtx to protect operations that don't go through the writer thread queue.
  std::mutex general_mtx_;
  std::mutex mtx_;
  std::condition_variable cv_;
  std::list<std::unique_ptr<LogRecordBatch>> write_queue_;

  std::thread writer_thread_;
  std::atomic<bool> stop_writer_ = false;
};

#endif  // WAL_MGR_H