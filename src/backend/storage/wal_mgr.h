#ifndef WAL_MGR_H
#define WAL_MGR_H
#include <cstddef>
#include <filesystem>
#include <fstream>

#include "storage.h"

class BufferPool;

/* --------- WAL-related ---------------*/
enum LR_TYPE
{
  UPDATE,
  COMMIT,
  ABORT,
};

#pragma pack(push, 1)
struct LogRecordHeader
{
  LSN lsn;
  LSN prev_lsn;
  LR_TYPE type;
  uint32_t lr_length; // Record length including the header
  // Todo: add checksum
};
#pragma pack(pop)

struct UpdatePagePayload {
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
  static UpdatePagePayload* create(uint32_t pid, uint16_t off, uint16_t len) {
    // sizeof(header) + payload for bef+aft
    size_t total = sizeof(UpdatePagePayload) + size_t(len)*2*sizeof(std::byte);
    void* mem = operator new(total);
    return new(mem) UpdatePagePayload{pid, off, len};
  }

  // convenience accessors
  const std::byte* bef() const { return data; }
  const std::byte* aft() const { return data + length; }

  // intrusively serialize header + payload as raw bytes
  template<class Archive>
  void serialize(Archive& ar, unsigned /*ver*/) {
    ar & page_id & offset & length;
    ar & boost::serialization::make_binary_object(data, length*2);
  }
};

class WAL_mgr
{
private:
  std::filesystem::path path_;
  std::ofstream wal_stream_;
  LSN next_lsn_ = 1;
  LSN flushed_lsn_ = 0;

public:
  explicit WAL_mgr(const std::filesystem::path& path)
    : path_(path), wal_stream_(path, std::ios::binary | std::ios::app)
  {}

  /**
   * @brief Appends a log record to the WAL file.
   * @param hdr {in} The header of the log record.
   * @param payload {in} The log record payload.
   * @return The LSN of the log.
   */
  LSN append_record(LogRecordHeader& hdr, const void* payload = nullptr);

  void recover(BufferPool &bfr_manager, const std::filesystem::path& path);

  void flush_to_lsn(LSN target)
  {
    if (target > flushed_lsn_) wal_stream_.flush();
  }
};

#endif //WAL_MGR_H
