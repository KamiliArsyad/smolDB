#ifndef STORAGE_H
#define STORAGE_H
#include <array>
#include <atomic>
#include <boost/serialization/access.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <list>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

enum class Col_type
{
  INT,
  FLOAT,
  STRING,
  DATETIME
};

struct Column
{
  uint8_t   id;
  std::string name;
  Col_type   type;
  bool      nullable;
  std::vector<std::byte> default_bytes;

private:
  friend class boost::serialization::access;
  template<class Ar>
  void serialize(Ar& ar, unsigned) {
      ar & id & name & type & nullable & default_bytes;
  }
};

using Schema = std::vector<Column>;

class Catalog
{
  std::unordered_map<uint8_t,Schema> m_tables;

  friend class boost::serialization::access;
  template<class Ar>
  void serialize(Ar& ar, unsigned) {
    ar & m_tables;
  }
public:
  void register_schema(const uint8_t table_id, Schema s)
  {
    m_tables[table_id] = std::move(s);
  }

  void dump(const std::filesystem::path& path) const;

  void load(const std::filesystem::path& path);
};

typedef uint64_t PageID;
using LSN = uint64_t;
typedef uint64_t RID;
constexpr size_t PAGE_SIZE = 8192;

struct PageHeader
{
  PageID id;
  LSN page_lsn; // Latest LSN of the log that modifies this page.
};

struct alignas(64) Page
{
  PageHeader hdr;
  std::array<std::byte, PAGE_SIZE - sizeof(PageHeader)> raw_array;

  [[nodiscard]]
  std::byte* data() noexcept { return raw_array.data(); }
  [[nodiscard]]
  const std::byte* data() const noexcept { return raw_array.data(); }
};

static_assert(sizeof(Page) == PAGE_SIZE, "Incorrect page size");

struct Frame
{
  Page page;
  std::atomic<bool> is_dirty = false;
  std::atomic<int>  pin_count = 0;
};

using FrameIter = std::list<Frame>::iterator;

class PageGuard;
class Disk_mgr;
class WAL_mgr;
class BufferPool;

class Disk_mgr {
public:
  explicit Disk_mgr(const std::filesystem::path& db_file_path)
    : path_(db_file_path)
  {
    // Try open existing file
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
      // Create it if missing
      std::ofstream create(path_, std::ios::out | std::ios::binary);
      create.close();
      // Re-open for read/write
      file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!file_.is_open()) {
      throw std::runtime_error("DiskManager: cannot open file " + path_.string());
    }
  }
  ~Disk_mgr()
  {
    if (file_.is_open()) file_.close();
  }

  // Reads the PAGE_SIZE bytes for `page_id` into `page`.
  // If the file is too short, zero-fills the rest of `page`.
  void read_page(PageID page_id, Page& page);

  // Writes the PAGE_SIZE bytes from `page` at the offset for `page_id`.
  // Always flushes to ensure durability.
  void write_page(PageID page_id, const Page& page);

private:
  std::filesystem::path path_;
  std::fstream         file_;

  // Compute the byte offset for the start of page `page_id`.
  static constexpr std::streamoff offset_for(PageID pid) {
    return static_cast<std::streamoff>(pid) * PAGE_SIZE;
  }

  void ensure_open();
};

template <typename Tuple>
class HeapFile
{
  static_assert(std::is_trivially_copyable_v<Tuple>,
    "Tuple must be POD for raw-copy heap file v0");
private:
  std::filesystem::path path_;
  std::fstream file_;

  void open_file() {
    file_.open(path_, std::ios::in | std::ios::out |
                        std::ios::binary | std::ios::app);
    if (!file_) {  // create file if it doesn't exist
      file_.clear();
      file_.open(path_, std::ios::out | std::ios::binary);
      file_.close();
      file_.open(path_, std::ios::in | std::ios::out |
                          std::ios::binary | std::ios::app);
    }
  }

public:
  explicit HeapFile(std::filesystem::path path)
  : path_(path)
  {
    open_file();
  }

  RID append(const Tuple& t)
  {
    file_.seekp(0, std::ios::end);
    const uint64_t off = file_.tellp();
    file_.write(reinterpret_cast<const char*>(&t), sizeof(Tuple));
    file_.flush();
    const RID rid = off / sizeof(Tuple);  // simple RID for now
    return rid;
  }

  std::vector<Tuple> full_scan()
  {
    file_.seekg(0, std::ios::end);
    const auto bytes = file_.tellg();
    size_t n   = bytes / sizeof(Tuple);
    std::vector<Tuple> vec(n);
    file_.seekg(0);
    file_.read(reinterpret_cast<char*>(vec.data()), bytes);

    return vec;
  }
};

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

#endif //STORAGE_H
