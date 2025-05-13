#ifndef STORAGE_H
#define STORAGE_H
#include <array>
#include <atomic>
#include <boost/serialization/access.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <list>
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
typedef uint64_t RID;
constexpr size_t PAGE_SIZE = 8192;

struct PageHeader
{
  PageID id;
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

/**
 * LRU BufferPool stub (in memory).
 */
class BufferPool
{
private:
  size_t capacity_;

  /**
   * @brief `begin` is MRU; `end()` is LRU.
   */
  std::list<Frame> lru_lists_;
  std::unordered_map<PageID, std::list<Frame>::iterator> cache_;

  FrameIter lookup_or_load_frame(PageID pid);
public:
  explicit BufferPool(const size_t capacity) : capacity_(capacity) {
    cache_.reserve(capacity_);
  }

  /**
   * @brief Fetches a specified `Page`
   * @return
   */
  PageGuard fetch_page(PageID pid);

  /**
   * @brief
   * @param mark_dirty
   */
  void unpin_page(PageID pid, bool mark_dirty);
};

/**
 * @brief RAII-handle for Page
 */
class PageGuard
{
public:
  // -- CTORS/ASSIGNMENT --

  // Empty guard
  PageGuard() noexcept = default;

  PageGuard(BufferPool* p, FrameIter it) noexcept:
  pool(p), it(it)
  {
    ++it->pin_count;
  }

  PageGuard(PageGuard&& g) noexcept
  {
    swap(g);
  }

  PageGuard& operator=(PageGuard&& g) noexcept
  {
    if (this != &g) { release(); swap(g); }
    return *this;
  }

  ~PageGuard() noexcept
  {
    release();
  }

  // -- PAGE ACCESS --
  Page* operator->() const noexcept { return &it->page; }
  Page& operator* () const noexcept { return  it->page; }

  void mark_dirty() const noexcept
  {
    it->is_dirty = true;
  }

private:
  BufferPool*  pool {nullptr};
  FrameIter it;                   // nullptr-equivalent means “empty”

  void release() noexcept
  {
    if (pool)
    {
      // guard has a page
      pool->unpin_page(it->page.hdr.id, it->is_dirty);
    }
    pool = nullptr;
  }

  void swap(PageGuard& g) noexcept
  {
    std::swap(pool, g.pool);
    std::swap(it  , g.it  );
  }
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

#endif //STORAGE_H
