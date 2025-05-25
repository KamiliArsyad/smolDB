#include <unordered_map>

#include "dsk_mgr.h"
#include "storage.h"

#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

class WAL_mgr;
class PageGuard;

/**
 * LRU BufferPool.
 */
class BufferPool
{
private:
  size_t capacity_;

  Disk_mgr* disk_mgr_;
  WAL_mgr* wal_mgr_;

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

  explicit BufferPool(const size_t cap, Disk_mgr* d, WAL_mgr* w)
    : capacity_(cap), disk_mgr_(d), wal_mgr_(w)
  {
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

  void flush_page(FrameIter it) const;

  /**
   * @brief For checkpointing.
   */
  void flush_all();
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

#endif //BUFFERPOOL_H
