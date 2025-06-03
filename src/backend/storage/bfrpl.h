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

  /**
   * @brief Allocates a new page ID using the underlying Disk_mgr.
   */
  PageID allocate_page();
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
  pool(p), it_(it)
  {
    ++it_->pin_count;
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
  Page* operator->() const noexcept { return &it_->page; }
  Page& operator* () const noexcept { return  it_->page; }

  void mark_dirty() const noexcept
  {
    it_->is_dirty = true;
  }

  /**
   * @brief Get underlying iterator if needed by HeapFile, though direct use is discouraged
   * @return The underlying {FrameIter} iterator.
   */
  FrameIter get_frame_iterator() const { return it_; }

private:
  BufferPool*  pool {nullptr};
  FrameIter it_{};

  void release() noexcept
  {
    if (pool)
    {
      // guard has a page
      pool->unpin_page(it_->page.hdr.id, it_->is_dirty);
    }
    pool = nullptr;
    it_ = {};
  }

  void swap(PageGuard& g) noexcept
  {
    std::swap(pool, g.pool);
    std::swap(it_, g.it_);
  }
};

#endif //BUFFERPOOL_H
