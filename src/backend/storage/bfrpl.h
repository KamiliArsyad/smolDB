#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <condition_variable>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "dsk_mgr.h"
#include "storage.h"

namespace smoldb
{

class WAL_mgr;
class PageGuard;

// Forward declaration
class BufferPool;

/**
 * @struct BufferPoolShard
 * @brief A self-contained, non-movable, non-copyable shard of the Buffer Pool.
 *
 * Each shard manages its own subset of frames, its own cache, and its own
 * LRU list, all protected by a dedicated mutex. Because this struct contains
 * a std::mutex and std::condition_variable, it is fundamentally non-movable
 * and non-copyable. This contract is enforced by deleting the copy/move
 * constructors and assignment operators, preventing dangerous operations like
 * vector reallocation from causing race conditions. Shards must be constructed
 * in-place (e.g., with vector::emplace_back).
 */
struct BufferPoolShard
{
  /** A dedicated mutex protecting this shard's metadata (cache and lru_list).
   */
  std::mutex mutex_;
  /** A condition variable for threads to wait on when no frames can be evicted.
   */
  std::condition_variable cv_;
  /** Maps a PageID to an iterator in this shard's LRU list. */
  std::unordered_map<PageID, std::list<Frame>::iterator> cache_;
  /** The list of frames managed by this shard, ordered by recent use. */
  std::list<Frame> lru_list_;
  /** The maximum number of frames this shard can hold. */
  const size_t capacity_;

  /**
   * @brief Constructs a BufferPoolShard with a specific capacity.
   * @param capacity The number of frames this shard can manage.
   */
  explicit BufferPoolShard(size_t capacity) : capacity_(capacity) {}

  // This class is non-copyable and non-movable because it owns a mutex.
  // Deleting these operations prevents accidental misuse that could lead to
  // deadlocks or race conditions.
  BufferPoolShard(const BufferPoolShard&) = delete;
  BufferPoolShard& operator=(const BufferPoolShard&) = delete;
  BufferPoolShard(BufferPoolShard&&) = delete;
  BufferPoolShard& operator=(BufferPoolShard&&) = delete;
};

/**
 * @class BufferPool
 * @brief A high-concurrency, sharded, LRU-based buffer manager.
 *
 * This BufferPool distributes pages across multiple internal shards to minimize
 * lock contention. Each shard has its own lock, cache, and LRU list, allowing
 * for parallel operations on different pages. It provides page pinning via the
 * RAII-style PageGuard to ensure memory safety for its users.
 */
class BufferPool
{
 public:
  /**
   * @brief Constructs a sharded BufferPool.
   * @param capacity The total number of frames in the buffer pool.
   * @param d The Disk_mgr for reading/writing pages from/to disk.
   * @param w The WAL_mgr for ensuring log-ahead-of-data rule.
   * @param shard_count The number of concurrent shards to create. Defaults to
   * the hardware concurrency level.
   */
  explicit BufferPool(size_t capacity, Disk_mgr* d, WAL_mgr* w,
                      size_t shard_count = std::thread::hardware_concurrency());

  /**
   * @brief Fetches a page from the buffer pool, loading it from disk if
   * necessary. This method handles all locking and eviction logic internally.
   * @param pid The ID of the page to fetch.
   * @return A PageGuard object that ensures the page is pinned while in use.
   */
  PageGuard fetch_page(PageID pid);

  /**
   * @brief Unpins a page, allowing it to be considered for eviction.
   * This is typically called by the PageGuard destructor.
   * @param frame A pointer to the frame to unpin.
   * @param mark_dirty True if the page was modified and should be marked dirty.
   */
  void unpin_page(Frame* frame, bool mark_dirty);

  /**
   * @brief Flushes a specific dirty page to disk, respecting WAL order.
   * @param frame_iter Iterator to the frame to flush within its shard's list.
   * @param shard The shard to which the frame belongs (its lock must be held).
   */
  void flush_page(std::list<Frame>::iterator frame_iter,
                  BufferPoolShard& shard);

  /**
   * @brief Flushes all dirty pages from all shards to disk.
   * Used for clean shutdown or checkpointing.
   */
  void flush_all();

  /**
   * @brief Allocates a new page ID using the underlying Disk_mgr.
   * @return The newly allocated PageID.
   */
  PageID allocate_page();

  /**
   * @brief Unpins a page, making it eligible for eviction and notifying
   * waiters. This is the new, thread-safe unpin method that solves the livelock
   * problem.
   * @param frame A pointer to the frame to unpin.
   * @param mark_dirty True if the page was modified.
   * @param shard A pointer to the shard the frame belongs to.
   */
  void unpin_page_and_notify(Frame* frame, bool mark_dirty,
                             BufferPoolShard* shard);

 private:
  friend class BufferPoolTest;  // Allow test to access internals
  friend class ConcurrencyTest;

  /**
   * @brief Internal flush helper. Requires the shard's lock to be held by the
   * caller. This replaces the old public flush_page, as flushing now requires
   * shard context.
   */
  void flush_page_unlocked(std::list<Frame>::iterator frame_iter,
                           BufferPoolShard& shard);

  /**
   * @brief Determines which shard a given PageID belongs to.
   * This is the core routing function for the sharded design.
   * @param pid The PageID to route.
   * @return A reference to the responsible BufferPoolShard.
   */
  BufferPoolShard& get_shard(PageID pid);

  std::vector<std::unique_ptr<BufferPoolShard>> shards_;
  const size_t shard_count_;

  Disk_mgr* disk_mgr_;
  WAL_mgr* wal_mgr_;
};

class PageReader;
class PageWriter;

/**
 * @class PageGuard
 * @brief An RAII-style guard that manages the pinning and unpinning of a page.
 *
 * When a PageGuard is created, it pins the page in the buffer pool.
 * When it goes out of scope, its destructor automatically unpins the page,
 * making it eligible for eviction again. This prevents memory safety issues.
 * It provides access to the page's data via PageReader/PageWriter objects
 * that enforce physical page latching.
 */
class PageGuard
{
 public:
  PageGuard() noexcept = default;

  /**
   * @brief PageGuard also holds a pointer to the shard for efficient
   * notification on unpin.
   */
  PageGuard(BufferPool* p, Frame* f, BufferPoolShard* s) noexcept
      : pool(p), frame(f), shard(s)
  {
    if (frame)
    {
      frame->pin_count.fetch_add(1, std::memory_order_relaxed);
    }
  }

  PageGuard(PageGuard&& other) noexcept { swap(other); }
  PageGuard& operator=(PageGuard&& other) noexcept
  {
    if (this != &other)
    {
      release();
      swap(other);
    }
    return *this;
  }

  ~PageGuard() noexcept { release(); }

  // DELETED! Direct access is unsafe. Use read() or write().
  // Page* operator->() const noexcept { return &frame->page; }
  // Page& operator*() const noexcept { return frame->page; }

  void mark_dirty() const noexcept
  {
    if (frame)
    {
      frame->is_dirty.store(true, std::memory_order_release);
    }
  }

  // Deleted copy constructor and assignment to prevent accidental copies.
  PageGuard(const PageGuard&) = delete;
  PageGuard& operator=(const PageGuard&) = delete;

  // Grant read-only, latched access to the page.
  PageReader read() const;
  // Grant write, latched access to the page.
  PageWriter write();

 private:
  BufferPool* pool{nullptr};
  Frame* frame{nullptr};
  BufferPoolShard* shard{nullptr};

  void release() noexcept
  {
    if (frame && pool)
    {
      // Call the notification-aware unpin method.
      pool->unpin_page_and_notify(
          frame, frame->is_dirty.load(std::memory_order_relaxed), shard);
    }
    pool = nullptr;
    frame = nullptr;
    shard = nullptr;
  }

  void swap(PageGuard& other) noexcept
  {
    std::swap(pool, other.pool);
    std::swap(frame, other.frame);
    std::swap(shard, other.shard);
  }
};

/**
 * @class PageReader
 * @brief An RAII accessor that provides read-only access to a page's data.
 * It holds a shared lock on the physical page latch for its entire lifetime.
 */
class PageReader
{
 public:
  const Page* operator->() const { return &frame_->page; }
  const Page& operator*() const { return frame_->page; }

 private:
  friend class PageGuard;
  PageReader(const Frame* frame, std::shared_lock<std::shared_mutex> lock)
      : frame_(frame), lock_(std::move(lock))
  {
  }

  const Frame* frame_;
  std::shared_lock<std::shared_mutex> lock_;
};

/**
 * @class PageWriter
 * @brief An RAII accessor that provides write access to a page's data.
 * It holds a unique lock on the physical page latch for its entire lifetime.
 */
class PageWriter
{
 public:
  Page* operator->() { return &frame_->page; }
  Page& operator*() { return frame_->page; }

 private:
  friend class PageGuard;
  PageWriter(Frame* frame, std::unique_lock<std::shared_mutex> lock)
      : frame_(frame), lock_(std::move(lock))
  {
  }
  Frame* frame_;
  std::unique_lock<std::shared_mutex> lock_;
};

}  // namespace smoldb
#endif  // BUFFERPOOL_H