#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "dsk_mgr.h"
#include "storage.h"

class WAL_mgr;
class PageGuard;

// Forward declaration
class BufferPool;

/**
 * @struct BufferPoolShard
 * @brief A self-contained shard of the Buffer Pool.
 *
 * Each shard manages its own subset of frames, its own cache, and its own
 * LRU list, all protected by a dedicated mutex. This is the core of our
 * strategy to reduce lock contention and enable high concurrency. By
 * partitioning pages across multiple shards, threads operating on different
 * shards can proceed in parallel without blocking one another.
 */
struct BufferPoolShard
{
  /** A dedicated mutex protecting this shard's metadata (cache and lru_list).
   */
  std::mutex mutex_;
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

  // Non-copyable and non-assignable due to the std::mutex member.
  BufferPoolShard(const BufferPoolShard&) = delete;
  BufferPoolShard& operator=(const BufferPoolShard&) = delete;

  // Movable to allow placement in std::vector.
  BufferPoolShard(BufferPoolShard&& other) noexcept
      : cache_(std::move(other.cache_)),
        lru_list_(std::move(other.lru_list_)),
        capacity_(other.capacity_)
  {
  }
  BufferPoolShard& operator=(BufferPoolShard&& other) noexcept
  {
    if (this != &other)
    {
      cache_ = std::move(other.cache_);
      lru_list_ = std::move(other.lru_list_);
      // capacity_ is const and should not be moved.
    }
    return *this;
  }
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

 private:
  friend class BufferPoolTest;  // Allow test to access internals
  friend class ConcurrencyTest;

  /**
   * @brief Determines which shard a given PageID belongs to.
   * This is the core routing function for the sharded design.
   * @param pid The PageID to route.
   * @return A reference to the responsible BufferPoolShard.
   */
  BufferPoolShard& get_shard(PageID pid);

  std::vector<BufferPoolShard> shards_;
  const size_t shard_count_;

  Disk_mgr* disk_mgr_;
  WAL_mgr* wal_mgr_;
};

/**
 * @class PageGuard
 * @brief An RAII-style guard that manages the pinning and unpinning of a page.
 *
 * When a PageGuard is created, it pins the page in the buffer pool.
 * When it goes out of scope, its destructor automatically unpins the page,
 * making it eligible for eviction again. This prevents memory safety issues.
 */
class PageGuard
{
 public:
  PageGuard() noexcept = default;
  PageGuard(BufferPool* p, Frame* f) noexcept : pool(p), frame(f)
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

  Page* operator->() const noexcept { return &frame->page; }
  Page& operator*() const noexcept { return frame->page; }

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

 private:
  BufferPool* pool{nullptr};
  Frame* frame{nullptr};
  bool is_dirty_on_unpin_{false};

  void release() noexcept
  {
    if (frame && pool)
    {
      pool->unpin_page(frame, frame->is_dirty.load(std::memory_order_relaxed));
    }
    pool = nullptr;
    frame = nullptr;
  }

  void swap(PageGuard& other) noexcept
  {
    std::swap(pool, other.pool);
    std::swap(frame, other.frame);
  }
};

#endif  // BUFFERPOOL_H