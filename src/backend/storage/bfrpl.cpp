#include "bfrpl.h"

#include <cassert>

#include "dsk_mgr.h"
#include "wal_mgr.h"

BufferPool::BufferPool(size_t capacity, Disk_mgr* d, WAL_mgr* w,
                       size_t shard_count)
    : shard_count_(shard_count), disk_mgr_(d), wal_mgr_(w)
{
  assert(shard_count > 0 && "Shard count must be positive.");
  size_t capacity_per_shard = capacity / shard_count;
  assert(capacity_per_shard > 0 && "Total capacity too small for shard count.");

  for (size_t i = 0; i < shard_count_; ++i)
  {
    shards_.emplace_back(capacity_per_shard);
  }
}

BufferPoolShard& BufferPool::get_shard(PageID pid)
{
  // A simple hash-based partitioning strategy. This is a designated extension
  // point for more sophisticated, workload-aware partitioning in the future.
  return shards_[pid % shard_count_];
}

PageGuard BufferPool::fetch_page(PageID pid)
{
  auto& shard = get_shard(pid);
  std::unique_lock<std::mutex> lock(shard.mutex_);

  // 1. Check if page is already in this shard's cache.
  auto cache_it = shard.cache_.find(pid);
  if (cache_it != shard.cache_.end())
  {
    auto frame_it = cache_it->second;
    // Move frame to front of LRU list to mark it as most recently used.
    shard.lru_list_.splice(shard.lru_list_.begin(), shard.lru_list_, frame_it);
    // Return a guard that pins the frame. The lock is released upon exit.
    return PageGuard(this, &(*frame_it));
  }

  // 2. Page not in cache, must load from disk. Check for free space.
  if (shard.lru_list_.size() >= shard.capacity_)
  {
    // 2a. Shard is full, must evict a page.
    // Scan from the LRU end to find an unpinned page.
    auto victim_it = std::prev(shard.lru_list_.end());
    while (true)
    {
      if (victim_it->pin_count.load() == 0)
      {
        // Found a victim.
        break;
      }
      if (victim_it == shard.lru_list_.begin())
      {
        // Scanned the entire shard and all pages are pinned.
        throw std::runtime_error("BufferPool: All pages in shard are pinned.");
      }
      victim_it = std::prev(victim_it);
    }

    // If victim is dirty, write it to disk before evicting.
    if (victim_it->is_dirty.load())
    {
      flush_page(victim_it, shard);
    }

    // Remove victim from metadata.
    shard.cache_.erase(victim_it->page.hdr.id);
    shard.lru_list_.erase(victim_it);
  }

  // 3. Create a new frame for the page.
  shard.lru_list_.emplace_front();
  Frame& new_frame = shard.lru_list_.front();

  // 4. Read page data from disk into the new frame.
  // This I/O is done while holding the shard lock. This is the safest approach,
  // though a future optimization could release the lock if profiling shows
  // it to be a bottleneck.
  disk_mgr_->read_page(pid, new_frame.page);

  // 5. Setup frame metadata.
  new_frame.page.hdr.id = pid;
  new_frame.pin_count.store(0, std::memory_order_relaxed);
  new_frame.is_dirty.store(false, std::memory_order_relaxed);

  // 6. Update the cache to point to the new frame.
  shard.cache_[pid] = shard.lru_list_.begin();

  // 7. Return a guard for the newly loaded and pinned page.
  return PageGuard(this, &new_frame);
}

void BufferPool::unpin_page(Frame* frame, bool mark_dirty)
{
  if (mark_dirty)
  {
    frame->is_dirty.store(true, std::memory_order_release);
  }
  // This atomic fetch-sub is the core of the lock-free unpin operation.
  frame->pin_count.fetch_sub(1, std::memory_order_acq_rel);
}

void BufferPool::flush_page(std::list<Frame>::iterator frame_iter,
                            BufferPoolShard& shard)
{
  // This function MUST be called with the shard's lock already held.
  assert(wal_mgr_ && "Fatal: WAL manager not configured!");
  if (!frame_iter->is_dirty.load(std::memory_order_acquire)) return;

  // 1. ARIES: Ensure all log records for this page are on disk before the page
  // itself.
  wal_mgr_->flush_to_lsn(frame_iter->page.hdr.page_lsn);

  // 2. Write the page data to the db file.
  disk_mgr_->write_page(frame_iter->page.hdr.id, frame_iter->page);

  // 3. Mark the page as no longer dirty.
  frame_iter->is_dirty.store(false, std::memory_order_release);
}

void BufferPool::flush_all()
{
  for (auto& shard : shards_)
  {
    std::scoped_lock lock(shard.mutex_);
    for (auto it = shard.lru_list_.begin(); it != shard.lru_list_.end(); ++it)
    {
      if (it->is_dirty.load())
      {
        flush_page(it, shard);
      }
    }
  }
}

PageID BufferPool::allocate_page()
{
  if (!disk_mgr_)
  {
    throw std::runtime_error("BufferPool: Disk_mgr is not initialized.");
  }
  return disk_mgr_->allocate_page();
}