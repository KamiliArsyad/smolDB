#include "bfrpl.h"

#include <cassert>
#include <chrono>

#include "dsk_mgr.h"
#include "wal_mgr.h"

BufferPool::BufferPool(size_t capacity, Disk_mgr* d, WAL_mgr* w,
                       size_t shard_count)
    : shard_count_(shard_count), disk_mgr_(d), wal_mgr_(w)
{
  assert(shard_count > 0);
  size_t capacity_per_shard = capacity / shard_count;
  assert(capacity_per_shard > 0);

  shards_.reserve(shard_count);
  for (size_t i = 0; i < shard_count_; ++i)
  {
    shards_.emplace_back(std::make_unique<BufferPoolShard>(capacity_per_shard));
  }
}

BufferPoolShard& BufferPool::get_shard(PageID pid)
{
  return *shards_[pid % shard_count_];
}

PageGuard BufferPool::fetch_page(PageID pid)
{
  auto& shard = get_shard(pid);
  std::unique_lock<std::mutex> lock(shard.mutex_);

  auto cache_it = shard.cache_.find(pid);
  if (cache_it != shard.cache_.end())
  {
    auto frame_it = cache_it->second;
    shard.lru_list_.splice(shard.lru_list_.begin(), shard.lru_list_, frame_it);
    return PageGuard(this, &(*frame_it));
  }

  if (shard.lru_list_.size() >= shard.capacity_)
  {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    std::list<Frame>::iterator victim_it;
    bool victim_found = false;

    while (true)
    {
      for (auto it = std::prev(shard.lru_list_.end());; --it)
      {
        if (it->pin_count.load(std::memory_order_relaxed) == 0)
        {
          victim_it = it;
          victim_found = true;
          break;
        }
        if (it == shard.lru_list_.begin()) break;
      }
      if (victim_found) break;
      if (shard.cv_.wait_until(lock, deadline) == std::cv_status::timeout)
      {
        throw std::runtime_error(
            "BufferPool: Timed out waiting for a page to become unpinned.");
      }
    }

    if (victim_it->is_dirty.load(std::memory_order_acquire))
    {
      // Use the new internal helper which requires the lock to be held.
      flush_page_unlocked(victim_it, shard);
    }
    shard.cache_.erase(victim_it->page.hdr.id);
    shard.lru_list_.erase(victim_it);
  }

  shard.lru_list_.emplace_front();
  Frame& new_frame = shard.lru_list_.front();
  disk_mgr_->read_page(pid, new_frame.page);
  new_frame.page.hdr.id = pid;
  new_frame.pin_count.store(0, std::memory_order_relaxed);
  new_frame.is_dirty.store(false, std::memory_order_relaxed);
  shard.cache_[pid] = shard.lru_list_.begin();

  return PageGuard(this, &new_frame);
}

void BufferPool::unpin_page(Frame* frame, bool mark_dirty)
{
  if (mark_dirty)
  {
    frame->is_dirty.store(true, std::memory_order_release);
  }
  frame->pin_count.fetch_sub(1, std::memory_order_acq_rel);
}

// This is now a private helper. Its logic is the same, but its contract
// (caller holds the lock) is enforced by its private status.
void BufferPool::flush_page_unlocked(std::list<Frame>::iterator frame_iter,
                                     BufferPoolShard& shard)
{
  assert(wal_mgr_ != nullptr);
  if (!frame_iter->is_dirty.load(std::memory_order_acquire)) return;
  wal_mgr_->flush_to_lsn(frame_iter->page.hdr.page_lsn);
  disk_mgr_->write_page(frame_iter->page.hdr.id, frame_iter->page);
  frame_iter->is_dirty.store(false, std::memory_order_release);
}

void BufferPool::flush_all()
{
  for (auto& shard_ptr : shards_)
  {
    auto& shard = *shard_ptr;
    std::scoped_lock lock(shard.mutex_);
    for (auto it = shard.lru_list_.begin(); it != shard.lru_list_.end(); ++it)
    {
      if (it->is_dirty.load())
      {
        flush_page_unlocked(it, shard);
      }
    }
  }
}

PageID BufferPool::allocate_page()
{
  assert(disk_mgr_ != nullptr);
  return disk_mgr_->allocate_page();
}