#include "bfrpl.h"
#include "dsk_mgr.h"
#include "wal_mgr.h"

FrameIter BufferPool::lookup_or_load_frame(PageID pid)
{
  if (cache_.contains(pid))
  {
    auto it = cache_[pid];
    // Move to MRU
    if (it != lru_lists_.begin())
    { // Only move if not already MRU
        lru_lists_.splice(lru_lists_.begin(), lru_lists_, it);
    }
    return cache_[pid];
  }

  if (lru_lists_.size() == capacity_)
  {
    auto victim_it = std::prev(lru_lists_.end());
    // Iterate backwards from LRU to find an unpinned page
    while (victim_it != lru_lists_.begin() && victim_it->pin_count.load() != 0)
    {
        victim_it = std::prev(victim_it);
    }

    // Final check for the beginning element if all others were pinned
    if (victim_it->pin_count.load() != 0 && victim_it == lru_lists_.begin())
    {
         // All pages are pinned, cannot evict. This is a critical error.
         // Or, if the list has only one element and it's pinned.
        throw std::runtime_error("BufferPool: All pages are pinned, cannot evict.");
    }

    if (victim_it->pin_count.load() == 0)
    {
        flush_page(victim_it);
        cache_.erase(victim_it->page.hdr.id);
        lru_lists_.erase(victim_it);
    } else {
        // This case should ideally be caught by the check above.
        // If capacity is 1 and the page is pinned, or all pages pinned.
        throw std::runtime_error("BufferPool: No page to evict (all pinned or capacity issue).");
    }
  }

  const FrameIter it = lru_lists_.emplace(lru_lists_.begin());
  disk_mgr_->read_page(pid, it->page);

  it->page.hdr.id = pid;
  it->pin_count.store(0, std::memory_order_relaxed);
  it->is_dirty.store(false, std::memory_order_relaxed);

  return cache_[pid] = it;
}

PageGuard BufferPool::fetch_page(PageID pid)
{
  const auto it = lookup_or_load_frame(pid);
  return PageGuard{this, it};
}

void BufferPool::unpin_page(PageID pid, bool mark_dirty)
{
  auto it = cache_.find(pid);
  if (it == cache_.end())
  {
    // Potentially an error or a race condition if unpin is called on a non-cached page.
    // Depending on strictness, could assert or throw.
    // For now, assume it might happen if page was evicted due to an error elsewhere.
    return;
  }

  FrameIter list_iter = it->second;
  Frame &f = *it->second;

  if (mark_dirty) f.is_dirty.store(true, std::memory_order_relaxed);

  const int pins_left = f.pin_count.fetch_sub(
    1,
    std::memory_order_acq_rel);
  assert(pins_left > 0 && "pin_count underflow");
}

void BufferPool::flush_page(FrameIter it) const
{
  assert(wal_mgr_ && "Fatal: WAL manager not configured!");
  if (!it->is_dirty.load(std::memory_order_acquire)) return;

  wal_mgr_->flush_to_lsn(it->page.hdr.page_lsn);

  disk_mgr_->write_page(it->page.hdr.id, it->page);

  it->is_dirty.store(false, std::memory_order_release);
}

void BufferPool::flush_all()
{
  for (auto it = lru_lists_.begin(); it != lru_lists_.end(); ++it)
  {
    // Check pin count before flushing. Typically, flush_all is part of a shutdown
    // or checkpoint where pages should ideally be unpinned.
    // If a page is pinned, flushing it might be problematic depending on consistency model.
    // For simplicity, we flush regardless of pin_count, assuming it's a controlled operation.
    flush_page(it);
  }
}

PageID BufferPool::allocate_page() {
  if (!disk_mgr_) {
    throw std::runtime_error("BufferPool: Disk_mgr is not initialized, cannot allocate page.");
  }
  return disk_mgr_->allocate_page();
}