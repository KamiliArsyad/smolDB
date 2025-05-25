#include "bfrpl.h"
#include "dsk_mgr.h"

FrameIter BufferPool::lookup_or_load_frame(PageID pid) {
  if (cache_.contains(pid))
  {
    auto it = cache_[pid];

    // Move to MRU
    lru_lists_.splice(lru_lists_.begin(), lru_lists_, it);
    return cache_[pid];
  }

  if (lru_lists_.size() == capacity_) {
    auto victim = std::prev(lru_lists_.end());
    while (victim->pin_count.load() != 0)
    {
      victim = std::prev(victim);
    }

    if (victim->pin_count.load() == 0)
    {
      flush_page(victim);
      cache_.erase(victim->page.hdr.id);
      lru_lists_.erase(victim);
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
  if (it == cache_.end()) return;

  Frame &f = *it->second;

  if (mark_dirty) f.is_dirty.store(true, std::memory_order_relaxed);

  const int pins_left = f.pin_count.fetch_sub(
    1,
    std::memory_order_relaxed);
  assert(pins_left >= 0 && "pin_count_underflow");

  if (pins_left == 0)
  {
    // Move to the back for eviction.
    lru_lists_.splice(
      lru_lists_.end(),
      lru_lists_,
      it->second);
  }
}

void BufferPool::flush_page(FrameIter it) const
{
  if (!it->is_dirty.load()) return;

  wal_mgr_->flush_to_lsn(it->page.hdr.page_lsn);

  disk_mgr_->write_page(it->page.hdr.id, it->page);

  it->is_dirty.store(false);
}

void BufferPool::flush_all()
{
  for (auto it = lru_lists_.begin(); it != lru_lists_.end(); ++it)
  {
    flush_page(it);
  }
}
