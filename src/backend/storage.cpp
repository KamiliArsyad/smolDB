#include "storage.h"
#include <atomic>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <fstream>

void Catalog::dump(const std::filesystem::path& path) const {
    std::ofstream ofs{path, std::ios::binary};
    boost::archive::binary_oarchive oa{ofs};
    oa << *this;
}

void Catalog::load(const std::filesystem::path& path) {
    std::ifstream ifs{path, std::ios::binary};
    boost::archive::binary_iarchive ia{ifs};
    ia >> *this;
}

FrameIter BufferPool::lookup_or_load_frame(PageID pid) {
  if (cache_.contains(pid))
  {
    auto it = cache_[pid];

    // Move to MRU
    lru_lists_.splice(lru_lists_.begin(), lru_lists_, it);
    return cache_[pid];
  }

  // TODO: Replace the following stub.

  if (lru_lists_.size() == capacity_) {
    auto victim = std::prev(lru_lists_.end());
    cache_.erase(victim->page.hdr.id);
    lru_lists_.erase(victim);
  }

  const FrameIter it = lru_lists_.emplace(lru_lists_.begin());

  it->page.hdr.id = pid;
  std::memset(it->page.data(), 0, PAGE_SIZE - sizeof(PageHeader));

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
    true,
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