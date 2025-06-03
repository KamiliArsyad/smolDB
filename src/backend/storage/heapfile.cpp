#include "heapfile.h"

#include <cassert>
#include <cstring>

template <typename Tuple>
HeapFile<Tuple>::HeapFile(BufferPool* buf, WAL_mgr* wal, PageID first,
                          size_t tuple_count_hint)
    : buffer_pool_(buf), wal_mgr_(wal), first_page_(first), last_page_(first) {
  static_assert(std::is_trivially_copyable_v<Tuple>);
  slots_per_page_ = (PAGE_SIZE - sizeof(PageHeader)) / sizeof(Tuple);
  // Compute last_page_ if tuple_count_hint given
  if (tuple_count_hint)
    last_page_ = first_page_ +
                 (tuple_count_hint + slots_per_page_ - 1) / slots_per_page_ - 1;
}

// Helper functions
template <typename Tuple>
void HeapFile<Tuple>::serialize(const Tuple& tup, std::byte* out) {
  std::memcpy(out, &tup, sizeof(Tuple));
}
template <typename Tuple>
void HeapFile<Tuple>::deserialize(const std::byte* in, Tuple& tup) {
  std::memcpy(&tup, in, sizeof(Tuple));
}

// Append tuple, allocate page if needed
template <typename Tuple>
RID HeapFile<Tuple>::append(const Tuple& tuple) {
  for (PageID pid = first_page_;; ++pid) {
    PageGuard guard = buffer_pool_->fetch_page(pid);

    // Try each slot for empty
    auto* page_data = guard->data();
    for (uint16_t slot = 0; slot < slots_per_page_; ++slot) {
      std::byte* slot_ptr = page_data + slot * sizeof(Tuple);
      Tuple check;
      deserialize(slot_ptr, check);
      if (check == Tuple{}) {  // convention: default-initialized means unused
        wal_mgr_->append_record(/* ... appropriate WAL args ... */);

        serialize(tuple, slot_ptr);
        guard.mark_dirty();
        if (pid > last_page_) last_page_ = pid;
        return {pid, slot};
      }
    }
    // All slots full; will try next page (BufferPool allocs new page on miss)
  }
}

// Read tuple by RID
template <typename Tuple>
bool HeapFile<Tuple>::get(RID rid, Tuple& out_tuple) const {
  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto* page_data = guard->data();
  if (rid.slot >= slots_per_page_) return false;
  deserialize(page_data + rid.slot * sizeof(Tuple), out_tuple);
  return true;
}

// Scan all tuples
template <typename Tuple>
void HeapFile<Tuple>::full_scan(std::vector<Tuple>& out) const {
  for (PageID pid = first_page_; pid <= last_page_; ++pid) {
    PageGuard guard = buffer_pool_->fetch_page(pid);
    auto* page_data = guard->data();
    for (uint16_t slot = 0; slot < slots_per_page_; ++slot) {
      Tuple tup;
      deserialize(page_data + slot * sizeof(Tuple), tup);
      if (tup != Tuple{}) out.push_back(tup);
    }
  }
}
