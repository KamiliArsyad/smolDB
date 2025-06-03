#ifndef HEAPFILE_H
#define HEAPFILE_H
#include <filesystem>
#include <fstream>
#include <vector>
#include <stdexcept>

#include "storage.h"

#pragma once
#include <cstdint>
#include "bfrpl.h"    // BufferPool, PageGuard

struct RID {
  PageID page_id;
  uint16_t slot; // 0-based slot number
  bool operator==(const RID& o) const { return page_id == o.page_id && slot == o.slot; }
};

/**
 * @brief HeapFile manages the logical access of records.
 * @details HeapFile is basically a logical "Table" abstraction.
 *            It understands page layout, record layout,
 *            free space management at the page level, record
 *            insertion/deletion/update management logic, and
 *            how to navigate between pages.
 * @tparam Tuple The record type.
 */
template <typename Tuple>
class HeapFile {
public:
  HeapFile(BufferPool* buf, WAL_mgr* wal, PageID first, size_t tuple_count_hint = 0);

  // Appends and returns RID of tuple
  RID append(const Tuple& tuple);

  // Reads a tuple by RID
  bool get(RID rid, Tuple& out_tuple) const;

  // Appends all records to out
  void full_scan(std::vector<Tuple>& out) const;

  // For testability: expose page/slot configuration
  size_t slots_per_page() const { return slots_per_page_; }
  PageID first_page() const { return first_page_; }
  PageID last_page() const { return last_page_; }

private:
  BufferPool* buffer_pool_;
  WAL_mgr* wal_mgr_;
  PageID first_page_;
  PageID last_page_;
  size_t slots_per_page_;

  // Helper
  static void serialize(const Tuple&, std::byte*);
  static void deserialize(const std::byte*, Tuple&);
};

#endif //HEAPFILE_H