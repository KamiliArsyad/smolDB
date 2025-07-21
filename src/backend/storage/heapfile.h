#ifndef HEAPFILE_H
#define HEAPFILE_H
#include <filesystem>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <vector>

#include "bfrpl.h"  // BufferPool, PageGuard
#include "storage.h"

namespace smoldb
{

class Transaction;  // Forward declaration
class WAL_mgr;      // Forward declaration

struct RID
{
  PageID page_id;
  uint16_t slot;  // 0-based slot number
  bool operator==(const RID &o) const
  {
    return page_id == o.page_id && slot == o.slot;
  }
  bool operator!=(const RID &o) const { return !(*this == o); }

  // For gtest printing
  friend std::ostream &operator<<(std::ostream &os, const RID &rid)
  {
    return os << "RID(" << rid.page_id << ", " << rid.slot << ")";
  }
};

/**
 * @brief HeapFile manages the logical access of records.
 * @details HeapFile is basically a logical "Table" abstraction.
 *            It understands page layout, record layout,
 *            free space management at the page level, record
 *            insertion/deletion/update management logic, and
 *            how to navigate between pages.
 *            It stores variable-length tuples in fixed-size slots.
 */
class HeapFile
{
 public:
  HeapFile(BufferPool *buffer_pool, WAL_mgr *wal_mgr, PageID first_page_id,
           size_t max_tuple_size);

  // Appends and returns RID of tuple
  RID append(Transaction *txn, std::span<const std::byte> tuple_data);

  // Reads a tuple by RID
  bool get(Transaction *txn, RID rid, std::vector<std::byte> &out_tuple) const;

  // Updates a tuple by RID. Returns false if RID is invalid or tuple too large.
  bool update(Transaction *txn, RID rid,
              std::span<const std::byte> new_tuple_data);

  // Deletes a tuple by RID. Returns false if RID is invalid.
  bool delete_row(Transaction *txn, RID rid);
  bool get_next_tuple(RID &rid, std::vector<std::byte> &out_tuple) const;

  // Appends all records to out
  void full_scan(std::vector<std::vector<std::byte>> &out) const;

  // For testability: expose page/slot configuration
  size_t slot_size() const { return slot_size_; }
  size_t slots_per_page() const { return slots_per_page_; }
  PageID first_page_id() const { return first_page_id_; }
  PageID last_page_id() const { return last_page_id_.load(); }

  static constexpr size_t BITMAP_SIZE_BYTES = 128;
  static constexpr size_t BITMAP_SIZE_BITS = BITMAP_SIZE_BYTES * 8;
  static constexpr uint32_t DELETED_FLAG = 0x80000000;

 private:
  // --- Bitmap manipulation helpers ---
  // Gets a pointer to the start of the bitmap on a page
  std::byte *get_bitmap_ptr(Page &page) const;
  const std::byte *get_bitmap_ptr(const Page &page) const;
  // Sets or clears the bit for a given slot index
  void set_slot_bit(std::byte *bitmap, uint16_t slot_idx);
  void clear_slot_bit(std::byte *bitmap, uint16_t slot_idx);
  // Finds the first clear bit (free slot) in the bitmap
  std::optional<uint16_t> find_first_clear_bit(const std::byte *bitmap) const;
  // Finds the next set bit (occupied slot) from a starting position
  std::optional<uint16_t> find_next_set_bit(const std::byte *bitmap,
                                            uint16_t start_slot_idx) const;

  BufferPool *buffer_pool_;
  WAL_mgr *wal_mgr_;
  PageID first_page_id_;
  std::atomic<PageID> last_page_id_;
  size_t max_tuple_size_;
  size_t slot_size_;  // max_tuple_size_ + sizeof(uint32_t) for length
  size_t slots_per_page_;

  // Helper to get a pointer to a specific slot within a page
  std::byte *get_slot_ptr(Page &page, uint16_t slot_idx) const;
  const std::byte *get_slot_ptr(const Page &page, uint16_t slot_idx) const;

  // Helpers to read/write the size prefix in a slot
  bool is_deleted(const std::byte *slot_ptr) const;
  uint32_t get_real_length(const std::byte *slot_ptr) const;
  void set_tuple_metadata(std::byte *slot_ptr, uint32_t size, bool is_deleted);

  // Helper to get a pointer to the tuple data within a slot
  const std::byte *get_tuple_data_ptr(const std::byte *slot_ptr) const;
  std::byte *get_tuple_data_ptr(std::byte *slot_ptr);
};

}  // namespace smoldb
#endif  // HEAPFILE_H