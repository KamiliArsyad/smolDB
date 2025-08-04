#include "heapfile.h"

#include <cassert>
#include <cstring>

#include "../executor/trx.h"
#include "wal_mgr.h"

using namespace smoldb;

// Each slot stores: [ 4-byte tuple length | tuple data ... ]
constexpr size_t TUPLE_LENGTH_PREFIX_SIZE = sizeof(uint32_t);

// A helper to define the physical region on the page that a log record will
// cover. This ensures that the bitmap modifications and data modifications are
// logged atomically.
struct LogPayloadRegion
{
  uint16_t offset;
  uint16_t length;
};

HeapFile::HeapFile(BufferPool *buffer_pool, WAL_mgr *wal_mgr,
                   PageID first_page_id, size_t max_tuple_size)
    : buffer_pool_(buffer_pool),
      wal_mgr_(wal_mgr),
      first_page_id_(first_page_id),
      last_page_id_(first_page_id),
      max_tuple_size_(max_tuple_size)
{
  assert(buffer_pool_ != nullptr && "BufferPool cannot be null");
  assert(wal_mgr_ != nullptr && "WAL manager cannot be null");

  slot_size_ = max_tuple_size_ + TUPLE_LENGTH_PREFIX_SIZE;
  slots_per_page_ =
      (PAGE_SIZE - sizeof(PageHeader) - BITMAP_SIZE_BYTES) / slot_size_;

  assert(slots_per_page_ > 0 && "Tuple size too large, no slots fit on page");
  assert(slots_per_page_ <= BITMAP_SIZE_BITS &&
         "More slots than bitmap can track");

  // Initialize the first page by fetching it, which formats it if new.
  // A new page from Disk_mgr is zero-filled, so all tuple lengths are 0,
  // which correctly indicates all slots are empty. We just need to pin/unpin it
  // to ensure it's in the buffer pool.
  PageGuard guard = buffer_pool_->fetch_page(first_page_id_);
  // On a new page, the memory is zero-filled, so the bitmap is already all 0s
  // (all free).
}

// --- Bitmap Helper Implementations ---
std::byte *HeapFile::get_bitmap_ptr(Page &page) const
{
  return page.data();  // Bitmap starts at the beginning of the page's data area
}
const std::byte *HeapFile::get_bitmap_ptr(const Page &page) const
{
  return page.data();
}

void HeapFile::set_slot_bit(std::byte *bitmap, uint16_t slot_idx)
{
  bitmap[slot_idx / 8] |= (std::byte{1} << (slot_idx % 8));
}

void HeapFile::clear_slot_bit(std::byte *bitmap, uint16_t slot_idx)
{
  bitmap[slot_idx / 8] &= ~(std::byte{1} << (slot_idx % 8));
}

std::optional<uint16_t> HeapFile::find_first_clear_bit(
    const std::byte *bitmap) const
{
  for (uint16_t i = 0; i < slots_per_page_; ++i)
  {
    if (!static_cast<bool>(bitmap[i / 8] >> i % 8 & std::byte{1}))
    {
      return i;
    }
  }
  return std::nullopt;
}

std::optional<uint16_t> HeapFile::find_next_set_bit(
    const std::byte *bitmap, uint16_t start_slot_idx) const
{
  for (uint16_t i = start_slot_idx; i < slots_per_page_; ++i)
  {
    if (static_cast<bool>(bitmap[i / 8] >> i % 8 & std::byte{1}))
    {
      return i;
    }
  }
  return std::nullopt;
}

std::byte *HeapFile::get_slot_ptr(Page &page, uint16_t slot_idx) const
{
  return page.data() + BITMAP_SIZE_BYTES + (slot_idx * slot_size_);
}

const std::byte *HeapFile::get_slot_ptr(const Page &page,
                                        uint16_t slot_idx) const
{
  return page.data() + BITMAP_SIZE_BYTES + (slot_idx * slot_size_);
}

bool HeapFile::is_deleted(const std::byte *slot_ptr) const
{
  uint32_t metadata;
  std::memcpy(&metadata, slot_ptr, sizeof(uint32_t));
  return (metadata & DELETED_FLAG) != 0;
}

uint32_t HeapFile::get_real_length(const std::byte *slot_ptr) const
{
  uint32_t metadata;
  std::memcpy(&metadata, slot_ptr, sizeof(uint32_t));
  return metadata & ~DELETED_FLAG;  // Mask out the deleted flag bit
}

void HeapFile::set_tuple_metadata(std::byte *slot_ptr, uint32_t size,
                                  bool is_deleted)
{
  uint32_t metadata = size;
  if (is_deleted)
  {
    metadata |= DELETED_FLAG;
  }
  std::memcpy(slot_ptr, &metadata, sizeof(uint32_t));
}

const std::byte *HeapFile::get_tuple_data_ptr(const std::byte *slot_ptr) const
{
  return slot_ptr + TUPLE_LENGTH_PREFIX_SIZE;
}

std::byte *HeapFile::get_tuple_data_ptr(std::byte *slot_ptr)
{
  return slot_ptr + TUPLE_LENGTH_PREFIX_SIZE;
}

RID HeapFile::append(Transaction *txn, std::span<const std::byte> tuple_data)
{
  if (tuple_data.size() > max_tuple_size_)
  {
    throw std::invalid_argument("Tuple is larger than max_tuple_size");
  }
  assert(txn != nullptr && "Cannot perform append without a transaction");

  PageID current_pid = last_page_id_.load();
  while (true)
  {
    PageGuard guard = buffer_pool_->fetch_page(current_pid);
    auto pristine_page_writer = guard.write();  // Exclusive latch on page

    // --- 1. Read-Only Phase: Find a free slot from the pristine page ---
    const std::byte *bitmap = get_bitmap_ptr(*pristine_page_writer);
    auto free_slot_opt = find_first_clear_bit(bitmap);

    if (free_slot_opt.has_value())
    {
      uint16_t slot_idx = *free_slot_opt;

      // Determine the physical region on the page we need to log.
      // This is a contiguous block covering the changed bitmap byte and the new
      // slot.
      uint16_t bitmap_byte_offset = (slot_idx / 8);
      std::byte *slot_ptr = get_slot_ptr(*pristine_page_writer, slot_idx);
      uint16_t slot_offset = slot_ptr - pristine_page_writer->data();

      LogPayloadRegion region = {
          .offset = bitmap_byte_offset,
          .length = (slot_offset + slot_size_) - bitmap_byte_offset};

      // --- 2. Staging Phase: Prepare the after-image in a temporary buffer ---
      std::vector<std::byte> after_image_buffer(region.length);
      // Copy the current state into our buffer
      std::memcpy(after_image_buffer.data(),
                  pristine_page_writer->data() + region.offset, region.length);

      // Modify the buffer, not the live page
      std::byte *bitmap_in_buffer = after_image_buffer.data();
      std::byte *slot_in_buffer =
          after_image_buffer.data() + (slot_offset - region.offset);

      set_slot_bit(bitmap_in_buffer,
                   slot_idx % 8);  // Bit is relative to the start of the byte
      set_tuple_metadata(slot_in_buffer, tuple_data.size(), false);
      std::memcpy(get_tuple_data_ptr(slot_in_buffer), tuple_data.data(),
                  tuple_data.size());

      // --- 3. Log-Ahead Phase: Write WAL record and wait for durability ---
      auto *payload =
          UpdatePagePayload::create(current_pid, region.offset, region.length);
      std::memcpy(const_cast<std::byte *>(payload->bef()),
                  pristine_page_writer->data() + region.offset, region.length);
      std::memcpy(const_cast<std::byte *>(payload->aft()),
                  after_image_buffer.data(), region.length);

      LogRecordHeader hdr{};
      hdr.type = UPDATE;
      hdr.txn_id = txn->get_id();
      hdr.prev_lsn = txn->get_prev_lsn();
      hdr.lr_length = sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) +
                      (2 * region.length);
      LSN lsn = wal_mgr_->append_record(hdr, payload);
      operator delete(payload);
      txn->set_prev_lsn(lsn);

      // --- 4. Apply Phase: Atomically modify the live page ---
      pristine_page_writer->hdr.page_lsn = lsn;
      std::memcpy(pristine_page_writer->data() + region.offset,
                  after_image_buffer.data(), region.length);
      guard.mark_dirty();

      return {current_pid, slot_idx};
    }

    // No space, allocate and loop to try the new page
    PageID new_page_id = buffer_pool_->allocate_page();
    last_page_id_.store(new_page_id);
    current_pid = new_page_id;
  }
}

bool HeapFile::get(Transaction *txn, RID rid,
                   std::vector<std::byte> &out_tuple) const
{
  assert(txn != nullptr && "Cannot perform get without a transaction");

  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto page = guard.read();
  if (rid.slot >= slots_per_page_)
  {
    return false;
  }

  const std::byte *bitmap = get_bitmap_ptr(*page);
  bool is_set = static_cast<bool>((bitmap[rid.slot / 8] >> (rid.slot % 8)) &
                                  std::byte{1});
  if (!is_set) return false;  // Slot is not occupied

  const std::byte *slot_ptr = get_slot_ptr(*page, rid.slot);
  if (is_deleted(slot_ptr)) return false;

  uint32_t size = get_real_length(slot_ptr);
  if (size == 0) return false;  // Should be redundant with bitmap check

  out_tuple.resize(size);
  const std::byte *tuple_data_ptr = get_tuple_data_ptr(slot_ptr);
  std::memcpy(out_tuple.data(), tuple_data_ptr, size);

  return true;
}

bool HeapFile::update(Transaction *txn, RID rid,
                      std::span<const std::byte> new_tuple_data)
{
  if (new_tuple_data.size() > max_tuple_size_)
  {
    throw std::invalid_argument("New tuple is larger than max_tuple_size");
  }
  assert(txn != nullptr && "Cannot perform update without a transaction");

  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto pristine_page_writer = guard.write();

  if (rid.slot >= slots_per_page_) return false;

  // Check bitmap to ensure row exists
  const std::byte *bitmap = get_bitmap_ptr(*pristine_page_writer);
  bool is_set = static_cast<bool>((bitmap[rid.slot / 8] >> (rid.slot % 8)) &
                                  std::byte{1});
  if (!is_set) return false;

  // The region is just the slot itself, since the bitmap doesn't change
  std::byte *slot_ptr = get_slot_ptr(*pristine_page_writer, rid.slot);

  if (is_deleted(slot_ptr)) return false;

  LogPayloadRegion region = {
      .offset = static_cast<uint16_t>(slot_ptr - pristine_page_writer->data()),
      .length = static_cast<uint16_t>(slot_size_)};

  // Staging phase
  std::vector<std::byte> after_image_buffer(region.length);
  set_tuple_metadata(after_image_buffer.data(), new_tuple_data.size(), false);
  std::memcpy(get_tuple_data_ptr(after_image_buffer.data()),
              new_tuple_data.data(), new_tuple_data.size());

  // Log-Ahead phase
  auto *payload =
      UpdatePagePayload::create(rid.page_id, region.offset, region.length);
  std::memcpy(const_cast<std::byte *>(payload->bef()),
              pristine_page_writer->data() + region.offset, region.length);
  std::memcpy(const_cast<std::byte *>(payload->aft()),
              after_image_buffer.data(), region.length);

  LogRecordHeader hdr{};
  hdr.type = UPDATE;
  hdr.txn_id = txn->get_id();
  hdr.prev_lsn = txn->get_prev_lsn();
  hdr.lr_length =
      sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) + (2 * region.length);
  LSN lsn = wal_mgr_->append_record(hdr, payload);
  operator delete(payload);
  txn->set_prev_lsn(lsn);

  // Apply phase
  pristine_page_writer->hdr.page_lsn = lsn;
  std::memcpy(pristine_page_writer->data() + region.offset,
              after_image_buffer.data(), region.length);
  guard.mark_dirty();

  return true;
}

bool HeapFile::delete_row(Transaction *txn, RID rid)
{
  assert(txn != nullptr && "Cannot perform delete without a transaction");
  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto pristine_page_writer = guard.write();

  if (rid.slot >= slots_per_page_) return false;

  const std::byte *bitmap = get_bitmap_ptr(*pristine_page_writer);
  bool is_occupied = static_cast<bool>(
      (bitmap[rid.slot / 8] >> (rid.slot % 8)) & std::byte{1});
  if (!is_occupied) return false;  // Slot isn't even used

  std::byte *slot_ptr = get_slot_ptr(*pristine_page_writer, rid.slot);
  if (is_deleted(slot_ptr)) return false;  // Already logically deleted

  // This operation is now an UPDATE of just the 4-byte length prefix.
  // The bitmap does NOT change.
  LogPayloadRegion region = {
      .offset = static_cast<uint16_t>(slot_ptr - pristine_page_writer->data()),
      .length = TUPLE_LENGTH_PREFIX_SIZE};

  // Staging phase: Prepare the new metadata (tombstone) in a buffer
  uint32_t current_len = get_real_length(slot_ptr);
  std::vector<std::byte> after_image_buffer(TUPLE_LENGTH_PREFIX_SIZE);
  set_tuple_metadata(after_image_buffer.data(), current_len, true);

  // Log-Ahead phase
  auto *payload =
      UpdatePagePayload::create(rid.page_id, region.offset, region.length);
  std::memcpy(const_cast<std::byte *>(payload->bef()),
              pristine_page_writer->data() + region.offset, region.length);
  std::memcpy(const_cast<std::byte *>(payload->aft()),
              after_image_buffer.data(), region.length);

  LogRecordHeader hdr{};
  hdr.type = UPDATE;  // Logical delete is an update
  hdr.txn_id = txn->get_id();
  hdr.prev_lsn = txn->get_prev_lsn();
  hdr.lr_length =
      sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) + (2 * region.length);
  LSN lsn = wal_mgr_->append_record(hdr, payload);
  operator delete(payload);
  txn->set_prev_lsn(lsn);

  // Apply phase
  pristine_page_writer->hdr.page_lsn = lsn;
  std::memcpy(pristine_page_writer->data() + region.offset,
              after_image_buffer.data(), region.length);
  guard.mark_dirty();

  return true;
}

bool HeapFile::get_next_tuple(RID &rid, std::vector<std::byte> &out_tuple) const
{
  PageID current_pid = rid.page_id;
  uint16_t current_slot = rid.slot;
  const PageID last_pid = last_page_id_.load();

  while (current_pid <= last_pid)
  {
    PageGuard guard = buffer_pool_->fetch_page(current_pid);
    auto page = guard.read();
    const std::byte *bitmap = get_bitmap_ptr(*page);

    auto next_slot_opt = find_next_set_bit(bitmap, current_slot);
    if (next_slot_opt.has_value())
    {
      uint16_t found_slot = *next_slot_opt;
      const std::byte *slot_ptr = get_slot_ptr(*page, found_slot);
      // Check for tombstone and skip if found
      if (is_deleted(slot_ptr))
      {
        current_slot = found_slot + 1;  // Continue scan from next slot
        continue;                       // Go to the top of the inner while loop
      }

      uint32_t size = get_real_length(slot_ptr);

      out_tuple.resize(size);
      const std::byte *tuple_data_ptr = get_tuple_data_ptr(slot_ptr);
      std::memcpy(out_tuple.data(), tuple_data_ptr, size);

      rid = {current_pid, found_slot};
      return true;
    }

    current_pid++;
    current_slot = 0;
  }
  return false;  // No more tuples
}

void HeapFile::full_scan(std::vector<std::vector<std::byte>> &out) const
{
  out.clear();
  RID rid = {first_page_id_, 0};
  std::vector<std::byte> tuple_data;
  while (get_next_tuple(rid, tuple_data))
  {
    out.push_back(tuple_data);
    rid.slot++;  // Start search from next slot
  }
}

boost::asio::awaitable<RID> HeapFile::async_append(
    Transaction *txn, std::span<const std::byte> tuple_data)
{
  // Implementation is the same as `append`, just async logging
  if (tuple_data.size() > max_tuple_size_)
  {
    throw std::runtime_error("Tuple size exceeds max tuple size");
  }

  PageID current_page_id = last_page_id_.load();
  PageGuard guard = buffer_pool_->fetch_page(current_page_id);
  std::optional<uint16_t> free_slot_idx;

  while (true)
  {
    {
      auto page = guard.write();
      free_slot_idx = find_first_clear_bit(get_bitmap_ptr(*page));
      if (free_slot_idx.has_value())
      {
        break;
      }
    }
    current_page_id++;
    guard = buffer_pool_->fetch_page(current_page_id);
  }

  last_page_id_.store(current_page_id);
  RID new_rid = {current_page_id, *free_slot_idx};

  // Log the update
  auto page_writer = guard.write();
  std::byte *slot_ptr_for_log = get_slot_ptr(*page_writer, *free_slot_idx);
  uint16_t region_offset =
      static_cast<uint16_t>(reinterpret_cast<uintptr_t>(slot_ptr_for_log) -
                            reinterpret_cast<uintptr_t>(page_writer->data()));
  uint16_t region_length = static_cast<uint16_t>(tuple_data.size());

  // Build an UPDATE batch: payload = UpdatePagePayload, extra = bef+aft
  auto batch =
      wal_mgr_->make_batch(UPDATE, txn->get_id(), txn->get_prev_lsn(),
                           sizeof(UpdatePagePayload), 2 * region_length);

  auto *upd = batch.payload<UpdatePagePayload>();
  *upd = {current_page_id, region_offset, region_length};

  // before image: whatever currently lives in the slot
  std::memcpy(const_cast<std::byte *>(upd->bef()),
              page_writer->data() + region_offset, region_length);

  // after image: incoming tuple data
  std::memcpy(const_cast<std::byte *>(upd->aft()), tuple_data.data(),
              region_length);

  LSN lsn = wal_mgr_->append_record_async(batch.done());
  txn->set_prev_lsn(lsn);

  set_slot_bit(get_bitmap_ptr(*page_writer), *free_slot_idx);
  std::byte *slot_ptr = get_slot_ptr(*page_writer, *free_slot_idx);
  set_tuple_metadata(slot_ptr, tuple_data.size(), false);
  std::memcpy(get_tuple_data_ptr(slot_ptr), tuple_data.data(),
              tuple_data.size());
  page_writer->hdr.page_lsn = lsn;
  guard.mark_dirty();

  co_return new_rid;
}

boost::asio::awaitable<bool> HeapFile::async_update(
    Transaction *txn, RID rid, std::span<const std::byte> new_tuple_data)
{
  if (new_tuple_data.size() > max_tuple_size_)
  {
    throw std::invalid_argument("New tuple is larger than max_tuple_size");
  }
  assert(txn != nullptr && "Cannot perform update without a transaction");

  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto pristine_page_writer = guard.write();

  if (rid.slot >= slots_per_page_) co_return false;

  // Check bitmap to ensure row exists
  const std::byte *bitmap = get_bitmap_ptr(*pristine_page_writer);
  bool is_set = static_cast<bool>((bitmap[rid.slot / 8] >> (rid.slot % 8)) &
                                  std::byte{1});
  if (!is_set) co_return false;

  // The region is just the slot itself, since the bitmap doesn't change
  std::byte *slot_ptr = get_slot_ptr(*pristine_page_writer, rid.slot);

  if (is_deleted(slot_ptr)) co_return false;

  LogPayloadRegion region = {
      .offset = static_cast<uint16_t>(slot_ptr - pristine_page_writer->data()),
      .length = static_cast<uint16_t>(slot_size_)};

  // Staging phase
  std::vector<std::byte> after_image_buffer(region.length);
  set_tuple_metadata(after_image_buffer.data(), new_tuple_data.size(), false);
  std::memcpy(get_tuple_data_ptr(after_image_buffer.data()),
              new_tuple_data.data(), new_tuple_data.size());

  // Log-Ahead phase
  auto builder =
      wal_mgr_->make_batch(UPDATE, txn->get_id(), txn->get_prev_lsn(),
                           sizeof(UpdatePagePayload), 2 * region.length);
  auto *upd = builder.payload<UpdatePagePayload>();
  *upd = {rid.page_id, region.offset, region.length};

  std::memcpy(const_cast<std::byte *>(upd->bef()),
              pristine_page_writer->data() + region.offset, region.length);

  std::memcpy(const_cast<std::byte *>(upd->aft()), after_image_buffer.data(),
              region.length);

  LSN lsn = wal_mgr_->append_record_async(builder.done());
  txn->set_prev_lsn(lsn);

  // Apply phase
  pristine_page_writer->hdr.page_lsn = lsn;
  std::memcpy(pristine_page_writer->data() + region.offset,
              after_image_buffer.data(), region.length);
  guard.mark_dirty();

  co_return true;
}

boost::asio::awaitable<bool> HeapFile::async_delete(Transaction *txn, RID rid)
{
  // Similar to delete_row, just with async logging
  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto page = guard.write();
  std::byte *slot_ptr = get_slot_ptr(*page, rid.slot);

  if (!is_deleted(slot_ptr))
  {
    size_t len = get_real_length(slot_ptr);
    std::vector<std::byte> old_data(len);
    std::memcpy(old_data.data(), get_tuple_data_ptr(slot_ptr), len);

    uint16_t region_offset =
        static_cast<uint16_t>(reinterpret_cast<uintptr_t>(slot_ptr) -
                              reinterpret_cast<uintptr_t>(page->data()));

    auto batch =
        wal_mgr_->make_batch(UPDATE, txn->get_id(), txn->get_prev_lsn(),
                             sizeof(UpdatePagePayload), len * 2);

    auto *upd = batch.payload<UpdatePagePayload>();
    *upd = {rid.page_id, region_offset, static_cast<uint16_t>(len)};

    // before image: tuple as it exists now
    std::memcpy(const_cast<std::byte *>(upd->bef()), old_data.data(), len);
    // after image: nothing (tombstone) – zero‑fill for determinism
    std::memset(const_cast<std::byte *>(upd->aft()), 0, len);

    LSN lsn = wal_mgr_->append_record_async(batch.done());
    txn->set_prev_lsn(lsn);

    set_tuple_metadata(slot_ptr, len, true);
    page->hdr.page_lsn = lsn;
    guard.mark_dirty();
    co_return true;
  }
  co_return false;
}
