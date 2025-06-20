// ===== ../smolDB/src/backend/storage/heapfile.cpp =====

#include "heapfile.h"

#include <cassert>
#include <cstring>

#include "../executor/trx.h"
#include "wal_mgr.h"

// Each slot stores: [ 4-byte tuple length | tuple data ... ]
constexpr size_t TUPLE_LENGTH_PREFIX_SIZE = sizeof(uint32_t);

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
  slots_per_page_ = (PAGE_SIZE - sizeof(PageHeader)) / slot_size_;
  assert(slots_per_page_ > 0 && "Tuple size too large, no slots fit on page");

  // Initialize the first page by fetching it, which formats it if new.
  // A new page from Disk_mgr is zero-filled, so all tuple lengths are 0,
  // which correctly indicates all slots are empty. We just need to pin/unpin it
  // to ensure it's in the buffer pool.
  PageGuard guard = buffer_pool_->fetch_page(first_page_id_);
}

std::byte *HeapFile::get_slot_ptr(Page &page, uint16_t slot_idx) const
{
  return page.data() + (slot_idx * slot_size_);
}

const std::byte *HeapFile::get_slot_ptr(const Page &page,
                                        uint16_t slot_idx) const
{
  return page.data() + (slot_idx * slot_size_);
}

uint32_t HeapFile::get_tuple_size(const std::byte *slot_ptr) const
{
  uint32_t size;
  std::memcpy(&size, slot_ptr, sizeof(uint32_t));
  return size;
}

void HeapFile::set_tuple_size(std::byte *slot_ptr, uint32_t size)
{
  std::memcpy(slot_ptr, &size, sizeof(uint32_t));
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
    auto page = guard.write();

    // Try to find an empty slot in the current page
    for (uint16_t slot_idx = 0; slot_idx < slots_per_page_; ++slot_idx)
    {
      std::byte *slot_ptr = get_slot_ptr(*page, slot_idx);
      if (get_tuple_size(slot_ptr) == 0)
      {  // 0 size means empty slot
        uint16_t offset = slot_ptr - page->data();

        // The payload for the WAL is the entire slot (size + data)
        std::vector<std::byte> after_image(slot_size_, std::byte{0});
        uint32_t size = tuple_data.size();
        std::memcpy(after_image.data(), &size, sizeof(uint32_t));
        std::memcpy(after_image.data() + TUPLE_LENGTH_PREFIX_SIZE,
                    tuple_data.data(), tuple_data.size());

        auto *payload =
            UpdatePagePayload::create(current_pid, offset, slot_size_);
        std::memset(const_cast<std::byte *>(payload->bef()), 0, slot_size_);
        std::memcpy(const_cast<std::byte *>(payload->aft()), after_image.data(),
                    slot_size_);

        LogRecordHeader hdr{};
        hdr.type = UPDATE;
        hdr.txn_id = txn->get_id();
        hdr.lr_length = sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) +
                        2 * slot_size_;
        hdr.prev_lsn = txn->get_prev_lsn();

        LSN lsn = wal_mgr_->append_record(hdr, payload);
        operator delete(payload);

        // Update the transaction's prev_lsn for chaining
        txn->set_prev_lsn(lsn);

        page->hdr.page_lsn = lsn;
        set_tuple_size(slot_ptr, tuple_data.size());
        std::memcpy(get_tuple_data_ptr(slot_ptr), tuple_data.data(),
                    tuple_data.size());

        guard.mark_dirty();
        return {current_pid, slot_idx};
      }
    }

    // No space on this page, allocate a new one.
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

  const std::byte *slot_ptr = get_slot_ptr(*page, rid.slot);
  uint32_t size = get_tuple_size(slot_ptr);

  if (size == 0)
  {
    return false;
  }

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
  auto page = guard.write();
  if (rid.slot >= slots_per_page_)
  {
    return false;
  }

  std::byte *slot_ptr = get_slot_ptr(*page, rid.slot);
  uint16_t offset = slot_ptr - page->data();
  uint32_t current_size = get_tuple_size(slot_ptr);
  if (current_size == 0)
  {
    return false;  // Cannot update a deleted/non-existent tuple
  }

  // Create before-image from current slot content
  std::vector<std::byte> before_image(slot_size_);
  std::memcpy(before_image.data(), slot_ptr, slot_size_);

  // Create after-image from new tuple data
  std::vector<std::byte> after_image(slot_size_, std::byte{0});
  uint32_t new_size = new_tuple_data.size();
  set_tuple_size(after_image.data(), new_size);
  std::memcpy(get_tuple_data_ptr(after_image.data()), new_tuple_data.data(),
              new_size);

  // Create and write the WAL record
  auto *payload = UpdatePagePayload::create(rid.page_id, offset, slot_size_);
  std::memcpy(const_cast<std::byte *>(payload->bef()), before_image.data(),
              slot_size_);
  std::memcpy(const_cast<std::byte *>(payload->aft()), after_image.data(),
              slot_size_);

  LogRecordHeader hdr{};
  hdr.type = UPDATE;
  hdr.txn_id = txn->get_id();
  hdr.lr_length =
      sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) + 2 * slot_size_;
  hdr.prev_lsn = txn->get_prev_lsn();

  LSN lsn = wal_mgr_->append_record(hdr, payload);
  operator delete(payload);

  txn->set_prev_lsn(lsn);

  // Apply change to the page
  page->hdr.page_lsn = lsn;
  std::memcpy(slot_ptr, after_image.data(), slot_size_);
  guard.mark_dirty();

  return true;
}

bool HeapFile::delete_row(Transaction *txn, RID rid)
{
  assert(txn != nullptr && "Cannot perform delete without a transaction");

  PageGuard guard = buffer_pool_->fetch_page(rid.page_id);
  auto page = guard.write();
  if (rid.slot >= slots_per_page_)
  {
    return false;
  }

  std::byte *slot_ptr = get_slot_ptr(*page, rid.slot);
  uint16_t offset = slot_ptr - page->data();
  if (get_tuple_size(slot_ptr) == 0)
  {
    return false;  // Already deleted
  }

  // Create before-image from current slot content
  std::vector<std::byte> before_image(slot_size_);
  std::memcpy(before_image.data(), slot_ptr, slot_size_);

  // After-image is an empty (zeroed) slot
  std::vector<std::byte> after_image(slot_size_, std::byte{0});

  // Create and write the WAL record
  auto *payload = UpdatePagePayload::create(rid.page_id, offset, slot_size_);
  std::memcpy(const_cast<std::byte *>(payload->bef()), before_image.data(),
              slot_size_);
  std::memcpy(const_cast<std::byte *>(payload->aft()), after_image.data(),
              slot_size_);

  LogRecordHeader hdr{};
  hdr.type = UPDATE;  // Deletion is an update to an empty state
  hdr.txn_id = txn->get_id();
  hdr.lr_length =
      sizeof(LogRecordHeader) + sizeof(UpdatePagePayload) + 2 * slot_size_;
  hdr.prev_lsn = txn->get_prev_lsn();

  LSN lsn = wal_mgr_->append_record(hdr, payload);
  operator delete(payload);

  txn->set_prev_lsn(lsn);

  // Apply change to the page (zero out the slot)
  page->hdr.page_lsn = lsn;
  std::memset(slot_ptr, 0, slot_size_);
  guard.mark_dirty();

  return true;
}

void HeapFile::full_scan(std::vector<std::vector<std::byte>> &out) const
{
  out.clear();
  PageID current_last_page = last_page_id_.load();
  for (PageID pid = first_page_id_; pid <= current_last_page; ++pid)
  {
    PageGuard guard = buffer_pool_->fetch_page(pid);
    auto page = guard.read();
    for (uint16_t slot_idx = 0; slot_idx < slots_per_page_; ++slot_idx)
    {
      const std::byte *slot_ptr = get_slot_ptr(*page, slot_idx);
      uint32_t size = get_tuple_size(slot_ptr);
      if (size > 0)
      {
        std::vector<std::byte> tuple_data(size);
        const std::byte *tuple_data_ptr = get_tuple_data_ptr(slot_ptr);
        std::memcpy(tuple_data.data(), tuple_data_ptr, size);
        out.push_back(std::move(tuple_data));
      }
    }
  }
}