#include "trx_mgr.h"

#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

#include "../storage/bfrpl.h"

TransactionManager::TransactionManager(LockManager* lock_manager,
                                       WAL_mgr* wal_manager,
                                       BufferPool* buffer_pool)
    : lock_manager_(lock_manager),
      wal_manager_(wal_manager),
      buffer_pool_(buffer_pool),
      next_txn_id_(1)  // Start with a valid transaction ID
{
  assert(lock_manager_ != nullptr);
  assert(wal_manager_ != nullptr);
  // assert(buffer_pool_ != nullptr); -- not necessary for isolated unit tests
}

TransactionID TransactionManager::begin()
{
  TransactionID new_txn_id = next_txn_id_.fetch_add(1);
  auto txn = std::make_unique<Transaction>(new_txn_id);

  LogRecordHeader hdr{};
  hdr.lsn = 0;  // Assigned by WAL_mgr
  hdr.prev_lsn = txn->get_prev_lsn();
  hdr.txn_id = new_txn_id;
  hdr.type = BEGIN;
  hdr.lr_length = sizeof(LogRecordHeader);

  // Write BEGIN record to WAL
  LSN lsn = wal_manager_->append_record(hdr);
  txn->set_prev_lsn(lsn);

  // Add to active transactions map
  std::scoped_lock lock(active_txns_mutex_);
  active_txns_.emplace(new_txn_id, std::move(txn));

  return new_txn_id;
}

Transaction* TransactionManager::get_transaction(TransactionID txn_id)
{
  std::scoped_lock lock(active_txns_mutex_);
  auto it = active_txns_.find(txn_id);
  if (it != active_txns_.end())
  {
    return it->second.get();
  }
  return nullptr;
}

void TransactionManager::commit(TransactionID txn_id)
{
  Transaction* txn = get_transaction(txn_id);
  if (!txn)
  {
    throw std::runtime_error("Transaction not found or already completed.");
  }

  // Write COMMIT record to WAL
  LogRecordHeader hdr{};
  hdr.lsn = 0;
  hdr.prev_lsn = txn->get_prev_lsn();
  hdr.txn_id = txn_id;
  hdr.type = COMMIT;
  hdr.lr_length = sizeof(LogRecordHeader);
  wal_manager_->append_record(hdr);

  // Release all locks
  lock_manager_->release_all(txn);

  // Remove from active transactions map
  std::scoped_lock lock(active_txns_mutex_);
  active_txns_.erase(txn_id);
}

void TransactionManager::abort(TransactionID txn_id)
{
  Transaction* txn = get_transaction(txn_id);
  if (!txn)
  {
    return;  // Already completed
  }

  // TODO: This is a highly inefficient way to read the WAL for rollback.
  // A real system would use the prev_lsn chain and an LSN->offset map.
  // For this breadth-first pass, we scan the whole log.
  std::vector<std::pair<LogRecordHeader, std::vector<char>>> txn_log_records;
  wal_manager_->read_all_records_for_txn(txn_id, txn_log_records);

  // Apply before-images in reverse chronological order
  for (auto it = txn_log_records.rbegin(); it != txn_log_records.rend(); ++it)
  {
    const auto& hdr = it->first;
    const auto& payload_data = it->second;

    if (hdr.type == UPDATE)
    {
      // inefficient but ok. bfrpl specifically only needed for this.
      assert(buffer_pool_ != nullptr);
      auto* upd =
          reinterpret_cast<const UpdatePagePayload*>(payload_data.data());
      PageGuard page = buffer_pool_->fetch_page(upd->page_id);

      // Apply the before-image to undo the change
      std::memcpy(page->data() + upd->offset, upd->bef(), upd->length);

      // Note: We are not writing Compensation Log Records (CLRs) yet.
      // This is a simplification.

      page.mark_dirty();
      // The guard's destructor will unpin the page.
    }
  }

  // Write ABORT record to WAL
  LogRecordHeader hdr{};
  hdr.lsn = 0;
  hdr.prev_lsn = txn->get_prev_lsn();
  hdr.txn_id = txn_id;
  hdr.type = ABORT;
  hdr.lr_length = sizeof(LogRecordHeader);
  wal_manager_->append_record(hdr);

  // Release all locks
  lock_manager_->release_all(txn);

  // Remove from active transactions map
  std::scoped_lock lock(active_txns_mutex_);
  active_txns_.erase(txn_id);
}