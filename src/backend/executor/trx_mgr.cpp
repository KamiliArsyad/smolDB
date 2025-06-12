#include "trx_mgr.h"

#include <stdexcept>

TransactionManager::TransactionManager(LockManager* lock_manager,
                                       WAL_mgr* wal_manager)
    : lock_manager_(lock_manager),
      wal_manager_(wal_manager),
      next_txn_id_(1)  // Start with a valid transaction ID
{
  assert(lock_manager_ != nullptr);
  assert(wal_manager_ != nullptr);
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
    // Aborting an already aborted/committed transaction is a no-op
    return;
  }

  // TODO: Implement actual rollback logic (Undo phase)

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