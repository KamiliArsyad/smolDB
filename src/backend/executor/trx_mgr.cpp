#include "trx_mgr.h"

#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

#include "../index/idx.h"
#include "../storage/bfrpl.h"
#include "../storage/db_hdr_page.h"

using namespace smoldb;

TransactionManager::TransactionManager(LockManager* lock_manager,
                                       WAL_mgr* wal_manager,
                                       BufferPool* buffer_pool)
    : lock_manager_(lock_manager),
      wal_manager_(wal_manager),
      buffer_pool_(buffer_pool)
{
  assert(lock_manager_ != nullptr);
  assert(wal_manager_ != nullptr);
  // assert(buffer_pool_ != nullptr); -- not necessary for isolated unit tests

  // Initialize next_txn_id_ from the header page.
  PageGuard header_page_guard = buffer_pool_->fetch_page(DB_HEADER_PAGE_ID);
  auto page = header_page_guard.read();
  auto* header_page_data = reinterpret_cast<const DBHeaderPage*>(page->data());

  // If the page is new/empty, next_txn_id will be 0. We start transactions
  // from 1.
  next_txn_id_.store(
      std::max((uint64_t)1, header_page_data->next_transaction_id_));
}

TransactionID TransactionManager::begin()
{
  TransactionID new_txn_id = next_txn_id_.fetch_add(1);

  // Now, persist this new counter value back to the header page.
  {
    PageGuard header_page_guard = buffer_pool_->fetch_page(DB_HEADER_PAGE_ID);
    auto page = header_page_guard.write();
    auto* header_page_data = reinterpret_cast<DBHeaderPage*>(page->data());
    header_page_data->next_transaction_id_ = next_txn_id_.load();
    header_page_guard.mark_dirty();  // Ensure this change is flushed eventually
  }

  auto txn = std::make_unique<Transaction>(new_txn_id);

  LogRecordHeader hdr{};
  hdr.lsn = 0;
  hdr.prev_lsn = 0;
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
    return;
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

  // We undo in reverse order of operations.
  const auto& index_undo_log = txn->get_index_undo_log();
  for (auto it = index_undo_log.rbegin(); it != index_undo_log.rend(); ++it)
  {
    const auto& action = *it;
    switch (action.type)
    {
      case IndexUndoType::REVERSE_INSERT:
        // The original operation was an insert, so we must delete.
        action.index->delete_entry(action.row);
        break;
      case IndexUndoType::REVERSE_DELETE:
        // The original operation was a delete, so we must re-insert.
        action.index->insert_entry(action.row, action.rid);
        break;
    }
  }

  LSN current_lsn = txn->get_prev_lsn();
  while (current_lsn != 0)
  {
    LogRecordHeader hdr;
    std::vector<char> payload_vec;
    assert(wal_manager_->get_record(current_lsn, hdr, payload_vec));
    LSN next_lsn_in_chain = 0;

    if (hdr.type == CLR)
    {
      next_lsn_in_chain =
          reinterpret_cast<const CLR_Payload*>(payload_vec.data())->undoNextLSN;
    }
    else
    {
      next_lsn_in_chain = hdr.prev_lsn;
      if (hdr.type == UPDATE)
      {
        const auto* upd =
            reinterpret_cast<const UpdatePagePayload*>(payload_vec.data());
        CLR_Payload* clr = CLR_Payload::create(upd->page_id, upd->offset,
                                               upd->length, hdr.prev_lsn);
        std::memcpy(const_cast<std::byte*>(clr->compensation_data()),
                    upd->bef(), upd->length);

        LogRecordHeader clr_hdr{};
        clr_hdr.type = CLR;
        clr_hdr.txn_id = hdr.txn_id;
        clr_hdr.prev_lsn = txn->get_prev_lsn();
        clr_hdr.lr_length =
            sizeof(LogRecordHeader) + sizeof(CLR_Payload) + upd->length;

        LSN clr_lsn = wal_manager_->append_record(clr_hdr, clr);
        txn->set_prev_lsn(clr_lsn);
        operator delete(clr);

        apply_undo(clr_lsn, upd);
      }
    }
    current_lsn = next_lsn_in_chain;
  }

  // Write the final ABORT record for the transaction.
  LogRecordHeader abort_hdr{};
  abort_hdr.type = ABORT;
  abort_hdr.txn_id = txn_id;
  abort_hdr.prev_lsn = txn->get_prev_lsn();
  abort_hdr.lr_length = sizeof(LogRecordHeader);
  wal_manager_->append_record(abort_hdr);

  lock_manager_->release_all(txn);

  std::scoped_lock lock(active_txns_mutex_);
  active_txns_.erase(txn_id);
}

void TransactionManager::apply_undo(LSN lsn, const UpdatePagePayload* payload)
{
  PageGuard guard = buffer_pool_->fetch_page(payload->page_id);
  auto page = guard.write();
  std::memcpy(page->data() + payload->offset, payload->bef(), payload->length);
  page->hdr.page_lsn = lsn;
}