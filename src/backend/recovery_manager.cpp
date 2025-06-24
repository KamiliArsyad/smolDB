#include "recovery_manager.h"

#include <algorithm>
#include <cstring>
#include <set>
#include <stdexcept>

RecoveryManager::RecoveryManager(BufferPool* buffer_pool, WAL_mgr* wal_mgr)
    : buffer_pool_(buffer_pool), wal_mgr_(wal_mgr)
{
  assert(buffer_pool_ != nullptr);
  assert(wal_mgr_ != nullptr);
}

void RecoveryManager::recover()
{
  analysis_phase();
#ifndef NDEBUG
  check_for_crash(RecoveryCrashPoint::AFTER_ANALYSIS);
#endif

  redo_phase();
#ifndef NDEBUG
  check_for_crash(RecoveryCrashPoint::DURING_REDO);
#endif

  undo_phase();
}

void RecoveryManager::analysis_phase()
{
  log_records_ = wal_mgr_->read_all_records();

  for (const auto& [lsn, entry] : log_records_)
  {
    const auto& hdr = entry.first;
    const auto& payload = entry.second;

    if (active_txn_table_.find(hdr.txn_id) == active_txn_table_.end())
    {
      active_txn_table_[hdr.txn_id] = {RecoveryState::IN_FLIGHT, hdr.lsn};
    }

    active_txn_table_.at(hdr.txn_id).last_lsn = hdr.lsn;
    if (hdr.type == COMMIT || hdr.type == ABORT)
    {
      active_txn_table_.erase(hdr.txn_id);
    }

    if (hdr.type == UPDATE || hdr.type == CLR)
    {
      PageID page_id;
      if (hdr.type == UPDATE)
      {
        page_id =
            reinterpret_cast<const UpdatePagePayload*>(payload.data())->page_id;
      }
      else
      {
        page_id = reinterpret_cast<const CLR_Payload*>(payload.data())->page_id;
      }
      if (dirty_page_table_.find(page_id) == dirty_page_table_.end())
      {
        dirty_page_table_[page_id] = hdr.lsn;
      }
    }
  }
}

void RecoveryManager::redo_phase()
{
  if (dirty_page_table_.empty()) return;

  LSN first_lsn_to_redo = -1;
  for (const auto& [pid, rec_lsn] : dirty_page_table_)
  {
    if (rec_lsn < first_lsn_to_redo) first_lsn_to_redo = rec_lsn;
  }

  for (auto it = log_records_.lower_bound(first_lsn_to_redo);
       it != log_records_.end(); ++it)
  {
    const auto& hdr = it->second.first;
    const auto* payload_data = it->second.second.data();

    if (hdr.type != UPDATE && hdr.type != CLR) continue;

    const auto* base_payload =
        reinterpret_cast<const UpdatePagePayload*>(payload_data);
    if (dirty_page_table_.find(base_payload->page_id) ==
        dirty_page_table_.end())
      continue;

    // Your deadlock fix is integrated here.
    {
      PageGuard guard = buffer_pool_->fetch_page(base_payload->page_id);
      auto page_reader = guard.read();
      if (page_reader->hdr.page_lsn >= hdr.lsn) continue;
    }

    const std::byte* data_to_apply;
    if (hdr.type == UPDATE)
    {
      data_to_apply = base_payload->aft();
    }
    else
    {  // CLR
      data_to_apply = reinterpret_cast<const CLR_Payload*>(payload_data)
                          ->compensation_data();
    }
    apply_change(hdr.lsn, base_payload->page_id, base_payload->offset,
                 base_payload->length, data_to_apply);
  }
}

void RecoveryManager::undo_phase()
{
  // Use std::greater to process highest LSNs first.
  std::set<LSN, std::greater<LSN>> to_be_undone;
  for (const auto& [txn_id, att_entry] : active_txn_table_)
  {
    if (att_entry.state == RecoveryState::IN_FLIGHT)
    {
      to_be_undone.insert(att_entry.last_lsn);
    }
  }

  while (!to_be_undone.empty())
  {
    LSN current_lsn = *to_be_undone.begin();
    to_be_undone.erase(to_be_undone.begin());

    const auto& [hdr, payload_vec] = log_records_.at(current_lsn);
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
#ifndef NDEBUG
        check_for_crash(RecoveryCrashPoint::DURING_UNDO);
#endif
        const auto* upd =
            reinterpret_cast<const UpdatePagePayload*>(payload_vec.data());
        CLR_Payload* clr = CLR_Payload::create(upd->page_id, upd->offset,
                                               upd->length, hdr.prev_lsn);
        std::memcpy(const_cast<std::byte*>(clr->compensation_data()),
                    upd->bef(), upd->length);

        LogRecordHeader clr_hdr{};
        clr_hdr.type = CLR;
        clr_hdr.txn_id = hdr.txn_id;
        clr_hdr.prev_lsn = hdr.prev_lsn;  // This is important for chaining
        clr_hdr.lr_length =
            sizeof(LogRecordHeader) + sizeof(CLR_Payload) + upd->length;
        LSN clr_lsn = wal_mgr_->append_record(clr_hdr, clr);
        operator delete(clr);

        apply_change(clr_lsn, upd->page_id, upd->offset, upd->length,
                     upd->bef());
      }
    }

    if (next_lsn_in_chain != 0)
    {
      to_be_undone.insert(next_lsn_in_chain);
    }
    else
    {  // End of this transaction's log chain.
      LogRecordHeader abort_hdr{};
      abort_hdr.type = ABORT;
      abort_hdr.txn_id = hdr.txn_id;
      // ABORT record logically follows the last record processed for the txn.
      abort_hdr.prev_lsn = current_lsn;
      abort_hdr.lr_length = sizeof(LogRecordHeader);
      wal_mgr_->append_record(abort_hdr);
    }
  }
}

void RecoveryManager::apply_change(LSN lsn, uint32_t page_id, uint16_t offset,
                                   uint16_t length, const std::byte* data)
{
  PageGuard guard = buffer_pool_->fetch_page(page_id);
  auto page = guard.write();
  std::memcpy(page->data() + offset, data, length);
  page->hdr.page_lsn = lsn;
}

#ifndef NDEBUG
void RecoveryManager::check_for_crash(RecoveryCrashPoint current_point)
{
  if (crash_point_ == current_point)
  {
    throw std::runtime_error("Simulated crash at point: " +
                             std::to_string((int)current_point));
  }
}
#endif