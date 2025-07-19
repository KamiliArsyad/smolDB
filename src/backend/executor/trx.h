#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <atomic>
#include <mutex>
#include <unordered_set>

#include "../index/idx_undo.h"
#include "../storage/heapfile.h"  // For RID
#include "../storage/storage.h"   // For LSN
#include "trx_types.h"

// Hash function for RID to be used in unordered_set
namespace std
{
template <>
struct hash<smoldb::RID>
{
  size_t operator()(const smoldb::RID& rid) const
  {
    return hash<smoldb::PageID>()(rid.page_id) ^
           (hash<uint16_t>()(rid.slot) << 1);
  }
};
}  // namespace std

namespace smoldb
{

enum class TransactionState
{
  ACTIVE,
  COMMITTED,
  ABORTED
};

/**
 * @brief Represents the state of an active transaction.
 * This object is managed by the TransactionManager.
 */
class Transaction
{
 public:
  explicit Transaction(TransactionID txn_id)
      : id_(txn_id),
        state_(TransactionState::ACTIVE),
        prev_lsn_(0)  // A transaction's first record has no predecessor
  {
  }

  // Getters
  TransactionID get_id() const { return id_; }
  TransactionState get_state() const { return state_; }
  LSN get_prev_lsn() const { return prev_lsn_; }
  const std::unordered_set<RID>& get_held_locks() const { return held_locks_; }
  const std::vector<IndexUndoAction>& get_index_undo_log() const
  {
    return index_undo_log_;
  }

  /**
   * @brief Tries to get previously read row by this trx.
   * @param rid {in} The RID of the row.
   * @param row {out} The output row. Left as is if row was not cached.
   * @return true iff the row exists and was cached.
   */
  bool get_cached_row(const RID rid, Row &row)
  {
    if (!read_cache_.contains(rid)) return false;
    row = read_cache_[rid];
    return true;
  }

  /**
   * @brief Caches a row read by this transaction.
   * @param rid The RID of the row to be cached.
   * @param row The row to be cached.
   */
  void cache_row(const RID rid, const Row &row)
  {
    read_cache_[rid] = row;
  }

  void remove_cache(const RID rid)
  {
    read_cache_.erase(rid);
  }

  // Setters (should be called with the transaction's mutex held)
  void set_state(TransactionState state) { state_ = state; }
  void set_prev_lsn(LSN prev_lsn) { prev_lsn_ = prev_lsn; }
  void add_held_lock(const RID& rid) { held_locks_.insert(rid); }
  void add_index_undo(IndexUndoAction&& undo_action)
  {
    index_undo_log_.emplace_back(std::move(undo_action));
  }

  std::mutex& get_mutex() { return mutex_; }

 private:
  friend class LockManager;
  friend class TransactionManager;

  TransactionID id_;
  TransactionState state_;
  LSN prev_lsn_;

  // A set of all RIDs locked by this transaction.
  std::unordered_set<RID> held_locks_;

  // A list of modifications made to in-memory indexes by this transaction.
  std::vector<IndexUndoAction> index_undo_log_;

  /**
   * @brief Optimization to read by caching the read row.
   * @remarks Once a row is read by a trx T, its state can only be changed by T
   * for as long as T has not committed. This means that subsequent reads can
   * continue to see this as long as we take care of update to the same row by
   * T <- THIS IS IMPORTANT.
   */
  std::unordered_map<RID, Row> read_cache_;

  // Mutex to protect modifications to the transaction's state (e.g., prev_lsn)
  std::mutex mutex_;
};

}  // namespace smoldb
#endif  // TRANSACTION_H