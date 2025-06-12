#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <atomic>
#include <mutex>
#include <unordered_set>

#include "../storage/heapfile.h"  // For RID
#include "../storage/storage.h"   // For LSN

// A unique identifier for a transaction.
using TransactionID = uint64_t;
constexpr TransactionID INVALID_TXN_ID = 0;

// Hash function for RID to be used in unordered_set
namespace std
{
template <>
struct hash<RID>
{
  size_t operator()(const RID& rid) const
  {
    return hash<PageID>()(rid.page_id) ^ (hash<uint16_t>()(rid.slot) << 1);
  }
};
}  // namespace std

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

  // Setters (should be called with the transaction's mutex held)
  void set_state(TransactionState state) { state_ = state; }
  void set_prev_lsn(LSN prev_lsn) { prev_lsn_ = prev_lsn; }
  void add_held_lock(const RID& rid) { held_locks_.insert(rid); }

  std::mutex& get_mutex() { return mutex_; }

 private:
  friend class LockManager;
  friend class TransactionManager;

  TransactionID id_;
  TransactionState state_;
  LSN prev_lsn_;

  // A set of all RIDs locked by this transaction.
  std::unordered_set<RID> held_locks_;

  // Mutex to protect modifications to the transaction's state (e.g., prev_lsn)
  std::mutex mutex_;
};

#endif  // TRANSACTION_H