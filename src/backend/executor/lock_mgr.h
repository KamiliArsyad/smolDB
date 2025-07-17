#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "trx.h"

enum class LockMode
{
  SHARED,
  EXCLUSIVE
};

/**
 * @brief Manages row-level locks using a sharded, blocking implementation
 * based on Strict Two-Phase Locking (S2PL).
 */
class LockManager
{
 public:
  LockManager();

  /**
   * @brief Acquires a shared lock on a specific RID for a transaction.
   * Blocks if the lock cannot be granted immediately.
   * @param txn The transaction requesting the lock.
   * @param rid The Row ID to lock.
   * @return true if lock was acquired, false on timeout (deadlock).
   */
  bool acquire_shared(Transaction* txn, const RID& rid);

  /**
   * @brief Acquires an exclusive lock on a specific RID for a transaction.
   * Blocks if the lock cannot be granted immediately.
   * @param txn The transaction requesting the lock.
   * @param rid The Row ID to lock.
   * @return true if lock was acquired, false on timeout (deadlock).
   */
  bool acquire_exclusive(Transaction* txn, const RID& rid);

  /**
   * @brief Releases all locks held by a transaction.
   * Called by the TransactionManager on commit or abort.
   * @param txn The transaction whose locks to release.
   */
  void release_all(Transaction* txn);

 private:
  // Forward-declare inner structs
  struct LockRequest;
  struct LockRequestQueue;

  // A single shard of the lock map
  struct Shard
  {
    std::unordered_map<RID, LockRequestQueue> lock_table;
    std::mutex mutex;

    Shard() = default;
    Shard(const Shard&) = delete;
    Shard& operator=(const Shard&) = delete;

    // Move: move the data, re-create the mutex
    Shard(Shard&& other) noexcept : lock_table(std::move(other.lock_table)) {}

    Shard& operator=(Shard&& other) noexcept
    {
      if (this != &other) lock_table = std::move(other.lock_table);
      return *this;
    }
  };

  struct LockRequest
  {
    TransactionID txn_id;
    LockMode mode;
    bool granted = false;
  };

  struct LockRequestQueue
  {
    std::list<LockRequest> requests;
    std::condition_variable cv;
    // The number of transactions currently holding a shared lock.
    int sharing_count = 0;
    // Is an exclusive lock held?
    bool is_exclusive = false;

    // True if a transaction is waiting to upgrade its S lock to an X lock.
    // Only one transaction can be in this state at a time to prevent deadlocks.
    bool is_upgrading = false;
  };

  std::vector<Shard> shards_;
  const size_t shard_count_;
  const std::chrono::milliseconds lock_timeout_;

  // Get the shard for a given RID
  Shard& get_shard(const RID& rid);
};

#endif  // LOCK_MANAGER_H