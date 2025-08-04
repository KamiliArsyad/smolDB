#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "../storage/bfrpl.h"
#include "../storage/wal_mgr.h"
#include "lock_mgr.h"
#include "trx.h"

namespace smoldb
{

/**
 * @brief Manages the lifecycle of transactions.
 * This is the main entry point for starting, committing, and aborting txns.
 */
class TransactionManager
{
 public:
  explicit TransactionManager(LockManager* lock_manager, WAL_mgr* wal_manager,
                              BufferPool* buffer_pool);

  /**
   * @brief Begins a new transaction.
   * @return The ID of the newly created transaction.
   */
  TransactionID begin();

  /**
   * @brief Commits a transaction.
   * @param txn_id The ID of the transaction to commit.
   */
  void commit(TransactionID txn_id);

  /**
   * @brief Aborts a transaction.
   * @param txn_id The ID of the transaction to abort.
   */
  void abort(TransactionID txn_id);

  /**
   * @brief Asynchronously commits a transaction.
   * @param txn_id The ID of the transaction to commit.
   * @return An awaitable that completes when the commit is durable.
   */
  boost::asio::awaitable<void> async_commit(TransactionID txn_id);

  /**
   * @brief Asynchronously aborts a transaction.
   * @param txn_id The ID of the transaction to abort.
   * @return An awaitable that completes when the abort is durable.
   */
  boost::asio::awaitable<void> async_abort(TransactionID txn_id);

  /**
   * @brief Retrieves a pointer to an active transaction.
   * @param txn_id The ID of the transaction.
   * @return A pointer to the Transaction object, or nullptr if not found.
   */
  Transaction* get_transaction(TransactionID txn_id);

  struct Shard
  {
    std::mutex shard_mutex_;
    std::unordered_map<TransactionID, std::unique_ptr<Transaction>> active_txns;
  };

 private:
  void apply_undo(LSN lsn, const UpdatePagePayload* payload);
  static constexpr size_t NUM_SHARDS = 256;

  LockManager* lock_manager_;
  WAL_mgr* wal_manager_;
  BufferPool* buffer_pool_;

  std::atomic<TransactionID> next_txn_id_;
  std::array<Shard, NUM_SHARDS> active_txn_shards;

  Shard& get_txn_map_shard(TransactionID txn_id)
  {
    return active_txn_shards[txn_id % NUM_SHARDS];
  }
};

}  // namespace smoldb

#endif  // TRANSACTION_MANAGER_H