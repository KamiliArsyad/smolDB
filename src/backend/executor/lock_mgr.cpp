#include "lock_mgr.h"

#include <cassert>

LockManager::LockManager() : shard_count_(std::thread::hardware_concurrency())
{
  shards_.resize(shard_count_);
}

LockManager::Shard& LockManager::get_shard(const RID& rid)
{
  // Simple hash-based sharding
  size_t shard_idx = std::hash<RID>()(rid) % shard_count_;
  return shards_[shard_idx];
}

bool LockManager::acquire_shared(Transaction* txn, const RID& rid)
{
  // For now, this is a stub. A real implementation would handle lock
  // compatibility checks, waiting, and deadlock detection.
  // For breadth-first, we'll just add the lock to the transaction's set.
  std::scoped_lock lock(txn->get_mutex());
  txn->add_held_lock(rid);
  return true;
}

bool LockManager::acquire_exclusive(Transaction* txn, const RID& rid)
{
  // For now, this is a stub. A real implementation would handle lock
  // compatibility checks, waiting, and deadlock detection.
  // For breadth-first, we'll just add the lock to the transaction's set.
  std::scoped_lock lock(txn->get_mutex());
  txn->add_held_lock(rid);
  return true;
}

void LockManager::release_all(Transaction* txn)
{
  // The actual lock release logic that signals waiting threads would go here.
  // For now, we just rely on the transaction's lock set being cleared when
  // the transaction object is destroyed.
  // In a real implementation, we would iterate over txn->get_held_locks(),
  // get the shard for each RID, lock the shard, find the LockRequestQueue,
  // remove the request, and notify waiters.
}