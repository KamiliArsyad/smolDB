#include "lock_mgr.h"

#include <cassert>

#include "lock_excepts.h"

using namespace smoldb;

LockManager::LockManager()
    : shard_count_(std::thread::hardware_concurrency()),
      lock_timeout_(100)  // 100ms timeout for deadlock prevention
{
  shards_.resize(shard_count_);
}

LockManager::Shard& LockManager::get_shard(const RID& rid)
{
  return shards_[std::hash<RID>()(rid) % shard_count_];
}

bool LockManager::acquire_shared(Transaction* txn, const RID& rid)
{
  Shard& shard = get_shard(rid);
  std::unique_lock lock(shard.mutex);
  auto& queue = shard.lock_table[rid];

  for (const auto& req : queue.requests)
  {
    if (req.txn_id == txn->get_id() && req.granted)
    {
      return true;  // Already holds a lock (S or X), grant is idempotent
    }
  }

  queue.requests.emplace_back(LockRequest{txn->get_id(), LockMode::SHARED});
  auto it = std::prev(queue.requests.end());

  auto can_grant_shared = [&]()
  {
    // Cannot grant a new S lock if an upgrade or exclusive lock is pending.
    for (auto const& req : queue.requests)
    {
      if (req.txn_id != txn->get_id() &&
          (req.mode == LockMode::EXCLUSIVE || req.mode == LockMode::UPGRADE))
      {
        return false;
      }
    }
    return true;
  };

  while (!can_grant_shared())
  {
    if (queue.cv.wait_for(lock, lock_timeout_) == std::cv_status::timeout)
    {
      queue.requests.erase(it);
      throw LockTimeoutException();
    }
  }

  it->granted = true;
  queue.sharing_count++;
  txn->add_held_lock(rid);
  return true;
}

bool LockManager::acquire_exclusive(Transaction* txn, const RID& rid)
{
  Shard& shard = get_shard(rid);
  std::unique_lock lock(shard.mutex);

  auto& queue = shard.lock_table[rid];

  // Find if a request for this transaction already exists.
  auto it = queue.requests.end();
  for (auto req_it = queue.requests.begin(); req_it != queue.requests.end();
       ++req_it)
  {
    if (req_it->txn_id == txn->get_id())
    {
      it = req_it;
      break;
    }
  }

  // Handle existing requests (idempotency and upgrades).
  if (it != queue.requests.end())
  {
    if (it->mode == LockMode::EXCLUSIVE)
    {
      return true;  // Already holds X lock.
    }
    // This is an upgrade request. Change mode to UPGRADE.
    it->mode = LockMode::UPGRADE;
  }
  else
  {
    // No existing request, add a new one.
    queue.requests.emplace_back(
        LockRequest{txn->get_id(), LockMode::EXCLUSIVE});
    it = std::prev(queue.requests.end());
  }

  // Unified wait loop.
  while (true)
  {
    // Check grant conditions.
    if (it->mode == LockMode::UPGRADE)
    {
      // Grant upgrade if this txn is the only one with a shared lock.
      if (queue.sharing_count == 1 &&
          it->granted)  // Must have been granted S lock
      {
        break;
      }
    }
    else if (it->mode == LockMode::EXCLUSIVE)
    {
      // Grant new exclusive lock if no other locks are held and it's our turn.
      if (queue.sharing_count == 0 && !queue.is_exclusive &&
          queue.requests.front().txn_id == txn->get_id())
      {
        break;
      }
    }

    // Deadlock prevention: only one upgrader allowed.
    if (it->mode == LockMode::UPGRADE)
    {
      for (const auto& other_req : queue.requests)
      {
        if (other_req.txn_id != it->txn_id &&
            other_req.mode == LockMode::UPGRADE)
        {
          it->mode = LockMode::SHARED;  // Revert before throwing
          throw DeadlockException();
        }
      }
    }

    // Wait or timeout.
    if (queue.cv.wait_for(lock, lock_timeout_) == std::cv_status::timeout)
    {
      if (it->mode == LockMode::UPGRADE)
        it->mode = LockMode::SHARED;
      else
        queue.requests.erase(it);
      throw LockTimeoutException();
    }
  }

  // --- Grant the lock ---
  bool was_upgrade = (it->mode == LockMode::UPGRADE);

  if (was_upgrade)
  {
    queue.sharing_count--;  // From 1 to 0
  }

  it->mode = LockMode::EXCLUSIVE;
  it->granted = true;
  queue.is_exclusive = true;

  if (!was_upgrade)  // Only add to held set if it was a new lock.
  {
    txn->add_held_lock(rid);
  }

  return true;
}

void LockManager::release_all(Transaction* txn)
{
  for (const auto& rid : txn->get_held_locks())
  {
    Shard& shard = get_shard(rid);
    std::scoped_lock lock(shard.mutex);
    auto iter = shard.lock_table.find(rid);

    if (iter == shard.lock_table.end())
    {
      continue;  // Should not happen in normal operation
    }

    auto& queue = iter->second;

    for (auto it = queue.requests.begin(); it != queue.requests.end(); ++it)
    {
      if (it->txn_id == txn->get_id() && it->granted)
      {
        if (it->mode == LockMode::EXCLUSIVE)
        {
          queue.is_exclusive = false;
        }
        else
        {
          queue.sharing_count--;
        }
        queue.requests.erase(it);
        break;
      }
    }
    // Wake up any waiting threads
    queue.cv.notify_all();
  }
}