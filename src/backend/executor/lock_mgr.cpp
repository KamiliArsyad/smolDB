#include "lock_mgr.h"

#include <cassert>

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
  std::unique_lock<std::mutex> lock(shard.mutex);

  // Get or create the request queue for this RID
  auto& queue = shard.lock_table[rid];

  for (const auto& req : queue.requests)
  {
    if (req.txn_id == txn->get_id() && req.granted)
    {
      return true;  // Already holds a lock (S or X), grant is idempotent
    }
  }

  // Add our request to the end of the queue
  queue.requests.emplace_back(LockRequest{txn->get_id(), LockMode::SHARED});
  auto it = std::prev(queue.requests.end());

  auto can_grant_shared = [&]()
  {
    // Grant if no exclusive lock is held and no exclusive requests are waiting
    // ahead of us. This prevents starving writers.
    if (queue.is_exclusive || queue.is_upgrading) return false;
    for (auto const& req : queue.requests)
    {
      if (req.txn_id == txn->get_id()) return true;       // We are at the front
      if (req.mode == LockMode::EXCLUSIVE) return false;  // Writer is waiting
    }
    return true;
  };

  while (!can_grant_shared())
  {
    if (queue.cv.wait_for(lock, lock_timeout_) == std::cv_status::timeout)
    {
      queue.requests.erase(it);
      return false;  // Deadlock timeout
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
  std::unique_lock<std::mutex> lock(shard.mutex);

  auto& queue = shard.lock_table[rid];

  bool is_upgrade = false;
  for (auto& req : queue.requests)
  {
    if (req.txn_id == txn->get_id() && req.granted)
    {
      if (req.mode == LockMode::EXCLUSIVE) return true;  // Idempotent
      if (req.mode == LockMode::SHARED)
      {
        is_upgrade = true;
        break;
      }
    }
  }

  if (is_upgrade)
  {
    // A transaction is requesting an upgrade.
    // To prevent deadlock, only one transaction can be waiting for an upgrade.
    if (queue.is_upgrading)
    {
      // Another transaction is already waiting to upgrade.
      // We must abort this transaction to prevent deadlock.
      return false;
    }

    queue.is_upgrading = true;

    // Wait until this transaction is the *only* holder of a shared lock.
    while (queue.sharing_count > 1)
    {
      if (queue.cv.wait_for(lock, lock_timeout_) == std::cv_status::timeout)
      {
        queue.is_upgrading = false;  // Clean up state
        queue.cv.notify_all();
        return false;
      }
    }

    // At this point, we are the only shared lock holder. Grant the upgrade.
    for (auto& req : queue.requests)
    {
      if (req.txn_id == txn->get_id())
      {
        req.mode = LockMode::EXCLUSIVE;
        break;
      }
    }
    queue.sharing_count--;  // From 1 to 0
    queue.is_exclusive = true;
    queue.is_upgrading = false;
    // No change to txn->held_locks_, as the RID is already tracked.
    queue.cv.notify_all();  // Wake others who might now be able to proceed
    return true;
  }

  queue.requests.emplace_back(LockRequest{txn->get_id(), LockMode::EXCLUSIVE});
  auto it = std::prev(queue.requests.end());

  auto can_grant_exclusive = [&]()
  {
    // Grant if we are the only one requesting (or at the front) and no other
    // locks are held
    return !queue.is_exclusive && queue.sharing_count == 0 &&
           queue.requests.front().txn_id == txn->get_id();
  };

  while (!can_grant_exclusive())
  {
    if (queue.cv.wait_for(lock, lock_timeout_) == std::cv_status::timeout)
    {
      queue.requests.erase(it);
      return false;  // Deadlock timeout
    }
  }

  it->granted = true;
  queue.is_exclusive = true;
  txn->add_held_lock(rid);
  return true;
}

void LockManager::release_all(Transaction* txn)
{
  for (const auto& rid : txn->get_held_locks())
  {
    Shard& shard = get_shard(rid);
    std::scoped_lock lock(shard.mutex);

    if (shard.lock_table.find(rid) == shard.lock_table.end())
    {
      continue;  // Should not happen in normal operation
    }

    auto& queue = shard.lock_table.at(rid);
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