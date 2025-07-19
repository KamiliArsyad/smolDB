#include "h_idx.h"

#include <mutex>

using namespace smoldb;

InMemoryHashIndex::InMemoryHashIndex(uint8_t key_column_id)
    : key_column_id_(key_column_id)
{
}

void InMemoryHashIndex::insert_entry(const Row& row, const RID& rid)
{
  std::unique_lock lock(mutex_);
  const auto key = row.get_value(key_column_id_);
  map_[key] = rid;
}

void InMemoryHashIndex::delete_entry(const Row& row)
{
  std::unique_lock lock(mutex_);
  const auto key = row.get_value(key_column_id_);
  map_.erase(key);
}

// TODO: Test this
bool InMemoryHashIndex::update_entry(const Row& old_row, const Row& new_row,
                                     const RID& rid)
{
  const IndexKey& old_key = old_row.get_value(key_column_id_);
  const IndexKey& new_key = new_row.get_value(key_column_id_);
  if (old_key == new_key)
  {
    return false;  // Non-key update, nothing to do for the index.
  }

  std::unique_lock lock(mutex_);
  map_.erase(old_key);
  map_[new_key] = rid;
  return true;
}

bool InMemoryHashIndex::get(const IndexKey& key, RID& out_rid) const
{
  std::shared_lock lock(mutex_);
  auto it = map_.find(key);
  if (it != map_.end())
  {
    out_rid = it->second;
    return true;
  }
  return false;
}

void InMemoryHashIndex::build(Table<>* source_table)
{
  std::unique_lock lock(mutex_);
  map_.clear();
  for (const auto& [rid, row] : *source_table)
  {
    const IndexKey& key = row.get_value(key_column_id_);
    map_[key] = rid;
  }
}