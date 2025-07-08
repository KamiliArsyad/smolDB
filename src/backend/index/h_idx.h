#ifndef SMOLDB_H_IDX_H
#define SMOLDB_H_IDX_H

#include <boost/functional/hash.hpp>
#include <shared_mutex>
#include <unordered_map>

#include "../access/access.h"
#include "idx.h"

namespace boost
{
template <class Clock, class Dur>
struct hash<std::chrono::time_point<Clock, Dur>>
{
  std::size_t operator()(
      std::chrono::time_point<Clock, Dur> const& tp) const noexcept
  {
    auto cnt = tp.time_since_epoch().count();
    return boost::hash<decltype(cnt)>()(cnt);
  }
};
}  // namespace boost

class InMemoryHashIndex : public Index
{
 public:
  explicit InMemoryHashIndex(uint8_t key_column_id);

  void insert_entry(const Row& row, const RID& rid) override;
  void delete_entry(const Row& row) override;
  bool update_entry(const Row& old_row, const Row& new_row,
                    const RID& rid) override;
  bool get(const IndexKey& key, RID& out_rid) const override;
  void build(Table<>* source_table) override;

 private:
  std::unordered_map<IndexKey, RID, boost::hash<IndexKey>> map_;
  mutable std::shared_mutex mutex_;
  const uint8_t key_column_id_;
};

#endif  // SMOLDB_H_IDX_H