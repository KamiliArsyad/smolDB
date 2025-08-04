#ifndef MOCKHEAPFILE_H
#define MOCKHEAPFILE_H
#include <vector>

#include "../executor/trx.h"
#include "bfrpl.h"

namespace smoldb
{

// MockHeapFile for testing Table in isolation
class MockHeapFile
{
 public:
  std::vector<std::vector<std::byte>> rows;
  size_t max_tuple_size = 256;

  // Mock constructor to match the real HeapFile signature
  MockHeapFile(BufferPool*, WAL_mgr*, PageID, size_t max_size)
      : max_tuple_size(max_size)
  {
  }

  RID append(Transaction*, std::span<const std::byte> t)
  {
    rows.emplace_back(t.begin(), t.end());
    return {static_cast<PageID>(rows.size() - 1), 0};
  }

  void full_scan(std::vector<std::vector<std::byte>>& out) const { out = rows; }

  bool get(Transaction*, RID rid, std::vector<std::byte>& out) const
  {
    if (rid.page_id >= rows.size() || rid.slot != 0) return false;
    out = rows[rid.page_id];
    return true;
  }

  PageID first_page_id() const { return 0; }
  PageID last_page_id() const { return rows.size() - 1; }

  bool update(Transaction* txn, RID rid,
              std::span<const std::byte> new_tuple_data)
  {
    return true;
  }

  bool delete_row(Transaction* txn, RID rid) { return true; }

  boost::asio::awaitable<RID> async_append(
      Transaction *txn, std::span<const std::byte> tuple_data)
  {
    co_return true;
  }

  boost::asio::awaitable<bool> async_update(
      Transaction *txn, RID rid, std::span<const std::byte> new_tuple_data)
  {
    co_return true;
  }

  boost::asio::awaitable<bool> async_delete(Transaction *txn, RID rid)
  {
    co_return true;
  }

  bool get_next_tuple(RID& rid, std::vector<std::byte>& out_tuple) const
  {
    return true;
  }
};

}  // namespace smoldb

#endif  // MOCKHEAPFILE_H
