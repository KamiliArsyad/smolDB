#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#include "access.h"
#include "executor/lock_mgr.h"
#include "executor/trx_mgr.h"
#include "storage/wal_mgr.h"

// Note: This MockHeapFile matches the structure of the one in test_access.cpp.
// In a larger project, this might be shared in a test utility header.
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
};

class AccessTest : public testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "smoldb_test_access";
    std::filesystem::create_directories(test_dir);
    // The mock heap doesn't need real managers, so passing nullptr is fine for
    // this test.
    heap_ = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);

    // Create lightweight, test-only manager instances.
    // The WAL needs a path, but it won't be heavily used here.
    dummy_wal_path_ = test_dir / "dummy.wal";
    lock_mgr_ = std::make_unique<LockManager>();
    wal_mgr_ = std::make_unique<WAL_mgr>(dummy_wal_path_);
    txn_mgr_ = std::make_unique<TransactionManager>(lock_mgr_.get(),
                                                    wal_mgr_.get(), nullptr);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir); }

  std::filesystem::path test_dir;
  std::filesystem::path dummy_wal_path_;
  std::unique_ptr<MockHeapFile> heap_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<WAL_mgr> wal_mgr_;
  std::unique_ptr<TransactionManager> txn_mgr_;
};

TEST_F(AccessTest, BasicTableInsert)
{
  Schema schema;
  schema.push_back({0, "id", Col_type::INT, false, {}});

  Table<MockHeapFile> table(std::move(heap_), 1, "test_table", schema,
                            lock_mgr_.get(), txn_mgr_.get());

  TransactionID txn_id = txn_mgr_->begin();

  Row row(schema);
  row.set_value("id", 123);

  table.insert_row(txn_id, row);
  txn_mgr_->commit(txn_id);

  auto rows = table.scan_all();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(boost::get<int32_t>(rows[0].get_value("id")), 123);
}