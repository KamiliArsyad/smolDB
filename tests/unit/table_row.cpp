#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#include "access.h"
#include "executor/lock_mgr.h"
#include "executor/trx_mgr.h"
#include "mock_heapfile.h"
#include "storage/bfrpl.h"    // Needed for the real BufferPool
#include "storage/dsk_mgr.h"  // Needed for the real BufferPool
#include "storage/wal_mgr.h"

class AccessTest : public testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "smoldb_test_access";
    std::filesystem::create_directories(test_dir);

    // We now need real managers to satisfy the TransactionManager constructor.
    dummy_db_path_ = test_dir / "dummy.db";
    dummy_wal_path_ = test_dir / "dummy.wal";

    disk_mgr_ = std::make_unique<Disk_mgr>(dummy_db_path_);
    wal_mgr_ = std::make_unique<WAL_mgr>(dummy_wal_path_);
    buffer_pool_ =
        std::make_unique<BufferPool>(16, disk_mgr_.get(), wal_mgr_.get());
    lock_mgr_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(
        lock_mgr_.get(), wal_mgr_.get(), buffer_pool_.get());
    // The heap can still be mocked for this test's purpose.
    heap_ = std::make_unique<MockHeapFile>(buffer_pool_.get(), wal_mgr_.get(),
                                           1, 256);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir); }

  std::filesystem::path test_dir;
  std::filesystem::path dummy_db_path_;
  std::filesystem::path dummy_wal_path_;
  std::unique_ptr<MockHeapFile> heap_;

  // Real dependencies required by the updated TransactionManager
  std::unique_ptr<Disk_mgr> disk_mgr_;
  std::unique_ptr<BufferPool> buffer_pool_;
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