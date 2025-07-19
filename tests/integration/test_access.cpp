#include <gtest/gtest.h>

#include <boost/variant.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "access.h"
#include "backend/smoldb.h"
#include "idx.h"
#include "mock_heapfile.h"

using namespace smoldb;

static Schema make_simple_schema()
{
  Column c0{0, "id", Col_type::INT, false, {}};
  Column c1{1, "name", Col_type::STRING, false, {}};
  return Schema{c0, c1};
}

TEST(RowTest, SetGetValueCorrect)
{
  Schema schema = make_simple_schema();
  Row row(schema);
  row.set_value(0, int32_t(42));
  row.set_value(1, std::string("Alice"));
  EXPECT_EQ(boost::get<int32_t>(row.get_value(0)), 42);
  EXPECT_EQ(boost::get<std::string>(row.get_value("name")), "Alice");
}

class TableTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "table_test_dummy";
    std::filesystem::create_directories(test_dir_);
    auto dummy_db_path = test_dir_ / "dummy.db";
    auto dummy_wal_path = test_dir_ / "dummy.wal";

    disk_mgr_ = std::make_unique<Disk_mgr>(dummy_db_path);
    wal_mgr_ = std::make_unique<WAL_mgr>(dummy_wal_path);
    buffer_pool_ =
        std::make_unique<BufferPool>(16, disk_mgr_.get(), wal_mgr_.get());
    lock_mgr_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(
        lock_mgr_.get(), wal_mgr_.get(), buffer_pool_.get());
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::filesystem::path test_dir_;
  std::unique_ptr<Disk_mgr> disk_mgr_;
  std::unique_ptr<WAL_mgr> wal_mgr_;
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TransactionManager> txn_mgr_;
};

TEST_F(TableTest, InsertScanSingleRow)
{
  Schema schema = make_simple_schema();
  auto mock_hf = std::make_unique<MockHeapFile>(buffer_pool_.get(),
                                                wal_mgr_.get(), 1, 256);

  Table<MockHeapFile> table(std::move(mock_hf), 1, "test_table", schema,
                            lock_mgr_.get(), txn_mgr_.get());
  TransactionID txn_id = txn_mgr_->begin();

  Row row(schema);
  row.set_value(0, int32_t(100));
  row.set_value(1, std::string("Name100"));
  table.insert_row(txn_id, row);
  txn_mgr_->commit(txn_id);

  std::vector<Row> rows = table.scan_all();
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(boost::get<int32_t>(rows[0].get_value(0)), 100);
}

// --------------------------------------------------------------------------------
// CATALOG TESTS (with full backend fixture via SmolDB)
// --------------------------------------------------------------------------------
class CatalogTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir =
        std::filesystem::temp_directory_path() / "catalog_test_integration";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config{test_dir, BUFFER_SIZE_FOR_TEST};
    db = std::make_unique<SmolDB>(config);
    db->startup();
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
};

TEST_F(CatalogTest, CreateGetList)
{
  Schema schema = make_simple_schema();
  db->create_table(10, "cat_table", schema);

  Table<>* tbl_by_name = db->get_table("cat_table");
  ASSERT_NE(tbl_by_name, nullptr);
  EXPECT_EQ(tbl_by_name->get_table_id(), 10u);

  Table<>* tbl_by_id = db->get_table(10);
  ASSERT_EQ(tbl_by_id, tbl_by_name);

  EXPECT_EQ(db->get_table(99), nullptr);
}

TEST_F(CatalogTest, DuplicateTableThrows)
{
  Schema schema = make_simple_schema();
  db->create_table(1, "dup_table", schema);
  EXPECT_THROW(db->create_table(1, "dup_table2", schema),
               std::invalid_argument);
  EXPECT_THROW(db->create_table(2, "dup_table", schema), std::invalid_argument);
}

TEST_F(CatalogTest, PersistAndReload)
{
  Schema schema = make_simple_schema();
  db->create_table(22, "users", schema);
  TransactionID txn_id = db->begin_transaction();

  Row row(schema);
  row.set_value("id", 42);
  row.set_value("name", "Zaphod");
  auto* table = db->get_table("users");
  RID rid = table->insert_row(txn_id, row);
  db->commit_transaction(txn_id);

  db->shutdown();

  // --- Simulate restart ---
  smoldb::DBConfig config(test_dir, BUFFER_SIZE_FOR_TEST);
  auto new_db = std::make_unique<SmolDB>(config);
  new_db->startup();

  auto* reloaded_table = new_db->get_table(22);
  ASSERT_NE(reloaded_table, nullptr);

  TransactionID new_txn_id = new_db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(reloaded_table->get_row(new_txn_id, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 42);
  new_db->commit_transaction(new_txn_id);
}