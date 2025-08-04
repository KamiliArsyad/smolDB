#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
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
namespace asio = boost::asio;

static Schema make_simple_schema()
{
  Column c0{0, "id", Col_type::INT, false, {}};
  Column c1{1, "name", Col_type::STRING, false, {}, 32};
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
    wal_mgr_ =
        std::make_unique<WAL_mgr>(dummy_wal_path, io_context_.get_executor());
    buffer_pool_ =
        std::make_unique<BufferPool>(16, disk_mgr_.get(), wal_mgr_.get());
    lock_mgr_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(
        lock_mgr_.get(), wal_mgr_.get(), buffer_pool_.get());
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  template <typename T>
  T run_awaitable(asio::awaitable<T> awaitable)
  {
    auto future =
        asio::co_spawn(io_context_, std::move(awaitable), asio::use_future);
    io_context_.run();
    io_context_.restart();
    return future.get();
  }

  std::filesystem::path test_dir_;
  std::unique_ptr<Disk_mgr> disk_mgr_;
  std::unique_ptr<WAL_mgr> wal_mgr_;
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TransactionManager> txn_mgr_;
  boost::asio::io_context io_context_;
};

enum class OpType
{
  SYNC,
  ASYNC
};

class TableValidationTest : public TableTest,
                            public ::testing::WithParamInterface<OpType>
{
 protected:
  // Helper to run an awaitable and get its result synchronously for tests
  template <typename T>
  T run_awaitable(asio::awaitable<T> awaitable)
  {
    auto future =
        asio::co_spawn(io_context_, std::move(awaitable), asio::use_future);
    io_context_.run();
    io_context_.restart();
    return future.get();
  }

  // Dispatcher function that calls either the sync or async version
  // based on the test parameter.
  void perform_insert(Table<MockHeapFile>& table, TransactionID txn_id,
                      const Row& row)
  {
    if (GetParam() == OpType::SYNC)
    {
      table.insert_row(txn_id, row);
    }
    else
    {
      run_awaitable(table.async_insert_row(txn_id, row));
    }
  }

  void perform_update(Table<MockHeapFile>& table, TransactionID txn_id, RID rid,
                      const Row& row)
  {
    if (GetParam() == OpType::SYNC)
    {
      table.update_row(txn_id, rid, row);
    }
    else
    {
      run_awaitable(table.async_update_row(txn_id, rid, row));
    }
  }
};

TEST_P(TableValidationTest, DMLOperationsWithMismatchedSchemaTypeThrow)
{
  Schema table_schema = {{0, "id", Col_type::INT, false, {}},
                         {1, "val", Col_type::STRING, false, {}, 16}};
  Schema bad_row_schema = {
      {0, "id", Col_type::INT, false, {}},
      {1, "val", Col_type::FLOAT, false, {}}};  // Type mismatch

  auto heap = std::make_unique<MockHeapFile>(buffer_pool_.get(), wal_mgr_.get(),
                                             1, 256);
  Table<MockHeapFile> table(std::move(heap), 1, "test", table_schema,
                            lock_mgr_.get(), txn_mgr_.get());

  Row bad_row(bad_row_schema);
  bad_row.set_value("id", 123);
  bad_row.set_value("val", 456.7f);
  RID dummy_rid = {0, 0};

  TransactionID txn_id = txn_mgr_->begin();

  EXPECT_THROW(perform_insert(table, txn_id, bad_row), std::invalid_argument);
  EXPECT_THROW(perform_update(table, txn_id, dummy_rid, bad_row),
               std::invalid_argument);
}

INSTANTIATE_TEST_SUITE_P(
    SyncAndAsyncValidation, TableValidationTest,
    ::testing::Values(OpType::SYNC, OpType::ASYNC),
    [](const ::testing::TestParamInfo<TableValidationTest::ParamType>& info)
    { return info.param == OpType::SYNC ? "Sync" : "Async"; });

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

TEST_F(TableTest, AsyncInsertRowMismatchedSchemaThrows)
{
  Schema table_schema = {{0, "id", Col_type::INT, false, {}}};
  Schema row_schema = {{0, "id", Col_type::INT, false, {}},
                       {1, "val", Col_type::INT, false, {}}};  // Mismatched

  auto heap = std::make_unique<MockHeapFile>(buffer_pool_.get(), wal_mgr_.get(),
                                             0, 256);
  Table<MockHeapFile> table(std::move(heap), 1, "test", table_schema,
                            lock_mgr_.get(), txn_mgr_.get());

  Row bad_row(row_schema);
  bad_row.set_value("id", 1);
  bad_row.set_value("val", 2);

  auto awaitable = [&]() -> asio::awaitable<void>
  {
    TransactionID txn = txn_mgr_->begin();
    co_await table.async_insert_row(txn, bad_row);
    // Should not reach here
  };

  EXPECT_THROW(run_awaitable(awaitable()), std::invalid_argument);
}

TEST_F(TableTest, AsyncUpdateRowValidatesSchema)
{
  Schema table_schema = {{0, "id", Col_type::INT, false, {}}};
  Schema row_schema = {
      {0, "id", Col_type::FLOAT, false, {}}};  // Mismatched type

  auto heap = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  Table<MockHeapFile> table(std::move(heap), 1, "test", table_schema,
                            lock_mgr_.get(), txn_mgr_.get());

  Row bad_row(row_schema);
  bad_row.set_value("id", 1.0f);
  RID dummy_rid = {0, 0};

  auto awaitable = [&]() -> asio::awaitable<bool>
  {
    TransactionID txn = txn_mgr_->begin();
    // The schema check in the Row constructor or `to_bytes` will throw
    // before the co_await even happens in this case, but this confirms the
    // validation path.
    co_return table.update_row(txn, dummy_rid, bad_row);
    // co_await table.async_update_row(txn, dummy_rid, bad_row);
  };

  // This exception comes from Row::to_bytes, proving validation happens.
  EXPECT_THROW(run_awaitable(awaitable()), std::invalid_argument);
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
    db = std::make_unique<SmolDB>(config, io_context_.get_executor());
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
  boost::asio::io_context io_context_;
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
  auto new_db = std::make_unique<SmolDB>(config, io_context_.get_executor());
  new_db->startup();

  auto* reloaded_table = new_db->get_table(22);
  ASSERT_NE(reloaded_table, nullptr);

  TransactionID new_txn_id = new_db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(reloaded_table->get_row(new_txn_id, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 42);
  new_db->commit_transaction(new_txn_id);
}