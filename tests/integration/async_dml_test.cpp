#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include "backend/smoldb.h"

using namespace smoldb;
namespace asio = boost::asio;

class AsyncDMLTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "async_dml_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config{test_dir};
    db = std::make_unique<SmolDB>(config, io_context_.get_executor());
    db->startup();

    Schema schema = {{0, "id", Col_type::INT, false, {}},
                     {1, "val", Col_type::INT, false, {}}};
    db->create_table(1, "test_table", schema);
    db->create_index(1, 0, "pk_test_table");  // Index on 'id'
  }

  void TearDown() override
  {
    finish_all_coroutines();

    db->shutdown();
    io_context_.run();
    io_context_.restart();

    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  void finish_all_coroutines()
  {
    for (auto& f : futs_) f.get();  // re-throw if any coroutine failed
    futs_.clear();
  }

  // Helper to run an awaitable and get its result synchronously
  template <typename T>
  T run_awaitable(asio::awaitable<T> awaitable)
  {
    auto future =
        asio::co_spawn(io_context_, std::move(awaitable), asio::use_future);
    io_context_.run();
    io_context_.restart();
    return future.get();
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  asio::io_context io_context_;
  std::vector<std::future<void>> futs_;
};

TEST_F(AsyncDMLTest, AsyncInsertAndCommitIsVisible)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 101);
  row.set_value("val", 202);

  auto insert_awaitable = [&]() -> asio::awaitable<RID>
  {
    TransactionID txn = db->begin_transaction();
    RID rid = co_await table->async_insert_row(txn, row);
    co_await db->async_commit_transaction(txn);
    co_return rid;
  };
  RID rid = run_awaitable(insert_awaitable());

  // Verify in a new transaction
  auto verify_awaitable = [&]() -> asio::awaitable<int>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    table->get_row(txn, rid, out_row);
    co_await db->async_commit_transaction(txn);
    co_return boost::get<int32_t>(out_row.get_value("val"));
  };
  EXPECT_EQ(202, run_awaitable(verify_awaitable()));
}

TEST_F(AsyncDMLTest, AsyncInsertAndAbortIsNotVisible)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 101);
  row.set_value("val", 202);

  auto insert_abort_awaitable = [&]() -> asio::awaitable<RID>
  {
    TransactionID txn = db->begin_transaction();
    RID rid = co_await table->async_insert_row(txn, row);
    co_await db->async_abort_transaction(txn);
    co_return rid;
  };
  RID rid = run_awaitable(insert_abort_awaitable());

  // Verify the row does not exist
  auto verify_awaitable = [&]() -> asio::awaitable<bool>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    bool found = table->get_row(txn, rid, out_row);
    co_await db->async_commit_transaction(txn);
    co_return found;
  };
  EXPECT_FALSE(run_awaitable(verify_awaitable()));
}

TEST_F(AsyncDMLTest, AsyncUpdateAndCommitIsVisible)
{
  // Setup initial row
  TransactionID setup_txn = db->begin_transaction();
  Table<>* table = db->get_table("test_table");
  Row initial_row(table->get_schema());
  initial_row.set_value("id", 1);
  initial_row.set_value("val", 100);
  RID rid = table->insert_row(setup_txn, initial_row);
  db->commit_transaction(setup_txn);

  // Update the row
  auto update_awaitable = [&]() -> asio::awaitable<void>
  {
    TransactionID txn = db->begin_transaction();
    Row new_row(table->get_schema());
    new_row.set_value("id", 1);
    new_row.set_value("val", 200);
    co_await table->async_update_row(txn, rid, new_row);
    co_await db->async_commit_transaction(txn);
  };
  run_awaitable(update_awaitable());

  // Verify
  auto verify_awaitable = [&]() -> asio::awaitable<int>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    table->get_row(txn, rid, out_row);
    co_await db->async_commit_transaction(txn);
    co_return boost::get<int32_t>(out_row.get_value("val"));
  };
  EXPECT_EQ(200, run_awaitable(verify_awaitable()));
}

TEST_F(AsyncDMLTest, AsyncUpdateAndAbortIsReverted)
{
  // Setup initial row
  TransactionID setup_txn = db->begin_transaction();
  Table<>* table = db->get_table("test_table");
  Row initial_row(table->get_schema());
  initial_row.set_value("id", 1);
  initial_row.set_value("val", 100);
  RID rid = table->insert_row(setup_txn, initial_row);
  db->commit_transaction(setup_txn);

  // Update and abort
  auto update_abort_awaitable = [&]() -> asio::awaitable<void>
  {
    TransactionID txn = db->begin_transaction();
    Row new_row(table->get_schema());
    new_row.set_value("id", 1);
    new_row.set_value("val", 200);
    co_await table->async_update_row(txn, rid, new_row);
    co_await db->async_abort_transaction(txn);
  };
  run_awaitable(update_abort_awaitable());

  // Verify original value is still there
  auto verify_awaitable = [&]() -> asio::awaitable<int>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    table->get_row(txn, rid, out_row);
    co_await db->async_commit_transaction(txn);
    co_return boost::get<int32_t>(out_row.get_value("val"));
  };
  EXPECT_EQ(100, run_awaitable(verify_awaitable()));
}

// This test targets the index undo logic for key-column updates.
TEST_F(AsyncDMLTest, AsyncUpdateKeyColumnAndAbortRestoresIndex)
{
  Table<>* table = db->get_table("test_table");

  auto setup_awaitable = [&]() -> asio::awaitable<RID>
  {
    TransactionID txn = db->begin_transaction();
    Row r(table->get_schema());
    r.set_value("id", 50);
    r.set_value("val", 500);
    RID rid = co_await table->async_insert_row(txn, r);
    co_await db->async_commit_transaction(txn);
    co_return rid;
  };
  RID rid = run_awaitable(setup_awaitable());

  auto update_abort_awaitable = [&]() -> asio::awaitable<void>
  {
    TransactionID txn = db->begin_transaction();
    Row new_row(table->get_schema());
    new_row.set_value("id", 51);  // Key update
    new_row.set_value("val", 501);
    co_await table->async_update_row(txn, rid, new_row);
    co_await db->async_abort_transaction(txn);
  };
  run_awaitable(update_abort_awaitable());

  auto verify_awaitable = [&]() -> asio::awaitable<bool>
  {
    TransactionID txn = db->begin_transaction();
    RID found_rid_50, found_rid_51;
    bool key_50_exists = table->get_rid_from_index(txn, 50, found_rid_50);
    bool key_51_exists = table->get_rid_from_index(txn, 51, found_rid_51);
    co_await db->async_commit_transaction(txn);
    // Key 50 should exist, key 51 should not.
    co_return key_50_exists && !key_51_exists;
  };
  EXPECT_TRUE(run_awaitable(verify_awaitable()));
}

TEST_F(AsyncDMLTest, ConcurrentAsyncInsertsForceNewPage)
{
  Table<>* table = db->get_table("test_table");
  const auto* heap = table->get_heap_file();
  int slots_per_page = heap->slots_per_page();

  TransactionID setup_txn = db->begin_transaction();
  for (int i = 0; i < slots_per_page - 1; ++i)
  {
    Row r(table->get_schema());
    r.set_value("id", i);
    r.set_value("val", i);
    table->insert_row(setup_txn, r);
  }
  db->commit_transaction(setup_txn);

  auto db_ref = std::shared_ptr<SmolDB>(db.get(), [](SmolDB*) {});
  const int num_concurrent_inserts = 5;

  for (int i = 0; i < num_concurrent_inserts; ++i)
  {
    int32_t v = static_cast<int32_t>(slots_per_page + i);

    futs_.emplace_back(asio::co_spawn(
        io_context_,
        [db_ref, table, v]() -> asio::awaitable<void>
        {
          TransactionID txn = db_ref->begin_transaction();
          Row r(table->get_schema());
          r.set_value("id", v);
          r.set_value("val", v);
          co_await table->async_insert_row(txn, r);
          co_await db_ref->async_commit_transaction(txn);
        },
        asio::use_future));
  }

  io_context_.run();
  io_context_.restart();

  auto final_count = (slots_per_page - 1) + num_concurrent_inserts;
  EXPECT_EQ(final_count, table->scan_all().size());
}

TEST_F(AsyncDMLTest, AsyncDeleteAndCommitIsDurable)
{
  // Setup: Insert a row that we can delete.
  TransactionID setup_txn = db->begin_transaction();
  Table<>* table = db->get_table("test_table");
  Row initial_row(table->get_schema());
  initial_row.set_value("id", 42);
  initial_row.set_value("val", 420);
  RID rid = table->insert_row(setup_txn, initial_row);
  db->commit_transaction(setup_txn);

  // Action: Asynchronously delete the row and commit.
  auto delete_awaitable = [&]() -> asio::awaitable<bool>
  {
    TransactionID txn = db->begin_transaction();
    bool success = co_await table->async_delete_row(txn, rid);
    co_await db->async_commit_transaction(txn);
    co_return success;
  };
  ASSERT_TRUE(run_awaitable(delete_awaitable()));

  // Verify: The row and its index entry are gone.
  auto verify_awaitable = [&]() -> asio::awaitable<std::pair<bool, bool>>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    bool row_found = table->get_row(txn, rid, out_row);
    RID found_rid;
    bool index_found = table->get_rid_from_index(txn, 42, found_rid);
    co_await db->async_commit_transaction(txn);
    co_return std::pair{!row_found, !index_found};
  };
  auto [row_is_gone, index_is_gone] = run_awaitable(verify_awaitable());
  EXPECT_TRUE(row_is_gone);
  EXPECT_TRUE(index_is_gone);
}

TEST_F(AsyncDMLTest, AsyncDeleteAndAbortIsReverted)
{
  // Setup: Insert a row.
  TransactionID setup_txn = db->begin_transaction();
  Table<>* table = db->get_table("test_table");
  Row initial_row(table->get_schema());
  initial_row.set_value("id", 42);
  initial_row.set_value("val", 420);
  RID rid = table->insert_row(setup_txn, initial_row);
  db->commit_transaction(setup_txn);

  // Action: Asynchronously delete the row and then abort.
  auto delete_abort_awaitable = [&]() -> asio::awaitable<void>
  {
    TransactionID txn = db->begin_transaction();
    co_await table->async_delete_row(txn, rid);
    co_await db->async_abort_transaction(txn);
  };
  run_awaitable(delete_abort_awaitable());

  // Verify: The row and its index entry are still present.
  auto verify_awaitable = [&]() -> asio::awaitable<std::pair<bool, bool>>
  {
    TransactionID txn = db->begin_transaction();
    Row out_row;
    bool row_found = table->get_row(txn, rid, out_row);
    RID found_rid;
    bool index_found = table->get_rid_from_index(txn, 42, found_rid);
    co_await db->async_commit_transaction(txn);
    co_return std::pair{row_found, index_found};
  };
  auto [row_is_present, index_is_present] = run_awaitable(verify_awaitable());
  EXPECT_TRUE(row_is_present);
  EXPECT_TRUE(index_is_present);
}

TEST_F(AsyncDMLTest, AsyncDeleteInvalidRidReturnsFalse)
{
  Table<>* table = db->get_table("test_table");
  RID invalid_rid = {999, 999};

  auto delete_awaitable = [&]() -> asio::awaitable<bool>
  {
    TransactionID txn = db->begin_transaction();
    bool success = co_await table->async_delete_row(txn, invalid_rid);
    co_await db->async_abort_transaction(
        txn);  // Abort is fine, nothing happened
    co_return success;
  };

  EXPECT_FALSE(run_awaitable(delete_awaitable()));
}
