#include <gtest/gtest.h>

#include <barrier>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <thread>

#include "backend/proc/proc.h"
#include "backend/proc/proc_ctx.h"
#include "backend/smoldb.h"
#include "lock_excepts.h"

using namespace smoldb;
namespace asio = boost::asio;

// The user-defined procedure, updated to be a coroutine.
class TransferPointsProcedure : public TransactionProcedure
{
 public:
  std::string get_name() const override { return "transfer_points"; }

  asio::awaitable<ProcedureStatus> execute(TransactionContext& ctx,
                                           const ProcedureParams& params,
                                           ProcedureResult& result) override
  {
    // 1. Parameter Validation
    if (!params.count("from_user") || !params.count("to_user") ||
        !params.count("amount"))
    {
      co_return ProcedureStatus::ABORT;
    }
    const auto from_id = boost::get<int32_t>(params.at("from_user"));
    const auto to_id = boost::get<int32_t>(params.at("to_user"));
    const auto amount = boost::get<int32_t>(params.at("amount"));

    if (amount <= 0) co_return ProcedureStatus::ABORT;

    // 2. Data Access
    Table<>* table = ctx.get_table("user_points");
    Schema schema = table->get_schema();
    RID from_rid, to_rid;
    Row from_row(schema), to_row(schema);

    if (!table->get_rid_from_index(ctx.get_txn_id(), from_id, from_rid) ||
        !table->get_rid_from_index(ctx.get_txn_id(), to_id, to_rid))
    {
      co_return ProcedureStatus::ABORT;  // User not found
    }
    if (!table->get_row(ctx.get_txn_id(), from_rid, from_row) ||
        !table->get_row(ctx.get_txn_id(), to_rid, to_row))
    {
      // Should not happen if index is correct
      co_return ProcedureStatus::ABORT;
    }

    // 3. Business Rule Enforcement
    int32_t from_balance = boost::get<int32_t>(from_row.get_value("points"));
    if (from_balance < amount)
    {
      result["message"] = "Insufficient points";
      co_return ProcedureStatus::ABORT;
    }

    // 4. State Modification
    int32_t to_balance = boost::get<int32_t>(to_row.get_value("points"));
    from_row.set_value("points", from_balance - amount);
    to_row.set_value("points", to_balance + amount);
    table->update_row(ctx.get_txn_id(), from_rid, from_row);
    table->update_row(ctx.get_txn_id(), to_rid, to_row);

    // 5. Return Success
    result["message"] = "Transfer successful";
    co_return ProcedureStatus::SUCCESS;
  }
};

class TransferPointsTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "transfer_points_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config;
    config.db_directory = test_dir;
    db = std::make_unique<SmolDB>(config, io_context_.get_executor());
    db->startup();

    // Setup schema and initial data
    Schema schema = {{0, "user_id", Col_type::INT, false, {}},
                     {1, "points", Col_type::INT, false, {}}};
    db->create_table(1, "user_points", schema);
    db->create_index(1, 0, "pk_user_points");

    asio::co_spawn(
        io_context_,
        [&]() -> asio::awaitable<void>
        {
          TransactionID txn = db->begin_transaction();
          Table<>* table = db->get_table("user_points");
          Row r1(schema), r2(schema);
          r1.set_value("user_id", 101);
          r1.set_value("points", 100);
          r2.set_value("user_id", 202);
          r2.set_value("points", 50);
          co_await table->async_insert_row(txn, r1);
          co_await table->async_insert_row(txn, r2);
          co_await db->async_commit_transaction(txn);
        },
        asio::detached);
    io_context_.run();
    io_context_.restart();  // Reset for subsequent tests

    // Register the procedure
    db->get_procedure_manager()->register_procedure(
        std::make_unique<TransferPointsProcedure>());
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  asio::io_context io_context_;
};

TEST_F(TransferPointsTest, SuccessfulTransfer)
{
  ProcedureManager* proc_mgr = db->get_procedure_manager();
  ProcedureParams params = {
      {"from_user", 101}, {"to_user", 202}, {"amount", 10}};

  auto fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<ProcedureStatus>
      {
        auto [status, result] = co_await proc_mgr->async_execute_procedure(
            "transfer_points", params);
        co_return status;
      },
      asio::use_future);
  io_context_.run();
  io_context_.restart();
  ASSERT_EQ(fut.get(), ProcedureStatus::SUCCESS);

  // Verify
  auto verify_fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<std::pair<int32_t, int32_t>>
      {
        Table<>* table = db->get_table("user_points");
        TransactionID txn = db->begin_transaction();
        RID rid1, rid2;
        Row r1(db->get_table("user_points")->get_schema()),
            r2(db->get_table("user_points")->get_schema());
        table->get_rid_from_index(txn, 101, rid1);
        table->get_rid_from_index(txn, 202, rid2);
        table->get_row(txn, rid1, r1);
        table->get_row(txn, rid2, r2);
        co_await db->async_commit_transaction(txn);
        co_return std::make_pair(boost::get<int32_t>(r1.get_value("points")),
                                 boost::get<int32_t>(r2.get_value("points")));
      },
      asio::use_future);

  io_context_.run();
  io_context_.restart();
  auto balances = verify_fut.get();
  EXPECT_EQ(balances.first, 90);
  EXPECT_EQ(balances.second, 60);
}

TEST_F(TransferPointsTest, InsufficientPointsRollback)
{
  ProcedureManager* proc_mgr = db->get_procedure_manager();
  ProcedureParams params = {
      {"from_user", 101}, {"to_user", 202}, {"amount", 200}};

  auto fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<ProcedureStatus>
      {
        auto [status, result] = co_await proc_mgr->async_execute_procedure(
            "transfer_points", params);
        co_return status;
      },
      asio::use_future);
  io_context_.run();
  io_context_.restart();
  ASSERT_EQ(fut.get(), ProcedureStatus::ABORT);

  // Verify state is unchanged
  auto verify_fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<std::pair<int32_t, int32_t>>
      {
        Table<>* table = db->get_table("user_points");
        TransactionID txn = db->begin_transaction();
        RID rid1, rid2;
        Row r1(db->get_table("user_points")->get_schema()),
            r2(db->get_table("user_points")->get_schema());
        table->get_rid_from_index(txn, 101, rid1);
        table->get_rid_from_index(txn, 202, rid2);
        table->get_row(txn, rid1, r1);
        table->get_row(txn, rid2, r2);
        co_await db->async_commit_transaction(txn);
        co_return std::make_pair(boost::get<int32_t>(r1.get_value("points")),
                                 boost::get<int32_t>(r2.get_value("points")));
      },
      asio::use_future);

  io_context_.run();
  io_context_.restart();
  auto balances = verify_fut.get();
  EXPECT_EQ(balances.first, 100);
  EXPECT_EQ(balances.second, 50);
}

TEST_F(TransferPointsTest, ConcurrentTransfersAreAtomic)
{
  const int num_threads = 10;
  const int transfers_per_thread = 5;
  std::atomic<int> total_transfers = num_threads * transfers_per_thread;

  // Setup a thread pool to run the io_context
  std::vector<std::thread> executor_threads;
  auto work_guard = asio::make_work_guard(io_context_);
  for (int i = 0; i < num_threads; ++i)
  {
    executor_threads.emplace_back([this] { io_context_.run(); });
  }

  std::vector<std::thread> threads;
  std::barrier sync_point(num_threads);

  for (int i = 0; i < num_threads; ++i)
  {
    threads.emplace_back(
        [&]()
        {
          sync_point.arrive_and_wait();
          for (int j = 0; j < transfers_per_thread; ++j)
          {
            ProcedureManager* proc_mgr = db->get_procedure_manager();
            ProcedureParams params = {
                {"from_user", 101}, {"to_user", 202}, {"amount", 1}};

            ProcedureOptions options;
            options.max_retries = 5;

            // Each thread spawns work onto the shared io_context and waits.
            auto fut = asio::co_spawn(
                io_context_,
                [&]() -> asio::awaitable<void>
                {
                  co_await proc_mgr->async_execute_procedure("transfer_points",
                                                             params, options);
                },
                asio::use_future);
            try
            {
              fut.get();
            }
            catch (const TransactionAbortedException& e)
            {
              total_transfers.fetch_sub(1);
            }
          }
        });
  }

  for (auto& t : threads)
  {
    t.join();
  }

  // Shutdown the executor pool
  work_guard.reset();
  for (auto& t : executor_threads)
  {
    t.join();
  }
  io_context_.restart();

  // Verify final state
  auto verify_fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<std::pair<int32_t, int32_t>>
      {
        Table<>* table = db->get_table("user_points");
        TransactionID txn = db->begin_transaction();
        RID rid1, rid2;
        Row r1(db->get_table("user_points")->get_schema()),
            r2(db->get_table("user_points")->get_schema());
        table->get_rid_from_index(txn, 101, rid1);
        table->get_rid_from_index(txn, 202, rid2);
        table->get_row(txn, rid1, r1);
        table->get_row(txn, rid2, r2);
        co_await db->async_commit_transaction(txn);
        co_return std::make_pair(boost::get<int32_t>(r1.get_value("points")),
                                 boost::get<int32_t>(r2.get_value("points")));
      },
      asio::use_future);

  io_context_.run();
  auto balances = verify_fut.get();

  EXPECT_EQ(balances.first, 100 - total_transfers);
  EXPECT_EQ(balances.second, 50 + total_transfers);
}