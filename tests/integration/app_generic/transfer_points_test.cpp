#include <gtest/gtest.h>

#include <barrier>
#include <boost/asio/io_context.hpp>
#include <thread>

#include "backend/proc/proc.h"
#include "backend/proc/proc_ctx.h"
#include "backend/smoldb.h"
#include "lock_excepts.h"

using namespace smoldb;

// The user-defined procedure, implemented here in the test file.
class TransferPointsProcedure : public TransactionProcedure
{
 public:
  std::string get_name() const override { return "transfer_points"; }

  ProcedureStatus execute(TransactionContext& ctx,
                          const ProcedureParams& params, ProcedureResult& result) override
  {
    // 1. Parameter Validation
    if (!params.count("from_user") || !params.count("to_user") ||
        !params.count("amount"))
    {
      return ProcedureStatus::ABORT;
    }
    const auto from_id = boost::get<int32_t>(params.at("from_user"));
    const auto to_id = boost::get<int32_t>(params.at("to_user"));
    const auto amount = boost::get<int32_t>(params.at("amount"));

    if (amount <= 0) return ProcedureStatus::ABORT;

    // 2. Data Access
    Table<>* table = ctx.get_table("user_points");
    Schema schema = table->get_schema();
    RID from_rid, to_rid;
    Row from_row(schema), to_row(schema);

    if (!table->get_rid_from_index(ctx.get_txn_id(), from_id, from_rid) ||
        !table->get_rid_from_index(ctx.get_txn_id(), to_id, to_rid))
    {
      return ProcedureStatus::ABORT;  // User not found
    }
    if (!table->get_row(ctx.get_txn_id(), from_rid, from_row) ||
        !table->get_row(ctx.get_txn_id(), to_rid, to_row))
    {
      return ProcedureStatus::ABORT;  // Should not happen if index is correct
    }

    // 3. Business Rule Enforcement
    int32_t from_balance = boost::get<int32_t>(from_row.get_value("points"));
    if (from_balance < amount)
    {
      result["message"] = "Insufficient points";
      return ProcedureStatus::ABORT;
    }

    // 4. State Modification
    int32_t to_balance = boost::get<int32_t>(to_row.get_value("points"));
    from_row.set_value("points", from_balance - amount);
    to_row.set_value("points", to_balance + amount);
    table->update_row(ctx.get_txn_id(), from_rid, from_row);
    table->update_row(ctx.get_txn_id(), to_rid, to_row);

    // 5. Return Success
    result["message"] = "Transfer successful";
    return ProcedureStatus::SUCCESS;
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

    TransactionID txn = db->begin_transaction();
    Table<>* table = db->get_table("user_points");
    Row r1(schema), r2(schema);
    r1.set_value("user_id", 101);
    r1.set_value("points", 100);
    r2.set_value("user_id", 202);
    r2.set_value("points", 50);
    table->insert_row(txn, r1);
    table->insert_row(txn, r2);
    db->commit_transaction(txn);

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
  boost::asio::io_context io_context_;
};

TEST_F(TransferPointsTest, SuccessfulTransfer)
{
  ProcedureManager* proc_mgr = db->get_procedure_manager();
  ProcedureParams params = {
      {"from_user", 101}, {"to_user", 202}, {"amount", 10}};
  proc_mgr->execute_procedure("transfer_points", params);

  // Verify
  Table<>* table = db->get_table("user_points");
  TransactionID txn = db->begin_transaction();
  RID rid1, rid2;
  Row r1, r2;
  ASSERT_TRUE(table->get_rid_from_index(txn, 101, rid1));
  ASSERT_TRUE(table->get_rid_from_index(txn, 202, rid2));
  table->get_row(txn, rid1, r1);
  table->get_row(txn, rid2, r2);
  db->commit_transaction(txn);

  EXPECT_EQ(boost::get<int32_t>(r1.get_value("points")), 90);
  EXPECT_EQ(boost::get<int32_t>(r2.get_value("points")), 60);
}

TEST_F(TransferPointsTest, InsufficientPointsRollback)
{
  ProcedureManager* proc_mgr = db->get_procedure_manager();
  ProcedureParams params = {
      {"from_user", 101}, {"to_user", 202}, {"amount", 200}};
  proc_mgr->execute_procedure("transfer_points", params);

  // Verify state is unchanged
  Table<>* table = db->get_table("user_points");
  TransactionID txn = db->begin_transaction();
  RID rid1, rid2;
  Row r1, r2;
  table->get_rid_from_index(txn, 101, rid1);
  table->get_rid_from_index(txn, 202, rid2);
  table->get_row(txn, rid1, r1);
  table->get_row(txn, rid2, r2);
  db->commit_transaction(txn);

  EXPECT_EQ(boost::get<int32_t>(r1.get_value("points")), 100);
  EXPECT_EQ(boost::get<int32_t>(r2.get_value("points")), 50);
}

TEST_F(TransferPointsTest, ConcurrentTransfersAreAtomic)
{
  const int num_threads = 10;
  const int transfers_per_thread = 5;
  std::atomic<int> total_transfers = num_threads * transfers_per_thread;

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

            try
            {
              proc_mgr->execute_procedure("transfer_points", params, options);
            }
            catch (const TransactionAbortedException &e)
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

  // Verify final state
  Table<>* table = db->get_table("user_points");
  TransactionID txn = db->begin_transaction();
  RID rid1, rid2;
  Row r1, r2;
  table->get_rid_from_index(txn, 101, rid1);
  table->get_rid_from_index(txn, 202, rid2);
  table->get_row(txn, rid1, r1);
  table->get_row(txn, rid2, r2);
  db->commit_transaction(txn);

  EXPECT_EQ(boost::get<int32_t>(r1.get_value("points")), 100 - total_transfers);
  EXPECT_EQ(boost::get<int32_t>(r2.get_value("points")), 50 + total_transfers);
}