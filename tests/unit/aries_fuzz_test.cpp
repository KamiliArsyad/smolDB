
#include <gtest/gtest.h>

#include <map>
#include <random>
#include <thread>
#include <vector>

#include "backend/smoldb.h"

// Fuzz test that cycles through chaotic operations, crashes, recovery, and
// verification.
class AriesFuzzTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "aries_fuzz_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    // Initial setup: create the DB and a table with some rows.
    // This happens once before all the fuzzing cycles.
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});

    auto db = std::make_unique<SmolDB>(test_dir_);
    db->startup();
    db->create_table(1, table_name_, schema);

    TransactionID setup_txn = db->begin_transaction();
    Table<>* table = db->get_table(table_name_);
    for (int i = 0; i < num_rows_; ++i)
    {
      Row row(schema);
      row.set_value("id", 0);  // All rows start at value 0
      rids_.push_back(table->insert_row(setup_txn, row));
      // Initialize our canonical model of the DB state.
      canonical_state_[rids_.back().page_id * 1000 + rids_.back().slot] = 0;
    }
    db->commit_transaction(setup_txn);
    db->shutdown();  // Clean shutdown ensures this initial state is durable.
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  // Runs one cycle of chaotic concurrent operations.
  void chaos_phase()
  {
    auto db = std::make_unique<SmolDB>(test_dir_);
    db->startup();
    Table<>* table = db->get_table(table_name_);
    Schema schema = table->get_schema();

    std::vector<std::thread> threads;
    const int ops_per_thread = 20;

    for (int i = 0; i < num_threads_; ++i)
    {
      threads.emplace_back(
          [&, i]()
          {
            std::mt19937 gen(std::random_device{}() + i);
            std::uniform_int_distribution<> rid_dist(0, num_rows_ - 1);
            std::bernoulli_distribution commit_dist(
                0.7);  // 70% chance to commit

            for (int j = 0; j < ops_per_thread; ++j)
            {
              RID rid = rids_[rid_dist(gen)];
              TransactionID txn = db->begin_transaction();

              try
              {
                Row row(schema);
                int32_t new_val = gen();
                row.set_value("id", new_val);
                table->update_row(txn, rid, row);

                if (commit_dist(gen))
                {
                  db->commit_transaction(txn);
                  // Update our canonical model only on successful commit.
                  std::scoped_lock lock(mtx_);
                  canonical_state_[rid.page_id * 1000 + rid.slot] = new_val;
                }
                else
                {
                  db->abort_transaction(txn);
                }
              }
              catch (const std::exception& e)
              {
                // This can happen due to deadlocks, which is fine. Just abort.
                db->abort_transaction(txn);
              }
            }
          });
    }

    for (auto& t : threads) t.join();

    // Crash the DB without a clean shutdown. Some txns are committed, some
    // aborted. In-flight transactions from the chaos phase are now "losers".
    db.reset();
  }

  // Recovers the DB and verifies its state against our canonical model.
  void verification_phase()
  {
    auto db = std::make_unique<SmolDB>(test_dir_);
    db->startup();  // Trigger ARIES recovery.
    Table<>* table = db->get_table(table_name_);
    ASSERT_NE(table, nullptr);

    // Scan all rows from the recovered database.
    TransactionID scan_txn = db->begin_transaction();
    std::map<uint64_t, int32_t> recovered_state;
    for (const auto& rid : rids_)
    {
      Row row;
      ASSERT_TRUE(table->get_row(scan_txn, rid, row));
      recovered_state[rid.page_id * 1000 + rid.slot] =
          boost::get<int32_t>(row.get_value("id"));
    }
    db->commit_transaction(scan_txn);

    // The recovered state MUST EXACTLY match our canonical model of committed
    // data.
    ASSERT_EQ(canonical_state_.size(), recovered_state.size());
    for (const auto& [key, val] : canonical_state_)
    {
      ASSERT_TRUE(recovered_state.count(key));
      ASSERT_EQ(val, recovered_state.at(key))
          << "Value mismatch for RID key " << key;
    }

    db->shutdown();
  }

  const std::string table_name_ = "fuzz_table";
  const int num_rows_ = 8;
  const int num_threads_ = 4;
  std::vector<RID> rids_;

  // The "ground truth" model of what the database state should be.
  std::mutex mtx_;
  std::map<uint64_t, int32_t> canonical_state_;

  std::filesystem::path test_dir_;
};

TEST_F(AriesFuzzTest, RecoverAfterChaos)
{
  const int cycles = 10;
  for (int i = 0; i < cycles; ++i)
  {
    std::cout << "--- Starting Fuzz Cycle " << i + 1 << "/" << cycles << " ---"
              << std::endl;
    chaos_phase();
    ASSERT_NO_THROW(verification_phase());
  }
}