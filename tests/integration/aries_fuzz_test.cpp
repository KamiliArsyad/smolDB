#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "backend/smoldb.h"

using namespace smoldb;

// Fuzz test that cycles through chaotic operations, crashes, recovery, and
// verification.
class AriesFuzzTest : public ::testing::Test
{
 protected:
  // Using a std::map gives us a stable key (page_id * C + slot) to represent
  // RID.
  using CanonicalState = std::map<uint64_t, std::optional<int32_t>>;
  static constexpr auto DELETED = std::nullopt;

  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "aries_fuzz_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});

    smoldb::DBConfig config{test_dir_};
    auto db = std::make_unique<SmolDB>(config);
    db->startup();
    db->create_table(1, table_name_, schema);

    TransactionID setup_txn = db->begin_transaction();
    Table<>* table = db->get_table(table_name_);
    for (int i = 0; i < num_rows_; ++i)
    {
      Row row(schema);
      row.set_value("id", 0);
      rids_.push_back(table->insert_row(setup_txn, row));
      canonical_state_[rid_to_key(rids_.back())] = 0;
    }
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  uint64_t rid_to_key(RID rid) { return rid.page_id * 10000 + rid.slot; }

  void chaos_phase()
  {
    smoldb::DBConfig config(test_dir_);
    auto db = std::make_unique<SmolDB>(config);
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
            std::discrete_distribution<> op_dist(
                {50, 25});  // 50% update, 25% delete
            std::bernoulli_distribution commit_dist(0.7);

            for (int j = 0; j < ops_per_thread; ++j)
            {
              RID rid = rids_[rid_dist(gen)];
              TransactionID txn = db->begin_transaction();
              try
              {
                bool committed = false;
                // Check if row is currently considered deleted in our model.
                // This isn't perfect under concurrency but good enough for
                // fuzzing.
                bool is_deleted;
                {
                  std::scoped_lock lock(mtx_);
                  is_deleted = (canonical_state_[rid_to_key(rid)] == DELETED);
                }

                int op = is_deleted
                             ? 1
                             : op_dist(gen);  // If deleted, try to re-insert.

                if (op == 0)
                {  // UPDATE
                  Row row(schema);
                  int32_t new_val = gen();
                  row.set_value("id", new_val);
                  if (table->update_row(txn, rid, row) && commit_dist(gen))
                  {
                    db->commit_transaction(txn);
                    std::scoped_lock lock(mtx_);
                    canonical_state_[rid_to_key(rid)] = new_val;
                    committed = true;
                  }
                }
                else
                {  // DELETE
                  if (table->delete_row(txn, rid) && commit_dist(gen))
                  {
                    db->commit_transaction(txn);
                    std::scoped_lock lock(mtx_);
                    canonical_state_[rid_to_key(rid)] = DELETED;
                    committed = true;
                  }
                }

                if (!committed) db->abort_transaction(txn);
              }
              catch (const std::exception& e)
              {
                db->abort_transaction(txn);
              }
            }
          });
    }

    for (auto& t : threads) t.join();
    db.reset();  // CRASH
  }

  void verification_phase()
  {
    smoldb::DBConfig config{test_dir_};
    auto db = std::make_unique<SmolDB>(config);
    db->startup();
    Table<>* table = db->get_table(table_name_);
    ASSERT_NE(table, nullptr);

    TransactionID scan_txn = db->begin_transaction();
    CanonicalState recovered_state;
    for (const auto& rid : rids_)
    {
      Row row;
      if (table->get_row(scan_txn, rid, row))
      {
        recovered_state[rid_to_key(rid)] =
            boost::get<int32_t>(row.get_value("id"));
      }
      else
      {
        recovered_state[rid_to_key(rid)] = DELETED;
      }
    }
    db->commit_transaction(scan_txn);

    ASSERT_EQ(canonical_state_.size(), recovered_state.size());
    for (const auto& [key, canonical_val] : canonical_state_)
    {
      ASSERT_TRUE(recovered_state.count(key));
      auto recovered_val = recovered_state.at(key);
      if (canonical_val.has_value())
      {
        ASSERT_TRUE(recovered_val.has_value());
        ASSERT_EQ(canonical_val.value(), recovered_val.value())
            << "Value mismatch for RID key " << key;
      }
      else
      {
        ASSERT_FALSE(recovered_val.has_value())
            << "Row for RID key " << key << " should be deleted.";
      }
    }
    db->shutdown();
  }

  const std::string table_name_ = "fuzz_table";
  const int num_rows_ = 8;
  const int num_threads_ = 4;
  std::vector<RID> rids_;

  // The "ground truth" model of what the database state should be.
  std::mutex mtx_;
  CanonicalState canonical_state_;

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
