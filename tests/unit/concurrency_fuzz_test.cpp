// ===== ../smolDB/tests/unit/concurrency_fuzz_test.cpp =====

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <thread>
#include <vector>

#define private public
#include "backend/smoldb.h"
#undef private

/**
 * @class DMLAtomicityHarness
 * @brief A framework for fuzz-testing the atomicity of DML operations.
 *
 * This harness verifies that uncommitted data from aborted transactions is
never
 * visible to other concurrent transactions. It does this by orchestrating
or two
 * types of threads:
 * - Mutators: Attempt to change data (e.g., update, delete) and then randomly
 * commit or abort.
 * - Readers: Continuously read data and verify that every state they see
 * corresponds to a state that was explicitly committed.
 *
 * The specific DML action is provided as a lambda, making the harness reusable
 * for testing UPDATE, DELETE, or more complex operations.
 */
class DMLAtomicityHarness
{
 public:
  // A special value in our ground truth set to signify the row is deleted.
  static constexpr int32_t DELETED_SENTINEL = -1;

  DMLAtomicityHarness(SmolDB& db, std::string table_name, std::vector<RID> rids)
      : db_(db), table_name_(std::move(table_name)), rids_(std::move(rids))
  {
    initialize_ground_truth();
  }

  // The core mutation logic provided by the user.
  // It should return the new integer value on a successful update, or
  // DELETED_SENTINEL on a successful delete.
  using MutatorFn =
      std::function<std::optional<int32_t>(TransactionID, RID, int32_t)>;

  void run(size_t num_mutators, size_t num_readers,
           std::chrono::milliseconds duration, const MutatorFn& mutator)
  {
    std::atomic<bool> stop_flag = false;
    std::vector<std::thread> threads;
    std::atomic<int32_t> error_count = 0;

    // --- Spawn Reader Threads ---
    for (size_t i = 0; i < num_readers; ++i)
    {
      threads.emplace_back(
          [&]()
          {
            while (!stop_flag.load())
            {
              if (!verify_random_row_state())
              {
                error_count++;
                stop_flag.store(true);  // Stop all threads on first error
                return;
              }
            }
          });
    }

    // --- Spawn Mutator Threads ---
    for (size_t i = 0; i < num_mutators; ++i)
    {
      threads.emplace_back(
          [&]()
          {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> rid_dist(0, rids_.size() - 1);
            std::bernoulli_distribution commit_dist(0.5);  // 50/50 commit/abort

            while (!stop_flag.load())
            {
              RID rid = rids_[rid_dist(gen)];
              TransactionID txn_id = db_.begin_transaction();
              int32_t new_value = gen();  // A new random value for the update

              try
              {
                auto result = mutator(txn_id, rid, new_value);
                if (result.has_value())
                {
                  if (commit_dist(gen))
                  {
                    // First, update our test's ground truth model. At this
                    // point, the change is NOT visible to other transactions
                    // because we still hold the exclusive lock.
                    update_ground_truth(rid, result.value());

                    // NOW, commit the transaction. This releases the lock and
                    // makes the change visible, but our ground truth is already
                    // up-to-date.
                    db_.commit_transaction(txn_id);
                  }
                  else
                  {
                    db_.abort_transaction(txn_id);
                  }
                }
                else
                {
                  // The mutator might fail (e.g. trying to update a deleted
                  // row) which is ok. Just abort the txn.
                  db_.abort_transaction(txn_id);
                }
              }
              catch (const std::exception& e)
              {
                // Catch transaction errors (e.g. lock timeouts)
                // and just abort.
                db_.abort_transaction(txn_id);
              }
            }
          });
    }

    std::this_thread::sleep_for(duration);
    stop_flag.store(true);

    for (auto& t : threads)
    {
      t.join();
    }

    ASSERT_EQ(error_count.load(), 0)
        << "Atomicity violation detected! A reader saw uncommitted data.";
  }

 private:
  SmolDB& db_;
  std::string table_name_;
  std::vector<RID> rids_;
  std::mutex ground_truth_mutex_;
  // Maps an RID to a set of all its committed integer values.
  // If the set contains DELETED_SENTINEL, the row is considered deleted.
  std::unordered_map<RID, std::set<int32_t>, std::hash<RID>> ground_truth_;

  void initialize_ground_truth()
  {
    Table<>* table = db_.get_table(table_name_);
    TransactionID txn_id = db_.begin_transaction();
    for (const auto& rid : rids_)
    {
      Row row;
      ASSERT_TRUE(table->get_row(txn_id, rid, row));
      int32_t initial_value = boost::get<int32_t>(row.get_value("id"));
      ground_truth_[rid] = {initial_value};
    }
    db_.commit_transaction(txn_id);
  }

  void update_ground_truth(RID rid, int32_t new_value)
  {
    std::scoped_lock lock(ground_truth_mutex_);
    if (new_value == DELETED_SENTINEL)
    {
      ground_truth_[rid] = {DELETED_SENTINEL};  // Mark as deleted
    }
    else
    {
      ground_truth_[rid].insert(new_value);
    }
  }

  bool verify_random_row_state()
  {
    Table<>* table = db_.get_table(table_name_);
    Schema schema = table->get_schema();
    Row row(schema);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rid_dist(0, rids_.size() - 1);
    RID rid_to_read = rids_[rid_dist(gen)];

    TransactionID txn_id = db_.begin_transaction();
    bool success = false;
    try
    {
      if (table->get_row(txn_id, rid_to_read, row))
      {
        int32_t observed_value = boost::get<int32_t>(row.get_value("id"));
        std::scoped_lock lock(ground_truth_mutex_);
        const auto& valid_states = ground_truth_.at(rid_to_read);
        if (valid_states.count(observed_value) == 0)
        {
          std::cerr << "ATOMICTY VIOLATION: Read value " << observed_value
                    << " for RID " << rid_to_read
                    << " which is not in the committed set." << std::endl;
          success = false;
        }
        else
        {
          success = true;
        }
      }
      else  // Row was not found (deleted)
      {
        std::scoped_lock lock(ground_truth_mutex_);
        const auto& valid_states = ground_truth_.at(rid_to_read);
        if (valid_states.count(DELETED_SENTINEL) == 0)
        {
          std::cerr << "ATOMICTY VIOLATION: Failed to read RID " << rid_to_read
                    << " which should exist." << std::endl;
          success = false;
        }
        else
        {
          success = true;
        }
      }
      db_.commit_transaction(txn_id);
    }
    catch (const std::exception&)
    {
      db_.abort_transaction(txn_id);
      success = true;  // A-ok if reader txn fails, just try again
    }
    return success;
  }
};

class AtomicityFuzzTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "atomicity_fuzz_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    // Use a small buffer pool to increase pressure
    db = std::make_unique<SmolDB>(test_dir, 16);
    db->startup();

    // Create a table with a few rows to work on
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});
    db->create_table(1, table_name, schema);

    Table<>* table = db->get_table(table_name);
    TransactionID txn = db->begin_transaction();
    for (int i = 0; i < num_rows_to_test; ++i)
    {
      Row row(schema);
      row.set_value("id", i * 1000);  // Initial values
      rids.push_back(table->insert_row(txn, row));
    }
    db->commit_transaction(txn);
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  const std::string table_name = "fuzz_table";
  const int num_rows_to_test = 4;
  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  std::vector<RID> rids;
};

/**
 * @test AtomicityFuzzTest.DMLAtomicityOnAbort
 * @brief Verifies that aborted UPDATE and DELETE ops are not seen by readers.
 *
 * This test uses the DMLAtomicityHarness to create a high-contention scenario.
 * Mutator threads will randomly pick a row and then randomly choose to either
 * UPDATE its integer value or DELETE it entirely. They then randomly commit or
 * abort. Reader threads concurrently read rows and validate their state against
 * a model of committed data, ensuring no "dirty reads" occur.
 */
TEST_F(AtomicityFuzzTest, DMLAtomicityOnAbort)
{
  DMLAtomicityHarness harness(*db, table_name, rids);

  // Define a mutator that randomly updates or deletes a row
  auto update_or_delete_mutator = [&](TransactionID txn_id, RID rid,
                                      int32_t new_val) -> std::optional<int32_t>
  {
    Table<>* table = db->get_table(table_name);
    Schema schema = table->get_schema();
    std::bernoulli_distribution action_dist(0.75);  // 75% chance to update
    std::random_device rd;
    std::mt19937 gen(rd());

    if (action_dist(gen))
    {
      // Perform an UPDATE
      Row new_row(schema);
      new_row.set_value("id", new_val);
      if (table->update_row(txn_id, rid, new_row))
      {
        return new_val;
      }
    }
    else
    {
      // Perform a DELETE
      if (table->delete_row(txn_id, rid))
      {
        return DMLAtomicityHarness::DELETED_SENTINEL;
      }
    }
    return std::nullopt;  // Operation failed (e.g., row already deleted)
  };

  const size_t num_threads = std::thread::hardware_concurrency();
  harness.run(/*num_mutators=*/num_threads, /*num_readers=*/num_threads * 2,
              /*duration=*/std::chrono::milliseconds(500),
              update_or_delete_mutator);
}

/**
 * @test ConcurrencyFuzzTest.HammerTheBufferPool
 * @brief A non-deterministic stress test for the BufferPool.
 *
 * This test's primary purpose is to create a high-contention environment to
 * reveal subtle race conditions, livelocks, or deadlocks in the BufferPool's
 * sharded implementation. It is best run under thread-sanitizer (TSan) or
 * on a multi-core machine for extended periods.
 *
 * It works by spawning a number of threads equal to x * hardware concurrency
 * and having each thread repeatedly and randomly request pages from a fixed
 * set. A small buffer pool size is intentionally chosen to force frequent
 * evictions, maximizing pressure on the eviction and page-loading logic.
 */
TEST(ConcurrencyFuzzTest, HammerTheBufferPool)
{
  const size_t num_threads = 3 * std::thread::hardware_concurrency();
  const int num_pages_to_hammer = 1000;
  const std::chrono::seconds test_duration = std::chrono::seconds(3);

  auto test_dir = std::filesystem::temp_directory_path() / "fuzz_test";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  SmolDB db(test_dir, 32);
  db.startup();

  std::atomic<bool> stop_flag = false;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < num_threads; ++i)
  {
    threads.emplace_back(
        [&]()
        {
          std::random_device rd;
          std::mt19937 gen(rd());
          std::uniform_int_distribution<> distrib(0, num_pages_to_hammer - 1);

          while (!stop_flag.load())
          {
            PageID pid = distrib(gen);
            ASSERT_NO_THROW({
              auto guard = db.buffer_pool_->fetch_page(pid);
              guard.mark_dirty();
            });
          }
        });
  }

  std::this_thread::sleep_for(test_duration);
  stop_flag.store(true);

  for (auto& t : threads)
  {
    t.join();
  }

  ASSERT_NO_THROW(db.shutdown());

  std::filesystem::remove_all(test_dir);
}