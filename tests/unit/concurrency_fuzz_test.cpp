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
#include <barrier>

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
                if (result.has_value() && commit_dist(gen))
                {
                  std::scoped_lock lock(ground_truth_mutex_);
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

  // ASSUMES CALLER HOLD `ground_truth_mutex_`
  void update_ground_truth(RID rid, int32_t new_value)
  {
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
        std::scoped_lock lock(ground_truth_mutex_);
        int32_t observed_value = boost::get<int32_t>(row.get_value("id"));
        const auto& valid_states = ground_truth_.at(rid_to_read);
        if (valid_states.count(observed_value) == 0)
        {
          std::cerr << "ATOMICITY VIOLATION: Read value " << observed_value
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
          std::cerr << "ATOMICITY VIOLATION: Failed to read RID " << rid_to_read
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

/**
 * @class HeapFilePageRaceTest
 * @brief Test fixture for provoking read-write data races on a single page
 * within the HeapFile.
 *
 * This fixture creates a single HeapFile and pre-populates exactly one
 * page with rows. This provides a "battleground" page where reader and writer
 * threads will operate, maximizing contention to expose missing page-level
 * latching.
 */
class HeapFilePageRaceTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir =
        std::filesystem::temp_directory_path() / "heap_file_page_race_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    db = std::make_unique<SmolDB>(test_dir, 16);
    db->startup();
    lock_mgr = db->lock_manager_.get();
    txn_mgr = db->txn_manager_.get();

    PageID first_page_id = db->buffer_pool_->allocate_page();
    heap = std::make_unique<HeapFile>(db->buffer_pool_.get(),
                                      db->wal_mgr_.get(), first_page_id,
                                      /*max_tuple_size=*/32);

    TransactionID txn = txn_mgr->begin();
    size_t num_slots = heap->slots_per_page();
    ASSERT_GT(num_slots, 0);
    std::vector<std::byte> dummy_data(16, std::byte{0});
    for (size_t i = 0; i < num_slots; ++i)
    {
      battleground_rids_.push_back(
          heap->append(txn_mgr->get_transaction(txn), dummy_data));
    }
    ASSERT_EQ(heap->last_page_id(), first_page_id);
    txn_mgr->commit(txn);
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  LockManager* lock_mgr;
  TransactionManager* txn_mgr;
  std::unique_ptr<HeapFile> heap;
  std::vector<RID> battleground_rids_;
};

/**
 * @test HeapFilePageRaceTest.ProvokeReadWritePageRace
 * @brief Reliably triggers a read-vs-write data race if HeapFile methods are
 * not internally synchronized with a page latch.
 *
 * This test is specifically designed to fail under TSan if the page-level data
 * race bug is present. It works by having:
 * 1. A "Scanner" thread that continuously calls `full_scan()`, which reads the
 *    entire page's memory.
 * 2. Multiple "Mutator" threads that continuously `update()` different rows on
 *    that same page.
 *
 * The RID-level LockManager does NOT prevent this race, as the mutators lock
 * different RIDs and the scanner acquires no locks. Without a physical page
 * latch, TSan will detect the scanner's read is racing with the mutators'
 * writes on the same page memory.
 */
TEST_F(HeapFilePageRaceTest, ProvokeReadWritePageRace)
{
  const size_t num_mutator_threads =
      std::min((size_t)std::thread::hardware_concurrency() - 1,
               battleground_rids_.size());

  ASSERT_GT(num_mutator_threads, 0)
      << "Test requires at least 2 threads to find a race.";

  std::atomic<bool> stop_flag = false;
  std::vector<std::thread> threads;

  // --- The Scanner Thread ---
  threads.emplace_back(
      [&]()
      {
        std::vector<std::vector<std::byte>> scan_results;
        while (!stop_flag.load())
        {
          // This read of the page will race with the writes below
          // if there is no page latch.
          heap->full_scan(scan_results);
        }
      });

  // --- The Mutator Threads ---
  for (size_t i = 0; i < num_mutator_threads; ++i)
  {
    threads.emplace_back(
        [&, thread_id = i]()
        {
          std::vector<std::byte> thread_data(20, std::byte{uint8_t(thread_id)});

          while (!stop_flag.load())
          {
            TransactionID txn_id = txn_mgr->begin();
            Transaction* txn = txn_mgr->get_transaction(txn_id);
            RID rid = battleground_rids_[thread_id];

            lock_mgr->acquire_exclusive(txn, rid);
            heap->update(txn, rid,
                         thread_data);  // This write races with the scan
            txn_mgr->commit(txn_id);
          }
        });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  stop_flag.store(true);

  for (auto& t : threads)
  {
    t.join();
  }
}