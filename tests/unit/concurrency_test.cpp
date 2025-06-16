#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "backend/smoldb.h"

class ConcurrencyTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "concurrency_tests";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    db = std::make_unique<SmolDB>(test_dir, BUFFER_SIZE_FOR_TEST);
    db->startup();

    // Create a table with one row
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});
    db->create_table(1, "test_table", schema);

    TransactionID txn = db->begin_transaction();
    Table<>* table = db->get_table(1);
    Row row(schema);
    row.set_value("id", 100);
    initial_rid_ = table->insert_row(txn, row);
    db->commit_transaction(txn);
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  RID initial_rid_;
};

// Test that two transactions can acquire shared locks on the same resource
TEST_F(ConcurrencyTest, SharedLockCompatibility)
{
  Table<>* table = db->get_table(1);
  Row out_row1, out_row2;

  TransactionID txn1 = db->begin_transaction();
  TransactionID txn2 = db->begin_transaction();

  // Both should be able to get a shared lock without blocking
  ASSERT_TRUE(table->get_row(txn1, initial_rid_, out_row1));
  ASSERT_TRUE(table->get_row(txn2, initial_rid_, out_row2));

  EXPECT_EQ(boost::get<int32_t>(out_row1.get_value("id")), 100);
  EXPECT_EQ(boost::get<int32_t>(out_row2.get_value("id")), 100);

  db->commit_transaction(txn1);
  db->commit_transaction(txn2);
}

// Test that an exclusive lock blocks a shared lock request
// TEST_F(ConcurrencyTest, ExclusiveBlocksShared)
// {
//   Table<>* table = db->get_table(1);
//   Schema schema = table->get_schema();
//   Row new_row(schema);
//   new_row.set_value("id", 200);
//
//   TransactionID txn1 = db->begin_transaction();
//
//   // Txn1 acquires an exclusive lock by trying to "update" (via insert for now)
//   // We can't update yet, so we'll simulate the lock acquisition.
//   // The easiest way is to use a promise/future to sequence events.
//   auto* lm = db->lock_manager_.get();
//   auto* tm = db->txn_manager_.get();
//   lm->acquire_exclusive(tm->get_transaction(txn1), initial_rid_);
//
//   std::promise<void> x_lock_acquired;
//   std::future<bool> s_lock_result;
//
//   // Thread 2 will try to get a shared lock and should block
//   auto thread2 = std::thread(
//       [&]()
//       {
//         TransactionID txn2 = db->begin_transaction();
//         Row out_row;
//         x_lock_acquired.set_value();  // Signal that we are about to block
//         s_lock_result = std::async(std::launch::async, &Table<>::get_row, table,
//                                    txn2, initial_rid_, std::ref(out_row));
//         // We expect this to time out and return false
//         if (!s_lock_result.get())
//         {
//           db->abort_transaction(txn2);
//         }
//       });
//
//   x_lock_acquired.get_future().wait();
//   std::this_thread::sleep_for(
//       std::chrono::milliseconds(50));  // give it time to block
//
//   // Now release the exclusive lock
//   db->commit_transaction(txn1);
//
//   thread2.join();
// }