#include <gtest/gtest.h>

#include "backend/smoldb.h"

// A test fixture that is parameterized by the crash point.
class AriesCrashRecoveryTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<RecoveryCrashPoint>
{
 protected:
  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "aries_crash_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    // Phase 1: Create a durable DB state with one row.
    {
      smoldb::DBConfig config{test_dir_};
      auto db = std::make_unique<SmolDB>(config);
      db->startup();
      db->create_table(1, "users", make_simple_schema());
      Table<> *table = db->get_table("users");
      TransactionID setup_txn = db->begin_transaction();
      Row row(make_simple_schema());
      row.set_value("id", 100);
      rid_ = table->insert_row(setup_txn, row);
      db->commit_transaction(setup_txn);
      db->shutdown();
    }
    // Phase 2: Create a WAL file containing a loser transaction that updated
    // the row.
    {
      smoldb::DBConfig config(test_dir_);
      auto db = std::make_unique<SmolDB>(config);
      db->startup();
      Table<>* table = db->get_table("users");
      TransactionID loser_txn = db->begin_transaction();
      Row row(make_simple_schema());
      row.set_value("id", 999);
      table->update_row(loser_txn, rid_, row);
      db.reset();  // Crash
    }
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  Schema make_simple_schema()
  {
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});
    return schema;
  }

  std::filesystem::path test_dir_;
  RID rid_;
};

TEST_P(AriesCrashRecoveryTest, RecoversCorrectlyAfterCrashingDuringRecovery)
{
  RecoveryCrashPoint crash_point = GetParam();

  // Phase 3: Attempt recovery, but inject a crash at the specified point.
  // This is expected to throw. The system is left in a "half-recovered" state.
  {
    smoldb::DBConfig config(test_dir_);
    auto db = std::make_unique<SmolDB>(config);
    ASSERT_THROW(db->startup_with_crash_point(crash_point), std::runtime_error);
  }

  // Phase 4: Restart AGAIN. This time, recovery must run to completion.
  std::unique_ptr<SmolDB> db;
  ASSERT_NO_THROW({
    smoldb::DBConfig config(test_dir_);
    db = std::make_unique<SmolDB>(config);
    db->startup();
  });

  // Phase 5: Verify the final state is correct. The loser must be rolled back.
  Table<>* table = db->get_table("users");
  ASSERT_NE(table, nullptr);
  TransactionID verify_txn = db->begin_transaction();
  Row row;
  ASSERT_TRUE(table->get_row(verify_txn, rid_, row));
  EXPECT_EQ(boost::get<int32_t>(row.get_value("id")), 100);
  db->commit_transaction(verify_txn);
}

// Instantiate the test suite for all relevant crash points.
INSTANTIATE_TEST_SUITE_P(AriesRecoveryCrashPoints, AriesCrashRecoveryTest,
                         ::testing::Values(RecoveryCrashPoint::AFTER_ANALYSIS,
                                           RecoveryCrashPoint::DURING_REDO,
                                           RecoveryCrashPoint::DURING_UNDO));