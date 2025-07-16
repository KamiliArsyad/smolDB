#include <gtest/gtest.h>

#include "backend/smoldb.h"

// Helper to inspect the WAL file for debugging purposes.
static void inspect_wal(const std::filesystem::path& wal_path)
{
  std::cout << "\n--- Inspecting WAL: " << wal_path.string() << " ---\n";
  std::ifstream in(wal_path, std::ios::binary);
  if (!in.is_open())
  {
    std::cout << "  [WAL file does not exist or cannot be opened]\n"
              << std::endl;
    return;
  }

  std::cout << "  LSN | PrevLSN | TxnID | Type\n";
  std::cout << "  --------------------------------\n";

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;

    std::string type_str;
    switch (hdr.type)
    {
      case BEGIN:
        type_str = "BEGIN";
        break;
      case UPDATE:
        type_str = "UPDATE";
        break;
      case COMMIT:
        type_str = "COMMIT";
        break;
      case ABORT:
        type_str = "ABORT";
        break;
      case CLR:
        type_str = "CLR";
        break;
      default:
        type_str = "???";
        break;
    }

    std::cout << "  " << std::left << std::setw(4) << hdr.lsn << "| "
              << std::setw(8) << hdr.prev_lsn << "| " << std::setw(6)
              << hdr.txn_id << "| " << type_str << std::endl;

    if (hdr.lr_length > sizeof(LogRecordHeader))
    {
      in.seekg(hdr.lr_length - sizeof(LogRecordHeader), std::ios::cur);
    }
  }
  std::cout << "--- End of WAL Inspection ---\n" << std::endl;
}

class AriesTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "aries_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  void crash(std::unique_ptr<SmolDB>& db) { db.reset(); }

  std::unique_ptr<SmolDB> restart(size_t buffer_pool_size = 16)
  {
    smoldb::DBConfig config{test_dir_, buffer_pool_size};
    auto db = std::make_unique<SmolDB>(config);
    db->startup();
    return db;
  }

  Schema make_simple_schema()
  {
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});
    return schema;
  }

  std::filesystem::path test_dir_;
};

TEST_F(AriesTest, CrashAfterCommit)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup a durable state (create table, clean shutdown)
  {
    smoldb::DBConfig config{test_dir_};
    auto db = std::make_unique<SmolDB>(config);
    db->startup();
    db->create_table(1, "users", schema);
    db->shutdown();
  }

  // Phase 2: Perform an action and crash
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID txn = db->begin_transaction();
    Row row(schema);
    row.set_value("id", 123);
    rid = table->insert_row(txn, row);
    db->commit_transaction(txn);
    crash(db);
  }

  // Phase 3: Restart and verify recovery
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 123);
    db->commit_transaction(txn);
  }
}

TEST_F(AriesTest, CrashWithInFlightLoserTransaction)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup initial state
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row initial_row(schema);
    initial_row.set_value("id", 100);
    rid = table->insert_row(setup_txn, initial_row);
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  // Phase 2: Start a loser transaction and crash
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID loser_txn = db->begin_transaction();
    Row new_row(schema);
    new_row.set_value("id", 999);
    table->update_row(loser_txn, rid, new_row);
    crash(db);
  }

  // Phase 3: Restart and verify rollback
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 100);
    db->commit_transaction(txn);
  }
}

#ifndef NDEBUG
TEST_F(AriesTest, CrashDuringUndoRecoversCorrectly)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup initial state
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row initial_row(schema);
    initial_row.set_value("id", 200);
    rid = table->insert_row(setup_txn, initial_row);
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  // Phase 2: Create a WAL with a loser transaction
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID loser_txn = db->begin_transaction();
    Row new_row(schema);
    new_row.set_value("id", 888);
    table->update_row(loser_txn, rid, new_row);
    crash(db);  // WAL now contains an uncommitted UPDATE
  }

  // --- DEBUGGING STEP ---
  // Let's inspect the state of the WAL file that the next recovery attempt will
  // see.
  /*
  std::cout << "\n>>> WAL state before the final recovery attempt <<<\n";
  inspect_wal(test_dir_ / "smoldb.wal");
  */
  // --- END DEBUGGING STEP ---

  // Phase 3: Restart but inject a crash during the undo phase.
  {
    smoldb::DBConfig config(test_dir_);
    auto db = std::make_unique<SmolDB>(config);
    // The test fails here because it doesn't throw.
    ASSERT_THROW(db->startup_with_crash_point(RecoveryCrashPoint::DURING_UNDO),
                 std::runtime_error);
    crash(db);
  }

  // Phase 4: Restart AGAIN. Recovery must resume and complete successfully.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 200);
    db->commit_transaction(txn);
  }
}
#endif

TEST_F(AriesTest, RedoOnlyScenario)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup and shutdown cleanly to persist the table and an initial
  // row.
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row initial_row(schema);
    initial_row.set_value("id", 100);
    rid = table->insert_row(setup_txn, initial_row);
    db->commit_transaction(setup_txn);
    // This shutdown flushes the page with value 100 to disk.
    db->shutdown();
  }

  // Phase 2: Update the value, commit (writing to WAL), but crash before
  // flushing the buffer pool.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn = db->begin_transaction();
    Row new_row(schema);
    new_row.set_value("id", 200);
    table->update_row(txn, rid, new_row);
    db->commit_transaction(txn);
    // The page with value 200 is dirty in the buffer pool, but not on disk.
    crash(db);
  }

  // Phase 3: Restart. ARIES must redo the change.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    // Verify that the REDO phase successfully applied the logged change.
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 200);
    db->commit_transaction(txn);
  }
}

TEST_F(AriesTest, CrashAfterRuntimeAbort)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup initial state.
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row initial_row(schema);
    initial_row.set_value("id", 500);
    rid = table->insert_row(setup_txn, initial_row);
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  // Phase 2: Start a transaction, update a value, and then explicitly ABORT it.
  // Then, crash before the final ABORT record is guaranteed to be flushed.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn_to_abort = db->begin_transaction();
    Row new_row(schema);
    new_row.set_value("id", 555);
    table->update_row(txn_to_abort, rid, new_row);
    // The abort writes CLRs and an ABORT record to the WAL.
    db->abort_transaction(txn_to_abort);
    crash(db);
  }

  // Phase 3: Restart. Recovery should see the CLRs and the ABORT record (or
  // just the CLRs) and ensure the transaction is correctly rolled back.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    // Verify the state is the original, pre-abort state.
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 500);
    db->commit_transaction(txn);
  }
}

TEST_F(AriesTest, MultipleLoserTransactions)
{
  RID rid1, rid2;
  Schema schema = make_simple_schema();

  // Phase 1: Setup initial state with two rows.
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row row1(schema), row2(schema);
    row1.set_value("id", 1000);
    row2.set_value("id", 2000);
    rid1 = table->insert_row(setup_txn, row1);
    rid2 = table->insert_row(setup_txn, row2);
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  // Phase 2: Start two separate transactions, have each update a row, and then
  // crash.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");

    TransactionID loser1 = db->begin_transaction();
    TransactionID loser2 = db->begin_transaction();

    Row new_row1(schema), new_row2(schema);
    new_row1.set_value("id", 1111);
    new_row2.set_value("id", 2222);

    table->update_row(loser1, rid1, new_row1);
    table->update_row(loser2, rid2, new_row2);

    crash(db);
  }

  // Phase 3: Restart. ARIES must find and roll back both loser transactions.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn = db->begin_transaction();
    Row out_row1, out_row2;

    ASSERT_TRUE(table->get_row(txn, rid1, out_row1));
    EXPECT_EQ(boost::get<int32_t>(out_row1.get_value("id")), 1000);

    ASSERT_TRUE(table->get_row(txn, rid2, out_row2));
    EXPECT_EQ(boost::get<int32_t>(out_row2.get_value("id")), 2000);

    db->commit_transaction(txn);
  }
}

TEST_F(AriesTest, EmptyTransactionDoesNotAffectState)
{
  // Phase 1: Setup a known state and shut down.
  {
    auto db = restart();
    db->create_table(1, "users", make_simple_schema());
    db->shutdown();
  }

  // Phase 2: Start a transaction but do nothing, then crash.
  {
    auto db = restart();
    // This transaction will be in the ATT but have no update records.
    TransactionID loser_txn = db->begin_transaction();
    crash(db);
  }

  // Phase 3: Restart. Recovery should gracefully handle and abort the empty
  // txn.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    ASSERT_NE(table, nullptr);
    // Just verifying that the system is in a valid state is enough.
    EXPECT_EQ(table->scan_all().size(), 0);
    db->shutdown();
  }
}

TEST_F(AriesTest, UpdateSameRowMultipleTimesAndAbort)
{
  RID rid;
  Schema schema = make_simple_schema();

  // Phase 1: Setup initial state.
  {
    auto db = restart();
    db->create_table(1, "users", schema);
    Table<>* table = db->get_table("users");
    TransactionID setup_txn = db->begin_transaction();
    Row row(schema);
    row.set_value("id", 100);
    rid = table->insert_row(setup_txn, row);
    db->commit_transaction(setup_txn);
    db->shutdown();
  }

  // Phase 2: One transaction updates the same row twice, then crashes.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID loser_txn = db->begin_transaction();
    Row row_v2(schema), row_v3(schema);
    row_v2.set_value("id", 200);
    row_v3.set_value("id", 300);

    // Creates an LSN chain: UPDATE(to 200) -> UPDATE(to 300)
    table->update_row(loser_txn, rid, row_v2);
    table->update_row(loser_txn, rid, row_v3);

    crash(db);
  }

  // Phase 3: Restart. UNDO must follow the chain all the way back.
  {
    auto db = restart();
    Table<>* table = db->get_table("users");
    TransactionID txn = db->begin_transaction();
    Row out_row;
    ASSERT_TRUE(table->get_row(txn, rid, out_row));
    // Verify we are back at the original state, not the intermediate one.
    EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 100);
    db->commit_transaction(txn);
  }
}