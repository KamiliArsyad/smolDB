// ===== ../smolDB/tests/unit/transaction_test.cpp =====

#include <gtest/gtest.h>

#include "backend/smoldb.h"

class TransactionTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "transaction_tests";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config(test_dir, BUFFER_SIZE_FOR_TEST);
    db = std::make_unique<SmolDB>(config);
    db->startup();

    // Create a simple table for testing
    Schema schema;
    schema.push_back({0, "id", Col_type::INT, false, {}});
    db->create_table(1, "test_table", schema);
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
};

TEST_F(TransactionTest, CommitMakesChangesVisible)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 123);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // Verify in a new transaction
  TransactionID txn2 = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn2, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 123);
  db->commit_transaction(txn2);
}

TEST_F(TransactionTest, AbortRevertsChanges)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 456);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, row);
  db->abort_transaction(txn1);

  // Verify in a new transaction that the row does not exist
  TransactionID txn2 = db->begin_transaction();
  Row out_row;
  ASSERT_FALSE(table->get_row(txn2, rid, out_row));
  db->commit_transaction(txn2);
}

TEST_F(TransactionTest, AbortAfterCommitIsNoOp)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 789);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // This should not throw and should have no effect
  ASSERT_NO_THROW(db->abort_transaction(txn1));

  // Verify the row is still there
  TransactionID txn2 = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn2, rid, out_row));
  db->commit_transaction(txn2);
}

TEST_F(TransactionTest, UpdateAndCommit)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row initial_row(schema);
  initial_row.set_value("id", 1000);

  // Insert initial row
  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, initial_row);
  db->commit_transaction(txn1);

  // Update the row
  TransactionID txn2 = db->begin_transaction();
  Row updated_row(schema);
  updated_row.set_value("id", 2000);
  ASSERT_TRUE(table->update_row(txn2, rid, updated_row));
  db->commit_transaction(txn2);

  // Verify the update
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn3, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 2000);
  db->commit_transaction(txn3);
}

TEST_F(TransactionTest, UpdateAndAbort)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row initial_row(schema);
  initial_row.set_value("id", 3000);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, initial_row);
  db->commit_transaction(txn1);

  // Attempt to update, then abort
  TransactionID txn2 = db->begin_transaction();
  Row updated_row(schema);
  updated_row.set_value("id", 4000);
  ASSERT_TRUE(table->update_row(txn2, rid, updated_row));
  db->abort_transaction(txn2);

  // Verify the update was rolled back
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn3, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 3000);
  db->commit_transaction(txn3);
}

TEST_F(TransactionTest, DeleteAndCommit)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 5000);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // Delete the row
  TransactionID txn2 = db->begin_transaction();
  ASSERT_TRUE(table->delete_row(txn2, rid));
  db->commit_transaction(txn2);

  // Verify it's gone
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  ASSERT_FALSE(table->get_row(txn3, rid, out_row));
  db->commit_transaction(txn3);
}

TEST_F(TransactionTest, DeleteAndAbort)
{
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();
  Row row(schema);
  row.set_value("id", 6000);

  TransactionID txn1 = db->begin_transaction();
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // Attempt to delete, then abort
  TransactionID txn2 = db->begin_transaction();
  ASSERT_TRUE(table->delete_row(txn2, rid));
  db->abort_transaction(txn2);

  // Verify the row is still there
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn3, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 6000);
  db->commit_transaction(txn3);
}