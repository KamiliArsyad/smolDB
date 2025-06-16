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

    db = std::make_unique<SmolDB>(test_dir, BUFFER_SIZE_FOR_TEST);
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