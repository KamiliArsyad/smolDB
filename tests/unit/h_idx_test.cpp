#include <gtest/gtest.h>

#define private public
#include "backend/smoldb.h"
#undef private
#include "idx.h"

class HashIndexTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "h_idx_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    db = std::make_unique<SmolDB>(test_dir);
    db->startup();

    // Setup a table with a primary key-like column
    Schema schema = {{0, "id", Col_type::INT, false, {}},
                     {1, "val", Col_type::STRING, false, {}}};
    db->create_table(1, "users", schema);
    db->create_index(1, 0, "pk_users");  // Index on column 0 ('id')
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

TEST_F(HashIndexTest, InsertAndGet)
{
  Table<>* table = db->get_table(1);
  Index* index = table->get_index();
  ASSERT_NE(index, nullptr);

  TransactionID txn = db->begin_transaction();
  Row row(table->get_schema());
  row.set_value("id", 123);
  row.set_value("val", "alice");
  RID rid = table->insert_row(txn, row);
  db->commit_transaction(txn);

  RID found_rid;
  ASSERT_TRUE(index->get(123, found_rid));
  EXPECT_EQ(found_rid, rid);

  ASSERT_FALSE(index->get(999, found_rid));
}

TEST_F(HashIndexTest, DeleteRemovesFromIndex)
{
  Table<>* table = db->get_table(1);
  Index* index = table->get_index();

  TransactionID txn1 = db->begin_transaction();
  Row row(table->get_schema());
  row.set_value("id", 456);
  row.set_value("val", "bob");
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // Verify it's there
  RID found_rid;
  ASSERT_TRUE(index->get(456, found_rid));

  // Delete it
  TransactionID txn2 = db->begin_transaction();
  table->delete_row(txn2, rid);
  db->commit_transaction(txn2);

  // Verify it's gone
  ASSERT_FALSE(index->get(456, found_rid));
}

TEST_F(HashIndexTest, AbortInsertIsRolledBack)
{
  Table<>* table = db->get_table(1);
  Index* index = table->get_index();

  TransactionID txn = db->begin_transaction();
  Row row(table->get_schema());
  row.set_value("id", 789);
  row.set_value("val", "charlie");
  table->insert_row(txn, row);

  // Abort before commit
  db->abort_transaction(txn);

  // Verify the key is not in the index
  RID found_rid;
  ASSERT_FALSE(index->get(789, found_rid));
}

TEST_F(HashIndexTest, AbortDeleteIsRolledBack)
{
  Table<>* table = db->get_table(1);
  Index* index = table->get_index();

  TransactionID txn1 = db->begin_transaction();
  Row row(table->get_schema());
  row.set_value("id", 101);
  row.set_value("val", "dave");
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  TransactionID txn2 = db->begin_transaction();
  table->delete_row(txn2, rid);

  // Abort before commit
  db->abort_transaction(txn2);

  // Verify the key is still in the index
  RID found_rid;
  ASSERT_TRUE(index->get(101, found_rid));
  EXPECT_EQ(found_rid, rid);
}

TEST_F(HashIndexTest, BuildIndexOnRestart)
{
  Table<>* table = db->get_table(1);

  TransactionID txn = db->begin_transaction();
  Row r1(table->get_schema()), r2(table->get_schema());
  r1.set_value("id", 111);
  r1.set_value("val", "r1");
  r2.set_value("id", 222);
  r2.set_value("val", "r2");
  RID rid1 = table->insert_row(txn, r1);
  RID rid2 = table->insert_row(txn, r2);
  db->commit_transaction(txn);

  // Shutdown cleanly
  db->shutdown();
  db.reset();

  // Restart. This should trigger the build process.
  db = std::make_unique<SmolDB>(test_dir);
  db->startup();

  Table<>* new_table = db->get_table(1);
  Index* new_index = new_table->get_index();
  ASSERT_NE(new_index, nullptr);

  RID found_rid;
  ASSERT_TRUE(new_index->get(111, found_rid));
  EXPECT_EQ(found_rid.page_id, rid1.page_id);
  EXPECT_EQ(found_rid.slot, rid1.slot);

  ASSERT_TRUE(new_index->get(222, found_rid));
  EXPECT_EQ(found_rid.page_id, rid2.page_id);
  EXPECT_EQ(found_rid.slot, rid2.slot);
}