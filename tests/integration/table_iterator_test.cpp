#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <map>
#include <set>

#include "smoldb.h"

using namespace smoldb;

class TableIteratorTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "table_iterator_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config{test_dir};
    db = std::make_unique<SmolDB>(config, io_context_.get_executor());
    db->startup();
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  Schema make_simple_schema()
  {
    return {{0, "id", Col_type::INT, false, {}},
            {1, "val", Col_type::STRING, false, {}, 32}};
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  boost::asio::io_context io_context_;
};

TEST_F(TableIteratorTest, IterateEmptyTable)
{
  db->create_table(1, "empty_table", make_simple_schema());
  Table<>* table = db->get_table("empty_table");
  ASSERT_NE(table, nullptr);

  int count = 0;
  for (const auto& [rid, row] : *table)
  {
    count++;  // This loop should not execute
  }
  EXPECT_EQ(count, 0);
  EXPECT_EQ(table->begin(), table->end());
}

TEST_F(TableIteratorTest, IterateSinglePageTable)
{
  db->create_table(1, "test_table", make_simple_schema());
  Table<>* table = db->get_table("test_table");
  ASSERT_NE(table, nullptr);
  Schema schema = table->get_schema();

  TransactionID txn_id = db->begin_transaction();
  std::map<int, std::string> expected_data = {{10, "A"}, {20, "B"}, {30, "C"}};
  for (const auto& [id, val] : expected_data)
  {
    Row r(schema);
    r.set_value("id", id);
    r.set_value("val", val);
    table->insert_row(txn_id, r);
  }
  db->commit_transaction(txn_id);

  std::map<int, std::string> seen_data;
  int count = 0;
  for (const auto& [rid, row] : *table)
  {
    count++;
    int id = boost::get<int32_t>(row.get_value("id"));
    std::string val = boost::get<std::string>(row.get_value("val"));
    seen_data[id] = val;
  }

  EXPECT_EQ(count, 3);
  EXPECT_EQ(seen_data, expected_data);
  EXPECT_NE(table->begin(), table->end());
}

TEST_F(TableIteratorTest, IterateSkipsDeletedRows)
{
  db->create_table(1, "test_table", make_simple_schema());
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();

  TransactionID txn1 = db->begin_transaction();
  Row r1(schema), r2(schema), r3(schema);
  r1.set_value("id", 100);
  r1.set_value("val", "one");
  r2.set_value("id", 200);
  r2.set_value("val", "two");
  r3.set_value("id", 300);
  r3.set_value("val", "three");
  table->insert_row(txn1, r1);
  RID rid_to_delete = table->insert_row(txn1, r2);
  table->insert_row(txn1, r3);
  db->commit_transaction(txn1);

  TransactionID txn2 = db->begin_transaction();
  table->delete_row(txn2, rid_to_delete);
  db->commit_transaction(txn2);

  std::map<int, std::string> seen_data;
  int count = 0;
  for (const auto& [rid, row] : *table)
  {
    count++;
    int id = boost::get<int32_t>(row.get_value("id"));
    std::string val = boost::get<std::string>(row.get_value("val"));
    seen_data[id] = val;
  }

  EXPECT_EQ(count, 2);
  EXPECT_EQ(seen_data.count(200), 0);
  EXPECT_EQ(seen_data.count(100), 1);
  EXPECT_EQ(seen_data.count(300), 1);
}

TEST_F(TableIteratorTest, IterateMultiPageTable)
{
  // This test forces page splits to ensure the iterator handles page
  // transitions.
  db->create_table(1, "test_table", make_simple_schema());
  Table<>* table = db->get_table("test_table");
  Schema schema = table->get_schema();

  const HeapFile* heap = table->get_heap_file();
  size_t slots_per_page = heap->slots_per_page();

  TransactionID txn = db->begin_transaction();
  size_t num_rows = slots_per_page * 2 + 1;  // Force at least 3 pages.
  std::set<int> expected_ids;
  for (size_t i = 0; i < num_rows; ++i)
  {
    Row r(schema);
    r.set_value("id", (int)i);
    r.set_value("val", "val" + std::to_string(i));
    table->insert_row(txn, r);
    expected_ids.insert(i);
  }
  db->commit_transaction(txn);

  std::set<int> seen_ids;
  for (const auto& [rid, row] : *table)
  {
    seen_ids.insert(boost::get<int32_t>(row.get_value("id")));
  }

  EXPECT_EQ(seen_ids.size(), num_rows);
  EXPECT_EQ(seen_ids, expected_ids);
}