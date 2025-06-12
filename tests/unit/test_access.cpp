#include <gtest/gtest.h>

#include <boost/variant.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "access.h"
#include "bfrpl.h"
#include "dsk_mgr.h"
#include "wal_mgr.h"

// --------------------------------------------------------------------------------
// Mock HeapFile to test Table without disk I/O
// --------------------------------------------------------------------------------
class MockHeapFile
{
 public:
  std::vector<std::vector<std::byte>> rows;
  size_t max_tuple_size = 256;

  // Mock constructor to match the real HeapFile signature
  MockHeapFile(BufferPool*, WAL_mgr*, PageID, size_t max_size)
      : max_tuple_size(max_size)
  {
  }

  RID append(std::span<const std::byte> t)
  {
    if (t.size() > max_tuple_size)
    {
      throw std::invalid_argument("Tuple too large for mock heap file");
    }
    rows.emplace_back(t.begin(), t.end());
    // Use page_id as index and slot as 0 for simplicity in mock
    return {static_cast<PageID>(rows.size() - 1), 0};
  }

  bool get(RID rid, std::vector<std::byte>& out) const
  {
    if (rid.page_id >= rows.size() || rid.slot != 0) return false;
    out = rows[rid.page_id];
    return true;
  }

  std::vector<std::vector<std::byte>> full_scan() const { return rows; }
};

// --------------------------------------------------------------------------------
// Helper to build a “simple schema” consisting of exactly two columns:
//   0: id   (INT, non-nullable)
//   1: name (STRING, non-nullable)
// --------------------------------------------------------------------------------
static Schema make_simple_schema()
{
  Column c0{0, "id", Col_type::INT, false, {}};
  Column c1{1, "name", Col_type::STRING, false, {}};
  return Schema{c0, c1};
}

// --------------------------------------------------------------------------------
// ROW TESTS
// --------------------------------------------------------------------------------
TEST(RowTest, SetGetValueCorrect)
{
  Schema schema = make_simple_schema();
  Row row(schema);

  // Set an integer and a string correctly
  int32_t int_val = 42;
  std::string str_val = "Alice";

  row.set_value(0, int_val);
  row.set_value(1, str_val);

  // Retrieve by index
  auto v0 = row.get_value(0);
  auto v1 = row.get_value(1);
  EXPECT_EQ(boost::get<int32_t>(v0), int_val);
  EXPECT_EQ(boost::get<std::string>(v1), str_val);

  // Retrieve by column name
  auto vn0 = row.get_value("id");
  auto vn1 = row.get_value("name");
  EXPECT_EQ(boost::get<int32_t>(vn0), int_val);
  EXPECT_EQ(boost::get<std::string>(vn1), str_val);
}

TEST(RowTest, SetGetValueWrongType)
{
  Schema schema = make_simple_schema();
  Row row(schema);

  // Attempt to set a string into the INT column → invalid_argument
  EXPECT_THROW(row.set_value(0, std::string("Bob")), std::invalid_argument);

  // Attempt to set an int into the STRING column → invalid_argument
  EXPECT_THROW(row.set_value(1, int32_t(7)), std::invalid_argument);

  // Also by name:
  EXPECT_THROW(row.set_value("id", std::string("Bob")), std::invalid_argument);
  EXPECT_THROW(row.set_value("name", int32_t(7)), std::invalid_argument);
}

TEST(RowTest, ToFromBytes)
{
  Schema schema = make_simple_schema();
  Row row(schema);
  row.set_value(0, int32_t(7));
  row.set_value(1, std::string("Charlie"));

  // Serialize to a byte buffer
  std::vector<std::byte> data = row.to_bytes();

  // Reconstruct (deserialise) using the same schema
  Row row2 = Row::from_bytes(data, schema);
  EXPECT_EQ(boost::get<int32_t>(row2.get_value(0)), 7);
  EXPECT_EQ(boost::get<std::string>(row2.get_value(1)), "Charlie");
}

// --------------------------------------------------------------------------------
// TABLE TESTS (parametrized on the in-memory MockHeapFile)
// --------------------------------------------------------------------------------
TEST(TableTest, DefaultConstructorThrows)
{
  Schema schema = make_simple_schema();
  Row row(schema);
  row.set_value(0, int32_t(1));
  row.set_value(1, std::string("X"));

  // A default-constructed Table<MockHeapFile> has no heap_file_. Any call
  // should throw.
  Table<MockHeapFile> table_default;
  EXPECT_THROW(table_default.insert_row(row), std::runtime_error);
  EXPECT_THROW(table_default.scan_all(), std::runtime_error);
}

TEST(TableTest, InsertScanSingleRow)
{
  Schema schema = make_simple_schema();
  auto mock_hf = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  Table<MockHeapFile> table(std::move(mock_hf), 1, "test_table", schema);

  Row row(schema);
  row.set_value(0, int32_t(100));
  row.set_value(1, std::string("Name100"));

  RID rid = table.insert_row(row);
  EXPECT_EQ(rid, (RID{0, 0}));

  // Scan back the in-memory data
  std::vector<Row> rows = table.scan_all();
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(boost::get<int32_t>(rows[0].get_value(0)), 100);
  EXPECT_EQ(boost::get<std::string>(rows[0].get_value(1)), "Name100");
}

TEST(TableTest, InsertScanMultipleRows)
{
  Schema schema = make_simple_schema();
  auto mock_hf = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  Table<MockHeapFile> table(std::move(mock_hf), 2, "multi_table", schema);

  for (int i = 0; i < 3; ++i)
  {
    Row r(schema);
    r.set_value(0, int32_t(i));
    r.set_value(1, std::string("User") + std::to_string(i));
    RID rid = table.insert_row(r);
    EXPECT_EQ(rid, (RID{static_cast<PageID>(i), 0}));
  }

  std::vector<Row> rows = table.scan_all();
  ASSERT_EQ(rows.size(), 3u);
  for (size_t i = 0; i < rows.size(); ++i)
  {
    EXPECT_EQ(boost::get<int32_t>(rows[i].get_value(0)),
              static_cast<int32_t>(i));
    EXPECT_EQ(boost::get<std::string>(rows[i].get_value(1)),
              "User" + std::to_string(i));
  }
}

TEST(TableTest, SchemaMismatchThrows)
{
  Schema correct_schema = make_simple_schema();
  auto mock_hf = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  Table<MockHeapFile> table(std::move(mock_hf), 3, "schema_table",
                            correct_schema);

  Schema wrong_schema;
  Column single_col{0, "only", Col_type::INT, false, {}};
  wrong_schema.push_back(single_col);
  Row wrong_row(wrong_schema);
  wrong_row.set_value(0, int32_t(5));

  EXPECT_THROW(table.insert_row(wrong_row), std::invalid_argument);
}

// --------------------------------------------------------------------------------
// CATALOG TESTS (with full backend fixture)
// --------------------------------------------------------------------------------
class CatalogTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir =
        std::filesystem::temp_directory_path() / "catalog_test_integration";
    std::filesystem::create_directories(test_dir);

    db_path = test_dir / "test.db";
    wal_path = test_dir / "test.wal";
    catalog_path = test_dir / "catalog.dat";

    std::remove(db_path.c_str());
    std::remove(wal_path.c_str());
    std::remove(catalog_path.c_str());

    disk_mgr = std::make_unique<Disk_mgr>(db_path);
    wal_mgr = std::make_unique<WAL_mgr>(wal_path);
    buffer_pool =
        std::make_unique<BufferPool>(10, disk_mgr.get(), wal_mgr.get());

    catalog = std::make_unique<Catalog>();
    catalog->set_managers(buffer_pool.get(), wal_mgr.get());
  }

  void TearDown() override
  {
    buffer_pool.reset();
    wal_mgr.reset();
    disk_mgr.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::filesystem::path db_path;
  std::filesystem::path wal_path;
  std::filesystem::path catalog_path;

  std::unique_ptr<Disk_mgr> disk_mgr;
  std::unique_ptr<WAL_mgr> wal_mgr;
  std::unique_ptr<BufferPool> buffer_pool;
  std::unique_ptr<Catalog> catalog;
};

TEST_F(CatalogTest, CreateGetList)
{
  Schema schema = make_simple_schema();
  catalog->create_table(10, "cat_table", schema);

  {
    std::vector<uint8_t> ids = catalog->list_table_ids();
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 10u);
  }

  {
    Table<>* tbl_ptr = catalog->get_table(10);
    ASSERT_NE(tbl_ptr, nullptr);
    EXPECT_EQ(tbl_ptr->get_table_id(), 10);
    EXPECT_EQ(tbl_ptr->get_table_name(), "cat_table");
  }

  {
    Table<>* tbl_by_name = catalog->get_table("cat_table");
    ASSERT_NE(tbl_by_name, nullptr);
    EXPECT_EQ(tbl_by_name->get_table_id(), 10u);
  }

  EXPECT_EQ(catalog->get_table(99), nullptr);
  EXPECT_EQ(catalog->get_table("nope"), nullptr);
}

TEST_F(CatalogTest, DuplicateTableThrows)
{
  Schema schema = make_simple_schema();

  catalog->create_table(1, "dup_table", schema);
  EXPECT_THROW(catalog->create_table(1, "dup_table2", schema),
               std::invalid_argument);
  EXPECT_THROW(catalog->create_table(2, "dup_table", schema),
               std::invalid_argument);
}

TEST_F(CatalogTest, PersistAndReload)
{
  Schema schema = make_simple_schema();
  catalog->create_table(22, "users", schema);

  Row row(schema);
  row.set_value("id", 42);
  row.set_value("name", "Zaphod");
  auto* table = catalog->get_table("users");
  RID rid = table->insert_row(row);

  // Persist catalog
  catalog->dump(catalog_path);

  // Create a new catalog instance and load from disk
  auto new_catalog = std::make_unique<Catalog>();
  new_catalog->set_managers(buffer_pool.get(), wal_mgr.get());
  new_catalog->load(catalog_path);
  new_catalog->reinit_tables();  // Manually re-init after load

  // Check if table exists and data is there
  auto* reloaded_table = new_catalog->get_table(22);
  ASSERT_NE(reloaded_table, nullptr);
  EXPECT_EQ(reloaded_table->get_table_name(), "users");

  Row out_row;
  ASSERT_TRUE(reloaded_table->get_row(rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 42);
  EXPECT_EQ(boost::get<std::string>(out_row.get_value("name")), "Zaphod");
}