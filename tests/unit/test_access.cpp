#include <gtest/gtest.h>

#include <boost/variant.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "access.h"
#include "backend/smoldb.h"

// MockHeapFile remains for testing Table in isolation
class MockHeapFile
{
 public:
  std::vector<std::vector<std::byte>> rows;
  MockHeapFile(BufferPool*, WAL_mgr*, PageID, size_t) {}
  RID append(std::span<const std::byte> t)
  {
    rows.emplace_back(t.begin(), t.end());
    return {static_cast<PageID>(rows.size() - 1), 0};
  }
  void full_scan(std::vector<std::vector<std::byte>>& out) const { out = rows; }
  bool get(RID rid, std::vector<std::byte>& out) const
  {
    if (rid.page_id >= rows.size() || rid.slot != 0) return false;
    out = rows[rid.page_id];
    return true;
  }
};

static Schema make_simple_schema()
{
  Column c0{0, "id", Col_type::INT, false, {}};
  Column c1{1, "name", Col_type::STRING, false, {}};
  return Schema{c0, c1};
}

// ... (Row tests and Table tests with MockHeapFile remain unchanged) ...
TEST(RowTest, SetGetValueCorrect)
{
  Schema schema = make_simple_schema();
  Row row(schema);
  row.set_value(0, int32_t(42));
  row.set_value(1, std::string("Alice"));
  EXPECT_EQ(boost::get<int32_t>(row.get_value(0)), 42);
  EXPECT_EQ(boost::get<std::string>(row.get_value("name")), "Alice");
}

TEST(TableTest, InsertScanSingleRow)
{
  Schema schema = make_simple_schema();
  auto mock_hf = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  Table<MockHeapFile> table(std::move(mock_hf), 1, "test_table", schema);
  Row row(schema);
  row.set_value(0, int32_t(100));
  row.set_value(1, std::string("Name100"));
  table.insert_row(row);
  std::vector<Row> rows = table.scan_all();
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(boost::get<int32_t>(rows[0].get_value(0)), 100);
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