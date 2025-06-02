#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include <boost/variant.hpp>

#include "access.h"

// --------------------------------------------------------------------------------
// Mock HeapFile to test Table without disk I/O
// --------------------------------------------------------------------------------
class MockHeapFile {
public:
    // Simply store every appended byte‐vector in memory:
    std::vector<std::vector<std::byte>> rows;

    MockHeapFile() = default;

    // Mimic append: store the raw bytes and return a RID = index into `rows`
    RID append(const std::vector<std::byte>& t) {
        rows.push_back(t);
        return static_cast<RID>(rows.size() - 1);
    }

    // Return everything we've stored so far
    std::vector<std::vector<std::byte>> full_scan() const {
        return rows;
    }
};

// --------------------------------------------------------------------------------
// Helper to build a “simple schema” consisting of exactly two columns:
//   0: id   (INT, non-nullable)
//   1: name (STRING, non-nullable)
// --------------------------------------------------------------------------------
static Schema make_simple_schema() {
    Column c0 { 0, "id",   Col_type::INT,    false, {} };
    Column c1 { 1, "name", Col_type::STRING, false, {} };
    return Schema{ c0, c1 };
}

// --------------------------------------------------------------------------------
// ROW TESTS
// --------------------------------------------------------------------------------
TEST(RowTest, SetGetValueCorrect) {
    Schema schema = make_simple_schema();
    Row row(schema);

    // Set an integer and a string correctly
    int32_t int_val = 42;
    std::string str_val = "Alice";

    row.set_value(0,   int_val);
    row.set_value(1,   str_val);

    // Retrieve by index
    auto v0 = row.get_value(0);
    auto v1 = row.get_value(1);
    EXPECT_EQ(boost::get<int32_t>(v0),        int_val);
    EXPECT_EQ(boost::get<std::string>(v1),    str_val);

    // Retrieve by column name
    auto vn0 = row.get_value("id");
    auto vn1 = row.get_value("name");
    EXPECT_EQ(boost::get<int32_t>(vn0),        int_val);
    EXPECT_EQ(boost::get<std::string>(vn1),    str_val);
}

TEST(RowTest, SetGetValueWrongType) {
    Schema schema = make_simple_schema();
    Row row(schema);

    // Attempt to set a string into the INT column → invalid_argument
    EXPECT_THROW(row.set_value(0, std::string("Bob")), std::invalid_argument);

    // Attempt to set an int into the STRING column → invalid_argument
    EXPECT_THROW(row.set_value(1, int32_t(7)), std::invalid_argument);

    // Also by name:
    EXPECT_THROW(row.set_value("id",   std::string("Bob")),   std::invalid_argument);
    EXPECT_THROW(row.set_value("name",  int32_t(7)),          std::invalid_argument);
}

TEST(RowTest, ToFromBytes) {
    Schema schema = make_simple_schema();
    Row row(schema);
    row.set_value(0,  int32_t(7));
    row.set_value(1,  std::string("Charlie"));

    // Serialize to a byte buffer
    std::vector<std::byte> data = row.to_bytes();

    // Reconstruct (deserialise) using the same schema
    Row row2 = Row::from_bytes(data, schema);
    EXPECT_EQ(boost::get<int32_t>( row2.get_value(0) ),    7);
    EXPECT_EQ(boost::get<std::string>( row2.get_value(1) ), "Charlie");
}

// --------------------------------------------------------------------------------
// TABLE TESTS (parametrized on the in-memory MockHeapFile)
// --------------------------------------------------------------------------------
TEST(TableTest, DefaultConstructorThrows) {
    Schema schema = make_simple_schema();
    Row row(schema);
    row.set_value(0, int32_t(1));
    row.set_value(1, std::string("X"));

    // A default-constructed Table<MockHeapFile> has no heap_file_. Any call should throw.
    Table<MockHeapFile> table_default;
    EXPECT_THROW(table_default.insert_row(row),  std::runtime_error);
    EXPECT_THROW(table_default.scan_all(),      std::runtime_error);
}

TEST(TableTest, InsertScanSingleRow) {
    Schema schema = make_simple_schema();
    auto mock_hf = std::make_unique<MockHeapFile>();
    Table<MockHeapFile> table(std::move(mock_hf), 1, "test_table", schema);

    Row row(schema);
    row.set_value(0, int32_t(100));
    row.set_value(1, std::string("Name100"));

    // Insert one row, RID should be 0
    RID rid = table.insert_row(row);
    EXPECT_EQ(rid, 0u);

    // Scan back the in-memory data
    std::vector<Row> rows = table.scan_all();
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(boost::get<int32_t>(rows[0].get_value(0)),         100);
    EXPECT_EQ(boost::get<std::string>(rows[0].get_value(1)),     "Name100");
}

TEST(TableTest, InsertScanMultipleRows) {
    Schema schema = make_simple_schema();
    auto mock_hf = std::make_unique<MockHeapFile>();
    Table<MockHeapFile> table(std::move(mock_hf), 2, "multi_table", schema);

    // Insert three rows in a loop
    for (int i = 0; i < 3; ++i) {
        Row r(schema);
        r.set_value(0, int32_t(i));
        r.set_value(1, std::string("User") + std::to_string(i));
        RID rid = table.insert_row(r);
        EXPECT_EQ(rid, static_cast<RID>(i));
    }

    // Now scan them back; we should get 3 rows in insertion order
    std::vector<Row> rows = table.scan_all();
    ASSERT_EQ(rows.size(), 3u);
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(boost::get<int32_t>( rows[i].get_value(0) ),    static_cast<int32_t>(i));
        EXPECT_EQ(boost::get<std::string>( rows[i].get_value(1) ), "User" + std::to_string(i));
    }
}

TEST(TableTest, SchemaMismatchThrows) {
    // This Table expects two columns: (INT, STRING)
    Schema correct_schema = make_simple_schema();
    auto mock_hf = std::make_unique<MockHeapFile>();
    Table<MockHeapFile> table(std::move(mock_hf), 3, "schema_table", correct_schema);

    // We create a Row with the wrong schema (only one column)
    Schema wrong_schema;
    Column single_col { 0, "only", Col_type::INT, false, {} };
    wrong_schema.push_back(single_col);
    Row wrong_row(wrong_schema);
    wrong_row.set_value(0, int32_t(5));

    EXPECT_THROW(table.insert_row(wrong_row), std::invalid_argument);
}

// --------------------------------------------------------------------------------
// CATALOG TESTS
// --------------------------------------------------------------------------------
TEST(CatalogTest, CreateGetList) {
    Catalog catalog;

    // Prepare a simple two‐column schema
    Schema schema = make_simple_schema();

    // Create a temporary directory for the on‐disk heap files
    std::filesystem::path temp_dir = std::filesystem::current_path() / "test_catalog_data";
    std::filesystem::create_directory(temp_dir);
    catalog.set_data_directory(temp_dir);

    // Create a single table with ID = 10
    catalog.create_table(10, "cat_table", schema);

    // list_table_ids should return exactly {10}
    {
        std::vector<uint8_t> ids = catalog.list_table_ids();
        ASSERT_EQ(ids.size(), 1u);
        EXPECT_EQ(ids[0], 10u);
    }

    // get_table(10) should return a non-null pointer
    {
        Table<>* tbl_ptr = catalog.get_table(10);
        ASSERT_NE(tbl_ptr, nullptr);
        EXPECT_EQ(tbl_ptr->get_table_id(),   10);
        EXPECT_EQ(tbl_ptr->get_table_name(), "cat_table");
    }

    // get_table("cat_table") should also find it
    {
        Table<>* tbl_by_name = catalog.get_table("cat_table");
        ASSERT_NE(tbl_by_name, nullptr);
        EXPECT_EQ(tbl_by_name->get_table_id(), 10u);
    }

    // Nonexistent lookups
    EXPECT_EQ(catalog.get_table(99),   nullptr);
    EXPECT_EQ(catalog.get_table("nope"), nullptr);

    // Clean up the temp directory
    std::filesystem::remove_all(temp_dir);
}

TEST(CatalogTest, DuplicateTableThrows) {
    Catalog catalog;
    Schema schema = make_simple_schema();

    catalog.create_table(1, "dup_table", schema);
    EXPECT_THROW(catalog.create_table(1, "dup_table2", schema), std::invalid_argument);
}
