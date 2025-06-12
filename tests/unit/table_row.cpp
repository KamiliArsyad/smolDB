#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#include "access.h"

// Note: This MockHeapFile matches the structure of the one in test_access.cpp.
// In a larger project, this might be shared in a test utility header.
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
    rows.emplace_back(t.begin(), t.end());
    return {static_cast<PageID>(rows.size() - 1), 0};
  }

  std::vector<std::vector<std::byte>> full_scan() const { return rows; }
};

class AccessTest : public testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "smoldb_test_access";
    std::filesystem::create_directories(test_dir);
    // The mock heap doesn't need real managers, so passing nullptr is fine for
    // this test.
    heap_ = std::make_unique<MockHeapFile>(nullptr, nullptr, 0, 256);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir); }

  std::filesystem::path test_dir;
  std::unique_ptr<MockHeapFile> heap_;
};

TEST_F(AccessTest, BasicTableInsert)
{
  Schema schema;
  schema.push_back({0, "id", Col_type::INT, false, {}});

  Table<MockHeapFile> table(std::move(heap_), 1, "test_table", schema);

  Row row(schema);
  row.set_value("id", 123);

  table.insert_row(row);

  auto rows = table.scan_all();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(boost::get<int32_t>(rows[0].get_value("id")), 123);
}