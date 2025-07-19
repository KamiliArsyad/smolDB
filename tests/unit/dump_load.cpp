#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#define private public
#include "access.h"
#undef private
#include "idx.h"

using namespace smoldb;

TEST(DumpLoadTest, Storage)
{
  // 1) build a catalog with metadata
  Catalog cat;
  Schema sch;
  Column c;
  c.id = 42;
  c.name = "foo";
  c.type = Col_type::INT;
  c.nullable = true;
  c.default_bytes = {std::byte{0x10}, std::byte{0x20}};
  sch.push_back(c);

  // In a real scenario, create_table would be called. For this test,
  // we manually construct the metadata to ensure it serializes.
  cat.m_schemas_.emplace(1, TableMetadata{sch, 5, "test_table", 256});

  // 2) dump & reload
  auto tmp = std::filesystem::temp_directory_path();
  auto p1 = tmp / "catalog1.bin";
  auto p2 = tmp / "catalog2.bin";

  cat.dump(p1);
  Catalog loaded;
  loaded.load(p1);
  loaded.dump(p2);

  // 3) compare files
  std::ifstream f1(p1, std::ios::binary), f2(p2, std::ios::binary);
  std::vector<char> b1{std::istreambuf_iterator(f1), {}},
      b2{std::istreambuf_iterator(f2), {}};
  ASSERT_EQ(b1, b2);
}