#include <filesystem>
#include <gtest/gtest.h>

#include "storage.h"
#include "bfrpl.h"

struct TxHeader { int32_t id; std::chrono::system_clock::time_point t; };

TEST(HeapFileTest, AppendScan)
{
  auto tmp = std::filesystem::temp_directory_path();
  auto path = tmp/"trx.dat";
  // Clear previous run(s)
  std::remove(path.c_str());
  constexpr int test_size = 1000;

  HeapFile<TxHeader> hf(path.string());

  for (int i = 0; i < test_size; i++)
  {
    const TxHeader tx = {i, {}};
    hf.append(tx);
  }

  auto v = hf.full_scan();
  ASSERT_EQ(v.size(), test_size);
}