#include <gtest/gtest.h>
#define private public  // <- test-only peek
#include <boost/asio/io_context.hpp>

#include "bfrpl.h"
#include "dsk_mgr.h"
#include "wal_mgr.h"
#undef private

/* -------- helpers --------------------------------------------------- */
static auto get_disk_mgr(std::filesystem::path& tmp_path)
{
  const auto path = tmp_path / "disk.dat";
  std::remove(path.c_str());
  return new smoldb::Disk_mgr(path);
}

// Helper to get a frame pointer. Requires locking the corresponding shard.
static smoldb::Frame* get_frame_ptr(smoldb::BufferPool& bp, smoldb::PageID pid)
{
  auto& shard = bp.get_shard(pid);
  std::scoped_lock lock(shard.mutex_);
  auto it = shard.cache_.find(pid);
  if (it == shard.cache_.end())
  {
    return nullptr;
  }
  return &(*(it->second));
}

// Test Fixture to control sharding for deterministic tests.
class BufferPoolTest : public ::testing::Test
{
 protected:
  // Use a fixed number of shards for predictable page->shard mapping.
  static constexpr size_t SHARD_COUNT = 4;
  std::filesystem::path temp_dir_path;
  smoldb::Disk_mgr* disk_mgr;
  smoldb::WAL_mgr* wal_mgr;

  boost::asio::io_context io_context_;
  void SetUp() override
  {
    temp_dir_path = std::filesystem::temp_directory_path();
    disk_mgr = get_disk_mgr(temp_dir_path);
    const auto path = temp_dir_path / "wal.dat";
    std::remove(path.c_str());
    wal_mgr = new smoldb::WAL_mgr(path, io_context_.get_executor());
  }

  void TearDown() override
  {
    delete disk_mgr;
    delete wal_mgr;
  }

  // Helper to get a PageID that is guaranteed to fall into a specific shard.
  smoldb::PageID page_in_shard(size_t shard_idx, size_t page_num)
  {
    return (page_num * SHARD_COUNT) + shard_idx;
  }
};

TEST_F(BufferPoolTest, PinUnpinCounts)
{
  smoldb::BufferPool bp{10, disk_mgr, wal_mgr, SHARD_COUNT};
  smoldb::PageID pid = page_in_shard(0, 1);  // Page 1, in shard 0

  {
    auto g1 = bp.fetch_page(pid);
    smoldb::Frame* f = get_frame_ptr(bp, pid);
    ASSERT_NE(f, nullptr);
    EXPECT_EQ(f->pin_count.load(), 1);

    {
      auto g2 = bp.fetch_page(pid);  // same page, extra pin
      EXPECT_EQ(f->pin_count.load(), 2);
    }  // g2 dtor
    EXPECT_EQ(f->pin_count.load(), 1);
  }  // g1 dtor
  EXPECT_EQ(get_frame_ptr(bp, pid)->pin_count.load(), 0);
}

TEST_F(BufferPoolTest, EvictionInShard)
{
  // Create a pool where each shard has capacity for exactly 1 page.
  smoldb::BufferPool bp{SHARD_COUNT, disk_mgr, wal_mgr, SHARD_COUNT};

  smoldb::PageID pid1_shard0 = page_in_shard(0, 1);
  smoldb::PageID pid2_shard0 = page_in_shard(0, 2);
  smoldb::PageID pid1_shard1 = page_in_shard(1, 1);

  // Load one page into shard 0.
  {
    auto g = bp.fetch_page(pid1_shard0);
  }
  ASSERT_NE(get_frame_ptr(bp, pid1_shard0), nullptr);

  // Load one page into shard 1. It should not evict from shard 0.
  {
    auto g = bp.fetch_page(pid1_shard1);
  }
  ASSERT_NE(get_frame_ptr(bp, pid1_shard0), nullptr);
  ASSERT_NE(get_frame_ptr(bp, pid1_shard1), nullptr);

  // Load a second page into shard 0. This MUST evict the first one from shard
  // 0.
  {
    auto g = bp.fetch_page(pid2_shard0);
  }
  ASSERT_EQ(get_frame_ptr(bp, pid1_shard0), nullptr);  // Evicted
  ASSERT_NE(get_frame_ptr(bp, pid2_shard0), nullptr);  // Present
  ASSERT_NE(get_frame_ptr(bp, pid1_shard1), nullptr);  // Unaffected
}

TEST_F(BufferPoolTest, MRUTouchOnHit)
{
  // Capacity of 2 per shard.
  smoldb::BufferPool bp{2 * SHARD_COUNT, disk_mgr, wal_mgr, SHARD_COUNT};

  smoldb::PageID p1 = page_in_shard(0, 1);
  smoldb::PageID p2 = page_in_shard(0, 2);

  {
    bp.fetch_page(p1);
  }  // Shard 0 LRU: [p1]
  {
    bp.fetch_page(p2);
  }  // Shard 0 LRU: [p2, p1] (p2 is MRU)

  // Re-touch p1, making it the new MRU.
  {
    bp.fetch_page(p1);
  }  // Shard 0 LRU: [p1, p2]

  auto& shard = bp.get_shard(p1);
  std::scoped_lock lock(shard.mutex_);
  ASSERT_EQ(shard.lru_list_.front().page.hdr.id, p1);
  ASSERT_EQ(std::prev(shard.lru_list_.end())->page.hdr.id, p2);
}