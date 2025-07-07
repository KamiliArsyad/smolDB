#include <gtest/gtest.h>

#include <filesystem>
#include <numeric>
#include <vector>

#include "bfrpl.h"
#include "dsk_mgr.h"
#include "executor/lock_mgr.h"
#include "executor/trx_mgr.h"
#include "heapfile.h"
#include "smoldb.h"
#include "wal_mgr.h"

class HeapFileTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "heap_file_test";
    std::filesystem::create_directories(test_dir);

    db_path = test_dir / "test.db";
    wal_path = test_dir / "test.wal";

    std::remove(db_path.c_str());
    std::remove(wal_path.c_str());

    disk_mgr = std::make_unique<Disk_mgr>(db_path);
    wal_mgr = std::make_unique<WAL_mgr>(wal_path);
    buffer_pool = std::make_unique<BufferPool>(BUFFER_SIZE_FOR_TEST,
                                               disk_mgr.get(), wal_mgr.get());
    lock_mgr = std::make_unique<LockManager>();
    txn_mgr = std::make_unique<TransactionManager>(
        lock_mgr.get(), wal_mgr.get(), buffer_pool.get());
  }

  void TearDown() override
  {
    buffer_pool.reset();
    txn_mgr.reset();
    lock_mgr.reset();
    wal_mgr.reset();
    disk_mgr.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::filesystem::path db_path;
  std::filesystem::path wal_path;

  std::unique_ptr<Disk_mgr> disk_mgr;
  std::unique_ptr<WAL_mgr> wal_mgr;
  std::unique_ptr<BufferPool> buffer_pool;
  std::unique_ptr<LockManager> lock_mgr;
  std::unique_ptr<TransactionManager> txn_mgr;
};

TEST_F(HeapFileTest, AppendAndGet)
{
  PageID first_page = buffer_pool->allocate_page();
  HeapFile heap(buffer_pool.get(), wal_mgr.get(), first_page, 64);
  TransactionID txn_id = txn_mgr->begin();
  Transaction* txn = txn_mgr->get_transaction(txn_id);

  std::vector<std::byte> tuple1(10);
  std::byte v{0};
  for (auto& b : tuple1)
  {
    b = v;
    v = std::byte{uint8_t(v) + 1};
  }

  RID rid1 = heap.append(txn, tuple1);
  EXPECT_EQ(rid1.page_id, first_page);
  EXPECT_EQ(rid1.slot, 0);

  std::vector<std::byte> tuple2(20);
  std::byte v2{0};
  for (auto& b : tuple2)
  {
    b = v2;
    v2 = std::byte{uint8_t(v2) + 1};
  }
  RID rid2 = heap.append(txn, tuple2);
  EXPECT_EQ(rid2.page_id, first_page);
  EXPECT_EQ(rid2.slot, 1);

  std::vector<std::byte> out_tuple;
  ASSERT_TRUE(heap.get(txn, rid1, out_tuple));
  EXPECT_EQ(out_tuple, tuple1);

  ASSERT_TRUE(heap.get(txn, rid2, out_tuple));
  EXPECT_EQ(out_tuple, tuple2);

  RID invalid_rid = {first_page, 99};
  EXPECT_FALSE(heap.get(txn, invalid_rid, out_tuple));
  txn_mgr->commit(txn_id);
}

TEST_F(HeapFileTest, FullScan)
{
  PageID first_page = buffer_pool->allocate_page();
  HeapFile heap(buffer_pool.get(), wal_mgr.get(), first_page, 32);
  TransactionID txn_id = txn_mgr->begin();
  Transaction* txn = txn_mgr->get_transaction(txn_id);

  std::vector<std::vector<std::byte>> inserted_tuples;
  for (int i = 0; i < 5; ++i)
  {
    std::vector<std::byte> t(i + 8);
    std::fill(t.begin(), t.end(), std::byte{static_cast<uint8_t>(i)});
    heap.append(txn, t);
    inserted_tuples.push_back(t);
  }

  std::vector<std::vector<std::byte>> scanned_tuples;
  heap.full_scan(scanned_tuples);

  ASSERT_EQ(scanned_tuples.size(), inserted_tuples.size());
  for (size_t i = 0; i < inserted_tuples.size(); ++i)
  {
    EXPECT_EQ(scanned_tuples[i], inserted_tuples[i]);
  }
  txn_mgr->commit(txn_id);
}

TEST_F(HeapFileTest, PageAllocation)
{
  PageID first_page = buffer_pool->allocate_page();
  // Max tuple size such that only one tuple fits per page
  size_t slot_size_needed =
      PAGE_SIZE - sizeof(PageHeader) - HeapFile::BITMAP_SIZE_BYTES;
  size_t max_tuple_size = slot_size_needed - sizeof(uint32_t);
  HeapFile heap(buffer_pool.get(), wal_mgr.get(), first_page, max_tuple_size);
  TransactionID txn_id = txn_mgr->begin();
  Transaction* txn = txn_mgr->get_transaction(txn_id);

  ASSERT_EQ(heap.slots_per_page(), 1);

  std::vector<std::byte> tuple(16, std::byte{1});
  RID rid1 = heap.append(txn, tuple);
  EXPECT_EQ(rid1.page_id, first_page);
  EXPECT_EQ(rid1.slot, 0);

  // This append should go to a new page
  RID rid2 = heap.append(txn, tuple);
  EXPECT_EQ(rid2.page_id, first_page + 1);
  EXPECT_EQ(rid2.slot, 0);

  EXPECT_EQ(heap.last_page_id(), first_page + 1);

  std::vector<std::vector<std::byte>> scanned_tuples;
  heap.full_scan(scanned_tuples);
  ASSERT_EQ(scanned_tuples.size(), 2);
  txn_mgr->commit(txn_id);
}