#include <gtest/gtest.h>

// Make private members of HeapFile public for testing
#define private public
#include "heapfile.h"
#include "smoldb.h"
#undef private

class HeapFileBitmapTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "heap_file_bitmap_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config{test_dir, BUFFER_SIZE_FOR_TEST};
    db = std::make_unique<SmolDB>(config);
    db->startup();
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

TEST_F(HeapFileBitmapTest, BitHelpers)
{
  HeapFile heap(db->buffer_pool_.get(), db->wal_mgr_.get(), 1, 64);
  std::array<std::byte, HeapFile::BITMAP_SIZE_BYTES> bitmap{};

  // Test set
  heap.set_slot_bit(bitmap.data(), 0);
  heap.set_slot_bit(bitmap.data(), 8);
  heap.set_slot_bit(bitmap.data(), 9);

  EXPECT_EQ(bitmap[0], std::byte{0b00000001});
  EXPECT_EQ(bitmap[1], std::byte{0b00000011});

  // Test clear
  heap.clear_slot_bit(bitmap.data(), 8);
  EXPECT_EQ(bitmap[1], std::byte{0b00000010});
}

TEST_F(HeapFileBitmapTest, FindsFirstFreeSlot)
{
  HeapFile heap(db->buffer_pool_.get(), db->wal_mgr_.get(), 1, 64);
  PageGuard guard = db->buffer_pool_->fetch_page(1);
  auto page = guard.write();
  std::byte* bitmap = heap.get_bitmap_ptr(*page);

  heap.set_slot_bit(bitmap, 0);
  heap.set_slot_bit(bitmap, 1);
  heap.set_slot_bit(bitmap, 3);

  auto free_slot = heap.find_first_clear_bit(bitmap);
  ASSERT_TRUE(free_slot.has_value());
  EXPECT_EQ(*free_slot, 2);
}

TEST_F(HeapFileBitmapTest, FindsNextSetBit)
{
  HeapFile heap(db->buffer_pool_.get(), db->wal_mgr_.get(), 1, 64);
  PageGuard guard = db->buffer_pool_->fetch_page(1);
  auto page = guard.write();
  std::byte* bitmap = heap.get_bitmap_ptr(*page);

  heap.set_slot_bit(bitmap, 1);
  heap.set_slot_bit(bitmap, 5);
  heap.set_slot_bit(bitmap, 10);

  auto next_slot = heap.find_next_set_bit(bitmap, 0);
  ASSERT_TRUE(next_slot.has_value());
  EXPECT_EQ(*next_slot, 1);

  next_slot = heap.find_next_set_bit(bitmap, 2);
  ASSERT_TRUE(next_slot.has_value());
  EXPECT_EQ(*next_slot, 5);

  next_slot = heap.find_next_set_bit(bitmap, 6);
  ASSERT_TRUE(next_slot.has_value());
  EXPECT_EQ(*next_slot, 10);

  next_slot = heap.find_next_set_bit(bitmap, 11);
  ASSERT_FALSE(next_slot.has_value());
}

TEST_F(HeapFileBitmapTest, InsertAndGetWithBitmap)
{
  db->create_table(1, "test_table", {{0, "id", Col_type::INT, false, {}}});
  Table<>* table = db->get_table("test_table");
  TransactionID txn = db->begin_transaction();

  Row row(table->get_schema());
  row.set_value("id", 123);
  RID rid = table->insert_row(txn, row);
  db->commit_transaction(txn);

  // Verify with a new transaction
  txn = db->begin_transaction();
  Row out_row;
  ASSERT_TRUE(table->get_row(txn, rid, out_row));
  EXPECT_EQ(boost::get<int32_t>(out_row.get_value("id")), 123);

  // Also check underlying heap file
  PageGuard guard = db->buffer_pool_->fetch_page(rid.page_id);
  auto page = guard.read();
  auto* heap = table->heap_file_.get();
  const std::byte* bitmap = heap->get_bitmap_ptr(*page);
  bool is_set = static_cast<bool>((bitmap[rid.slot / 8] >> (rid.slot % 8)) &
                                  std::byte{1});
  EXPECT_TRUE(is_set);
  db->commit_transaction(txn);
}

/* Not used for now as we implement tombstones which might not necessarily be
 * cleared during deletion.
TEST_F(HeapFileBitmapTest, DeleteClearsBitmap)
{
  db->create_table(1, "test_table", {{0, "id", Col_type::INT, false, {}}});
  Table<>* table = db->get_table("test_table");

  // Insert
  TransactionID txn1 = db->begin_transaction();
  Row row(table->get_schema());
  row.set_value("id", 456);
  RID rid = table->insert_row(txn1, row);
  db->commit_transaction(txn1);

  // Delete
  TransactionID txn2 = db->begin_transaction();
  table->delete_row(txn2, rid);
  db->commit_transaction(txn2);

  // Verify row is gone
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  ASSERT_FALSE(table->get_row(txn3, rid, out_row));
  db->commit_transaction(txn3);

  // Verify bitmap bit is cleared
  PageGuard guard = db->buffer_pool_->fetch_page(rid.page_id);
  auto page = guard.read();
  auto* heap = table->heap_file_.get();
  const std::byte* bitmap = heap->get_bitmap_ptr(*page);
  bool is_set = static_cast<bool>((bitmap[rid.slot / 8] >> (rid.slot % 8)) &
                                  std::byte{1});
  EXPECT_FALSE(is_set);
}
*/