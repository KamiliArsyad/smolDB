#include <gtest/gtest.h>

#define private public
#include "heapfile.h"
#include "smoldb.h"
#undef private

using namespace smoldb;

class HeapFileTombstoneTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "heap_tombstone_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config{test_dir};
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

TEST_F(HeapFileTombstoneTest, DeleteCreatesTombstoneAndIsNotReused)
{
  db->create_table(1, "test_table", {{0, "id", Col_type::INT, false, {}}});
  Table<>* table = db->get_table("test_table");
  auto* heap = table->heap_file_.get();

  // 1. Insert two rows to have a known state.
  TransactionID txn1 = db->begin_transaction();
  Row row1(table->get_schema()), row2(table->get_schema());
  row1.set_value("id", 100);
  row2.set_value("id", 200);
  RID rid1 = table->insert_row(txn1, row1);
  RID rid2 = table->insert_row(txn1, row2);
  db->commit_transaction(txn1);

  // 2. Delete the first row.
  TransactionID txn2 = db->begin_transaction();
  ASSERT_TRUE(table->delete_row(txn2, rid1));
  db->commit_transaction(txn2);

  // 3. Verify the logical state: row1 is gone, row2 is present.
  TransactionID txn3 = db->begin_transaction();
  Row out_row;
  EXPECT_FALSE(table->get_row(txn3, rid1, out_row));
  EXPECT_TRUE(table->get_row(txn3, rid2, out_row));
  db->commit_transaction(txn3);

  // 4. Verify the PHYSICAL state (THE CRITICAL TEST)
  PageGuard guard = db->buffer_pool_->fetch_page(rid1.page_id);

  {
    // Scoped guard so it won't deadlock with the next step
    auto page = guard.read();

    // Check that the bitmap bit for the "deleted" slot is STILL SET.
    const std::byte* bitmap = heap->get_bitmap_ptr(*page);
    bool is_bit_set = static_cast<bool>(
        (bitmap[rid1.slot / 8] >> (rid1.slot % 8)) & std::byte{1});
    ASSERT_TRUE(is_bit_set)
        << "The bitmap bit for the tombstone should remain set.";

    // Check that the slot itself is marked with the deleted flag.
    const std::byte* slot_ptr = heap->get_slot_ptr(*page, rid1.slot);
    ASSERT_TRUE(heap->is_deleted(slot_ptr))
        << "The slot should be marked as a tombstone.";
  }

  // 5. Verify space is NOT reused.
  // The next append should go to a new slot (slot 2), not reuse the tombstone's
  // slot (slot 0).
  TransactionID txn4 = db->begin_transaction();
  Row row3(table->get_schema());
  row3.set_value("id", 300);
  RID rid3 = table->insert_row(txn4, row3);
  db->commit_transaction(txn4);

  EXPECT_NE(rid3.slot, rid1.slot)
      << "A new append should not reuse a tombstone slot.";
  EXPECT_EQ(rid3.slot, rid1.slot + 2);  // Assuming slot 1 was rid2
}
