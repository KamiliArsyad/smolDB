#include <gtest/gtest.h>

#include <cstring>

#define private public
#include <boost/asio/io_context.hpp>

#include "bfrpl.h"
#include "dsk_mgr.h"
#include "smoldb.h"
#include "wal_mgr.h"
#undef private

using namespace smoldb;

// Helpers
static std::filesystem::path tmpfile(const char* name)
{
  auto p = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove(p);  // ensure a clean slate
  return p;
}

// Creates a minimal “UPDATE” record that overwrites `len` bytes starting at
// `offset` on `page_id` with the provided *value*.
static LogRecordHeader make_update(uint64_t txn, PageID page_id,
                                   uint16_t offset, uint16_t len,
                                   const void* after_img,
                                   std::vector<char>& buf)
{
  // payload = header + 2×len bytes (before + after)
  const size_t pay_sz = sizeof(UpdatePagePayload) + len * 2;
  buf.resize(pay_sz);

  auto* upd = reinterpret_cast<UpdatePagePayload*>(buf.data());
  upd->page_id = page_id;
  upd->offset = offset;
  upd->length = len;

  // before-image: zero-filled (or copy the real “before” value if you have it)
  std::memset(const_cast<std::byte*>(upd->bef()), 0, len);

  // after-image: copy caller-supplied bytes
  std::memcpy(const_cast<std::byte*>(upd->aft()), after_img, len);

  LogRecordHeader h{};
  h.type = UPDATE;
  h.lr_length = static_cast<uint32_t>(sizeof(LogRecordHeader) + pay_sz);
  return h;
}
// ──────────────────────────────────────────────────────────────────────────────

/**
 * Appends one UPDATE record, “crashes”, then recovers into a fresh BufferPool.
 */
TEST(WAL_mgr, AppendRecover)
{
  namespace fs = std::filesystem;

  auto wal_path = tmpfile("wal_append_recover.log");
  auto db_path = tmpfile("wal_append_recover.db");

  // 1. Build a WAL with a single UPDATE
  boost::asio::io_context io_context_;
  WAL_mgr wal(wal_path, io_context_.get_executor());

  constexpr PageID kPid = 3;
  constexpr uint32_t kOff = 40;
  constexpr uint32_t kLen = 8;
  constexpr uint64_t kVal = 0xDEADBEEFCAFEBABEull;

  std::vector<char> buf;
  auto hdr = make_update(/*txn=*/1, kPid, kOff, kLen, &kVal, buf);
  wal.append_record(hdr, buf.data());
  wal.flush_to_lsn(hdr.lsn);

  // 2. Simulate restart: open _new_ WAL_mgr + disk + buffer-pool and recover.
  boost::asio::io_context io_context;
  WAL_mgr wal_replayer(wal_path, io_context.get_executor());  // read-only role
  Disk_mgr disk(db_path);
  BufferPool pool(BUFFER_SIZE_FOR_TEST, &disk, &wal_replayer);

  ASSERT_NO_THROW(wal_replayer.recover(pool, wal_path));

  auto g = pool.fetch_page(kPid).write();
  uint64_t read_back{};
  std::memcpy(&read_back, g->data() + kOff, sizeof(read_back));
  EXPECT_EQ(read_back, kVal);  // after-image applied
}

/**
 * LSNs must be monotonically increasing.
 */
TEST(WAL_mgr, MonotonicLSN)
{
  boost::asio::io_context io_context;
  WAL_mgr wal(tmpfile("wal_monotonic.log"), io_context.get_executor());

  std::vector<char> buf(1, 0);
  LogRecordHeader h1{};
  h1.type = UPDATE;
  h1.lr_length = sizeof(h1) + 1;
  LogRecordHeader h2 = h1;

  LSN l1 = wal.append_record(h1, buf.data());
  LSN l2 = wal.append_record(h2, buf.data());
  EXPECT_LT(l1, l2);
}

/**
 * Recover must leave the page in the state of the *latest* UPDATE.
 */
TEST(WAL_mgr, LastWriteWins)
{
  auto wal_path = tmpfile("wal_last_write_wins.log");
  boost::asio::io_context io_context;
  WAL_mgr wal(wal_path, io_context.get_executor());

  constexpr PageID kPid = 1;
  constexpr uint32_t kOff = 0;
  constexpr uint32_t kLen = 4;
  uint32_t v1 = 0xAABBCCDD, v2 = 0x11223344;

  std::vector<char> b1, b2;
  auto h1 = make_update(1, kPid, kOff, kLen, &v1, b1);
  auto h2 = make_update(1, kPid, kOff, kLen, &v2, b2);

  wal.append_record(h1, b1.data());
  wal.append_record(h2, b2.data());
  wal.flush_to_lsn(h2.lsn);

  Disk_mgr disk(tmpfile("wal_last_write_wins.db"));
  BufferPool pool(BUFFER_SIZE_FOR_TEST, &disk, &wal);

  wal.recover(pool, wal_path);
  auto g = pool.fetch_page(kPid).write();
  uint32_t final;
  std::memcpy(&final, g->data() + kOff, sizeof(final));
  EXPECT_EQ(final, v2);  // newest value survives
}

// ===========================================================================
// == NEW TESTS FOR LSN INDEXING AND GET_RECORD
// ===========================================================================

TEST(WAL_mgr, GetRecordFindsWrittenRecord)
{
  auto wal_path = tmpfile("wal_get_record.log");

  // Phase 1: Write two records and destroy the WAL_mgr to ensure flush.
  uint32_t v1 = 1111;
  uint64_t v2 = 22222222;
  std::vector<char> b1, b2;
  LSN lsn1, lsn2;

  {
    boost::asio::io_context io_context;
    WAL_mgr wal(wal_path, io_context.get_executor());
    auto h1 = make_update(1, 10, 0, sizeof(v1), &v1, b1);
    auto h2 = make_update(1, 11, 8, sizeof(v2), &v2, b2);
    lsn1 = wal.append_record(h1, b1.data());
    lsn2 = wal.append_record(h2, b2.data());
  }  // WAL_mgr is destroyed.

  // Phase 2: Create new WAL_mgr; its constructor should build the index.
  boost::asio::io_context io_context;
  WAL_mgr wal_reader(wal_path, io_context.get_executor());
  LogRecordHeader hdr_out;
  std::vector<char> payload_out;

  // Test getting the first record
  ASSERT_TRUE(wal_reader.get_record(lsn1, hdr_out, payload_out));
  EXPECT_EQ(hdr_out.lsn, lsn1);
  EXPECT_EQ(hdr_out.type, UPDATE);
  auto* upd1 = reinterpret_cast<UpdatePagePayload*>(payload_out.data());
  EXPECT_EQ(upd1->page_id, 10);
  EXPECT_EQ(upd1->length, sizeof(v1));
  EXPECT_EQ(*reinterpret_cast<const uint32_t*>(upd1->aft()), v1);

  // Test getting the second record
  ASSERT_TRUE(wal_reader.get_record(lsn2, hdr_out, payload_out));
  EXPECT_EQ(hdr_out.lsn, lsn2);
  auto* upd2 = reinterpret_cast<UpdatePagePayload*>(payload_out.data());
  EXPECT_EQ(upd2->page_id, 11);
  EXPECT_EQ(upd2->length, sizeof(v2));
  EXPECT_EQ(*reinterpret_cast<const uint64_t*>(upd2->aft()), v2);

  // Test getting a non-existent record
  ASSERT_FALSE(wal_reader.get_record(999, hdr_out, payload_out));
}

TEST(WAL_mgr, IndexIsRebuiltOnStartup)
{
  auto wal_path = tmpfile("wal_index_rebuild.log");
  LSN lsn1, lsn2, lsn3;
  {
    boost::asio::io_context io_context;
    WAL_mgr wal(wal_path, io_context.get_executor());
    std::vector<char> dummy_buf(1);
    LogRecordHeader h{};
    h.type = BEGIN;
    h.lr_length = sizeof(h) + 1;
    lsn1 = wal.append_record(h, dummy_buf.data());
    h.lsn = 0;  // reset lsn for append_record to assign new
    lsn2 = wal.append_record(h, dummy_buf.data());
    h.lsn = 0;
    lsn3 = wal.append_record(h, dummy_buf.data());
  }  // Destructor runs, flushing data to disk.

  // Create a new instance. This triggers the constructor's index rebuild logic.
  boost::asio::io_context io_context;
  WAL_mgr wal_reloaded(wal_path, io_context.get_executor());

  // We can inspect the private member because of `#define private public`
  ASSERT_EQ(wal_reloaded.lsn_to_offset_idx_.size(), 3);
  EXPECT_TRUE(wal_reloaded.lsn_to_offset_idx_.count(lsn1));
  EXPECT_TRUE(wal_reloaded.lsn_to_offset_idx_.count(lsn2));
  EXPECT_TRUE(wal_reloaded.lsn_to_offset_idx_.count(lsn3));
  EXPECT_FALSE(wal_reloaded.lsn_to_offset_idx_.count(999));

  // Verify the offsets are reasonable (monotonic and correctly spaced)
  EXPECT_LT(wal_reloaded.lsn_to_offset_idx_.at(lsn1),
            wal_reloaded.lsn_to_offset_idx_.at(lsn2));
  EXPECT_LT(wal_reloaded.lsn_to_offset_idx_.at(lsn2),
            wal_reloaded.lsn_to_offset_idx_.at(lsn3));
}

TEST(WAL_mgr, GetRecordHandlesVaryingPayloadSizes)
{
  auto wal_path = tmpfile("wal_varying_payloads.log");
  std::vector<char> b_small, b_large;
  uint16_t val_small = 0xABCD;
  std::string val_large(100, 'X');
  LSN lsn_small, lsn_large;

  {
    boost::asio::io_context io_context;
    WAL_mgr wal(wal_path, io_context.get_executor());
    auto h_small = make_update(1, 1, 0, sizeof(val_small), &val_small, b_small);
    auto h_large =
        make_update(1, 2, 0, val_large.size(), val_large.data(), b_large);
    lsn_small = wal.append_record(h_small, b_small.data());
    lsn_large = wal.append_record(h_large, b_large.data());
  }

  boost::asio::io_context io_context;
  WAL_mgr wal_reader(wal_path, io_context.get_executor());
  LogRecordHeader hdr_out;
  std::vector<char> payload_out;

  // Verify small record
  ASSERT_TRUE(wal_reader.get_record(lsn_small, hdr_out, payload_out));
  ASSERT_EQ(payload_out.size(),
            sizeof(UpdatePagePayload) + 2 * sizeof(val_small));
  auto* upd_small = reinterpret_cast<UpdatePagePayload*>(payload_out.data());
  EXPECT_EQ(*reinterpret_cast<const uint16_t*>(upd_small->aft()), val_small);

  // Verify large record
  ASSERT_TRUE(wal_reader.get_record(lsn_large, hdr_out, payload_out));
  ASSERT_EQ(payload_out.size(),
            sizeof(UpdatePagePayload) + 2 * val_large.size());
  auto* upd_large = reinterpret_cast<UpdatePagePayload*>(payload_out.data());
  std::string after_str(reinterpret_cast<const char*>(upd_large->aft()),
                        upd_large->length);
  EXPECT_EQ(after_str, val_large);
}