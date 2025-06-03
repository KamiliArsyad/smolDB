#include <cstring>
#include <gtest/gtest.h>

#include "bfrpl.h"
#include "dsk_mgr.h"
#include "wal_mgr.h"

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
static std::filesystem::path tmpfile(const char* name)
{
  auto p = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove(p);      // ensure a clean slate
  return p;
}

// Creates a minimal “UPDATE” record that overwrites `len` bytes starting at
// `offset` on `page_id` with the provided *value*.
static LogRecordHeader make_update(uint64_t txn,
                                   PageID      page_id,
                                   uint16_t    offset,
                                   uint16_t    len,
                                   const void* after_img,
                                   std::vector<char>& buf)
{
  // payload = header + 2×len bytes (before + after)
  const size_t pay_sz = sizeof(UpdatePagePayload) + len * 2;
  buf.resize(pay_sz);

  auto* upd   = reinterpret_cast<UpdatePagePayload*>(buf.data());
  upd->page_id = page_id;
  upd->offset  = offset;
  upd->length  = len;

  // before-image: zero-filled (or copy the real “before” value if you have it)
  std::memset(const_cast<std::byte*>(upd->bef()), 0, len);

  // after-image: copy caller-supplied bytes
  std::memcpy(const_cast<std::byte*>(upd->aft()), after_img, len);

  LogRecordHeader h{};
  h.type      = UPDATE;
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
  auto db_path  = tmpfile("wal_append_recover.db");

  // 1. Build a WAL with a single UPDATE
  WAL_mgr wal(wal_path);

  constexpr PageID  kPid   = 3;
  constexpr uint32_t kOff  = 40;
  constexpr uint32_t kLen  = 8;
  constexpr uint64_t kVal  = 0xDEADBEEFCAFEBABEull;

  std::vector<char> buf;
  auto hdr = make_update(/*txn=*/1, kPid, kOff, kLen, &kVal, buf);
  wal.append_record(hdr, buf.data());
  wal.flush_to_lsn(hdr.lsn);

  // 2. Simulate restart: open _new_ WAL_mgr + disk + buffer-pool and recover.
  WAL_mgr wal_replayer(wal_path);               // read-only role
  Disk_mgr disk(db_path);
  BufferPool pool(/*capacity=*/4, &disk, &wal_replayer);

  ASSERT_NO_THROW(wal_replayer.recover(pool, wal_path));

  PageGuard g = pool.fetch_page(kPid);
  uint64_t read_back{};
  std::memcpy(&read_back, g->data() + kOff, sizeof(read_back));
  EXPECT_EQ(read_back, kVal);                   // after-image applied
}

/**
 * LSNs must be monotonically increasing.
 */
TEST(WAL_mgr, MonotonicLSN)
{
  WAL_mgr wal(tmpfile("wal_monotonic.log"));

  std::vector<char> buf(1, 0);
  LogRecordHeader h1{}; h1.type = UPDATE; h1.lr_length = sizeof(h1) + 1;
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
  WAL_mgr wal(wal_path);

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
  BufferPool pool(2, &disk, &wal);

  wal.recover(pool, wal_path);
  PageGuard g = pool.fetch_page(kPid);
  uint32_t final;
  std::memcpy(&final, g->data() + kOff, sizeof(final));
  EXPECT_EQ(final, v2);          // newest value survives
}
