#include <gtest/gtest.h>
#define private public              // <- test-only peek
#include "../../src/backend/storage.h"
#undef  private

/* -------- helpers --------------------------------------------------- */
static auto frame_of(BufferPool& bp, PageID pid) {
  return bp.cache_.at(pid);       // cache_ has been made public by the macro
}

/* -------- tests ----------------------------------------------------- */

TEST(BufferPool, PinUnpinCounts)
{
  BufferPool bp{/*capacity=*/2};

  {
    auto g1 = bp.fetch_page(7);
    auto f = frame_of(bp, 7);
    EXPECT_EQ(f->pin_count.load(), 1);

    {
      auto g2 = bp.fetch_page(7);      // same page, extra pin
      EXPECT_EQ(f->pin_count.load(), 2);
    }                                   // g2 dtor
    EXPECT_EQ(f->pin_count.load(), 1);
  }                                       // g1 dtor
  EXPECT_EQ(frame_of(bp, 7)->pin_count.load(), 0);
}

TEST(BufferPool, DirtyBitPersists)
{
  BufferPool bp{1};
  {
    auto g = bp.fetch_page(13);
    g.mark_dirty();
  }                                       // unpin → dirty=true

  EXPECT_TRUE(frame_of(bp, 13)->is_dirty.load());
}

TEST(BufferPool, EvictionAtCapacityOne)
{
  BufferPool bp{1};

  /* first page fills the only slot */
  { auto g = bp.fetch_page(1); }

  /* second page forces eviction of page 1 */
  { auto g = bp.fetch_page(2); }

  EXPECT_EQ(frame_of(bp, 2)->page.hdr.id, 2);
  EXPECT_EQ(bp.cache_.size(), 1);
  EXPECT_TRUE(bp.cache_.find(1) == bp.cache_.end());
}

TEST(BufferPool, MRUTouchOnHit)
{
  BufferPool bp{2};

  { bp.fetch_page(1); }          // LRU list: [1]
  { bp.fetch_page(2); }          //           [2,1] (2 = MRU)

  /* re-touch page-1 → becomes MRU ([1,2]) */
  { bp.fetch_page(1); }
  auto lru_tail = std::prev(bp.lru_lists_.end());
  EXPECT_EQ(lru_tail->page.hdr.id, 2);
}

TEST(BufferPool, NoErrorOverload)
{
  BufferPool bp{100};

  for (int i = 0; i < 1000; i++)
  {
    bp.fetch_page(i);
    bp.fetch_page(i);
  }
}