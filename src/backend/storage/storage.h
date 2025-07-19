// ===== ../smolDB/src/backend/storage/storage.h =====

#ifndef STORAGE_H
#define STORAGE_H
#include <array>
#include <atomic>
#include <boost/serialization/binary_object.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <list>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace smoldb
{

typedef uint64_t PageID;
using LSN = uint64_t;
constexpr size_t PAGE_SIZE = 8192;
constexpr PageID INVALID_PAGE_ID = static_cast<PageID>(-1);

struct PageHeader
{
  PageID id;
  LSN page_lsn;  // Latest LSN of the log that modifies this page.
};

struct alignas(64) Page
{
  PageHeader hdr;
  std::array<std::byte, PAGE_SIZE - sizeof(PageHeader)> raw_array;

  [[nodiscard]]
  std::byte* data() noexcept
  {
    return raw_array.data();
  }
  [[nodiscard]]
  const std::byte* data() const noexcept
  {
    return raw_array.data();
  }
};

static_assert(sizeof(Page) == PAGE_SIZE, "Incorrect page size");

struct Frame
{
  Page page;
  std::atomic<bool> is_dirty = false;
  std::atomic<int> pin_count = 0;
  // Physical latch protecting the raw data of the page.
  // Must be acquired before any read/write to page.data().
  std::shared_mutex page_latch;
};

using FrameIter = std::list<Frame>::iterator;

}  // namespace smoldb

#endif  // STORAGE_H