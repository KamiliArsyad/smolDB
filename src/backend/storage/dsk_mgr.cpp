#include "dsk_mgr.h"

#include <cstring>
#include <mutex>

void Disk_mgr::ensure_open() {
  // This is an internal helper, assumes caller holds the lock.
  assert(path_ != "");
  if (!file_.is_open()) {
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
      throw std::runtime_error("DiskManager: cannot reopen file " + path_.string());
    }
  }
}

void Disk_mgr::read_page(PageID page_id, Page& page) {
  std::scoped_lock lock(file_mutex_);
  ensure_open();
  auto off = offset_for(page_id);
  file_.seekg(off, std::ios::beg);
  if (!file_) {
    file_.clear();
    // reading beyond EOF: zero-fill entire page
    std::memset(&page, 0, PAGE_SIZE);
    return;
  }

  file_.read(reinterpret_cast<char*>(&page), PAGE_SIZE);
  std::streamsize got = file_.gcount();
  if (got < static_cast<std::streamsize>(PAGE_SIZE)) {
    // zero-fill the rest
    std::memset(reinterpret_cast<char*>(&page) + got, 0, PAGE_SIZE - got);
  }

  // Clear eofbit/failbit after a partial read, so the stream is usable again.
  file_.clear();
}

void Disk_mgr::write_page(PageID page_id, const Page& page) {
  std::scoped_lock lock(file_mutex_);
  ensure_open();
  auto off = offset_for(page_id);
  file_.seekp(off, std::ios::beg);
  if (!file_) {
    throw std::runtime_error("DiskManager: seekp failed");
  }

  file_.write(reinterpret_cast<const char*>(&page), PAGE_SIZE);
  if (!file_) {
    throw std::runtime_error("DiskManager: write failed");
  }

  file_.flush();
  if (!file_) {
    throw std::runtime_error("DiskManager: flush failed");
  }
}

PageID Disk_mgr::allocate_page() {
  // Atomically fetch and increment. The returned value is the new PageID.
  // This effectively reserves a new page ID. The actual file extension
  // happens when write_page is called for this new page_id if it's beyond
  // the current file size.
  return next_page_id_.fetch_add(1);
}