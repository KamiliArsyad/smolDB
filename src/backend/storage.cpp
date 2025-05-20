#include "storage.h"
#include <atomic>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <fstream>

void Catalog::dump(const std::filesystem::path& path) const {
    std::ofstream ofs{path, std::ios::binary};
    boost::archive::binary_oarchive oa{ofs};
    oa << *this;
}

void Catalog::load(const std::filesystem::path& path) {
    std::ifstream ifs{path, std::ios::binary};
    boost::archive::binary_iarchive ia{ifs};
    ia >> *this;
}

FrameIter BufferPool::lookup_or_load_frame(PageID pid) {
  if (cache_.contains(pid))
  {
    auto it = cache_[pid];

    // Move to MRU
    lru_lists_.splice(lru_lists_.begin(), lru_lists_, it);
    return cache_[pid];
  }

  if (lru_lists_.size() == capacity_) {
    auto victim = std::prev(lru_lists_.end());
    while (victim->pin_count.load() != 0)
    {
      victim = std::prev(victim);
    }

    if (victim->pin_count.load() == 0)
    {
      flush_page(victim);
      cache_.erase(victim->page.hdr.id);
      lru_lists_.erase(victim);
    }
  }

  const FrameIter it = lru_lists_.emplace(lru_lists_.begin());
  disk_mgr_->read_page(pid, it->page);

  it->page.hdr.id = pid;
  it->pin_count.store(0, std::memory_order_relaxed);
  it->is_dirty.store(false, std::memory_order_relaxed);

  return cache_[pid] = it;
}

PageGuard BufferPool::fetch_page(PageID pid)
{
  const auto it = lookup_or_load_frame(pid);
  return PageGuard{this, it};
}

void BufferPool::unpin_page(PageID pid, bool mark_dirty)
{
  auto it = cache_.find(pid);
  if (it == cache_.end()) return;

  Frame &f = *it->second;

  if (mark_dirty) f.is_dirty.store(true, std::memory_order_relaxed);

  const int pins_left = f.pin_count.fetch_sub(
    1,
    std::memory_order_relaxed);
  assert(pins_left >= 0 && "pin_count_underflow");

  if (pins_left == 0)
  {
    // Move to the back for eviction.
    lru_lists_.splice(
      lru_lists_.end(),
      lru_lists_,
      it->second);
  }
}

void BufferPool::flush_page(FrameIter it) const
{
  if (!it->is_dirty.load()) return;

  wal_mgr_->flush_to_lsn(it->page.hdr.page_lsn);

  disk_mgr_->write_page(it->page.hdr.id, it->page);

  it->is_dirty.store(false);
}

void BufferPool::flush_all()
{
  for (auto it = lru_lists_.begin(); it != lru_lists_.end(); ++it)
  {
    flush_page(it);
  }
}

void Disk_mgr::ensure_open() {
  if (!file_.is_open()) {
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
      throw std::runtime_error("DiskManager: cannot reopen file " + path_.string());
    }
  }
}

void Disk_mgr::read_page(PageID page_id, Page& page) {
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
}

void Disk_mgr::write_page(PageID page_id, const Page& page) {
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

LSN WAL_mgr::append_record(LogRecordHeader& hdr, const void* payload)
{
  hdr.lsn = next_lsn_++;
  wal_stream_.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
  if (payload)
  {
    wal_stream_.write(static_cast<const char*>(payload), hdr.lr_length - sizeof(hdr));
  }
  wal_stream_.flush();
  flushed_lsn_ = hdr.lsn;
  return hdr.lsn;
}

void WAL_mgr::recover(BufferPool& bfr_manager, const std::filesystem::path& path)
{
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open())
  {
    throw std::runtime_error("Unable to open WAL file: " + path.string());
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break; // truncated header

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    in.read(buf.data(), payload_len);
    if (static_cast<uint32_t>(in.gcount()) != payload_len) {
      throw std::runtime_error("Corrupt WAL: unexpected payload size");
    }

    switch (hdr.type) {
    case UPDATE: {
        // reinterpret the payload buffer as our struct
        auto* upd = reinterpret_cast<const UpdatePagePayload*>(buf.data());

        // 1) pin the page
        PageGuard page = bfr_manager.fetch_page(upd->page_id);

        // 2) apply the "after" image
        std::memcpy(page->data() + upd->offset, upd->aft(), upd->length);

        // 3) bump page’s LSN so we don't reapply older records
        page->hdr.page_lsn = hdr.lsn;

        // 4) unpin as dirty so it'll be flushed later
        bfr_manager.unpin_page(upd->page_id, true);
        break;
    }

    case COMMIT:
    case ABORT:
      // nothing to do for now
      break;
    default:
      // skip unknown record types
      break;
    }
  }

  in.close();
}

