#ifndef DISKMANAGER_H
#define DISKMANAGER_H
#include <fstream>
#include <iosfwd>
#include <stdexcept>

#include "storage.h"

class Disk_mgr {
public:
  explicit Disk_mgr(const std::filesystem::path& db_file_path)
    : path_(db_file_path)
  {
    // Try open existing file
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
      // Create it if missing
      std::ofstream create(path_, std::ios::out | std::ios::binary);
      create.close();
      // Re-open for read/write
      file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!file_.is_open()) {
      throw std::runtime_error("DiskManager: cannot open file " + path_.string());
    }
  }
  ~Disk_mgr()
  {
    if (file_.is_open()) file_.close();
  }

  // Reads the PAGE_SIZE bytes for `page_id` into `page`.
  // If the file is too short, zero-fills the rest of `page`.
  void read_page(PageID page_id, Page& page);

  // Writes the PAGE_SIZE bytes from `page` at the offset for `page_id`.
  // Always flushes to ensure durability.
  void write_page(PageID page_id, const Page& page);

private:
  std::filesystem::path path_;
  std::fstream file_;

  // Compute the byte offset for the start of page `page_id`.
  static constexpr std::streamoff offset_for(PageID pid) {
    return static_cast<std::streamoff>(pid) * PAGE_SIZE;
  }

  void ensure_open();
};


#endif //DISKMANAGER_H
