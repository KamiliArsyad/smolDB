#ifndef DISKMANAGER_H
#define DISKMANAGER_H
#include <fstream>
#include <iosfwd>
#include <stdexcept>

#include "storage.h"

namespace smoldb
{

class Disk_mgr
{
 public:
  explicit Disk_mgr(const std::filesystem::path& db_file_path)
      : path_(db_file_path)
  {
    // Try open existing file
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open())
    {
      // Create it if missing
      std::ofstream create(path_, std::ios::out | std::ios::binary);
      create.close();
      // Re-open for read/write
      file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!file_.is_open())
    {
      throw std::runtime_error("DiskManager: cannot open file " +
                               path_.string());
    }

    // Initialize next_page_id_ based on current file size
    file_.seekg(0, std::ios::end);
    std::streamoff file_size_bytes = file_.tellg();
    if (file_size_bytes == -1)
    {  // Should not happen for a valid file
      file_size_bytes = 0;
    }

    PageID num_pages = file_size_bytes / PAGE_SIZE;
    // CRITICAL FIX: The next page to allocate is always after the last existing
    // page. If the file is empty or only has the header page, start allocating
    // from Page 1.
    next_page_id_.store(std::max((PageID)1, num_pages));
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

  /**
   * @brief Allocates a new {PageID} and conceptually extends the file.
   */
  PageID allocate_page();

 private:
  std::filesystem::path path_;
  std::fstream file_;
  std::mutex file_mutex_;
  std::atomic<PageID> next_page_id_;

  // Compute the byte offset for the start of page `page_id`.
  static constexpr std::streamoff offset_for(PageID pid)
  {
    return static_cast<std::streamoff>(pid) * PAGE_SIZE;
  }

  void ensure_open();
};

}  // namespace smoldb
#endif  // DISKMANAGER_H
