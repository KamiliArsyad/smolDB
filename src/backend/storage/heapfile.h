#ifndef HEAPFILE_H
#define HEAPFILE_H
#include <filesystem>
#include <fstream>
#include <vector>
#include <stdexcept>

#include "storage.h"

/**
 * @brief HeapFile manages the logical access of records.
 * @details HeapFile is basically a logical "Table" abstraction.
 *            It understands page layout, record layout,
 *            free space management at the page level, record
 *            insertion/deletion/update management logic, and
 *            how to navigate between pages.
 * @tparam Tuple The record type.
 */
template <typename Tuple>
class HeapFile
{
  static_assert(std::is_trivially_copyable_v<Tuple> ||
                std::is_same_v<Tuple, std::vector<std::byte>>,
    "Tuple must be POD or std::vector<std::byte> for heap file");
private:
  std::filesystem::path path_;
  std::fstream file_;

  void open_file() {
    file_.open(path_, std::ios::in | std::ios::out |
                        std::ios::binary | std::ios::app);
    if (!file_) {  // create file if it doesn't exist
      file_.clear();
      file_.open(path_, std::ios::out | std::ios::binary);
      file_.close();
      file_.open(path_, std::ios::in | std::ios::out |
                          std::ios::binary | std::ios::app);
    }
    if (!file_) {
      throw std::runtime_error("Failed to open heap file: " + path_.string());
    }
  }

public:
  explicit HeapFile(std::filesystem::path path)
  : path_(std::move(path))
  {
    open_file();
  }

  RID append(const Tuple& t)
  {
    file_.seekp(0, std::ios::end);
    const uint64_t off = file_.tellp();

    if constexpr (std::is_same_v<Tuple, std::vector<std::byte>>) {
      // For variable-length data like std::vector<std::byte>,
      // we need to store the size first
      uint32_t size = static_cast<uint32_t>(t.size());
      file_.write(reinterpret_cast<const char*>(&size), sizeof(size));
      file_.write(reinterpret_cast<const char*>(t.data()), t.size());
    } else {
      // For fixed-size POD types
      file_.write(reinterpret_cast<const char*>(&t), sizeof(Tuple));
    }

    file_.flush();
    if (!file_) {
      throw std::runtime_error("Failed to write to heap file");
    }

    // For simplicity, RID is the byte offset divided by record size
    // This works for fixed-size records, but for variable-size we might
    // want a different RID scheme in the future
    const RID rid = off / (sizeof(Tuple) + (std::is_same_v<Tuple, std::vector<std::byte>> ? sizeof(uint32_t) : 0));
    return rid;
  }

  std::vector<Tuple> full_scan()
  {
    file_.seekg(0, std::ios::end);
    const auto file_size = file_.tellg();
    file_.seekg(0, std::ios::beg);

    std::vector<Tuple> vec;

    if constexpr (std::is_same_v<Tuple, std::vector<std::byte>>) {
      // Handle variable-length records
      while (file_.tellg() < file_size) {
        uint32_t size;
        file_.read(reinterpret_cast<char*>(&size), sizeof(size));
        if (file_.gcount() != sizeof(size)) break;

        std::vector<std::byte> record(size);
        file_.read(reinterpret_cast<char*>(record.data()), size);
        if (static_cast<uint32_t>(file_.gcount()) != size) break;

        vec.push_back(std::move(record));
      }
    } else {
      // Handle fixed-size records
      size_t n = file_size / sizeof(Tuple);
      vec.resize(n);
      file_.read(reinterpret_cast<char*>(vec.data()), file_size);
      if (file_.gcount() != file_size) {
        throw std::runtime_error("Failed to read complete heap file");
      }
    }

    return vec;
  }

  // Get record by RID (simplified implementation)
  Tuple get_record(RID rid) {
    if constexpr (std::is_same_v<Tuple, std::vector<std::byte>>) {
      // For variable-length records, we need to scan to find the right record
      // This is inefficient but works for our current use case
      auto all_records = full_scan();
      if (rid >= all_records.size()) {
        throw std::out_of_range("RID out of range");
      }
      return all_records[rid];
    } else {
      // For fixed-size records
      file_.seekg(rid * sizeof(Tuple), std::ios::beg);
      Tuple record;
      file_.read(reinterpret_cast<char*>(&record), sizeof(Tuple));
      if (file_.gcount() != sizeof(Tuple)) {
        throw std::runtime_error("Failed to read record at RID " + std::to_string(rid));
      }
      return record;
    }
  }

  // Get file path
  const std::filesystem::path& get_path() const { return path_; }
};

#endif //HEAPFILE_H