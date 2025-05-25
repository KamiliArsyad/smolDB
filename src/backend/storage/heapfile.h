#ifndef HEAPFILE_H
#define HEAPFILE_H
#include <filesystem>
#include <fstream>

#include "storage.h"

template <typename Tuple>
class HeapFile
{
  static_assert(std::is_trivially_copyable_v<Tuple>,
    "Tuple must be POD for raw-copy heap file v0");
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
  }

public:
  explicit HeapFile(std::filesystem::path path)
  : path_(path)
  {
    open_file();
  }

  RID append(const Tuple& t)
  {
    file_.seekp(0, std::ios::end);
    const uint64_t off = file_.tellp();
    file_.write(reinterpret_cast<const char*>(&t), sizeof(Tuple));
    file_.flush();
    const RID rid = off / sizeof(Tuple);  // simple RID for now
    return rid;
  }

  std::vector<Tuple> full_scan()
  {
    file_.seekg(0, std::ios::end);
    const auto bytes = file_.tellg();
    size_t n   = bytes / sizeof(Tuple);
    std::vector<Tuple> vec(n);
    file_.seekg(0);
    file_.read(reinterpret_cast<char*>(vec.data()), bytes);

    return vec;
  }
};

#endif //HEAPFILE_H
