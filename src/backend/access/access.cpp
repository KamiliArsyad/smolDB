#include "access.h"

#include <fstream>
#include <sstream>
#include <cstring>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

// Row implementation
std::vector<std::byte> Row::to_bytes() const {
  std::ostringstream oss;
  boost::archive::binary_oarchive oa(oss);
  oa << *this;

  std::string str = oss.str();
  std::vector<std::byte> bytes(str.size());
  std::memcpy(bytes.data(), str.data(), str.size());
  return bytes;
}

Row Row::from_bytes(const std::vector<std::byte>& data, const Schema& schema) {
  std::string str(reinterpret_cast<const char*>(data.data()), data.size());
  std::istringstream iss(str);
  boost::archive::binary_iarchive ia(iss);

  Row row;
  ia >> row;

  // Ensure the schema matches (in case of schema evolution)
  if (row.schema_ != schema) {
    // For now, just update the schema. In the future, we might want schema migration logic
    row.schema_ = schema;
  }

  return row;
}

// Catalog implementation
void Catalog::dump(const std::filesystem::path& path) const {
  std::ofstream ofs{path, std::ios::binary};
  boost::archive::binary_oarchive oa{ofs};
  oa << *this;
}

void Catalog::load(const std::filesystem::path& path) {
  std::ifstream ifs{path, std::ios::binary};
  boost::archive::binary_iarchive ia{ifs};
  ia >> *this;

  // After loading, reinitialize tables if data directory is set
  if (!data_directory_.empty()) {
    reinit_tables();
  }
}
