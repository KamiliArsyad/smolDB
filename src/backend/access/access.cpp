#include "access.h"

#include <fstream>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

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

