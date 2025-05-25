#ifndef ACCESS_H
#define ACCESS_H
#include <cstdint>
#include <filesystem>
#include <string>

#include <boost/serialization/access.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>

enum class Col_type
{
  INT,
  FLOAT,
  STRING,
  DATETIME
};

struct Column
{
  uint8_t   id;
  std::string name;
  Col_type   type;
  bool      nullable;
  std::vector<std::byte> default_bytes;

private:
  friend class boost::serialization::access;
  template<class Ar>
  void serialize(Ar& ar, unsigned) {
    ar & id & name & type & nullable & default_bytes;
  }
};

using Schema = std::vector<Column>;

class Catalog
{
  std::unordered_map<uint8_t,Schema> m_tables;

  friend class boost::serialization::access;
  template<class Ar>
  void serialize(Ar& ar, unsigned) {
    ar & m_tables;
  }
public:
  void register_schema(const uint8_t table_id, Schema s)
  {
    m_tables[table_id] = std::move(s);
  }

  void dump(const std::filesystem::__cxx11::path& path) const;

  void load(const std::filesystem::__cxx11::path& path);
};

#endif //ACCESS_H
