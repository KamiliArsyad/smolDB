#pragma once
#include <boost/variant.hpp>
#include <chrono>

namespace smoldb
{

enum class Col_type
{
  INT,
  FLOAT,
  STRING,
  DATETIME
};

using INT_TYPE = int32_t;
using FLOAT_TYPE = float;
using STRING_TYPE = std::string;
using DATETIME_TYPE = std::chrono::system_clock::time_point;

constexpr size_t type_size(Col_type t)
{
  switch (t)
  {
    case Col_type::INT:
      return sizeof(INT_TYPE);
    case Col_type::FLOAT:
      return sizeof(FLOAT_TYPE);
    case Col_type::STRING:
      return 0;  // String size is not fixed here
    case Col_type::DATETIME:
      return sizeof(DATETIME_TYPE);
  }
  return 0;
}

using Value = boost::variant<int32_t, float, std::string, DATETIME_TYPE>;
}  // namespace smoldb
