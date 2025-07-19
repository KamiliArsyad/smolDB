#pragma once
#include <boost/variant.hpp>
#include <chrono>

namespace smoldb
{
using datetime = std::chrono::system_clock::time_point;
using Value = boost::variant<int32_t, float, std::string, datetime>;
}  // namespace smoldb
