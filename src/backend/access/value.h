#pragma once
#include <boost/variant.hpp>
#include <chrono>
using datetime = std::chrono::system_clock::time_point;
using Value = boost::variant<int32_t, float, std::string, datetime>;
