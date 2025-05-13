cmake_minimum_required(VERSION 3.31)
project(smolDB)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(src)
add_subdirectory(tests/unit)

enable_testing()
