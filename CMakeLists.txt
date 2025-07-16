cmake_minimum_required(VERSION 3.31)
project(smolDB)

set(CMAKE_CXX_STANDARD 20)

# Centralized control for ThreadSanitizer
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_TSAN)
    message(STATUS "ThreadSanitizer enabled")
    # Add TSan compiler and linker flags
    add_compile_options(-fsanitize=thread -g -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

add_subdirectory(src)

enable_testing()
add_subdirectory(tests/unit)
add_subdirectory(tests/integration)