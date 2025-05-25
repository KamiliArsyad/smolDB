add_subdirectory(googletest)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

set(UNIT_SRCS
        test1.cpp
        dump_load.cpp
        ${CMAKE_SOURCE_DIR}/src/backend/storage.cpp
        buffer_evict.cpp
        heap_file_load_scan.cpp
        wal-redo.cpp
)


# 'Google_Tests_run' is the target name
add_executable(Google_Tests_run ${UNIT_SRCS})

target_include_directories(Google_Tests_run
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/backend
)

target_link_libraries(Google_Tests_run gtest gtest_main backend_lib)