add_subdirectory(googletest)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

file(GLOB UNIT_SRCS
        *.cpp
)

# 'Google_Tests_run' is the target name
add_executable(Google_Tests_run ${UNIT_SRCS})

target_include_directories(Google_Tests_run
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/
)

target_link_libraries(Google_Tests_run gtest gtest_main backend_lib storage_lib access_lib executor_lib)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")