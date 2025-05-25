file(GLOB ACCESS_SRCS
        *.cpp
        *.h
)

add_library(access_lib STATIC ${ACCESS_SRCS})

target_include_directories(access_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
