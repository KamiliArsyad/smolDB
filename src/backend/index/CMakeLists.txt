file(GLOB IDX_SRCS
        *.cpp
        *.h
)

add_library(idx_lib STATIC ${IDX_SRCS})

target_include_directories(idx_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
