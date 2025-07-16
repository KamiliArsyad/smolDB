file(GLOB PROC_SRCS
        *.cpp
        *.h
)

add_library(proc_lib STATIC ${PROC_SRCS})

target_include_directories(proc_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)