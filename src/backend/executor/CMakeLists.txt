file(GLOB EXECUTOR_SRCS
        *.cpp
        *.h
)

add_library(executor_lib STATIC ${EXECUTOR_SRCS})

target_include_directories(executor_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
