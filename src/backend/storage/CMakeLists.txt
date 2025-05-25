file(GLOB STORAGE_SRCS
        *.cpp
        *.h
)

add_library(storage_lib STATIC ${STORAGE_SRCS}
        BufferPool.cpp
        BufferPool.h)

target_include_directories(storage_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
