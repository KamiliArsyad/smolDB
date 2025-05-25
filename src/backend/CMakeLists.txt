file(GLOB BACKEND_SRCS
        *.cpp
        *.h
)

add_subdirectory(storage)
add_subdirectory(access)

add_library(backend_lib STATIC ${BACKEND_SRCS})

target_include_directories(backend_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(smolDB main.cpp)

find_package(
        Boost REQUIRED COMPONENTS serialization
)

# Might need to consider linking the library in private instead.
target_link_libraries(backend_lib
    PUBLIC Boost::serialization
    storage_lib
    access_lib
)

target_link_libraries(smolDB
        PRIVATE backend_lib
)