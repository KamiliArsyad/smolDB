# This file builds the gRPC server executable.

add_library(server_lib STATIC server.cpp)

target_include_directories(server_lib
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src/backend
)

include(FetchContent)
FetchContent_Declare(
        asio-grpc
        GIT_REPOSITORY https://github.com/Tradias/asio-grpc.git
        GIT_TAG v3.5.0
)
FetchContent_MakeAvailable(asio-grpc)

find_package(Boost REQUIRED COMPONENTS context)

target_link_libraries(server_lib
        PUBLIC
        -Wl,--start-group
        backend_lib
        storage_lib
        access_lib
        executor_lib
        idx_lib
        proc_lib
        -Wl,--end-group
        smoldb_proto
        asio-grpc::asio-grpc
        Boost::context
)

add_executable(smolDB_server main.cpp server.cpp)

target_link_libraries(smolDB_server
        PRIVATE
        server_lib
)