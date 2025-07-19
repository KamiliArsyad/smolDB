# This file builds the gRPC server executable.

add_library(server_lib STATIC server.cpp)

target_include_directories(server_lib
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src/backend
)

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
        gRPC::grpc++_reflection
        smoldb_proto
)

add_executable(smolDB_server main.cpp server.cpp)

target_link_libraries(smolDB_server
        PRIVATE
        server_lib
)