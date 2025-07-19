#[[
Apparently making the proper CMakeLists for gRPC protobuf file generation is super hard.
I spent a whole day trying to learn this, run things, re-writing whole script, etc. to finally make this run.
Tho lots of what's written here is infused with LLM-generated parts (like the custom command thing)
and multiple patches here and there. So, take the design correctness of this file with a grain of salt.
]]

# DO NOT USE find_path. We need to construct the path to the proto file directly.
set(PROTO_FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/smoldb.proto")

# Explicitly define the names of all files that will be generated.
set(PROTO_PB_H "${CMAKE_CURRENT_BINARY_DIR}/smoldb.pb.h")
set(PROTO_PB_CC "${CMAKE_CURRENT_BINARY_DIR}/smoldb.pb.cc")
set(PROTO_GRPC_PB_H "${CMAKE_CURRENT_BINARY_DIR}/smoldb.grpc.pb.h")
set(PROTO_GRPC_PB_CC "${CMAKE_CURRENT_BINARY_DIR}/smoldb.grpc.pb.cc")


# This custom command tells CMake exactly how to generate the gRPC files.
add_custom_command(
        # OUTPUT: Specifies all files this command will create.
        OUTPUT ${PROTO_PB_H} ${PROTO_PB_CC} ${PROTO_GRPC_PB_H} ${PROTO_GRPC_PB_CC}

        # COMMAND: Use the VARIABLE provided by FindProtobuf, not a target name.
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}

        # ARGS: The arguments passed to protoc.
        ARGS
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        -I${CMAKE_CURRENT_SOURCE_DIR}
        ${PROTO_FILE_PATH} # <-- AND THIS NOW CORRECTLY USES THE FULL FILE PATH

        # DEPENDS: This ensures the rule re-runs if smoldb.proto changes.
        DEPENDS ${PROTO_FILE_PATH}

        # COMMENT: A message that will be printed during the build.
        COMMENT "Generating Protobuf and gRPC C++ sources from smoldb.proto"
)

add_library(smoldb_proto STATIC
        ${PROTO_PB_CC}
        ${PROTO_GRPC_PB_CC}
)

# Make the generated headers' directory available to any target that links this library.
target_include_directories(smoldb_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${PROTOBUF_INCLUDE_DIRS}
)

# The library depends on Protobuf and gRPC++.
target_link_libraries(smoldb_proto
        PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)