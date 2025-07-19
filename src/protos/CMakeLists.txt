# This file handles the generation of C++ code from .proto files.

# Find the .proto file in the current directory.
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")

# Generate C++ source and header files from the .proto file.
# The generated files will be placed in the build directory.
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Create a library from the generated source files.
# This keeps our generated code neatly packaged.
add_library(smoldb_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})

# The library needs to include the directory where Protobuf headers are found.
target_include_directories(smoldb_proto PUBLIC ${PROTOBUF_INCLUDE_DIRS})

# Link the generated code library against the main Protobuf library.
target_link_libraries(smoldb_proto PUBLIC protobuf::libprotobuf)