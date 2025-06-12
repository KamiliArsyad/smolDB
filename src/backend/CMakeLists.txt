# Define the source files for the backend library explicitly.
# Exclude main.cpp, which is only for the final executable.
set(BACKEND_LIB_SRCS
        smoldb.cpp
        server.cpp
)

# Define the header files. While not strictly needed for compilation here,
# it's good practice for IDEs and dependency tracking.
set(BACKEND_LIB_HDRS
        smoldb.h
        server.h
)

# Include subdirectories that define their own libraries (storage_lib, access_lib)
add_subdirectory(storage)
add_subdirectory(access)
add_subdirectory(executor)

# Create the library from its specific sources.
add_library(backend_lib STATIC
        ${BACKEND_LIB_SRCS}
        ${BACKEND_LIB_HDRS}
)

# The library needs to know where its own headers are.
target_include_directories(backend_lib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

# The final executable for the backend application.
add_executable(smolDB main.cpp)

# Find necessary external packages.
find_package(
        Boost REQUIRED COMPONENTS serialization
)

# Link the backend library against its dependencies.
# The executable will get these transitively when it links backend_lib.
target_link_libraries(backend_lib
        PUBLIC
        Boost::serialization
        storage_lib
        access_lib
        executor_lib
)

# Link the main executable against our newly defined backend library.
target_link_libraries(smolDB
        PRIVATE backend_lib
)