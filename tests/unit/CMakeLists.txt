add_subdirectory(googletest)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# --- WORKAROUND for TSan static init bug ---
# Create a temporary source file to define the __tsan_default_options function.
# This is cleaner than adding it to one of the test source files.
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/tsan_options.cpp
        "extern \"C\" const char* __tsan_default_options() { return \"report_atomic_races=0\"; }"
)

file(GLOB UNIT_SRCS
        *.cpp
)

# Add our temporary options file to the list of sources for the executable
add_executable(Google_Tests_run ${UNIT_SRCS} ${CMAKE_CURRENT_BINARY_DIR}/tsan_options.cpp)

target_include_directories(Google_Tests_run
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/
)

target_link_libraries(Google_Tests_run gtest gtest_main backend_lib storage_lib access_lib executor_lib idx_lib)

enable_testing()

if (ENABLE_TSAN) # Check the TSan option from your root CMakeLists.txt
    message(STATUS "TSan is enabled, configuring ASLR wrapper for tests.")

    # Find the setarch utility.
    find_program(SETARCH_COMMAND setarch)

    if (SETARCH_COMMAND AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
        # On Linux, construct the wrapper command.
        execute_process(
                COMMAND uname -m
                OUTPUT_VARIABLE CMAKE_SYSTEM_PROCESSOR_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        set(ASLR_WRAPPER ${SETARCH_COMMAND} ${CMAKE_SYSTEM_PROCESSOR_ARCH} -R)
        message(STATUS "Found setarch: Disabling ASLR for tests to ensure TSan stability.")

        # Define a test named 'smoldb_unit_tests'.
        # The COMMAND specifies the wrapper AND the full path to the executable.
        # The $<TARGET_FILE:...> is a CMake "generator expression" that correctly
        # resolves to the full path of the executable, fixing the "No such file" error.
        add_test(
                NAME smoldb_unit_tests
                COMMAND ${ASLR_WRAPPER} $<TARGET_FILE:Google_Tests_run>
        )
    else ()
        # If not on Linux or setarch isn't found, add the test without the wrapper.
        add_test(NAME smoldb_unit_tests COMMAND Google_Tests_run)
    endif ()
else ()
    # If TSan is NOT enabled, just add the test normally.
    add_test(NAME smoldb_unit_tests COMMAND Google_Tests_run)
endif ()