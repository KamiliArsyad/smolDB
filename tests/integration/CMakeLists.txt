# This assumes gtest & gtest_main targets are available from the root CMake file.

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/tsan_options.cpp
        "extern \"C\" const char* __tsan_default_options() { return \"report_atomic_races=0\"; }"
)

# Explicitly list the integration test source files.
set(INTEGRATION_SRCS
        aries_crash_test.cpp
        aries_fuzz_test.cpp
        aries_test.cpp
        concurrency_fuzz_test.cpp
        concurrency_test.cpp
        heap_file_tombstone_test.cpp
        h_idx_test.cpp
        table_iterator_test.cpp
        test_access.cpp
        transaction_test.cpp
)

# Use a distinct name for the executable target.
add_executable(smoldb_integration_tests_run ${INTEGRATION_SRCS} ${CMAKE_CURRENT_BINARY_DIR}/tsan_options.cpp)

target_include_directories(smoldb_integration_tests_run
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/
)

target_link_libraries(smoldb_integration_tests_run gtest gtest_main backend_lib storage_lib access_lib executor_lib idx_lib)

# Use the executable target name in the COMMAND for add_test.
if (ENABLE_TSAN)
    find_program(SETARCH_COMMAND setarch)
    if (SETARCH_COMMAND AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
        execute_process(COMMAND uname -m OUTPUT_VARIABLE CMAKE_SYSTEM_PROCESSOR_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(ASLR_WRAPPER ${SETARCH_COMMAND} ${CMAKE_SYSTEM_PROCESSOR_ARCH} -R)
        add_test(
                NAME smoldb_integration_tests
                COMMAND ${ASLR_WRAPPER} $<TARGET_FILE:smoldb_integration_tests_run>
        )
    else ()
        add_test(NAME smoldb_integration_tests COMMAND smoldb_integration_tests_run)
    endif ()
else ()
    add_test(NAME smoldb_integration_tests COMMAND smoldb_integration_tests_run)
endif ()