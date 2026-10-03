include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG v3.7.1
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(Catch2)

# Catch2's CMake helpers (Catch.cmake) live in its extras/ folder.
FetchContent_GetProperties(Catch2)
list(APPEND CMAKE_MODULE_PATH "${catch2_SOURCE_DIR}/extras")

add_executable(sequencer_tests
    test_partition.cpp
    test_contract.cpp
)
target_link_libraries(sequencer_tests PRIVATE sequencer_core
                      Catch2::Catch2WithMain)
target_compile_options(sequencer_tests PRIVATE -Wall -Wextra -Wpedantic)

include(Catch)
# PRE_TEST: list the tests when ctest runs, not right after linking. A
# ThreadSanitizer build can crash at startup on some kernels (run it via
# `setarch $(uname -m) -R`), and POST_BUILD discovery would turn that into a
# build failure.
catch_discover_tests(sequencer_tests DISCOVERY_MODE PRE_TEST)
