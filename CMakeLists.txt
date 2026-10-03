cmake_minimum_required(VERSION 3.20)
project(Sequencer VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(EXCHANGE_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto" CACHE PATH
    "Path to the shared proto folder")
if(NOT TARGET exchange_proto)
  add_subdirectory(${EXCHANGE_PROTO_DIR} ${CMAKE_BINARY_DIR}/proto)
endif()

find_package(Threads REQUIRED)

add_library(sequencer_core STATIC src/partition.cpp src/service.cpp)
target_include_directories(sequencer_core PUBLIC include)
target_link_libraries(sequencer_core PUBLIC exchange_proto Threads::Threads)
target_compile_features(sequencer_core PUBLIC cxx_std_20)
target_compile_options(sequencer_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(sequencer src/main.cpp)
target_link_libraries(sequencer PRIVATE sequencer_core)
target_compile_options(sequencer PRIVATE -Wall -Wextra -Wpedantic)

option(SEQUENCER_BUILD_TESTS "Build sequencer tests" ${PROJECT_IS_TOP_LEVEL})
if(SEQUENCER_BUILD_TESTS)
  include(CTest)
  add_subdirectory(tests)
endif()
