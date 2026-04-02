cmake_minimum_required(VERSION 3.14)
project(duckdb_grpc_server CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(MSVC AND MSVC_VERSION LESS 1910)
  message(FATAL_ERROR "Visual Studio 2017 (MSVC 19.10+) or later is required.")
endif()

# ── Apache Arrow (for IPC serialization, no Flight needed) ────────────────────
find_package(Arrow REQUIRED)

# ── gRPC + Protobuf ──────────────────────────────────────────────────────────
find_package(Protobuf REQUIRED)
find_package(gRPC QUIET)

if(NOT gRPC_FOUND)
  # Try pkg-config fallback
  find_library(GRPC_LIBRARY NAMES grpc++ grpcpp
    PATHS $ENV{VCPKG_ROOT}/installed/x64-windows/lib /usr/local/lib /usr/lib)
  find_library(GRPC_GRPC_LIBRARY NAMES grpc
    PATHS $ENV{VCPKG_ROOT}/installed/x64-windows/lib /usr/local/lib /usr/lib)
  find_path(GRPC_INCLUDE_DIR grpcpp/grpcpp.h
    PATHS $ENV{VCPKG_ROOT}/installed/x64-windows/include /usr/local/include /usr/include)

  if(NOT GRPC_LIBRARY OR NOT GRPC_INCLUDE_DIR)
    message(FATAL_ERROR
      "gRPC not found.\n"
      "  vcpkg: vcpkg install grpc:x64-windows\n")
  endif()
  message(STATUS "gRPC: found manually at ${GRPC_LIBRARY}")
endif()

# ── Find protoc and grpc_cpp_plugin ───────────────────────────────────────────
find_program(PROTOC protoc PATHS $ENV{VCPKG_ROOT}/installed/x64-windows/tools/protobuf)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATHS $ENV{VCPKG_ROOT}/installed/x64-windows/tools/grpc)

if(NOT PROTOC)
  message(FATAL_ERROR "protoc not found. Install protobuf tools.")
endif()
if(NOT GRPC_CPP_PLUGIN)
  message(FATAL_ERROR "grpc_cpp_plugin not found. Install gRPC tools.")
endif()

message(STATUS "protoc: ${PROTOC}")
message(STATUS "grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")

# ── Generate C++ from proto ───────────────────────────────────────────────────
set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/duckdb_service.proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRCS ${PROTO_GEN_DIR}/duckdb_service.pb.cc)
set(PROTO_HDRS ${PROTO_GEN_DIR}/duckdb_service.pb.h)
set(GRPC_SRCS  ${PROTO_GEN_DIR}/duckdb_service.grpc.pb.cc)
set(GRPC_HDRS  ${PROTO_GEN_DIR}/duckdb_service.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND ${PROTOC}
    --cpp_out=${PROTO_GEN_DIR}
    --grpc_out=${PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
    duckdb_service.proto
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating C++ from duckdb_service.proto"
)

# ── DuckDB ────────────────────────────────────────────────────────────────────
find_path(DUCKDB_INCLUDE_DIR duckdb.h
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb/include
        $ENV{VCPKG_ROOT}/installed/x64-windows/include
        /usr/local/include /usr/include)
find_library(DUCKDB_LIBRARY NAMES duckdb duckdb_static
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb/lib
        $ENV{VCPKG_ROOT}/installed/x64-windows/lib
        /usr/local/lib /usr/lib)

if(NOT DUCKDB_INCLUDE_DIR OR NOT DUCKDB_LIBRARY)
  message(FATAL_ERROR
    "DuckDB not found.\n"
    "  Place duckdb.h + duckdb.lib under third_party/duckdb/{include,lib}")
endif()

message(STATUS "Arrow     : ${Arrow_VERSION}")
message(STATUS "DuckDB    : ${DUCKDB_INCLUDE_DIR}")

# ── Executable ────────────────────────────────────────────────────────────────
add_executable(duckdb_grpc_server
  src/main_grpc.cpp
  src/grpc_server.cpp
  src/connection_pool.cpp
  ${PROTO_SRCS}
  ${GRPC_SRCS}
)

target_include_directories(duckdb_grpc_server PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${DUCKDB_INCLUDE_DIR}
  ${PROTO_GEN_DIR}
)

# Link libraries
if(gRPC_FOUND)
  target_link_libraries(duckdb_grpc_server PRIVATE
    Arrow::arrow_shared
    gRPC::grpc++
    protobuf::libprotobuf
    ${DUCKDB_LIBRARY}
  )
else()
  target_include_directories(duckdb_grpc_server PRIVATE ${GRPC_INCLUDE_DIR})
  target_link_libraries(duckdb_grpc_server PRIVATE
    Arrow::arrow_shared
    ${GRPC_LIBRARY}
    ${GRPC_GRPC_LIBRARY}
    protobuf::libprotobuf
    ${DUCKDB_LIBRARY}
  )
endif()

# ── Windows ───────────────────────────────────────────────────────────────────
if(WIN32)
  target_compile_definitions(duckdb_grpc_server PRIVATE
    _WIN32_WINNT=0x0601
    WIN32_LEAN_AND_MEAN
    NOMINMAX
    _CRT_SECURE_NO_WARNINGS
  )
endif()

# ── Compiler flags ────────────────────────────────────────────────────────────
if(MSVC)
  target_compile_options(duckdb_grpc_server PRIVATE
    /std:c++14 /MP /W3 /wd4100 /wd4127 /wd4244 /wd4267
    $<$<CONFIG:Release>:/O2 /GL>
  )
  target_link_options(duckdb_grpc_server PRIVATE
    $<$<CONFIG:Release>:/LTCG>
  )
else()
  target_compile_options(duckdb_grpc_server PRIVATE
    -Wall -Wextra
    $<$<CONFIG:Release>:-O2>
  )
endif()

# ── Post-build: copy DuckDB DLL ──────────────────────────────────────────────
if(WIN32)
  get_filename_component(DUCKDB_DIR "${DUCKDB_LIBRARY}" DIRECTORY)
  set(DUCKDB_DLL "${DUCKDB_DIR}/duckdb.dll")
  if(EXISTS "${DUCKDB_DLL}")
    add_custom_command(TARGET duckdb_grpc_server POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${DUCKDB_DLL}" "$<TARGET_FILE_DIR:duckdb_grpc_server>"
      COMMENT "Copying duckdb.dll")
  endif()
endif()

install(TARGETS duckdb_grpc_server RUNTIME DESTINATION bin)
