cmake_minimum_required(VERSION 3.14)
project(duckdb_flight_server CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ── MSVC version guard ────────────────────────────────────────────────────────
if(MSVC AND MSVC_VERSION LESS 1910)
  message(FATAL_ERROR "Visual Studio 2017 (MSVC 19.10+) or later is required.")
endif()

# ── Apache Arrow + Arrow Flight ───────────────────────────────────────────────
# Minimum Arrow version: 3.0.0 (IpcWriteOptions, Flight stable)
# Recommended:           12.0.0+
#
# vcpkg (recommended):
#   vcpkg install "arrow[flight,parquet]:x64-windows"
#
# Ubuntu:
#   apt install libarrow-flight-dev   (or libarrow-dev + cmake -DArrow_FIND_COMPONENTS=Flight)
#
# The [flight] feature pulls gRPC and protobuf automatically.
find_package(Arrow       REQUIRED)
find_package(ArrowFlight REQUIRED)

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
    "  vcpkg:  vcpkg install duckdb:x64-windows\n"
    "  manual: place duckdb.h + duckdb.lib under third_party/duckdb/{include,lib}\n"
    "  https://github.com/duckdb/duckdb/releases/latest")
endif()

message(STATUS "Arrow     : ${Arrow_VERSION}")
message(STATUS "DuckDB    : ${DUCKDB_INCLUDE_DIR}")

# ── Executable ────────────────────────────────────────────────────────────────
add_executable(duckdb_flight_server
  src/main.cpp
  src/flight_server.cpp
  src/duck_bridge.cpp
  src/connection_pool.cpp
)

target_include_directories(duckdb_flight_server PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${DUCKDB_INCLUDE_DIR}
)

target_link_libraries(duckdb_flight_server PRIVATE
  Arrow::arrow_shared
  ArrowFlight::arrow_flight_shared
  ${DUCKDB_LIBRARY}
)

# ── Windows ───────────────────────────────────────────────────────────────────
if(WIN32)
  target_compile_definitions(duckdb_flight_server PRIVATE
    _WIN32_WINNT=0x0601   # Windows 7+
    WIN32_LEAN_AND_MEAN
    NOMINMAX
    _CRT_SECURE_NO_WARNINGS
  )
  # NOTE: no ws2_32 / mswsock needed — gRPC (via Arrow Flight) handles networking
endif()

# ── Compiler flags ────────────────────────────────────────────────────────────
if(MSVC)
  target_compile_options(duckdb_flight_server PRIVATE
    /std:c++14 /MP /W3 /wd4100 /wd4127
    $<$<CONFIG:Release>:/O2 /GL>
  )
  target_link_options(duckdb_flight_server PRIVATE
    $<$<CONFIG:Release>:/LTCG>
  )
else()
  target_compile_options(duckdb_flight_server PRIVATE
    -Wall -Wextra
    $<$<CONFIG:Release>:-O2>
  )
endif()

# ── Post-build: copy DuckDB DLL ───────────────────────────────────────────────
if(WIN32)
  get_filename_component(DUCKDB_DIR "${DUCKDB_LIBRARY}" DIRECTORY)
  set(DUCKDB_DLL "${DUCKDB_DIR}/duckdb.dll")
  if(EXISTS "${DUCKDB_DLL}")
    add_custom_command(TARGET duckdb_flight_server POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${DUCKDB_DLL}" "$<TARGET_FILE_DIR:duckdb_flight_server>"
      COMMENT "Copying duckdb.dll")
  endif()
endif()

install(TARGETS duckdb_flight_server RUNTIME DESTINATION bin)
