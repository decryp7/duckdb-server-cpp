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
#
# Arrow CMake packaging varies across versions and vcpkg baselines:
#   - Newer Arrow: separate ArrowFlight package (ArrowFlightConfig.cmake)
#   - Older Arrow / some vcpkg baselines: Flight is bundled in the Arrow package
# We try both approaches for maximum compatibility.
find_package(Arrow REQUIRED)

# Try finding ArrowFlight as a separate package first.
find_package(ArrowFlight QUIET)
if(NOT ArrowFlight_FOUND)
  # Fallback: check if Arrow Flight targets were exported by the Arrow package.
  if(TARGET arrow_flight_shared OR TARGET ArrowFlight::arrow_flight_shared)
    message(STATUS "ArrowFlight: found via Arrow package (bundled)")
    set(ArrowFlight_FOUND TRUE)
  else()
    # Last resort: try to find the library manually.
    find_library(ARROW_FLIGHT_LIBRARY NAMES arrow_flight
      PATHS ${Arrow_DIR}/../../lib
            ${Arrow_DIR}/../../../lib
            $ENV{VCPKG_ROOT}/installed/x64-windows/lib
            /usr/local/lib /usr/lib)
    find_path(ARROW_FLIGHT_INCLUDE_DIR arrow/flight/api.h
      PATHS ${Arrow_DIR}/../../include
            ${Arrow_DIR}/../../../include
            $ENV{VCPKG_ROOT}/installed/x64-windows/include
            /usr/local/include /usr/include)
    if(ARROW_FLIGHT_LIBRARY AND ARROW_FLIGHT_INCLUDE_DIR)
      message(STATUS "ArrowFlight: found manually at ${ARROW_FLIGHT_LIBRARY}")
      set(ArrowFlight_FOUND TRUE)
    else()
      message(FATAL_ERROR
        "ArrowFlight not found.\n"
        "  Make sure Arrow was installed with the [flight] feature:\n"
        "    vcpkg install \"arrow[flight]:x64-windows\"\n"
        "  Or install libarrow-flight-dev on Linux.")
    endif()
  endif()
else()
  message(STATUS "ArrowFlight: found via find_package")
endif()

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

# Link Arrow Flight — handle different target names across versions.
if(TARGET ArrowFlight::arrow_flight_shared)
  set(FLIGHT_TARGET ArrowFlight::arrow_flight_shared)
elseif(TARGET arrow_flight_shared)
  set(FLIGHT_TARGET arrow_flight_shared)
elseif(ARROW_FLIGHT_LIBRARY)
  set(FLIGHT_TARGET ${ARROW_FLIGHT_LIBRARY})
else()
  message(FATAL_ERROR "No Arrow Flight link target found")
endif()

target_link_libraries(duckdb_flight_server PRIVATE
  Arrow::arrow_shared
  ${FLIGHT_TARGET}
  ${DUCKDB_LIBRARY}
)

# Add manual include dir if ArrowFlight was found manually.
if(ARROW_FLIGHT_INCLUDE_DIR)
  target_include_directories(duckdb_flight_server PRIVATE ${ARROW_FLIGHT_INCLUDE_DIR})
endif()

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
