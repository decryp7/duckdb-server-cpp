@echo off
REM ============================================================================
REM  DuckDB gRPC Server (C++ Edition) — High Performance Launch Script
REM
REM  This script starts the C++ DuckDB gRPC server.
REM  Uses the same proto/duckdb_service.proto as the C# server.
REM  Compatible with the C# client and benchmark.
REM
REM  Prerequisites:
REM    - Build the C++ project in VS2017 (requires vcpkg with gRPC + DuckDB)
REM    - VCPKG_ROOT environment variable must be set
REM
REM  The C++ server uses the DuckDB C API directly for maximum performance:
REM    - Zero-copy query results via duckdb_value_varchar
REM    - Connection pool for concurrent reads
REM    - Write serializer with INSERT merging
REM
REM  Usage:
REM    run_cpp_server.bat
REM    run_cpp_server.bat --db C:\data\analytics.duckdb
REM    run_cpp_server.bat --port 9000 --readers 32
REM
REM  Stop: Press Ctrl+C in this window
REM ============================================================================

echo.
echo  DuckDB gRPC Server (C++) — High Performance Mode
echo  ==================================================
echo.

cpp\bin\Debug\DuckFlightServerCpp.exe ^
    --port 17777 ^
    --readers 64 ^
    --batch-ms 50 ^
    --batch-max 5000 ^
    %*
