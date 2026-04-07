@echo off
REM ============================================================================
REM  DuckDB gRPC Server — Performance Benchmark
REM
REM  Runs performance tests against a running DuckDB gRPC server.
REM
REM  Prerequisites:
REM    1. Start the server first: run_server.bat
REM    2. The server must be listening on localhost:19100
REM
REM  Modes:
REM    --quick    Fast smoke test (4 scenarios, ~30 seconds)
REM               Tests: 1 reader, 10 readers, 1 writer, 10 writers
REM
REM    (default)  Standard suite (~3-5 minutes)
REM               Tests: readers 1-300, writers 1-300, mixed workloads,
REM               large result sets up to 24M rows
REM
REM    --full     Full suite (~15+ minutes)
REM               Standard + max concurrency search (ramp until 5%% errors)
REM               + 60-second sustained throughput tests
REM
REM  Metrics reported:
REM    - Throughput:  operations per second (higher = better)
REM    - Latency:     avg/min/max/P50/P95/P99 in milliseconds (lower = better)
REM    - Errors:      failed operations (should be 0)
REM
REM  Usage:
REM    run_benchmark.bat              Standard suite
REM    run_benchmark.bat --quick      Quick smoke test
REM    run_benchmark.bat --full       Full suite with max concurrency
REM    run_benchmark.bat --port 9000  Custom server port
REM ============================================================================

echo.
echo  DuckDB gRPC Server — Performance Benchmark
echo  =============================================
echo.

benchmark\bin\Release\DuckDbBenchmark.exe %*
