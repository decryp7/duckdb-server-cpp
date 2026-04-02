@echo off
REM ============================================================================
REM  DuckDB gRPC Server — High Performance Launch Script
REM
REM  This script starts the C# DuckDB gRPC server with settings optimized
REM  for maximum concurrent read and write throughput.
REM
REM  Configuration:
REM    --db :memory:       In-memory database (fastest, no disk I/O)
REM                        Change to a file path for persistence:
REM                        --db C:\data\analytics.duckdb
REM
REM    --port 17777        Default gRPC listen port
REM
REM    --readers 64        Connection pool size for concurrent SELECT queries.
REM                        Each concurrent DoGet/Query uses one connection.
REM                        Set higher if you have many parallel readers.
REM                        Rule of thumb: 2x to 4x your CPU core count.
REM
REM    --batch-ms 50       Write batching window in milliseconds.
REM                        The server collects INSERT/UPDATE/DELETE statements
REM                        over this window and executes them in one transaction.
REM                        Higher = more writes per transaction = higher throughput
REM                        but higher latency for individual writes.
REM                        Default: 5ms. Set to 50ms for write-heavy workloads.
REM
REM    --batch-max 5000    Maximum statements per write transaction.
REM                        Once this many writes accumulate, the batch flushes
REM                        immediately regardless of the time window.
REM                        Higher = better throughput during write spikes.
REM
REM    --batch-size 16384  Rows per gRPC response message for SELECT queries.
REM                        Larger batches = fewer gRPC round-trips = higher
REM                        throughput for large result sets.
REM                        Smaller batches = lower latency for first rows.
REM                        16384 is a good balance for analytics workloads.
REM
REM    --memory-limit 8GB  DuckDB memory limit for query processing.
REM                        Controls how much RAM DuckDB uses for sorts, joins,
REM                        aggregations, and hash tables. Default is 80%% of RAM.
REM                        Set explicitly to prevent DuckDB from using all memory.
REM
REM    --threads 0         DuckDB internal thread count (0 = auto = all CPUs).
REM                        DuckDB parallelizes queries internally. Setting this
REM                        to your CPU core count maximizes query throughput.
REM                        0 means DuckDB auto-detects.
REM
REM  Performance features applied automatically by the server:
REM    - PRAGMA enable_object_cache     (faster metadata lookups)
REM    - SET preserve_insertion_order=false (faster bulk inserts)
REM    - Multi-row INSERT merging       (N individual INSERTs become 1)
REM    - DDL auto-detection             (CREATE/DROP run outside transactions)
REM    - Write serializer batching      (concurrent writes share transactions)
REM
REM  Usage:
REM    run_server.bat                         Launch with defaults below
REM    run_server.bat --db C:\data\my.duckdb  Override database path
REM    run_server.bat --port 9000             Override port
REM
REM  Stop: Press Ctrl+C in this window
REM ============================================================================

echo.
echo  DuckDB gRPC Server — High Performance Mode
echo  ============================================
echo.

server\bin\Debug\DuckArrowServer.exe ^
    --port 17777 ^
    --readers 64 ^
    --batch-ms 50 ^
    --batch-max 5000 ^
    --batch-size 16384 ^
    --memory-limit 8GB ^
    %*
