@echo off
REM ============================================================================
REM  DuckDB gRPC Server — Low Latency / High Concurrency Launch Script
REM
REM  Optimized for sub-1000ms latency at 100+ concurrent readers/writers.
REM
REM  Configuration:
REM    --db data.duckdb    File-based database for persistence.
REM
REM    --port 19100        gRPC listen port.
REM
REM    --readers 128       Connection pool sized for 100+ concurrent readers.
REM                        Must be >= your peak concurrent reader count.
REM                        If readers > pool size, requests queue and latency spikes.
REM
REM    --batch-ms 1        Write batch window: 1ms for lowest write latency.
REM                        Trades throughput for latency. Each write commits
REM                        within ~1ms instead of waiting 50ms to batch more.
REM
REM    --batch-max 64      Small write batches commit faster.
REM                        64 statements per transaction is enough for low-latency
REM                        while still benefiting from INSERT merging.
REM
REM    --batch-size 512    Rows per gRPC response message.
REM                        Smaller = faster time-to-first-row for the client.
REM                        512 rows per message keeps protobuf overhead low
REM                        while delivering results quickly.
REM
REM    --memory-limit 8GB  DuckDB memory for sorts, joins, aggregations.
REM
REM  Performance features applied automatically:
REM    - PRAGMA enable_object_cache     (faster metadata lookups)
REM    - SET preserve_insertion_order=false (faster bulk inserts)
REM    - Multi-row INSERT merging       (N INSERTs become 1 statement)
REM    - Connection pool pre-warmed     (no cold-start penalty)
REM    - StringBuilder for value serialization (fewer allocations)
REM
REM  Usage:
REM    run_server.bat                         Launch with defaults below
REM    run_server.bat --db :memory:           In-memory mode (fastest, no persistence)
REM    run_server.bat --readers 256           More concurrent readers
REM
REM  Stop: Press Ctrl+C
REM ============================================================================

echo.
echo  DuckDB gRPC Server — Low Latency Mode
echo  ========================================
echo  Optimized for 100+ concurrent readers/writers
echo.

server\bin\Release\DuckDbServer.exe ^
    --db data.duckdb ^
    --port 19100 ^
    --shards 8 ^
    --readers 128 ^
    --batch-ms 1 ^
    --batch-max 64 ^
    --batch-size 512 ^
    --memory-limit 8GB ^
    %*
