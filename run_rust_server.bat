@echo off
REM ============================================================================
REM  DuckDB gRPC Server (Rust Edition) — Maximum Performance
REM
REM  The fastest server implementation: zero GC, async I/O, lock-free pool.
REM
REM  Prerequisites:
REM    - Rust toolchain: https://rustup.rs/
REM    - First build: cd rust-server && cargo build --release
REM
REM  Uses the same proto/duckdb_service.proto as C# and C++ servers.
REM  Compatible with all clients (C#, C++, Python, Go, etc.)
REM
REM  Stop: Press Ctrl+C
REM ============================================================================

echo.
echo  DuckDB gRPC Server (Rust) — Maximum Performance
echo  ==================================================
echo.

rust-server\target\release\duckdb-grpc-server.exe ^
    --db data.duckdb ^
    --port 19100 ^
    --shards 8 ^
    --readers 512 ^
    --batch_ms 1 ^
    --batch_max 512 ^
    --batch_size 2048 ^
    %*
