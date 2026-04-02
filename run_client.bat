@echo off
REM ============================================================================
REM  DuckDB gRPC Client — Example Runner
REM
REM  Runs the client examples against a running DuckDB gRPC server.
REM
REM  Prerequisites:
REM    1. Start the server first: run_server.bat
REM    2. The server must be listening on localhost:17777 (default)
REM
REM  Examples executed:
REM    1. Basic query     — SELECT with column access
REM    2. DataTable       — ADO.NET DataTable binding (1000 rows)
REM    3. Writes          — CREATE TABLE + INSERT + SELECT
REM    4. Stats           — Server metrics (queries, errors, pool size)
REM    5. Concurrent      — 20 parallel async queries
REM    6. DI pattern      — Dependency injection with IDasFlightClient
REM
REM  Usage:
REM    run_client.bat
REM ============================================================================

echo.
echo  DuckDB gRPC Client Examples
echo  ============================
echo.

client\bin\Debug\DuckArrowClient.exe %*
