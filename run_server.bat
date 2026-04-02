@echo off
REM Run the C# DuckDB gRPC Server
echo Starting DuckDB gRPC Server...
server\bin\Debug\DuckArrowServer.exe %*
