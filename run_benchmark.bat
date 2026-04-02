@echo off
REM Run the DuckDB Performance Benchmark
REM Usage: run_benchmark.bat [--quick | --full]
echo Starting DuckDB Benchmark...
benchmark\bin\Debug\DuckArrowBenchmark.exe %*
