@echo off
REM ============================================================================
REM  Generate C# code from proto/duckdb_service.proto
REM
REM  Prerequisites:
REM    - protoc.exe and grpc_csharp_plugin.exe on PATH
REM    - Or install Grpc.Tools NuGet and use the tools from there:
REM      packages\Grpc.Tools.2.46.6\tools\windows_x64\protoc.exe
REM      packages\Grpc.Tools.2.46.6\tools\windows_x64\grpc_csharp_plugin.exe
REM
REM  Usage:
REM    generate_proto.bat
REM
REM  Output:
REM    shared\Generated\DuckdbService.cs       (protobuf messages)
REM    shared\Generated\DuckdbServiceGrpc.cs   (gRPC service stubs)
REM ============================================================================

set PROTO_DIR=proto
set OUT_DIR=shared\Generated
set PROTO_FILE=%PROTO_DIR%\duckdb_service.proto

REM Try Grpc.Tools from NuGet packages first
set PROTOC=packages\Grpc.Tools.2.46.6\tools\windows_x64\protoc.exe
set GRPC_PLUGIN=packages\Grpc.Tools.2.46.6\tools\windows_x64\grpc_csharp_plugin.exe

if not exist "%PROTOC%" (
    REM Fall back to PATH
    set PROTOC=protoc
    set GRPC_PLUGIN=grpc_csharp_plugin
)

echo Generating C# from %PROTO_FILE%...
echo   protoc: %PROTOC%
echo   plugin: %GRPC_PLUGIN%
echo   output: %OUT_DIR%

if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

"%PROTOC%" --csharp_out="%OUT_DIR%" --grpc_out="%OUT_DIR%" --plugin=protoc-gen-grpc="%GRPC_PLUGIN%" --proto_path="%PROTO_DIR%" duckdb_service.proto

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Generated successfully:
    dir /b "%OUT_DIR%\*.cs"
) else (
    echo.
    echo ERROR: Code generation failed.
    echo Make sure protoc and grpc_csharp_plugin are available.
    echo.
    echo Install Grpc.Tools:
    echo   nuget install Grpc.Tools -Version 2.46.6 -OutputDirectory packages
    exit /b 1
)
