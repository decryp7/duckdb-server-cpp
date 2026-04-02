@echo off
REM ============================================================================
REM  Build gRPC + Protobuf from source using VS2017
REM
REM  This script:
REM    1. Clones gRPC v1.49.0 (last version with C++14/VS2017 ABI compat)
REM    2. Builds with VS2022 (which supports modern Abseil)
REM    3. Installs to C:\grpc-install (headers, libs, tools)
REM    4. The VS2017 .vcxproj references C:\grpc-install
REM
REM  Prerequisites:
REM    - Visual Studio 2017 with C++ workload
REM    - CMake on PATH
REM    - Git on PATH
REM
REM  Output:
REM    C:\grpc-install\include\   (headers)
REM    C:\grpc-install\lib\       (static libs)
REM    C:\grpc-install\bin\       (protoc.exe, grpc_cpp_plugin.exe)
REM
REM  Usage:
REM    build_grpc.bat
REM
REM  Takes ~15-30 minutes depending on machine speed.
REM ============================================================================

set GRPC_VERSION=v1.35.0
set GRPC_SRC=C:\grpc-src
set GRPC_INSTALL=C:\grpc-install
set GRPC_BUILD=C:\grpc-build

echo.
echo  ============================================
echo  Building gRPC %GRPC_VERSION% from source
echo  ============================================
echo.
echo  Source:  %GRPC_SRC%
echo  Build:   %GRPC_BUILD%
echo  Install: %GRPC_INSTALL%
echo.

REM ── Step 1: Clone gRPC with submodules ──────────────────────────────────
if not exist "%GRPC_SRC%" (
    echo [1/5] Cloning gRPC %GRPC_VERSION% with submodules...
    echo       This downloads ~500MB. Please wait...
    git clone --recurse-submodules -b %GRPC_VERSION% --depth 1 https://github.com/grpc/grpc "%GRPC_SRC%"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: git clone failed
        exit /b 1
    )
) else (
    echo [1/5] gRPC source already exists at %GRPC_SRC%, skipping clone.
)

REM ── Step 2: Create build directory ──────────────────────────────────────
echo [2/5] Configuring CMake with VS2022...
if not exist "%GRPC_BUILD%" mkdir "%GRPC_BUILD%"

cmake -B "%GRPC_BUILD%" -S "%GRPC_SRC%" ^
    -G "Visual Studio 15 2017" -A x64 ^
    -DCMAKE_INSTALL_PREFIX="%GRPC_INSTALL%" ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DgRPC_INSTALL=ON ^
    -DgRPC_BUILD_TESTS=OFF ^
    -DgRPC_BUILD_CSHARP_EXT=OFF ^
    -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF ^
    -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF ^
    -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF ^
    -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF ^
    -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF ^
    -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: CMake configure failed
    exit /b 1
)

REM ── Step 3: Build Release ───────────────────────────────────────────────
echo [3/5] Building Release... (this takes 10-20 minutes)
cmake --build "%GRPC_BUILD%" --config Release --parallel

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Build failed
    exit /b 1
)

REM ── Step 4: Install to GRPC_INSTALL ─────────────────────────────────────
echo [4/5] Installing to %GRPC_INSTALL%...
cmake --install "%GRPC_BUILD%" --config Release

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Install failed
    exit /b 1
)

REM ── Step 5: Verify ──────────────────────────────────────────────────────
echo [5/5] Verifying installation...
echo.

if exist "%GRPC_INSTALL%\bin\protoc.exe" (
    echo   protoc.exe:          OK
) else (
    echo   protoc.exe:          MISSING
)

if exist "%GRPC_INSTALL%\bin\grpc_cpp_plugin.exe" (
    echo   grpc_cpp_plugin.exe: OK
) else (
    echo   grpc_cpp_plugin.exe: MISSING
)

if exist "%GRPC_INSTALL%\lib\grpc++.lib" (
    echo   grpc++.lib:          OK
) else (
    if exist "%GRPC_INSTALL%\lib\grpc++_unsecure.lib" (
        echo   grpc++_unsecure.lib: OK
    ) else (
        echo   grpc++.lib:          MISSING
    )
)

if exist "%GRPC_INSTALL%\include\grpcpp\grpcpp.h" (
    echo   grpcpp headers:      OK
) else (
    echo   grpcpp headers:      MISSING
)

echo.
echo  ============================================
echo  gRPC installed to %GRPC_INSTALL%
echo  ============================================
echo.
echo  Set environment variable for the VS2017 project:
echo    setx GRPC_ROOT "%GRPC_INSTALL%"
echo.
echo  Or add to your VS2017 project:
echo    Include: %GRPC_INSTALL%\include
echo    Lib:     %GRPC_INSTALL%\lib
echo    Tools:   %GRPC_INSTALL%\bin\protoc.exe
echo             %GRPC_INSTALL%\bin\grpc_cpp_plugin.exe
echo.
