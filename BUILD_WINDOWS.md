# Building on Windows — Arrow Flight Edition

## Supported toolchains

| IDE | MSVC | CMake generator |
|-----|------|-----------------|
| Visual Studio 2017 | 19.10–19.16 | `"Visual Studio 15 2017"` |
| Visual Studio 2019 | 19.20+ | `"Visual Studio 16 2019"` |
| Visual Studio 2022 | 19.30+ | `"Visual Studio 17 2022"` |

---

## Prerequisites

### 1. Visual Studio with C++ workload
"Desktop development with C++" + Windows 10/11 SDK.

### 2. CMake ≥ 3.14
https://cmake.org/download/

### 3. vcpkg (recommended)

```powershell
git clone https://github.com/microsoft/vcpkg.git C:\vcpkg
C:\vcpkg\bootstrap-vcpkg.bat

# The [flight] feature pulls gRPC + protobuf + cares automatically.
# [parquet] is optional but recommended for read_parquet() queries.
C:\vcpkg\vcpkg install "arrow[flight,parquet]:x64-windows" duckdb:x64-windows
```

### 4. Manual (no vcpkg)
Download Arrow from https://arrow.apache.org/install/ and DuckDB from
https://github.com/duckdb/duckdb/releases. Place under `third_party/`.

---

## Build

### Visual Studio 2017

```powershell
cmake -B build -G "Visual Studio 15 2017" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows `
  -DCMAKE_BUILD_TYPE=Release

cmake --build build --config Release -j
```

### Visual Studio 2019

```powershell
cmake -B build -G "Visual Studio 16 2019" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows
cmake --build build --config Release -j
```

### Visual Studio 2022

```powershell
cmake -B build -G "Visual Studio 17 2022" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows
cmake --build build --config Release -j
```

---

## Running

```powershell
# Default: in-memory DB, port 17777, auto CPU sizing, plaintext
.\build\Release\duckdb_flight_server.exe

# Persistent DB
.\build\Release\duckdb_flight_server.exe --db C:\data\analytics.duckdb

# TLS
.\build\Release\duckdb_flight_server.exe `
  --db C:\data\analytics.duckdb `
  --tls-cert C:\certs\server.crt `
  --tls-key  C:\certs\server.key

# Version
.\build\Release\duckdb_flight_server.exe --version
```

### All flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `:memory:` | DuckDB file path |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `17777` | gRPC listen port |
| `--readers` | `nCPU×2` | Read connection pool size |
| `--batch-ms` | `5` | Write batch window (ms) |
| `--batch-max` | `512` | Max writes per batch |
| `--tls-cert` | — | TLS certificate PEM path |
| `--tls-key` | — | TLS private key PEM path |

---

## Self-signed TLS certificate (dev/test)

```powershell
# Generate CA + server cert
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 365 -nodes `
  -subj "/CN=DAS CA"

openssl req -newkey rsa:4096 -keyout server.key -out server.csr -nodes `
  -subj "/CN=localhost"

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key `
  -CAcreateserial -out server.crt -days 365

# Start server with TLS
.\duckdb_flight_server.exe --tls-cert server.crt --tls-key server.key

# .NET client: pass ca.crt as SslCredentials
```

---

## Windows Firewall

```powershell
netsh advfirewall firewall add rule `
  name="DuckDB Flight Server" `
  dir=in action=allow protocol=TCP localport=17777
```

## Run as a Windows Service

```powershell
# Using NSSM (https://nssm.cc/)
nssm install DuckFlightSvc "C:\path\to\duckdb_flight_server.exe"
nssm set DuckFlightSvc AppParameters "--db C:\data\analytics.duckdb"
nssm set DuckFlightSvc AppStdout "C:\logs\das-stdout.log"
nssm set DuckFlightSvc AppStderr "C:\logs\das-stderr.log"
nssm start DuckFlightSvc
```
