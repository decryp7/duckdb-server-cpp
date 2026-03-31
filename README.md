# DuckDB Arrow Flight Server  v4.1.8

A C++11 server (Visual Studio 2017) that exposes a DuckDB database as an **Apache Arrow Flight** service,
with a **.NET 4.6.2** client library.

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  .NET 4.6.2 Client (Apache.Arrow.Flight + Grpc.Core 2.46)          │
│                                                                     │
│  DasFlightClient                                                    │
│    ├── DoGet(ticket = SQL)  ──────────► Arrow record batch stream   │
│    ├── DoAction("execute", SQL) ──────► empty stream (ok)           │
│    │                            ◄────── gRPC INTERNAL (error)       │
│    ├── DoAction("ping")   ──────────► "pong"                        │
│    └── DoAction("stats")  ──────────► JSON metrics                  │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  gRPC / HTTP/2 — multiplexed, optionally TLS                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  C++11 DuckDB Flight Server (Visual Studio 2017)                    │
│                                                                     │
│  ┌───────────────────────────┐  ┌────────────────────────────────┐  │
│  │  DoGet (reads)            │  │  DoAction("execute") (writes)  │  │
│  │                           │  │                                │  │
│  │  ConnectionPool           │  │  WriteSerializer               │  │
│  │  (N read connections)     │  │  (batch DML into transactions) │  │
│  │                           │  │                                │  │
│  │  DuckDbRecordBatchSource  │  │  Single writer connection      │  │
│  │  (zero-copy streaming)    │  │                                │  │
│  └─────────────┬─────────────┘  └──────────────┬─────────────────┘  │
│                │                                │                    │
│                └────────────┬───────────────────┘                    │
│                             ▼                                        │
│               DuckDB (Arrow C Data Interface)                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Why Arrow Flight?

| | Custom TCP (v3.x) | Arrow Flight (v4.0) |
|---|---|---|
| Protocol | Bespoke binary | Standard gRPC / HTTP/2 |
| TLS | Manual (stunnel) | Built-in |
| Streaming | Custom chunked frames | Native (HTTP/2 framing) |
| Interop | .NET only | Python, Java, Go, Rust, R, … |
| Auth | None | gRPC interceptors / Bearer tokens |
| Observability | None | gRPC status codes, standard tracing |

---

## File map

```
.
├── README.md
├── CHANGELOG.md
├── PROTOCOL.md                     Flight RPC surface description
├── BUILD_WINDOWS.md                Build guide (VS 2017)
├── DuckDB-Arrow-Server-Design.pptx Architecture slides
├── CMakeLists.txt
│
├── include/
│   ├── flight_server.hpp           DuckFlightServer + ServerConfig
│   ├── connection_pool.hpp         Thread-safe DuckDB connection pool
│   ├── write_serializer.hpp        Single-writer transaction batcher
│   └── duck_bridge.hpp             DuckDB → Arrow IPC helpers
│
├── src/
│   ├── main.cpp                    CLI, TLS setup, Flight server startup
│   ├── flight_server.cpp           DoGet / DoAction / DuckDBRecordBatchReader
│   ├── duck_bridge.cpp             Arrow C Data Interface bridge
│   └── connection_pool.cpp         (stub; impl in header)
│
└── client/
    ├── DuckArrowClient.csproj      net462; Apache.Arrow.Flight + Grpc.Core
    ├── DasFlightClient.cs          Thread-safe Flight client (one per app)
    ├── DasFlightPool.cs            Optional pool for channel isolation
    ├── FlightQueryResult.cs        Schema + batches; ToRows() / ToDataTable()
    ├── DasException.cs             Typed exception
    ├── ArrowStreamReader.cs        IPC bytes reader (kept for interop)
    ├── DataTableExtensions.cs      ToDataTable() on ArrowStreamReader
    └── Example.cs                  9 annotated usage examples
```

---

## Building on Windows

### Prerequisites

1. **Visual Studio 2017** with the "Desktop development with C++" workload
2. **CMake 3.27.9** (last version with VS2017 generator support) — https://github.com/Kitware/CMake/releases/tag/v3.27.9
3. **vcpkg** — package manager for C++ dependencies

> **Important:** CMake 3.28+ dropped the Visual Studio 2017 generator.
> Use CMake **3.27.9 or earlier** when building with VS2017.

### 1. Install vcpkg

```powershell
git clone https://github.com/microsoft/vcpkg.git C:\vcpkg
C:\vcpkg\bootstrap-vcpkg.bat
```

> **Note:** You can clone vcpkg to any location. Replace `C:\vcpkg` in all
> commands below with your chosen path.

### 2. Install dependencies

```powershell
C:\vcpkg\vcpkg install "arrow[flight,parquet]:x64-windows" duckdb:x64-windows
```

### 3. Configure and build

Open a terminal in the repository root and run:

```powershell
cmake -B build -G "Visual Studio 15 2017" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows `
  -DCMAKE_BUILD_TYPE=Release

cmake --build build --config Release -j
```

> **Tip:** If vcpkg is installed somewhere other than `C:\vcpkg`, update the
> `-DCMAKE_TOOLCHAIN_FILE=` path accordingly.

The built executable will be at `build\Release\duckdb_flight_server.exe`.

### 4. Run

```powershell
# In-memory database (default)
.\build\Release\duckdb_flight_server.exe

# Persistent database
.\build\Release\duckdb_flight_server.exe --db C:\data\analytics.duckdb

# With TLS
.\build\Release\duckdb_flight_server.exe `
  --tls-cert server.crt --tls-key server.key

# Show all options
.\build\Release\duckdb_flight_server.exe --help
```

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

## Quick start

### Server (Windows)

```powershell
vcpkg install "arrow[flight,parquet]:x64-windows" duckdb:x64-windows

cmake -B build -G "Visual Studio 15 2017" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake
cmake --build build --config Release -j

.\build\Release\duckdb_flight_server.exe --db C:\data\analytics.duckdb
```

### Client (.NET 4.6.2)

```csharp
using DuckArrowClient;
using System.Data;

// One client handles all concurrent callers (HTTP/2 multiplexing)
using (var client = new DasFlightClient("server", 17777))
{
    // Read query → stream of Arrow batches
    using (var result = client.Query("SELECT * FROM sales"))
    {
        DataTable dt = result.ToDataTable();
        myDataGridView.DataSource = dt;
    }

    // Write / DDL
    client.Execute("INSERT INTO events VALUES (1, 'login')");

    // Stats
    Console.WriteLine(client.GetStats());
}

// TLS
var creds = new Grpc.Core.SslCredentials(File.ReadAllText("ca.crt"));
using (var client = new DasFlightClient("server", 17777, creds))
    client.Ping();
```

---

## Architecture

### DoGet — reads
The ticket payload is the UTF-8 SQL string. The server borrows a read connection,
creates a `DuckDBRecordBatchReader` (implements `arrow::RecordBatchReader`) that holds
the connection for the stream's lifetime, and wraps it in `RecordBatchStream`. Batches
flow zero-copy from DuckDB's Arrow cursor directly into gRPC's write buffer.

### DoAction("execute") — writes
SQL is routed to the `WriteSerializer`, which batches concurrent DML requests into one
`BEGIN…COMMIT` transaction per window. DDL is detected by keyword and executed outside
the batch. On failure, gRPC returns `INTERNAL` status — the .NET client re-throws as
`DasException`.

### Connection model
gRPC/HTTP/2 multiplexes many concurrent RPCs over one TCP connection. You do not need
a connection pool for the gRPC channel. The `DasFlightPool` class is provided for
workloads that want hard channel-count limits or multiple server addresses.

---

## Tuning

| Workload | --readers | --batch-ms | --batch-max |
|---|---|---|---|
| Read-heavy (BI/analytics) | `nCPU×2` | 5 | 512 |
| Write-heavy (ETL) | `nCPU` | 50 | 5000 |
| Mixed | `nCPU×2` | 10 | 1000 |

gRPC thread count is managed by Arrow Flight internally (default: 4 × CPU).

---

## Migrating from v3.x

| v3.x (custom protocol) | v4.0 (Arrow Flight) |
|---|---|
| `DasConnection.Query(sql)` | `DasFlightClient.Query(sql)` |
| `DasConnection.Execute(sql)` | `DasFlightClient.Execute(sql)` |
| `ArrowStreamReader` | `FlightQueryResult` |
| `reader.ToDataTable()` | `result.ToDataTable()` |
| `DasConnectionPool` | not needed |
| `MSG_STATS` | `DoAction("stats")` |
| Custom `STATUS_STREAM` | built into Flight |

---

## Version history

| Version | Summary |
|---|---|
| **v4.0.0–v4.1.8** | Arrow Flight rewrite, interface pass, continuous bug fixes |
| v3.1.x | IOCP server, VS 2017 fixes, streaming, stats, auto-reconnect |
| v3.0.0 | Windows IOCP edition |
| v2.0.0 | Thread pool + connection pool scaling |
| v1.0.0 | Initial C++11 server |
