# DuckDB Arrow Flight Server  v4.1.8

A C++11 server that exposes a DuckDB database as an **Apache Arrow Flight** service,
with a **.NET 4.6.2** client library.

```
┌────────────────────────────────────────────────────────────────────────┐
│  .NET 4.6.2 client  (Apache.Arrow.Flight + Grpc.Core 2.46)            │
│                                                                        │
│  DasFlightClient  ──── DoGet(ticket=SQL) ────────────────────────────►│
│                   ◄─── Arrow record batch stream ────────────────────  │
│                  ─── DoAction("execute", SQL) ───────────────────────► │
│                  ◄─── empty stream (ok) / gRPC INTERNAL (error) ─────  │
│                                                                        │
│         gRPC / HTTP/2 — multiplexed, optionally TLS                   │
│                                                                        │
│  C++11 DuckDB Flight Server                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  arrow::flight::FlightServerBase (gRPC thread pool)           │    │
│  │  ┌──────────────────────┬────────────────────────────────┐    │    │
│  │  │  DoGet               │  DoAction("execute")           │    │    │
│  │  │  ConnectionPool      │  WriteSerializer (batch txns)  │    │    │
│  │  │  DuckDBRecordBatch   │  single writer connection      │    │    │
│  │  │  Reader (zero-copy)  │                                │    │    │
│  │  └──────────────────────┴────────────────────────────────┘    │    │
│  │  DuckDB (Arrow C Data Interface)                              │    │
│  └────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────┘
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
├── BUILD_WINDOWS.md                Build guide (VS 2017 / 2019 / 2022)
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

## Quick start

### Server (Windows)

```powershell
vcpkg install "arrow[flight,parquet]:x64-windows" duckdb:x64-windows

cmake -B build -G "Visual Studio 16 2019" -A x64 `
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
