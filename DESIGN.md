# DuckDB Arrow Flight Server — Design Document

Version 4.1.9 | Visual Studio 2017 | .NET Framework 4.6.2

---

## 1. Overview

This project exposes a DuckDB database over the network using the
**Apache Arrow Flight** protocol. Clients send SQL queries, and the server
streams results back as Arrow record batches over gRPC / HTTP/2.

There are two server implementations (C++ and C#) and one .NET client library.
Both servers expose the same Flight RPC surface and are fully interchangeable.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Clients (any Arrow Flight library)                                 │
│  .NET 4.6.2 / Python / Java / Go / Rust / R                        │
│                                                                     │
│    DoGet(SQL)         → Arrow record batch stream                   │
│    DoAction(execute)  → success / error                             │
│    DoAction(ping)     → "pong"                                      │
│    DoAction(stats)    → JSON metrics                                │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  gRPC / HTTP/2 transport (plaintext or TLS)                         │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Server (C++ or C#)                                                 │
│                                                                     │
│  ┌───────────────────────────┐  ┌────────────────────────────────┐  │
│  │  DoGet (reads)            │  │  DoAction("execute") (writes)  │  │
│  │                           │  │                                │  │
│  │  ConnectionPool           │  │  WriteSerializer               │  │
│  │  (N read connections)     │  │  (batch DML into transactions) │  │
│  │                           │  │                                │  │
│  │  Stream Arrow batches     │  │  DDL runs individually         │  │
│  │  (zero-copy in C++)       │  │  DML batched in BEGIN..COMMIT  │  │
│  └─────────────┬─────────────┘  └──────────────┬─────────────────┘  │
│                │                                │                    │
│                └────────────┬───────────────────┘                    │
│                             ▼                                        │
│               DuckDB (single database file or :memory:)              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Flight RPC Surface

All client-server communication uses standard Arrow Flight RPCs.
No custom protocol, no custom wire format.

| RPC | Purpose | Payload | Response |
|-----|---------|---------|----------|
| `DoGet(ticket)` | Read query | ticket = UTF-8 SQL | Arrow record batch stream |
| `DoAction("execute", sql)` | Write / DDL | body = UTF-8 SQL | Empty stream on success |
| `DoAction("ping", "")` | Liveness | empty | "pong" |
| `DoAction("stats", "")` | Metrics | empty | JSON object |
| `ListActions()` | Discovery | none | Action type descriptors |

### Stats JSON format

```json
{
  "queries_read": 1000,
  "queries_write": 22,
  "errors": 0,
  "reader_pool_size": 16,
  "port": 17777
}
```

---

## 4. Key Design Decisions

### 4.1 Why Arrow Flight?

| Concern | Old approach (v3.x) | Arrow Flight (v4.0) |
|---------|---------------------|---------------------|
| Protocol | Custom binary framing | Standard gRPC / HTTP/2 |
| TLS | Manual (stunnel) | Built-in |
| Streaming | Custom chunked frames | HTTP/2 native streaming |
| Client support | .NET only | Any language with Arrow |
| Auth | None | gRPC interceptors |

### 4.2 Read path: ConnectionPool + streaming

DuckDB supports many concurrent readers but only one writer at a time.
The server maintains a **fixed-size pool** of read connections:

1. A DoGet request arrives with a SQL query.
2. The server borrows a connection from the pool (blocks if none free).
3. The query executes and results stream as Arrow batches.
4. When streaming finishes, the connection returns to the pool.

The connection is held for the **entire duration** of the stream because
DuckDB cursors are attached to the connection that created them.
Releasing the connection while reading would corrupt the cursor.

### 4.3 Write path: WriteSerializer batching

To maximise write throughput, the server does not run each INSERT
individually. Instead:

1. Write requests arrive from multiple clients concurrently.
2. A background thread collects requests over a short time window.
3. All collected DML statements run inside one `BEGIN...COMMIT`.
4. If any statement fails, the batch rolls back and each retries alone.
5. DDL statements (CREATE, DROP, etc.) always run individually.

```
Thread A: INSERT ... ─┐
Thread B: INSERT ... ──┤ collected over 5 ms → BEGIN; A; B; C; D; COMMIT
Thread C: UPDATE ... ──┤
Thread D: DELETE ... ─┘
```

### 4.4 DDL detection

DDL statements cannot be mixed with DML inside an explicit transaction
in DuckDB. The `DdlDetector` class identifies DDL by extracting the first
keyword from the SQL and checking it against a known list:
`CREATE, DROP, ALTER, TRUNCATE, ATTACH, DETACH, VACUUM, PRAGMA, COPY, EXPORT, IMPORT, LOAD`.

### 4.5 Zero-copy data path (C++ only)

The C++ server uses DuckDB's Arrow C Data Interface for zero-copy transfer:

```
DuckDB internal buffer
  → duckdb_query_arrow_array()     (no copy)
  → arrow::ImportRecordBatch()     (no copy)
  → arrow::flight::RecordBatchStream → gRPC serialisation
```

No intermediate copy occurs before gRPC serialises to the wire.

The C# server uses DuckDB.NET.Data (ADO.NET), which reads values
through a DataReader and builds Arrow arrays via builders. This involves
a copy but is simpler and easier to maintain.

---

## 5. C++ Server Design

### 5.1 Class hierarchy

```
FlightServerBase (Arrow Flight)
  └── DuckFlightServer
        ├── ConnectionPool       (N read connections, RAII Handle)
        ├── WriteSerializer      (background thread, batch transactions)
        └── DuckDbRecordBatchSource (Arrow C Data Interface bridge)
```

### 5.2 Key files

| File | Purpose |
|------|---------|
| `src/main.cpp` | CLI parsing, signal handling, server startup |
| `src/flight_server.cpp` | DoGet, DoAction, ListActions implementation |
| `src/duck_bridge.cpp` | DuckDB → Arrow C Data Interface bridge |
| `include/flight_server.hpp` | DuckFlightServer class + ServerConfig |
| `include/connection_pool.hpp` | Thread-safe connection pool (header-only) |
| `include/write_serializer.hpp` | Write batching with DDL detection |
| `include/interfaces.hpp` | Pure-virtual interfaces for testability |

### 5.3 VS2017 compatibility notes

- **Atomics**: Initialised in constructor body, not via NSDMI (VS2017 bug).
- **C++ standard**: C++14 via `/std:c++14`.
- **`ARROW_ASSIGN_OR_RAISE`**: Not used inside `DoGet`'s try/catch
  because it expands to a bare `return` that bypasses the catch block.
- **Signal handler**: Uses `write()` instead of `std::cout` (async-signal-safe).
- **Constructor cleanup**: Lambda captures `this` and runs on exception,
  because C++ only destructs fully-constructed objects.

---

## 6. C# Server Design

### 6.1 Class hierarchy

```
FlightServer (Apache.Arrow.Flight.Server)
  └── DuckFlightServer
        ├── IConnectionPool → ConnectionPool
        ├── IWriteSerializer → WriteSerializer
        │     └── DdlDetector (static helper)
        ├── RecordBatchBuilder (static helper)
        └── ArrowTypeConverter (static helper)
```

### 6.2 Key files

| File | Purpose |
|------|---------|
| `Program.cs` | CLI parsing, signal handling, gRPC server startup |
| `DuckFlightServer.cs` | DoGet, DoAction, ListActions |
| `IConnectionPool.cs` | Interface for connection pool |
| `ConnectionPool.cs` | Thread-safe DuckDB connection pool |
| `IWriteSerializer.cs` | Interface for write serializer |
| `WriteSerializer.cs` | Batched writes with DDL detection |
| `DdlDetector.cs` | Identifies DDL vs DML statements |
| `ArrowTypeConverter.cs` | CLR type ↔ Arrow type mapping |
| `RecordBatchBuilder.cs` | Builds Arrow batches from DataReader |
| `ServerConfig.cs` | Configuration, stats, and WriteResult types |

### 6.3 Design principles

- **Interfaces for abstractions**: `IConnectionPool` and `IWriteSerializer`
  allow mocking in unit tests without a real database.
- **Small, focused classes**: Each class does one thing.
  `DdlDetector` only detects DDL. `RecordBatchBuilder` only builds batches.
  `ArrowTypeConverter` only converts types.
- **No underscore prefixes**: Variable names use camelCase without leading
  underscores, following a simple naming convention.
- **Clear comments**: Written for junior engineers to understand.

---

## 7. .NET Client Library Design

### 7.1 Class hierarchy

```
IDasFlightClient (interface)
  └── DasFlightClient
        └── FlightClient (Apache.Arrow.Flight)

IFlightQueryResult (interface)
  └── FlightQueryResult
        └── ArrowValueConverter (shared value boxing)

IDasFlightPool (interface)
  └── DasFlightPool
        └── IPoolLease → Lease
```

### 7.2 Key design points

- **One client per app**: HTTP/2 multiplexes all concurrent RPCs over
  one TCP connection. No per-caller pool needed.
- **Thread safety**: `DasFlightClient` is safe to share across threads.
  `_disposed` uses `Interlocked.CompareExchange` (not a plain bool).
- **Sync wrappers**: Use `Task.Run(() => Async()).GetAwaiter().GetResult()`
  to avoid SynchronizationContext deadlock on WPF/WinForms UI threads.
- **gRPC stream draining**: `DoActionFirstResult` reads the first result
  but continues draining the full stream to avoid server-side leaks.
- **Date handling**: `Date32`/`Date64` → `DateTimeOffset` (not `DateTime`)
  to match DataTable column types and avoid `InvalidCastException`.

---

## 8. Threading Model

```
┌──────────────────────────────────────────────────────┐
│  gRPC thread pool (managed by Arrow Flight / gRPC)   │
│                                                       │
│  Thread 1: DoGet  ──→ ConnectionPool.Borrow()         │
│  Thread 2: DoGet  ──→ ConnectionPool.Borrow()         │
│  Thread 3: DoAction("execute") ──→ WriteSerializer    │
│  Thread 4: DoAction("execute") ──→ WriteSerializer    │
│  Thread 5: DoAction("ping")                           │
│                                                       │
├──────────────────────────────────────────────────────┤
│                                                       │
│  ConnectionPool                                       │
│  ┌────┬────┬────┬────┐                                │
│  │ C1 │ C2 │ C3 │ C4 │  (N idle connections)         │
│  └────┴────┴────┴────┘                                │
│  Borrow blocks if all connections busy.               │
│                                                       │
├──────────────────────────────────────────────────────┤
│                                                       │
│  WriteSerializer (single background thread)           │
│  ┌──────────────────────────────────────────┐         │
│  │ Queue: [INSERT A] [INSERT B] [UPDATE C]  │         │
│  │                                          │         │
│  │ Every 5ms or 512 items:                  │         │
│  │   BEGIN → A → B → C → COMMIT             │         │
│  └──────────────────────────────────────────┘         │
│                                                       │
└──────────────────────────────────────────────────────┘
```

---

## 9. Error Handling Strategy

| Error type | Server response | Client sees |
|------------|-----------------|-------------|
| SQL syntax error | gRPC `INTERNAL` status | `DasException` |
| Connection pool timeout | gRPC `INTERNAL` status | `DasException` |
| Null action body | gRPC `INVALID_ARGUMENT` | `DasException` |
| Unknown action type | gRPC `UNIMPLEMENTED` | `DasException` |
| DML failure in batch | Rollback + retry individually | Accurate per-statement result |
| COMMIT failure | Retry each statement individually | Accurate per-statement result |

The C++ server increments `stat_errors_` for every failed RPC.

---

## 10. TLS Configuration

Both servers support TLS via `--tls-cert` and `--tls-key` flags.

**Server side** (either C++ or C#):
```
duckdb_flight_server.exe --tls-cert server.crt --tls-key server.key
```

**Client side** (.NET):
```csharp
var creds = new SslCredentials(File.ReadAllText("ca.crt"));
var client = new DasFlightClient("server", 17777, creds);
```

---

## 11. Tuning Guide

| Workload | --readers | --batch-ms | --batch-max |
|----------|-----------|------------|-------------|
| Read-heavy (BI/analytics) | `nCPU x 2` | 5 | 512 |
| Write-heavy (ETL) | `nCPU` | 50 | 5000 |
| Mixed | `nCPU x 2` | 10 | 1000 |

- **--readers**: More readers = more concurrent SELECT queries.
  Too many wastes memory; too few causes queuing.
- **--batch-ms**: Longer window = more writes per transaction = higher throughput,
  but higher latency for individual writes.
- **--batch-max**: Cap on how many statements go in one transaction.
  Prevents unbounded memory use during write spikes.

---

## 12. Dependency Versions

### C++ server (vcpkg pinned baseline: September 2022)

| Dependency | Version | Why pinned? |
|------------|---------|-------------|
| Apache Arrow | 9.0 | Last version with C++11 support |
| Protobuf | 3.21.x | Last version without Abseil dependency |
| gRPC | ~1.48 | Compatible with Protobuf 3.21.x |
| DuckDB | manual | Not in vcpkg at Sept 2022 baseline |
| CMake | 3.27.9 | Last version with VS2017 generator |

**Why pinned?** Arrow Flight depends on gRPC → Protobuf → Abseil.
Abseil requires VS2019+. Pinning to the September 2022 baseline keeps
the entire dependency chain VS2017-compatible.

### C# server and client (.NET Framework 4.6.2)

| Package | Version | Notes |
|---------|---------|-------|
| Apache.Arrow | 14.0.1 | Core Arrow types |
| Apache.Arrow.Flight | 14.0.1 | Flight client + server |
| Grpc.Core | 2.46.6 | Last gRPC with .NET Framework support |
| DuckDB.NET.Data | 1.0.6 | ADO.NET provider for DuckDB |
| Google.Protobuf | 3.21.12 | gRPC serialisation |

---

## 13. File Map

```
.
├── DESIGN.md                       This document
├── README.md                       Quick start and overview
├── CHANGELOG.md                    Version history
├── PROTOCOL.md                     Flight RPC surface description
├── BUILD_WINDOWS.md                VS2017 build guide
├── CMakeLists.txt                  C++ build configuration
├── vcpkg.json                      Pinned vcpkg baseline for VS2017
├── .gitignore
│
├── include/                        C++ headers
│   ├── interfaces.hpp              Abstract interfaces
│   ├── flight_server.hpp           DuckFlightServer class
│   ├── connection_pool.hpp         ConnectionPool (header-only)
│   ├── write_serializer.hpp        WriteSerializer
│   └── duck_bridge.hpp             DuckDB → Arrow bridge
│
├── src/                            C++ implementation
│   ├── main.cpp                    Entry point
│   ├── flight_server.cpp           DoGet / DoAction / ListActions
│   ├── duck_bridge.cpp             Arrow C Data Interface bridge
│   └── connection_pool.cpp         Placeholder (impl in header)
│
├── server/                         C# server
│   ├── DuckArrowServer.sln         VS2017 solution
│   ├── DuckArrowServer.csproj      Project file
│   ├── packages.config             NuGet packages
│   ├── Program.cs                  Entry point
│   ├── DuckFlightServer.cs         Flight server (DoGet/DoAction)
│   ├── IConnectionPool.cs          Connection pool interface
│   ├── ConnectionPool.cs           Connection pool implementation
│   ├── IWriteSerializer.cs         Write serializer interface
│   ├── WriteSerializer.cs          Write serializer implementation
│   ├── DdlDetector.cs              DDL vs DML detection
│   ├── ArrowTypeConverter.cs       CLR ↔ Arrow type mapping
│   ├── RecordBatchBuilder.cs       DataReader → Arrow batches
│   └── ServerConfig.cs             Config, stats, WriteResult
│
├── client/                         C# client library
│   ├── DuckArrowClient.sln         VS2017 solution
│   ├── DuckArrowClient.csproj      Project file
│   ├── packages.config             NuGet packages
│   ├── Interfaces.cs               IDasFlightClient, IFlightQueryResult
│   ├── DasFlightClient.cs          Flight client implementation
│   ├── DasFlightPool.cs            Optional connection pool
│   ├── FlightQueryResult.cs        Query result + type mapping
│   ├── ArrowValueConverter.cs      Arrow → CLR value boxing
│   ├── DasException.cs             Typed exception
│   ├── ArrowStreamReader.cs        Legacy IPC reader
│   ├── DataTableExtensions.cs      DataTable helpers
│   └── Example.cs                  Usage examples
│
└── third_party/                    Manual dependencies (not in Git)
    └── duckdb/
        ├── include/duckdb.h
        └── lib/duckdb.lib + duckdb.dll
```

---

## 14. Version History

| Version | Changes |
|---------|---------|
| **v4.1.9** | C# server port, design document, VS2017 build fixes |
| **v4.0.0 – v4.1.8** | Arrow Flight rewrite, 39 bugs fixed across 9 audit passes |
| v3.1.x | IOCP server, VS2017 fixes, streaming, stats |
| v3.0.0 | Windows IOCP edition |
| v2.0.0 | Thread pool + connection pool scaling |
| v1.0.0 | Initial C++11 server |
