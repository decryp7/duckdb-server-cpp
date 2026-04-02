# DuckDB gRPC Server  v5.0

A DuckDB database server over **gRPC** with a **.NET 4.6.2** client library.
Built for Visual Studio 2017. No Apache Arrow dependency.

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Clients (any language with gRPC + protobuf)                        │
│                                                                     │
│  C# (.NET 4.6.2)    C++ (protoc)    Python / Java / Go / Rust      │
│  DuckDbClient     generated       generated from .proto          │
│                                                                     │
│    Query(sql)        ──────────► stream of rows (protobuf)          │
│    Execute(sql)      ──────────► success / error                    │
│    Ping()            ──────────► "pong"                              │
│    GetStats()        ──────────► server metrics                      │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  gRPC / HTTP/2 (proto/duckdb_service.proto)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Server (C# or C++)                                                 │
│                                                                     │
│  ┌───────────────────────────┐  ┌────────────────────────────────┐  │
│  │  Query (reads)            │  │  Execute (writes)              │  │
│  │                           │  │                                │  │
│  │  ConnectionPool           │  │  WriteSerializer               │  │
│  │  (N read connections)     │  │  (batch DML into transactions) │  │
│  │  ConcurrentBag +          │  │  INSERT merging                │  │
│  │  SemaphoreSlim            │  │  DDL auto-detection            │  │
│  └─────────────┬─────────────┘  └──────────────┬─────────────────┘  │
│                │                                │                    │
│                └────────────┬───────────────────┘                    │
│                             ▼                                        │
│                        DuckDB                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Quick start

### 1. Build

```powershell
# Clone and open in VS2017
git clone https://github.com/decryp7/duckdb-server-cpp.git
cd duckdb-server-cpp

# Restore NuGet packages
nuget restore DuckDbServer.sln

# Open DuckDbServer.sln in VS2017 → Build Solution
```

### 2. Run the server

```powershell
run_server.bat
```

### 3. Run the client examples

```powershell
run_client.bat
```

### 4. Run the benchmark

```powershell
run_benchmark.bat --quick
```

---

## Protocol

Defined in `proto/duckdb_service.proto`. Pure protobuf — no Arrow, no FlatBuffers.

| RPC | Type | Request | Response |
|-----|------|---------|----------|
| `Query` | Server-streaming | `{sql}` | `{columns}` then `{rows}` batches |
| `Execute` | Unary | `{sql}` | `{success, error}` |
| `Ping` | Unary | `{}` | `{message: "pong"}` |
| `GetStats` | Unary | `{}` | `{queries_read, queries_write, errors, ...}` |

Generate clients for any language:
```bash
protoc --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=grpc_python_plugin proto/duckdb_service.proto
protoc --java_out=. --grpc_out=. --plugin=protoc-gen-grpc=grpc_java_plugin proto/duckdb_service.proto
protoc --go_out=. --grpc_out=. --plugin=protoc-gen-grpc=grpc_go_plugin proto/duckdb_service.proto
```

---

## Client usage (.NET 4.6.2)

```csharp
using DuckDbClient;
using System.Data;

// One client handles all concurrent callers (HTTP/2 multiplexing)
using (var client = new DuckDbClient("server", 17777))
{
    // Read query → rows
    using (var result = client.Query("SELECT * FROM sales"))
    {
        DataTable dt = result.ToDataTable();
        myDataGridView.DataSource = dt;
    }

    // Write
    client.Execute("INSERT INTO events VALUES (1, 'login')");

    // Ping
    client.Ping();

    // Stats
    Console.WriteLine(client.GetStats());
}

// TLS
var creds = new Grpc.Core.SslCredentials(File.ReadAllText("ca.crt"));
using (var client = new DuckDbClient("server", 17777, creds))
    client.Ping();
```

---

## File map

```
.
├── DuckDbServer.sln              VS2017 solution (all projects)
├── nuget.config                     Shared NuGet packages directory
├── generate_proto.bat               Regenerate C# code from .proto
├── run_server.bat                   Launch server (high performance)
├── run_client.bat                   Run client examples
├── run_benchmark.bat                Run performance benchmark
├── run_cpp_server.bat               Launch C++ server
│
├── proto/
│   └── duckdb_service.proto         Protocol definition (all languages)
│
├── shared/                          Shared proto types (C# class library)
│   ├── DuckDbShared.csproj
│   └── Generated/                   Pre-generated from .proto
│       ├── DuckdbService.cs         Protobuf message classes
│       └── DuckdbServiceGrpc.cs     gRPC server base + client stubs
│
├── server/                          C# server (.NET 4.6.2, x64)
│   ├── DuckDbServer.csproj
│   ├── Program.cs                   CLI, signal handling, gRPC startup
│   ├── DuckDbServer.cs          Query / Execute / Ping / GetStats
│   ├── DatabaseManager.cs           Shared DB connections + PRAGMA tuning
│   ├── IConnectionPool.cs           Connection pool interface
│   ├── ConnectionPool.cs            ConcurrentBag + SemaphoreSlim pool
│   ├── IWriteSerializer.cs          Write serializer interface
│   ├── WriteSerializer.cs           Batched writes + INSERT merging
│   ├── DdlDetector.cs               DDL vs DML detection
│   ├── InsertParser.cs              Parse INSERT for merging
│   ├── InsertBatcher.cs             Merge INSERTs into multi-row
│   └── ServerConfig.cs              Config, stats, WriteResult
│
├── client/                          C# client (.NET 4.6.2, x86)
│   ├── DuckDbClient.csproj
│   ├── DuckDbClient.cs           gRPC client (Query/Execute/Ping/Stats)
│   ├── Interfaces.cs                IDuckDbClient, IQueryResult
│   ├── DuckDbException.cs              Typed exception
│   └── Example.cs                   6 usage examples
│
├── benchmark/                       Performance benchmark (.NET 4.6.2, x86)
│   ├── DuckDbBenchmark.csproj
│   ├── Program.cs                   --quick / standard / --full modes
│   ├── BenchmarkRunner.cs           6 scenario types
│   └── BenchmarkResult.cs           Latency tracking + reporting
│
├── cpp/                             C++ server (VS2017, x64, vcpkg)
│   └── DuckDbServerCpp.vcxproj  Pre-build protoc + gRPC server
│
├── include/                         C++ headers
│   ├── grpc_server.hpp              DuckGrpcServer class
│   ├── connection_pool.hpp          Thread-safe connection pool
│   ├── write_serializer.hpp         Batched write transactions
│   └── insert_batcher.hpp           INSERT merging
│
├── src/                             C++ implementation
│   ├── main_grpc.cpp                Entry point
│   ├── grpc_server.cpp              Query / Execute / Ping / GetStats
│   └── connection_pool.cpp          (stub; impl in header)
│
├── PROTOCOL_GRPC.md                 Protocol design rationale
├── DESIGN.md                        Architecture documentation
├── CHANGELOG.md                     Version history
└── CLAUDE.md                        AI context file
```

---

## Performance tuning

### Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `:memory:` | DuckDB file path |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `17777` | gRPC listen port |
| `--readers` | `nCPU×2` | Read connection pool size |
| `--batch-ms` | `5` | Write batch window (ms) |
| `--batch-max` | `512` | Max writes per batch |
| `--batch-size` | `8192` | Rows per gRPC response message |
| `--memory-limit` | 80% RAM | DuckDB memory limit (e.g. `4GB`) |
| `--threads` | nCPU | DuckDB internal thread count |
| `--tls-cert` | — | TLS certificate PEM path |
| `--tls-key` | — | TLS private key PEM path |

### Workload presets

| Workload | --readers | --batch-ms | --batch-max | --batch-size |
|----------|-----------|------------|-------------|--------------|
| Low-latency (100 concurrent) | 128 | 1 | 64 | 512 |
| Read-heavy (BI) | `nCPU×2` | 5 | 512 | 8192 |
| Write-heavy (ETL) | `nCPU` | 50 | 5000 | 8192 |
| Mixed | `nCPU×2` | 10 | 1000 | 4096 |

### Performance features (automatic)

- **Connection pool**: `ConcurrentBag` + `SemaphoreSlim` (lock-free under low contention)
- **INSERT merging**: N individual INSERTs become 1 multi-row INSERT
- **Write batching**: concurrent writes share one `BEGIN...COMMIT` transaction
- **DDL detection**: `CREATE`/`DROP`/`ALTER` run outside transactions automatically
- **DuckDB tuning**: `enable_object_cache`, `preserve_insertion_order=false`
- **Bulk row read**: `GetValues()` reads all columns in one call

### Client best practices

```csharp
// ONE client per app — HTTP/2 multiplexes all concurrent RPCs
using (var client = new DuckDbClient("server", 17777))
{
    // Use async for concurrent queries
    var tasks = queries.Select(sql => client.QueryAsync(sql));
    var results = await Task.WhenAll(tasks);
}
```

---

## Dependencies

| Component | Dependencies |
|-----------|-------------|
| C# server | Grpc.Core 2.46.6, Google.Protobuf 3.24.4, DuckDB.NET.Data.Full 1.1.1 |
| C# client | Grpc.Core 2.46.6, Google.Protobuf 3.24.4 |
| C++ server | gRPC 1.35.0 (built from source via `build_grpc.bat`), DuckDB (third_party/) |

All versions communicate via the same gRPC wire protocol (HTTP/2 + protobuf).
Different gRPC versions are fully interoperable — only the `.proto` file must match.

No Apache Arrow dependency.

---

## Version history

| Version | Summary |
|---|---|
| **v5.0.0** | Custom gRPC protocol, removed Arrow dependency, pure protobuf data transfer, C++ and C# servers from same .proto |
| v4.0.0–v4.1.9 | gRPC rewrite, .NET client, 48 bugs fixed across 15 review rounds |
| v3.1.x | IOCP server, VS 2017 fixes, streaming, stats, auto-reconnect |
| v3.0.0 | Windows IOCP edition |
| v2.0.0 | Thread pool + connection pool scaling |
| v1.0.0 | Initial C++11 server |
