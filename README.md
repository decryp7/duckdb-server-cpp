# DuckDB gRPC Server  v5.4

Three server implementations (C#, C++, Rust) and a .NET 4.6.2 client.
All use the same `proto/duckdb_service.proto` — fully interoperable.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Client (any language with gRPC + protobuf)                  │
│    Query(sql) → stream of columnar rows                      │
│    Execute(sql) → success/error                              │
│    BulkInsert(table, data) → rows inserted                   │
│    Ping() → "pong"    GetStats() → metrics                   │
├──────────────────────────────────────────────────────────────┤
│  gRPC / HTTP/2 (proto/duckdb_service.proto)                  │
├──────────────────────────────────────────────────────────────┤
│  Server (C# / C++ / Rust)                                    │
│                                                              │
│  QueryCache ──► cache hit? return from memory (0.1ms)        │
│       │                                                      │
│       ▼ miss                                                 │
│  ShardedDuckDb (N independent database instances)            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ Shard 0  │ │ Shard 1  │ │ Shard 2  │ │ Shard 3  │        │
│  │ Pool+Wrt │ │ Pool+Wrt │ │ Pool+Wrt │ │ Pool+Wrt │        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘        │
│       └─────┬──────┴─────┬──────┘             │              │
│         READ: round-robin    WRITE: fan-out to all           │
└──────────────────────────────────────────────────────────────┘
```

---

## Quick start

```powershell
git clone https://github.com/decryp7/duckdb-server-cpp.git
cd duckdb-server-cpp
nuget restore DuckDbServer.sln    # C# dependencies
# Build Release in VS2017
run_server.bat                    # Start C# server
run_benchmark.bat --quick         # Benchmark
```

---

## Run scripts

| Script | What it does |
|--------|-------------|
| `run_server.bat` | C# server (Release, 4 shards, 128 readers) |
| `run_client.bat` | C# client examples |
| `run_benchmark.bat` | Performance benchmark (--quick / --full) |
| `run_cpp_server.bat` | C++ server (requires gRPC from build_grpc.bat) |
| `run_rust_server.bat` | Rust server (`cd rust-server && cargo build --release`) |

---

## Protocol

Defined in `proto/duckdb_service.proto`. Columnar encoding.

| RPC | Type | Description |
|-----|------|-------------|
| Query | Server-streaming | SQL → columnar rows (ColumnData with packed arrays) |
| Execute | Unary | DML/DDL → success/error |
| BulkInsert | Unary | Table + columnar data → rows inserted |
| Ping | Unary | → "pong" |
| GetStats | Unary | → metrics (reads, writes, errors, pool size) |

---

## Performance features

| Feature | Description |
|---------|-------------|
| **Sharding** | N independent DuckDB instances, read-all/write-all |
| **Query cache** | LRU with TTL, bypasses DuckDB on cache hit (~0.1ms) |
| **Columnar encoding** | Packed arrays per column (10 objects vs 11,000 per-cell) |
| **Connection pool** | ConcurrentBag + SemaphoreSlim (lock-free reads) |
| **Write batching** | Concurrent writes batched into single transactions |
| **INSERT merging** | N individual INSERTs → 1 multi-row INSERT |
| **DDL detection** | CREATE/DROP run outside transactions automatically |
| **DuckDB tuning** | threads=1, preserve_insertion_order=false, checkpoint 256MB |
| **Memory limit/shard** | Auto-calculates per-shard limit to prevent OOM (80%/N) |
| **Late materialization** | Extended to 1000 rows for faster LIMIT/pagination queries |
| **Allocator flush** | 128MB threshold reduces OS memory return overhead |
| **Protobuf Arena** | C++ uses Arena allocation for 40-60% fewer mallocs in Query |
| **Concurrent appends** | INSERT via Execute bypasses write queue for direct execution |
| **QueryArrow RPC** | Rust server streams Arrow IPC for zero-copy reads |
| **Sorted insert** | Optional sort_columns in BulkInsert for better zonemap effectiveness |
| **Parallel write fan-out** | std::async (C++), Task.Run (C#), thread::scope (Rust) |
| **Server GC** | .NET server GC mode (1 thread per CPU core) |
| **Table-level cache** | Write invalidates only affected table's cache entries |
| **Hybrid sharding** | File DB backup + N memory shards for reads (5-10x faster) |
| **Memory eviction** | LRU eviction drops cold tables from memory, fallback to file DB |
| **gRPC tuning** | 200 max streams, 2MB write buffer, keepalive, thread limiter |

---

## Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `:memory:` | DuckDB file path |
| `--port` | `19100` | gRPC listen port |
| `--shards` | `1` | Number of DuckDB instances |
| `--readers` | `nCPU×2` | Total connection pool size |
| `--batch-ms` | `5` | Write batch window (ms) |
| `--batch-max` | `512` | Max writes per batch |
| `--batch-size` | `8192` | Rows per gRPC response |
| `--memory-limit` | auto | DuckDB memory limit (auto: 80%/shards) |
| `--threads` | `1` | DuckDB threads per connection |
| `--backup-db` | — | Hybrid mode: file DB path for durable backup |
| `--temp-dir` | — | Temp directory for DuckDB spill-to-disk |
| `--timeout` | `30` | Query timeout (seconds, C# only) |

---

## Dependencies

| Component | Dependencies |
|-----------|-------------|
| C# server | Grpc.Core 2.46.6, Google.Protobuf 3.24.4, DuckDB.NET.Data.Full 1.1.1 |
| C# client | Grpc.Core 2.46.6, Google.Protobuf 3.24.4 |
| C++ server | gRPC 1.35.0 (build_grpc.bat), DuckDB (third_party/) |
| Rust server | tonic 0.12, prost 0.13, duckdb 1.1 (bundled) |

---

## File map

```
DuckDbServer.sln                 VS2017 solution (C# projects)
proto/duckdb_service.proto       Protocol definition (all languages)
generate_proto.bat               Regenerate C# proto code

server/                          C# server
  DuckDbServer.cs                gRPC handlers (thin, delegates to:)
  ShardedDuckDb.cs               Read-all/write-all sharding
  QueryCache.cs                  LRU cache with TTL
  ConnectionPool.cs              Lock-free pool (ConcurrentBag)
  WriteSerializer.cs             Batch writes + INSERT merging
  ColumnBuilder.cs               Columnar protobuf packing
  DatabaseManager.cs             Shared DB + PRAGMAs
  DdlDetector.cs                 DDL vs DML detection
  InsertParser.cs + Batcher.cs   INSERT merging

client/                          C# client (.NET 4.6.2)
  DuckDbClient.cs                gRPC client
  Interfaces.cs                  IDuckDbClient, IQueryResult

benchmark/                       Performance benchmark
  BenchmarkRunner.cs             6 scenario types
  Program.cs                     --quick / --full modes

rust-server/                     Rust server
  src/main.rs                    tonic gRPC + query_arrow
  src/shard.rs                   Sharding
  src/pool.rs                    Connection pool (try_clone)
  src/cache.rs                   Query cache
  src/writer.rs                  Write batching

cpp/                             C++ server (VS2017)
  DuckDbServerCpp.vcxproj        Project file
include/                         C++ headers
src/                             C++ implementation
```

---

## Version history

| Version | Summary |
|---|---|
| **v5.4** | Hybrid sharding (file backup + memory reads), memory eviction, query fallback |
| v5.3 | Table-level cache invalidation, gRPC thread limiter |
| v5.2 | QueryArrow RPC, concurrent appends, sorted insert, parallel fan-out, 28 bug fixes |
| v5.1 | Performance: auto memory_limit/shard, late materialization, allocator flush, Arena alloc (C++), temp_directory |
| **v5.0** | Custom gRPC, no Arrow, sharding, caching, 3 server implementations |
| v4.x | Arrow Flight (removed — .NET 4.6.2 incompatible) |
| v3.x | IOCP server |
| v1-2 | Initial C++ server |
