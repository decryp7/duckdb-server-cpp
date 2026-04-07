# DuckDB gRPC Server — Architecture Document

## Table of Contents

1. [Overview](#overview)
2. [Use Cases](#use-cases)
3. [System Architecture](#system-architecture)
4. [Class Diagrams](#class-diagrams)
5. [Sequence Diagrams](#sequence-diagrams)
6. [Component Details](#component-details)
7. [Protocol Design](#protocol-design)
8. [Performance Architecture](#performance-architecture)
9. [Deployment](#deployment)

---

## Overview

Three server implementations (C#, C++, Rust) expose DuckDB over gRPC using a custom
columnar protocol defined in `proto/duckdb_service.proto`. All servers are fully
interoperable — any client generated from the proto can talk to any server.

### Design Goals

- **Sub-millisecond cached queries** — LRU cache bypasses DuckDB entirely on hit
- **Linear read scaling** — N shards = N× read throughput via round-robin
- **Write consistency** — Fan-out writes to ALL shards keep data identical
- **VS2017 / .NET 4.6.2 compatibility** — C# server and client target legacy framework
- **Zero-copy where possible** — C++ memcpy from DuckDB buffers, Rust query_arrow()

---

## Use Cases

```mermaid
graph LR
    subgraph Actors
        A[Client Application]
        B[Benchmark Tool]
        C[Admin / DBA]
    end

    subgraph "DuckDB gRPC Server"
        UC1[Query - Read Data]
        UC2[Execute - Write Data]
        UC3[BulkInsert - Batch Load]
        UC4[Ping - Health Check]
        UC5[GetStats - Monitoring]
    end

    A --> UC1
    A --> UC2
    A --> UC3
    A --> UC4
    B --> UC1
    B --> UC2
    B --> UC3
    C --> UC4
    C --> UC5
```

### Use Case Descriptions

| Use Case | Actor | Description | Protocol |
|----------|-------|-------------|----------|
| **Query** | Client | Execute SQL SELECT, receive columnar streaming results | Server-streaming RPC |
| **Execute** | Client | Execute DML/DDL (INSERT, CREATE TABLE, etc.) | Unary RPC |
| **BulkInsert** | Client | Send columnar data for fast batch insertion | Unary RPC |
| **Ping** | Client/Admin | Health check, returns "pong" | Unary RPC |
| **GetStats** | Admin | Server metrics (reads, writes, errors, pool size) | Unary RPC |

---

## System Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        C1[C# Client<br/>.NET 4.6.2]
        C2[Any gRPC Client<br/>Python, Go, Java...]
    end

    subgraph "Transport Layer"
        GRPC[gRPC / HTTP/2<br/>proto/duckdb_service.proto<br/>Port 19100]
    end

    subgraph "Server Layer"
        CS[C# Server<br/>Grpc.Core 2.46.6]
        CPP[C++ Server<br/>gRPC 1.35.0]
        RS[Rust Server<br/>tonic 0.12]
    end

    subgraph "Caching Layer"
        QC[QueryCache<br/>LRU + TTL 60s<br/>Max 10K entries]
    end

    subgraph "Sharding Layer"
        SD[ShardedDuckDb<br/>Read: Round-Robin<br/>Write: Fan-out All]
    end

    subgraph "Shard 0"
        P0[ConnectionPool<br/>N readers]
        W0[WriteSerializer<br/>Batch + Merge]
        DB0[(DuckDB<br/>Instance 0)]
    end

    subgraph "Shard 1"
        P1[ConnectionPool<br/>N readers]
        W1[WriteSerializer<br/>Batch + Merge]
        DB1[(DuckDB<br/>Instance 1)]
    end

    subgraph "Shard N"
        PN[ConnectionPool<br/>N readers]
        WN[WriteSerializer<br/>Batch + Merge]
        DBN[(DuckDB<br/>Instance N)]
    end

    C1 --> GRPC
    C2 --> GRPC
    GRPC --> CS
    GRPC --> CPP
    GRPC --> RS
    CS --> QC
    CPP --> QC
    RS --> QC
    QC --> SD
    SD --> P0
    SD --> P1
    SD --> PN
    SD --> W0
    SD --> W1
    SD --> WN
    P0 --> DB0
    W0 --> DB0
    P1 --> DB1
    W1 --> DB1
    PN --> DBN
    WN --> DBN
```

### Request Flow

```mermaid
flowchart TD
    REQ[Incoming gRPC Request] --> TYPE{Request Type?}

    TYPE -->|Query| CACHE{Cache Hit?}
    CACHE -->|Yes| RETURN_CACHED[Return Cached Response<br/>~0.1ms]
    CACHE -->|No| SHARD_R[next_for_read<br/>Round-Robin]
    SHARD_R --> POOL[ConnectionPool.Borrow]
    POOL --> EXEC_Q[Execute SQL on DuckDB]
    EXEC_Q --> BUILD[Build Columnar Response]
    BUILD --> CACHE_PUT[Store in QueryCache]
    CACHE_PUT --> STREAM[Stream to Client]

    TYPE -->|Execute| WRITE_ALL[write_to_all<br/>Fan-out to ALL shards]
    WRITE_ALL --> WS[WriteSerializer.Submit<br/>Batch + Merge INSERTs]
    WS --> EXEC_W[BEGIN...COMMIT on DuckDB]
    EXEC_W --> INVALIDATE[Cache.Invalidate]
    INVALIDATE --> RESPOND_OK[Return Success]

    TYPE -->|BulkInsert| BUILD_SQL[Build multi-row INSERT SQL]
    BUILD_SQL --> WRITE_ALL2[write_to_all]
    WRITE_ALL2 --> WS2[WriteSerializer.Submit]
    WS2 --> EXEC_B[Execute on DuckDB]
    EXEC_B --> INVALIDATE2[Cache.Invalidate]
    INVALIDATE2 --> RESPOND_BULK[Return rows_inserted]

    TYPE -->|Ping| PONG[Return 'pong']
    TYPE -->|GetStats| STATS[Return Metrics]
```

---

## Class Diagrams

### C# Server Class Diagram

```mermaid
classDiagram
    class DuckDbServer {
        -ServerConfig config
        -ShardedDuckDb shardedDb
        -IQueryCache queryCache
        -int batchSize
        -long queriesRead
        -long queriesWrite
        -long errors
        +Query(QueryRequest, IServerStreamWriter, ServerCallContext)
        +Execute(ExecuteRequest, ServerCallContext) ExecuteResponse
        +BulkInsert(BulkInsertRequest, ServerCallContext) BulkInsertResponse
        +Ping(PingRequest, ServerCallContext) PingResponse
        +GetStats(StatsRequest, ServerCallContext) StatsResponse
        +Dispose()
    }

    class ShardedDuckDb {
        -Shard[] shards
        -long nextReadShard
        +int ShardCount
        +int TotalPoolSize
        +NextForRead() Shard
        +WriteToAll(string sql) WriteResult
        +Dispose()
    }

    class Shard {
        +DatabaseManager DbManager
        +IConnectionPool Pool
        +IWriteSerializer Writer
    }

    class ConnectionPool {
        -ConcurrentBag~DuckDBConnection~ idle
        -SemaphoreSlim semaphore
        -int poolSize
        +Borrow() IConnectionHandle
        +Dispose()
    }

    class WriteSerializer {
        -DuckDBConnection writerConnection
        -Queue~WriteRequest~ queue
        -Thread writerThread
        +Submit(string sql) WriteResult
        +Dispose()
    }

    class QueryCache {
        -ConcurrentDictionary entries
        -int maxEntries
        -int ttlSeconds
        +TryGet(string sql, out responses) bool
        +Put(string sql, responses)
        +Invalidate()
    }

    class DatabaseManager {
        -DuckDBConnection primaryConnection
        -string connectionString
        +CreateConnection() DuckDBConnection
        +Dispose()
    }

    class ColumnBuilder {
        -ColumnType type
        +Add(object value, int rowIndex)
        +Build() ColumnData
        +Reset()
    }

    class InsertBatcher {
        +MergeInserts(List~string~) List~BatchGroup~$
    }

    class DdlDetector {
        +IsDdl(string sql) bool$
    }

    class IConnectionPool {
        <<interface>>
        +Borrow() IConnectionHandle
        +int Size
    }

    class IWriteSerializer {
        <<interface>>
        +Submit(string sql) WriteResult
    }

    class IQueryCache {
        <<interface>>
        +TryGet(sql, out responses) bool
        +Put(sql, responses)
        +Invalidate()
    }

    DuckDbServer --> ShardedDuckDb
    DuckDbServer --> IQueryCache
    ShardedDuckDb --> Shard
    Shard --> DatabaseManager
    Shard --> IConnectionPool
    Shard --> IWriteSerializer
    ConnectionPool ..|> IConnectionPool
    WriteSerializer ..|> IWriteSerializer
    QueryCache ..|> IQueryCache
    WriteSerializer --> InsertBatcher
    WriteSerializer --> DdlDetector
    DuckDbServer --> ColumnBuilder
```

### C++ Server Class Diagram

```mermaid
classDiagram
    class DuckGrpcServer {
        -ServerConfig cfg_
        -vector~unique_ptr~Shard~~ shards_
        -QueryCache cache_
        -atomic~size_t~ next_read_shard_
        -atomic~long long~ stat_queries_read_
        -atomic~long long~ stat_queries_write_
        -atomic~long long~ stat_errors_
        +Query(ServerContext, QueryRequest, ServerWriter) Status
        +Execute(ServerContext, ExecuteRequest, ExecuteResponse) Status
        +BulkInsert(ServerContext, BulkInsertRequest, BulkInsertResponse) Status
        +Ping(ServerContext, PingRequest, PingResponse) Status
        +GetStats(ServerContext, StatsRequest, StatsResponse) Status
        -next_for_read() Shard&
        -write_to_all(string sql) WriteResult
    }

    class Shard {
        +duckdb_database db
        +unique_ptr~ConnectionPool~ pool
        +duckdb_connection writer_conn
        +unique_ptr~IWriteSerializer~ writer
        +~Shard()
    }

    class ConnectionPool {
        -duckdb_database db_
        -size_t pool_size_
        -queue~duckdb_connection~ idle_
        -mutex mu_
        -condition_variable cv_
        +borrow() Handle
        +size() size_t
        -apply_connection_pragmas(conn)$
    }

    class WriteSerializer {
        -duckdb_connection conn_
        -int batch_window_ms_
        -size_t batch_max_
        -atomic~bool~ stop_
        -queue~RequestPtr~ queue_
        -thread writer_thread_
        +submit(string sql) WriteResult
        +submit_async(string sql) future~WriteResult~
        -drain_loop()
        -execute_dml_run(batch, from, to)
        -is_ddl(string sql) bool$
    }

    class QueryCache {
        -unordered_map entries_
        -mutex mu_
        -size_t max_entries_
        -int ttl_seconds_
        +try_get(sql, out) bool
        +put(sql, responses)
        +invalidate()
    }

    DuckGrpcServer --> Shard
    DuckGrpcServer --> QueryCache
    Shard --> ConnectionPool
    Shard --> WriteSerializer
```

### Rust Server Class Diagram

```mermaid
classDiagram
    class DuckDbServerImpl {
        +Arc~ShardedDuckDb~ sharded_db
        +Arc~QueryCache~ query_cache
        +usize batch_size
        +u64 timeout_secs
        +u16 port
        +Arc~AtomicI64~ stat_reads
        +Arc~AtomicI64~ stat_writes
        +Arc~AtomicI64~ stat_errors
        +query(QueryRequest) Stream~QueryResponse~
        +execute(ExecuteRequest) ExecuteResponse
        +bulk_insert(BulkInsertRequest) BulkInsertResponse
        +ping(PingRequest) PingResponse
        +get_stats(StatsRequest) StatsResponse
    }

    class ShardedDuckDb {
        -Vec~Shard~ shards
        -AtomicUsize next_read
        +new(db_path, shard_count, readers, batch_ms, batch_max) Result
        +next_for_read() &Shard
        +write_to_all(sql) Result
    }

    class Shard {
        +Arc~ConnectionPool~ pool
        +Arc~WriteSerializer~ writer
    }

    class ConnectionPool {
        -Arc~ArrayQueue~Connection~~ queue
        -usize size
        +new(db_path, size) Result
        +borrow() Result~Handle~
        -apply_connection_pragmas(conn)$
    }

    class WriteSerializer {
        -Mutex~Sender~ request_tx
        -Mutex~Connection~ bulk_conn
        +from_conn(conn, batch_ms, batch_max) Result
        +submit(sql) Result
    }

    class QueryCache {
        -Mutex~HashMap~ entries
        -usize max_entries
        -Duration ttl
        +new(max_entries, ttl_seconds) Self
        +get(sql) Option~Vec~QueryResponse~~
        +put(sql, responses)
        +invalidate()
    }

    DuckDbServerImpl --> ShardedDuckDb
    DuckDbServerImpl --> QueryCache
    ShardedDuckDb --> Shard
    Shard --> ConnectionPool
    Shard --> WriteSerializer
```

---

## Sequence Diagrams

### Query (Cache Miss)

```mermaid
sequenceDiagram
    participant Client
    participant gRPC
    participant Server as DuckDbServer
    participant Cache as QueryCache
    participant Shard as ShardedDuckDb
    participant Pool as ConnectionPool
    participant DB as DuckDB

    Client->>gRPC: Query(sql)
    gRPC->>Server: Query(request, writer)

    Server->>Cache: TryGet(sql)
    Cache-->>Server: miss

    Server->>Shard: NextForRead()
    Shard-->>Server: shard[idx % N]

    Server->>Pool: Borrow()
    Pool-->>Server: connection handle

    Server->>DB: ExecuteQuery(sql)
    DB-->>Server: result chunks

    loop Each Chunk
        Server->>Server: BuildColumnarResponse(chunk)
        Server->>gRPC: Write(response)
        gRPC->>Client: stream response
    end

    Server->>Cache: Put(sql, responses)
    Server->>Pool: Return(connection)
```

### Query (Cache Hit)

```mermaid
sequenceDiagram
    participant Client
    participant gRPC
    participant Server as DuckDbServer
    participant Cache as QueryCache

    Client->>gRPC: Query(sql)
    gRPC->>Server: Query(request, writer)

    Server->>Cache: TryGet(sql)
    Cache-->>Server: hit (cached responses)

    loop Each Cached Response
        Server->>gRPC: Write(response)
        gRPC->>Client: stream response
    end

    Note over Server,Cache: DuckDB never touched<br/>~0.1ms latency
```

### Execute (Write with Fan-out)

```mermaid
sequenceDiagram
    participant Client
    participant gRPC
    participant Server as DuckDbServer
    participant Cache as QueryCache
    participant Shard as ShardedDuckDb
    participant WS0 as WriteSerializer[0]
    participant WS1 as WriteSerializer[1]
    participant DB0 as DuckDB[0]
    participant DB1 as DuckDB[1]

    Client->>gRPC: Execute(sql)
    gRPC->>Server: Execute(request)

    Server->>Shard: WriteToAll(sql)

    par Fan-out to all shards
        Shard->>WS0: Submit(sql)
        WS0->>WS0: Queue request
        WS0->>WS0: Collect batch (5ms window)
        WS0->>WS0: MergeInserts()
        WS0->>DB0: BEGIN
        WS0->>DB0: Execute merged SQL
        WS0->>DB0: COMMIT
        WS0-->>Shard: success
    and
        Shard->>WS1: Submit(sql)
        WS1->>DB1: BEGIN...COMMIT
        WS1-->>Shard: success
    end

    Shard-->>Server: WriteResult(ok)
    Server->>Cache: Invalidate()
    Server-->>gRPC: ExecuteResponse(success)
    gRPC-->>Client: success
```

### Write Batching (WriteSerializer Internal)

```mermaid
sequenceDiagram
    participant T1 as Thread A
    participant T2 as Thread B
    participant T3 as Thread C
    participant WS as WriteSerializer<br/>(Background Thread)
    participant DB as DuckDB

    par Concurrent submits
        T1->>WS: Submit("INSERT INTO t VALUES (1)")
        T2->>WS: Submit("INSERT INTO t VALUES (2)")
        T3->>WS: Submit("UPDATE t SET x=1")
    end

    Note over WS: Collect batch<br/>(5ms window or 512 max)

    WS->>WS: MergeInserts()<br/>Group 1: INSERT INTO t VALUES (1),(2)<br/>Group 2: UPDATE t SET x=1

    WS->>DB: BEGIN
    WS->>DB: INSERT INTO t VALUES (1),(2)
    WS->>DB: UPDATE t SET x=1
    WS->>DB: COMMIT

    WS-->>T1: WriteResult(ok)
    WS-->>T2: WriteResult(ok)
    WS-->>T3: WriteResult(ok)
```

### BulkInsert

```mermaid
sequenceDiagram
    participant Client
    participant Server as DuckDbServer
    participant Builder as SQLBuilder
    participant Shard as ShardedDuckDb
    participant WS as WriteSerializer
    participant Cache as QueryCache

    Client->>Server: BulkInsert(table, columns, data, row_count)

    Server->>Server: Validate(table, columns, data)

    Server->>Builder: Build INSERT SQL from columnar data
    Note over Builder: INSERT INTO table VALUES<br/>(v1,v2,...), (v3,v4,...), ...

    Server->>Shard: WriteToAll(sql)
    Shard->>WS: Submit(sql) [each shard]
    WS-->>Shard: success
    Shard-->>Server: WriteResult(ok)

    Server->>Cache: Invalidate()
    Server-->>Client: BulkInsertResponse(success, rows_inserted)
```

---

## Component Details

### Sharding Strategy: Read-All / Write-All

```
Write fan-out:              Read round-robin:
  Client writes "X"           Client reads
       │                          │
  ┌────┴────┐              ┌─────┴─────┐
  ▼    ▼    ▼              ▼           ▼
┌───┐┌───┐┌───┐         ┌───┐       ┌───┐
│S0 ││S1 ││S2 │         │S0 │  next  │S1 │  next ...
│ X ││ X ││ X │         │   │──────► │   │──────►
└───┘└───┘└───┘         └───┘       └───┘
All shards identical     Round-robin distribution
```

- **Reads**: Atomic counter `idx++ % N` distributes queries evenly
- **Writes**: Every shard executes the same write (data stays consistent)
- **Trade-off**: Write throughput = single shard speed; read throughput = N × single shard
- **Memory databases**: Each shard is independent `:memory:` instance
- **File databases**: Each shard uses `dbname_0.db`, `dbname_1.db`, etc.

### Connection Pool

| Language | Data Structure | Blocking Strategy | Timeout |
|----------|---------------|-------------------|---------|
| C# | ConcurrentBag + SemaphoreSlim | User-mode semaphore wait | 10s |
| C++ | std::queue + mutex + condition_variable | cv.wait_until | 10s |
| Rust | crossbeam::ArrayQueue | Spin + yield_now | 10s |

### Write Serialization Pipeline

```
Submit() ──► Queue ──► CollectBatch ──► ClassifyDDL ──► MergeINSERTs ──► Transaction
                       (5ms window)     (DDL alone)    (multi-row)      (BEGIN..COMMIT)
                       (max 512)                                         │
                                                                    Failure?
                                                                    ──► ROLLBACK
                                                                    ──► Retry individually
```

### Query Cache

- **Storage**: ConcurrentDictionary (C#), Mutex<HashMap> (Rust), mutex + unordered_map (C++)
- **Key**: Raw SQL string
- **Value**: List/Vec of serialized QueryResponse protobuf messages
- **TTL**: 60 seconds (configurable)
- **Max entries**: 10,000
- **Eviction**: Passive — expired entries removed on capacity check
- **Invalidation**: Full clear on any write (Execute or BulkInsert)

---

## Protocol Design

### Columnar Encoding

```
Row format (traditional):      Columnar format (this project):
┌─────────────────────┐        ┌──────────────────────────┐
│ Row 0: {id:1, x:10} │        │ Column "id": [1, 2, 3]  │  ← packed int32
│ Row 1: {id:2, x:20} │        │ Column "x":  [10,20,30] │  ← packed int32
│ Row 2: {id:3, x:30} │        │ null_indices: []         │
└─────────────────────┘        └──────────────────────────┘
11,000 objects (1000×10+1000)   10 objects (10 columns)
```

### Type Mapping

| DuckDB Type | Proto ColumnType | Proto Field | Size |
|-------------|-----------------|-------------|------|
| BOOLEAN | TYPE_BOOLEAN | bool_values | 1 byte |
| TINYINT | TYPE_INT8 | int32_values | widened to 4B |
| SMALLINT | TYPE_INT16 | int32_values | widened to 4B |
| INTEGER | TYPE_INT32 | int32_values | 4 bytes |
| BIGINT | TYPE_INT64 | int64_values | 8 bytes |
| UTINYINT | TYPE_UINT8 | int32_values | widened to 4B |
| USMALLINT | TYPE_UINT16 | int32_values | widened to 4B |
| UINTEGER | TYPE_UINT32 | int64_values | widened to 8B |
| UBIGINT | TYPE_UINT64 | int64_values | 8 bytes |
| FLOAT | TYPE_FLOAT | float_values | 4 bytes |
| DOUBLE | TYPE_DOUBLE | double_values | 8 bytes |
| VARCHAR | TYPE_STRING | string_values | variable |
| BLOB | TYPE_BLOB | blob_values | variable |
| DECIMAL | TYPE_DECIMAL | double_values | lossy to 8B |

---

## Performance Architecture

### DuckDB Tuning (per connection)

| Setting | Value | Rationale |
|---------|-------|-----------|
| `threads=1` | 1 thread per connection | Pool provides parallelism; avoids internal contention |
| `preserve_insertion_order=false` | Disabled | 1.5-3× faster scans without ORDER BY |
| `enable_object_cache` | Enabled | Caches Parquet/table metadata |
| `checkpoint_threshold` | 256MB | Reduces checkpoint frequency (default 16MB) |
| `memory_limit` | auto: 80%/N shards | Prevents N shards each claiming 80% RAM → OOM |
| `late_materialization_max_rows` | 1000 | Faster ORDER BY...LIMIT for pagination (default 50) |
| `allocator_flush_threshold` | 128MB | Reduces OS memory return overhead |
| `temp_directory` | configurable | Fast NVMe path for spill-to-disk |

### C++ Protobuf Arena Allocation

The C++ Query handler uses `google::protobuf::Arena` to allocate per-chunk
`QueryResponse` messages. This reduces malloc/free overhead by 40-60% for
large result sets with many chunks. The arena is created per-chunk and
destroyed automatically when the chunk is done processing.

### gRPC Tuning

| Parameter | Value | Purpose |
|-----------|-------|---------|
| Max concurrent streams | 200 | HTTP/2 multiplexing limit |
| Max message size | 64MB | Large result support |
| HTTP/2 write buffer | 2MB | Batches small writes |
| Keepalive interval | 30s | Detects dead connections |
| Keepalive timeout | 10s | Grace period before disconnect |
| BDP probe | Enabled | Auto-tunes TCP window |

### C# Server GC

```xml
<!-- App.config -->
<gcServer enabled="true"/>      <!-- One GC heap per CPU core -->
<gcConcurrent enabled="true"/>  <!-- Background Gen2 collection -->
```

---

## Deployment

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `:memory:` | DuckDB file path or `:memory:` |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `19100` | gRPC listen port |
| `--shards` | `1` | Number of DuckDB instances |
| `--readers` | `nCPU×2` | Total read connection pool |
| `--batch-ms` | `5` | Write batch window (ms) |
| `--batch-max` | `512` | Max writes per batch |
| `--batch-size` | `8192` | Rows per streaming response |
| `--memory-limit` | auto | DuckDB memory limit (auto: 80%/shards, or e.g. "8GB") |
| `--threads` | `1` | DuckDB threads per connection |
| `--timeout` | `30` | Query timeout (seconds, C# only) |
| `--backup-db` | — | Hybrid mode: file DB for durable backup + memory reads |
| `--temp-dir` | — | Temp directory for DuckDB spill-to-disk |
| `--tls-cert` | — | TLS certificate PEM path |
| `--tls-key` | — | TLS private key PEM path |

### Recommended Production Configuration

```bash
# High-throughput read-heavy workload (8-core machine)
--shards 8 --readers 128 --batch-ms 1 --batch-max 64 --memory-limit 8GB

# Hybrid mode: durable file backup + fast memory reads (5-10x)
--backup-db data.duckdb --shards 4 --readers 64 --memory-limit 8GB

# Write-heavy workload
--shards 2 --readers 32 --batch-ms 5 --batch-max 512

# Single-user development
--shards 1 --readers 4
```
