# DuckDB gRPC Server — Performance Guide

Detailed code-level optimization guide for the DuckDB gRPC server (C#, C++, Rust).

---

## Table of Contents

1. [Architecture Impact on Performance](#architecture-impact-on-performance)
2. [DuckDB Tuning (Per-Connection PRAGMAs)](#duckdb-tuning-per-connection-pragmas)
3. [Sharding: Read vs Write Trade-off](#sharding-read-vs-write-trade-off)
4. [Query Cache](#query-cache)
5. [Connection Pool](#connection-pool)
6. [Write Batching Pipeline](#write-batching-pipeline)
7. [Columnar Encoding](#columnar-encoding)
8. [C++ Specific Optimizations](#c-specific-optimizations)
9. [Rust Specific Optimizations](#rust-specific-optimizations)
10. [gRPC Tuning](#grpc-tuning)
11. [Recommended Configurations](#recommended-configurations)
12. [Bottleneck Analysis](#bottleneck-analysis)
13. [Future Improvements](#future-improvements)

---

## Architecture Impact on Performance

### Where Time Is Spent

```
┌──────────────────────────────────────────────────────┐
│ Typical Query Latency Breakdown                      │
│                                                      │
│  DuckDB execution     ████████████████████  85-95%   │
│  Protobuf encoding    ██                    3-8%     │
│  gRPC/HTTP/2 framing  █                     1-3%     │
│  Network I/O          ░                     1-2%     │
│  Cache check          ░                     <0.1%    │
└──────────────────────────────────────────────────────┘
```

**Key insight:** DuckDB query execution dominates latency. Server-side optimizations
(faster serialization, better pooling) provide marginal gains. The highest-impact
improvements are:

1. **Query caching** — eliminates DuckDB entirely for repeated queries (~0.1ms)
2. **Sharding** — N× read throughput (but increases write latency on file DBs)
3. **DuckDB PRAGMAs** — `preserve_insertion_order=false` gives 1.5-3× faster scans

---

## DuckDB Tuning (Per-Connection PRAGMAs)

Every connection (reader pool + writer) gets these settings:

### `SET threads=1`

**Impact: 2-5× better aggregate throughput under concurrency**

DuckDB's default is `threads=nCPU`. With a pool of N connections, this means
N × nCPU internal threads all competing for CPU. Setting `threads=1` eliminates
this contention — the pool itself provides parallelism (N connections = N parallel queries).

```
WITHOUT threads=1 (pool=8, nCPU=8):
  64 DuckDB threads fighting for 8 CPU cores → context switching hell

WITH threads=1 (pool=8):
  8 DuckDB threads, 1 per connection → clean parallel execution
```

**Where it's set:**
- C#: `ConnectionPool.ApplyConnectionPragmas()` and `WriteSerializer.ApplyWriterPragmas()`
- C++: `ConnectionPool::apply_connection_pragmas()` and constructor for `writer_conn`
- Rust: `ConnectionPool::apply_connection_pragmas()`

**Gotcha:** `SET threads` is PER-CONNECTION, not database-wide. It MUST be applied
on every new connection. The writer connection was previously missing this setting,
causing a ~5× write performance regression on multi-shard file databases.

### `SET preserve_insertion_order=false`

**Impact: 1.5-3× faster table scans**

DuckDB normally preserves the order rows were inserted, requiring extra metadata.
Disabling this allows DuckDB to scan in storage order, enabling better parallelism
and cache locality. Analytics queries almost never need insertion order.

### `SET checkpoint_threshold='256MB'`

**Impact: 2-5× faster sustained writes (file DB only)**

Checkpoints flush the WAL to the main database file. The default 16MB triggers
frequent checkpoints during write bursts. 256MB reduces checkpoint frequency by 16×.

**Trade-off:** More data at risk on crash (up to 256MB of WAL not yet checkpointed).
Recovery time increases proportionally.

### `SET late_materialization_max_rows=1000`

**Impact: Faster ORDER BY...LIMIT queries**

DuckDB 1.2+ defers column fetching for LIMIT queries — operating on row IDs first,
then fetching only the needed rows. Default is 50 rows. Increased to 1000 for
common pagination patterns (LIMIT 100, LIMIT 500).

### `SET allocator_flush_threshold='128MB'`

**Impact: Reduces memory return overhead**

Controls when DuckDB returns freed memory to the OS. Setting it to 128MB prevents
the allocator from doing frequent mmap/munmap syscalls between queries.

### `PRAGMA enable_object_cache`

**Impact: Faster metadata lookups**

Caches table schema, Parquet metadata, and other objects. Negligible memory overhead,
but avoids redundant catalog lookups for repeated queries.

### `SET memory_limit` (auto-calculated)

**Impact: Prevents OOM with multiple shards**

Without this, each shard claims 80% of RAM independently. With 8 shards, that's
640% of RAM — guaranteed OOM. The server auto-calculates:

```
memory_limit = 80% / num_shards
```

With 4 shards: each gets 20% of RAM. With 1 shard: uses DuckDB default (80%).

---

## Sharding: Read vs Write Trade-off

### How It Works

```
N shards = N independent DuckDB instances

READS:  Round-robin across shards → N× read throughput
WRITES: Fan-out to ALL shards → same write throughput as 1 shard
```

### The File DB Write Problem

Each `COMMIT` on a file-based database triggers `fsync()` to the WAL file for
durability. With N shards on the same physical disk, N fsyncs serialize at the
I/O layer:

```
1 shard:  1 × fsync = ~5ms (SSD) or ~50ms (HDD)
4 shards: 4 × fsync = ~20ms (SSD) or ~200ms (HDD)
8 shards: 8 × fsync = ~40ms (SSD) or ~400ms (HDD)
```

**Rule of thumb:**
- **In-memory database**: Use many shards (4-8). No fsync, no I/O contention.
- **File database on SSD**: Use 2-4 shards. Moderate fsync overhead.
- **File database on HDD**: Use 1-2 shards. fsync dominates write latency.

### Benchmark Reference

| Config | Single Write | 10× Concurrent Writes | Read (10×) |
|--------|-------------|----------------------|------------|
| 1 shard, memory | ~2ms | ~5ms (200 ops/s) | ~2ms (5000 ops/s) |
| 4 shards, memory | ~3ms | ~5ms (200 ops/s) | ~1ms (10000 ops/s) |
| 1 shard, file SSD | ~8ms | ~15ms (70 ops/s) | ~3ms (3000 ops/s) |
| 4 shards, file SSD | ~25ms | ~30ms (40 ops/s) | ~1ms (8000 ops/s) |
| 8 shards, file HDD | ~500ms | ~220ms (40 ops/s) | ~2ms (3500 ops/s) |

---

## Query Cache

### Design

- **Key:** Raw SQL string (exact match only)
- **Value:** Serialized protobuf QueryResponse messages
- **TTL:** 60 seconds (hardcoded, configurable in constructor)
- **Max entries:** 10,000
- **Invalidation:** Full clear on ANY write (Execute or BulkInsert)

### Performance Characteristics

| Scenario | Latency |
|----------|---------|
| Cache hit | ~0.1ms (just protobuf copy + gRPC write) |
| Cache miss | 5-500ms (DuckDB query + encode + cache store) |

### Table-Level Invalidation (v5.2+)

Instead of clearing the entire cache on every write, the server parses the SQL
to extract the affected table name and only invalidates cache entries whose SQL
key contains that table name (case-insensitive substring match):

```
Execute("INSERT INTO events VALUES (1, 'click')")
  → extract_table_name → "events"
  → cache.invalidate_table("events")
  → removes: "SELECT * FROM events WHERE ...", "SELECT count(*) FROM events"
  → keeps:   "SELECT * FROM users WHERE ...", "SELECT * FROM products"
```

Supported SQL patterns: `INSERT INTO`, `UPDATE`, `DELETE FROM`,
`CREATE/DROP/ALTER TABLE [IF [NOT] EXISTS]`. Unknown patterns fall back
to full invalidation (safe).

### Limitations

- **No parameterized caching:** `SELECT * FROM t WHERE id=1` and `SELECT * FROM t WHERE id=2` are different cache keys.
- **Substring match over-invalidation:** Table name "users" also invalidates queries mentioning "users_archive". Safe but imprecise.
- **Memory:** Large result sets consume cache memory. 10,000 entries × 1MB avg = ~10GB worst case.

---

## Connection Pool

### Implementation by Language

| Language | Structure | Borrow | Return | Timeout |
|----------|-----------|--------|--------|---------|
| C# | ConcurrentBag + SemaphoreSlim | User-mode semaphore | Bag.Add + semaphore.Release | 10s |
| C++ | std::queue + mutex + condition_variable | cv.wait_until | push + cv.notify_one | 10s |
| Rust | crossbeam ArrayQueue | Lock-free pop + spin/yield | Lock-free push | 10s |

### Sizing

Default: `2 × logical CPU count`. With shards: divided evenly across shards.

```
8-core machine, 4 shards: 16 total connections / 4 shards = 4 per shard
```

Each connection consumes ~10-50MB of DuckDB memory (depends on query complexity).
Over-provisioning wastes memory; under-provisioning causes borrow timeouts.

---

## Write Batching Pipeline

### Pipeline Stages

```
Submit() → Queue → CollectBatch → ClassifyDDL → MergeINSERTs → Transaction
             ↓       (1ms window)   (DDL alone)   (multi-row)   (BEGIN..COMMIT)
          Enqueue     (max 64)                                      │
          + block                                               Failure?
                                                                → ROLLBACK
                                                                → Retry individually
```

### Batch Window (`--batch-ms`)

The writer thread waits `batch_ms` milliseconds after the first request
for additional requests to arrive. Longer windows accumulate more requests
per transaction (higher throughput) but increase latency.

| batch_ms | Single Write Latency | 100× Concurrent Writes |
|----------|---------------------|----------------------|
| 0 | ~2ms | ~50ms (each runs individually) |
| 1 | ~3ms | ~5ms (batched into 1-2 transactions) |
| 5 | ~7ms | ~6ms (better batching, slightly higher base) |
| 50 | ~52ms | ~52ms (diminishing returns, latency dominated) |

**Recommendation:** `--batch-ms 1` for low-latency, `--batch-ms 5` for throughput.

### INSERT Merging

The `InsertBatcher` / `merge_inserts` function combines consecutive single-row
INSERTs targeting the same table into a single multi-row INSERT:

```sql
-- Before (3 parse + plan + execute cycles):
INSERT INTO events VALUES (1, 'click', '2024-01-01')
INSERT INTO events VALUES (2, 'view', '2024-01-02')
INSERT INTO events VALUES (3, 'buy', '2024-01-03')

-- After (1 parse + plan + execute cycle):
INSERT INTO events VALUES (1, 'click', '2024-01-01'), (2, 'view', '2024-01-02'), (3, 'buy', '2024-01-03')
```

**Impact:** 3-10× faster for batches of small INSERTs (avoids N parse/plan cycles).

---

## Columnar Encoding

### Wire Format

The protobuf protocol uses packed repeated fields per column instead of
per-cell typed values:

```
Row format (traditional):              Columnar format (this project):
  10,000 TypedValue objects              10 ColumnData objects
  + 1,000 Row objects                    with packed arrays
  = 11,000 protobuf objects              = 10 protobuf objects

  ~99% fewer allocations, ~80% less wire overhead for numeric data
```

### Type Packing Rules

| DuckDB Type | Proto Field | Size | C++ Strategy |
|-------------|-------------|------|-------------|
| INT32 | int32_values (packed) | 4B | memcpy fast path (no nulls) |
| INT64, UINT64 | int64_values (packed) | 8B | memcpy fast path (no nulls) |
| FLOAT | float_values (packed) | 4B | memcpy fast path (no nulls) |
| DOUBLE | double_values (packed) | 8B | memcpy fast path (no nulls) |
| INT8, INT16 | int32_values (packed) | widened | Per-element cast (1/2B → 4B) |
| UINT8, UINT16 | int32_values (packed) | widened | Per-element cast (1/2B → 4B) |
| UINT32 | int64_values (packed) | widened | Per-element cast (4B → 8B) |
| BOOLEAN | bool_values (packed) | 1B | Per-element loop |
| VARCHAR | string_values | var | Inline (≤12B) or pointer |
| Any with NULLs | + null_indices | 4B/null | Per-element validity check |

---

## C++ Specific Optimizations

### memcpy Fast Path

For non-null numeric columns with matching wire size (INT32, INT64, FLOAT, DOUBLE),
the server copies the entire column in a single `memcpy()` call:

```cpp
// FAST PATH: one memcpy for entire column (~10ns for 2048 rows × 4 bytes)
auto* field = cd->mutable_int32_values();
field->Resize(cap, 0);
std::memcpy(field->mutable_data(), vals, row_count * sizeof(int32_t));
```

This is 2-4× faster than per-element `add_int32_values()` calls because it
avoids repeated bounds checks, size increments, and function call overhead.

### Protobuf Arena Allocation

The Query handler allocates per-chunk `QueryResponse` objects from a protobuf Arena:

```cpp
google::protobuf::Arena chunk_arena;
auto* response = Arena::CreateMessage<QueryResponse>(&chunk_arena);
// ... populate response ...
to_cache.push_back(*response);  // copy out for caching
writer->Write(*response);       // stream to client
// arena destroyed here — all chunk memory freed in one call
```

**Impact:** 40-60% fewer malloc/free calls for large result sets. The arena
pre-allocates memory chunks and deallocates them all at once when destroyed.

### DuckDB String Layout

DuckDB uses a union for string storage (`duckdb_string_t`):
- Strings ≤ 12 bytes: stored inline in `value.inlined.inlined[12]`
- Strings > 12 bytes: stored via `value.pointer.ptr`
- Length is always at `value.inlined.length` (same offset for both layouts)

The C++ server checks the 12-byte threshold and reads from the correct location:

```cpp
idx_t len = vals[r].value.inlined.length;
if (len <= 12)
    cd->add_string_values(std::string(vals[r].value.inlined.inlined, len));
else
    cd->add_string_values(std::string(vals[r].value.pointer.ptr, len));
```

---

## Rust Specific Optimizations

### `query_arrow()` — Zero-Copy Columnar Results

The Rust server uses `query_arrow()` instead of row-by-row `query()`:

```rust
let arrow_result = stmt.query_arrow([])?;
let batches: Vec<_> = arrow_result.collect();
```

This returns Apache Arrow `RecordBatch` objects that share memory with DuckDB's
internal columnar buffers. No per-row allocation or type conversion is needed.

### `prepare_cached()` — Skip Parse/Plan

```rust
let mut stmt = conn.prepare_cached(&sql)?;
```

DuckDB caches the prepared statement per connection. Repeated queries with the
same SQL text skip the parser and planner entirely. 2-5× faster for small queries.

---

## gRPC Tuning

### Server-Side Settings

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `GRPC_ARG_MAX_CONCURRENT_STREAMS` | 200 | HTTP/2 multiplexing: 200 RPCs per TCP connection |
| `MaxReceiveMessageSize` | 64MB | Prevents truncation of large BulkInsert payloads |
| `MaxSendMessageSize` | 64MB | Prevents truncation of large query results |
| `GRPC_ARG_HTTP2_WRITE_BUFFER_SIZE` | 2MB | Batches small gRPC writes into larger HTTP/2 frames |
| `GRPC_ARG_HTTP2_BDP_PROBE` | 1 | Auto-tunes TCP window based on bandwidth-delay product |
| `GRPC_ARG_KEEPALIVE_TIME_MS` | 30000 | Sends keepalive ping every 30s to detect dead connections |
| `GRPC_ARG_KEEPALIVE_TIMEOUT_MS` | 10000 | Disconnects if no response within 10s |
| `NUM_CQS` (C++) | nCPU | One completion queue per CPU core |
| `MAX_POLLERS` (C++) | 2×nCPU | Bounds gRPC polling thread count |

### C# .NET GC

```xml
<gcServer enabled="true"/>      <!-- One GC heap per CPU core -->
<gcConcurrent enabled="true"/>  <!-- Background Gen2 collection -->
```

Server GC uses one heap per core (reduces lock contention) and performs Gen2
collection on a background thread (reduces pause times from ~100ms to ~10ms).

---

## Recommended Configurations

### Decision Tree: Which Mode to Use

```
Is your data persistent (survives restart)?
  ├─ NO  → Use :memory:  (fastest, no fsync, no disk)
  │        --db :memory: --shards 4-8 --readers 64-128
  │
  └─ YES → Does your data fit in RAM?
            ├─ YES → Use file DB with large buffer pool (simplest)
            │        --db data.duckdb --shards 4 --memory-limit 12GB
            │        DuckDB caches all pages in RAM → same speed as :memory: for reads
            │
            └─ NO  → Is write latency critical?
                     ├─ YES → Use hybrid mode (file backup + memory reads)
                     │        --backup-db data.duckdb --shards 4 --memory-limit 8GB
                     │        Reads: memory only (no disk). Writes: 1 fsync (not N).
                     │
                     └─ NO  → Use file DB with moderate buffer pool
                              --db data.duckdb --shards 2-4 --memory-limit 8GB
```

### Why Buffer Pool First, Hybrid Second

DuckDB's buffer manager caches frequently-accessed pages in RAM. When the buffer
pool is large enough to hold your entire dataset, file DB reads are as fast as
memory DB reads — every page is already cached, no disk I/O occurs.

```
File DB + 12GB buffer pool + 8GB data:
  → All pages fit in buffer pool → zero disk reads → same as :memory:

File DB + 4GB buffer pool + 8GB data:
  → Only 50% cached → frequent page evictions → disk I/O on cold queries
  → THIS is where hybrid mode helps
```

Hybrid mode's real advantages over buffer pool:
1. **Write speed**: memory shards skip fsync entirely (file DB: 5-50ms per COMMIT)
2. **No checkpoint pauses**: memory shards never checkpoint (file DB: stop-the-world)
3. **Guaranteed no page faults**: all data always in RAM (buffer pool can evict hot pages under memory pressure)

### Configuration Examples

```bash
# RECOMMENDED: Persistent data that fits in RAM — just use large buffer pool
--db data.duckdb --shards 4 --readers 64 --memory-limit 12GB --batch-ms 1

# Read-heavy analytics (no persistence needed)
--db :memory: --shards 8 --readers 128 --batch-ms 1 --batch-max 64

# Write-heavy (event ingestion, ETL) — minimize fsync
--db data.duckdb --shards 2 --readers 32 --batch-ms 5 --batch-max 512

# Hybrid: data exceeds RAM or write latency is critical
--backup-db data.duckdb --shards 4 --readers 64 --memory-limit 8GB

# Development (single user)
--db :memory: --shards 1 --readers 4
```

---

## Bottleneck Analysis

### Identifying the Bottleneck

| Symptom | Bottleneck | Fix |
|---------|-----------|-----|
| High read latency, low CPU | DuckDB query execution | Add indexes, optimize SQL, more shards |
| High read latency, high CPU | Thread contention | Verify threads=1, reduce shards |
| Pool timeout errors | Not enough connections | Increase --readers |
| High write latency, file DB | fsync I/O | Reduce --shards, use SSD, or :memory: |
| High write latency, memory DB | Batch window too large | Reduce --batch-ms |
| Memory errors (OOM) | Too many shards | Set --memory-limit, reduce --shards |
| Cache hit rate near 0% | Unique queries | Expected for ad-hoc workloads |
| Cache hit rate near 100% | Repeated queries | Ideal — cache is working |

### Profiling Commands

```sql
-- Check query execution time (DuckDB internal)
EXPLAIN ANALYZE SELECT ...;

-- Check table size and compression
PRAGMA storage_info('my_table');

-- Check database size
PRAGMA database_size;

-- Check current memory usage
PRAGMA memory_limit;
SELECT current_setting('memory_limit');
```

---

## Hybrid Sharding (`--backup-db`)

> **Note:** For most workloads, a file DB with a large buffer pool (`--memory-limit`)
> is simpler and equally fast for reads. Use hybrid mode only when:
> - **Write latency is critical** (fsync is your bottleneck)
> - **Data exceeds available RAM** (buffer pool can't cache everything)
> - **Checkpoint pauses are unacceptable** (stop-the-world on file DB)

### Architecture

```
Normal mode (--db data.duckdb --shards 4):
  Shard 0-3: all file DBs, reads round-robin, writes to all
  Every read hits disk → slower

Hybrid mode (--backup-db data.duckdb --shards 4):
  Shard 0:   file DB (write-only, durable backup, 1 reader for fallback)
  Shard 1-3: memory DBs (all reads round-robin + writes)
  Reads never touch disk → 5-10x faster
  On startup: ATTACH file DB → COPY tables → DETACH
```

### Performance Impact

| Metric | File-only (4 shards) | Hybrid (1 file + 3 memory) |
|--------|---------------------|---------------------------|
| Read latency | 3-5ms (disk I/O) | 0.5-1ms (memory only) |
| Write latency | 4 × fsync | 1 × fsync (file shard only) |
| Durability | Full | Full (file DB has all data) |
| RAM usage | Low | High (all data × N memory shards) |

### Memory Eviction

In hybrid mode, memory shards can evict cold (least-recently-accessed) tables
when memory is tight. Evicted tables still exist on the file DB for fallback reads:

```
Memory shard at capacity:
  1. Sort tables by last access time (LRU)
  2. DROP oldest table from memory shard
  3. Table still on file DB → fallback reads work (slower)
  4. Hot tables stay in memory → fast reads continue
```

### Query Fallback

When a query fails on a memory shard (table evicted or not yet synced),
the server automatically retries on the file backup shard:

```
Query("SELECT * FROM cold_table")
  → memory shard: "Table cold_table does not exist"
  → fallback to file shard (shard 0): success
  → result cached for future hits
```

### When to Use Hybrid vs Buffer Pool

| Scenario | Recommendation |
|----------|---------------|
| Data < 50% of RAM | File DB + large `--memory-limit` (buffer pool caches everything) |
| Data > RAM | Hybrid `--backup-db` (memory shards for hot data, file for cold) |
| Write-heavy + need durability | Hybrid `--backup-db` (1 fsync instead of N) |
| Read-only analytics | File DB + large buffer pool (simplest) |
| Max write speed, no durability | `:memory:` shards (no hybrid needed) |

---

## Concurrent Append Fast Path

### How It Works

When `Execute(sql)` receives an INSERT statement, it bypasses the WriteSerializer
queue and executes directly on each shard's dedicated `bulk_conn`:

```
Execute("INSERT INTO t VALUES (1, 'hello')")
  ├─ is_simple_insert(sql) → true
  ├─ bulk_execute_all(sql)        ← Direct execution on bulk_conn (fast)
  │   ├─ shard[0].bulk_conn: execute
  │   ├─ shard[1].bulk_conn: execute  (parallel via std::async/Task.Run/thread::scope)
  │   └─ shard[N].bulk_conn: execute
  └─ cache.invalidate()

Execute("UPDATE t SET x=1 WHERE id=2")
  ├─ is_simple_insert(sql) → false
  └─ write_to_all(sql)           ← WriteSerializer queue (batched, serialized)
```

**Why this is safe:** DuckDB guarantees concurrent appends never conflict. INSERTs
only add new rows — they don't modify or delete existing data. UPDATEs and DELETEs
CAN conflict and must still be serialized.

**Impact:** 2-5× faster INSERT throughput via Execute RPC. Eliminates batch_ms
wait time and WriteSerializer queue overhead for the common INSERT case.

### Benchmark: INSERT Fast Path vs UPDATE Serialized

| Scenario | Concurrency | Expected Ops/s | Path |
|----------|------------|----------------|------|
| INSERT (fast path) | 1 | 200-500 | bulk_conn direct |
| INSERT (fast path) | 10 | 500-2000 | bulk_conn direct (parallel) |
| UPDATE (serialized) | 1 | 50-200 | WriteSerializer queue |
| UPDATE (serialized) | 10 | 100-400 | WriteSerializer batch |

---

## BulkInsert (Appender API)

### How It Works

The BulkInsert RPC uses the DuckDB Appender API instead of building SQL strings:

```
BulkInsert(table="events", columns=[id, value, label], data=[...], row_count=10000)
  ├─ C++:  duckdb_appender_create → duckdb_append_* per cell → duckdb_appender_flush
  ├─ C#:   DuckDBConnection.CreateAppender → row.AppendValue per cell → Dispose (auto-flush)
  └─ Rust: build SQL + bulk_execute_all (no native Appender in duckdb-rs for dynamic types)
```

**Why Appender is faster:** The Appender bypasses the SQL parser, planner, and
optimizer entirely. It writes directly to DuckDB's storage layer. For 10,000 rows,
this avoids parsing a 500KB SQL string.

**Impact:** 10-100× faster than SQL INSERT for batch data loading.

### Sorted Insert (Optional)

When `sort_columns` is set in the BulkInsertRequest, data is sorted before
insertion for better zonemap effectiveness:

```sql
-- Normal: INSERT INTO events VALUES (3,'buy'), (1,'click'), (2,'view')
-- Sorted: INSERT INTO events SELECT * FROM (VALUES (3,'buy'), (1,'click'), (2,'view'))
--         AS _t("id","action") ORDER BY "id"
```

This uses the SQL path (not Appender) because DuckDB's Appender doesn't support ordering.
The trade-off: slower insert (~5-10×) but faster selective reads (up to 10× for
queries with WHERE clauses on the sort column due to zonemap pruning).

---

## QueryArrow RPC (Arrow IPC)

### How It Works

The `QueryArrow` RPC returns query results as Apache Arrow IPC byte streams
instead of protobuf-encoded columnar data:

```
QueryArrow("SELECT * FROM events")
  → ArrowResponse { ipc_data: <schema bytes>, row_count: 0 }     // Schema message
  → ArrowResponse { ipc_data: <batch bytes>,  row_count: 2048 }  // Data batch 1
  → ArrowResponse { ipc_data: <batch bytes>,  row_count: 2048 }  // Data batch 2
  → ...
```

**Server support:**
- **Rust:** Full implementation using Arrow IPC `StreamWriter`. Zero-copy from DuckDB's
  internal Arrow buffers → IPC bytes → gRPC stream.
- **C++:** Returns UNIMPLEMENTED (Arrow C++ library not linked).
- **C#:** Returns UNIMPLEMENTED (no Arrow support in .NET 4.6.2).

**Client support:** Any language with an Arrow IPC reader (PyArrow, arrow-rs, etc.)
can consume the stream. No protobuf deserialization needed — just Arrow IPC decode.

---

## Benchmarking

### Available Benchmark Scenarios

| # | Scenario | What It Measures | Flags |
|---|----------|-----------------|-------|
| 1 | Concurrent Readers | Read throughput scaling (1-300 threads) | Standard |
| 2 | Concurrent Writers | Write batching effectiveness (1-300 threads) | Standard |
| 3 | Mixed Read/Write | Read/write contention under concurrent load | Standard |
| 4 | Large Result Sets | Streaming throughput (100K-24M rows) | Standard |
| 5 | Max Concurrency | Find thread count where errors begin | Full |
| 6 | Sustained Throughput | 60s stability test (memory leaks, GC pressure) | Full |
| 7 | **Cache Hit** | QueryCache throughput (~0.1ms expected) | Quick/Standard |
| 8 | **INSERT Fast Path** | Concurrent append bypass of WriteSerializer | Quick/Standard |
| 9 | **UPDATE Serialized** | WriteSerializer batch queue baseline | Standard |
| 10 | **BulkInsert Appender** | Appender API batch insertion (1K-100K rows) | Standard |

### Running Benchmarks

```powershell
# Quick smoke test (6 scenarios, ~30s)
run_benchmark.bat --quick

# Standard suite (all 10 scenario types, ~5min)
run_benchmark.bat

# Full suite + max concurrency + 60s sustained (~15min)
run_benchmark.bat --full
```

### What to Look For

| Metric | Good | Concerning |
|--------|------|-----------|
| Cache hit latency | <0.5ms | >2ms (cache not working) |
| INSERT fast path vs UPDATE | INSERT 2-5× faster | Similar (fast path not triggering) |
| BulkInsert 10K rows | <50ms | >500ms (Appender not used?) |
| Sustained read 60s | Stable ops/s | Declining ops/s (memory leak) |
| Error count | 0 | >0 (pool exhaustion, timeout) |

---

## Implemented Optimizations (v5.2)

All major optimizations from the research phase have been implemented:

| Optimization | Server | Status |
|-------------|--------|--------|
| **Appender API for BulkInsert** | C++ (duckdb_appender), C# (CreateAppender) | Done — 10-100× faster |
| **Concurrent append fast path** | All 3 (is_simple_insert → bulk_conn) | Done — bypasses write queue for INSERTs |
| **QueryArrow RPC (Arrow IPC)** | Rust (StreamWriter), C++/C# (UNIMPLEMENTED stub) | Done — zero-copy for Rust clients |
| **Sorted insert** | All 3 (sort_columns → ORDER BY subquery) | Done — better zonemap effectiveness |
| **Parallel write fan-out** | C++ (std::async), C# (Task.Run), Rust (thread::scope) | Done |
| **Prepared stmt cache** | Rust (prepare_cached built-in) | Done |
| **Protobuf Arena allocation** | C++ (per-chunk Arena) | Done — 40-60% fewer mallocs |
| **Memory limit per shard** | All 3 (auto 80%/N) | Done — prevents OOM |
| **Late materialization** | All 3 (1000 rows) | Done — faster LIMIT queries |
| **Allocator flush threshold** | All 3 (128MB) | Done |
| **Temp directory config** | All 3 (--temp-dir) | Done |
| **Table-level cache invalidation** | All 3 (extract_table_name → invalidate_table) | Done — less cache churn |
| **gRPC ResourceQuota** | C++ (hw × 4 max threads) | Done — prevents thread explosion |
| **Hybrid sharding** | All 3 (--backup-db: file + memory) | Done — 5-10x read speed |
| **Memory eviction** | C# (LRU table eviction + file fallback) | Done — handles >RAM datasets |

## Remaining Future Opportunities

| Improvement | Expected Gain | Why Not Yet |
|-------------|--------------|-------------|
| **gRPC async/callback service** | Better scaling at 1000+ concurrent | Requires full C++ server rewrite; mitigated by ResourceQuota thread limiter |
| **Arrow IPC for C++ server** | Zero-copy (already done in Rust) | Requires Arrow C++ library (libarrow) linkage; Rust server provides this |

### Why Not FlatBuffers?

FlatBuffers deserialization is 4-15× faster than protobuf. However:
- The bottleneck is DuckDB (85-95%), not serialization (3-8%)
- Packed repeated fields in protobuf already achieve near-zero-copy for numeric data
- FlatBuffers gRPC support is less mature, especially for C# .NET 4.6.2
- **Expected improvement: <5% overall (not worth the complexity)**
