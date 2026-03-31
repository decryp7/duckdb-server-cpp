# CLAUDE.md — Project Context File

This file summarises the full development conversation so a future Claude session
can pick up exactly where this one left off without re-reading 19 conversation turns.

---

## Project identity

**Name**: DuckDB Arrow Flight Server  
**Current version**: 4.1.9 (packaged), runtime `DAS_VERSION "4.1.8"` in `main.cpp`  
**Language split**: C++14 server + .NET 4.6.2 (C# 7.3) client library  
**Primary constraint**: Must build with Visual Studio 2017 (MSVC 19.10+)

---

## Architecture in one diagram

```
.NET 4.6.2 client  (Apache.Arrow.Flight 14 + Grpc.Core 2.46)
  DasFlightClient ──── DoGet(ticket=SQL) ─────────────────────►
                  ◄─── Arrow record batch stream ──────────────
                  ───── DoAction("execute", SQL) ──────────────►
                  ◄─── empty stream (ok) / gRPC INTERNAL (err)

          gRPC / HTTP/2  (optionally TLS)

C++14 DuckDB Arrow Flight Server
  DuckFlightServer : FlightServerBase (gRPC thread pool)
  ┌──────────────────────────┬─────────────────────────────┐
  │  DoGet                   │  DoAction("execute")        │
  │  ConnectionPool          │  WriteSerializer            │
  │  DuckDbRecordBatchSource │  (batch DML transactions)   │
  │  (Arrow C Data Interface)│  single writer connection   │
  └──────────────────────────┴─────────────────────────────┘
  DuckDB (duckdb_query_arrow → ArrowSchema + ArrowArray)
```

---

## Evolution history (brief)

| Version | What changed |
|---------|-------------|
| v1–v3.x | Custom TCP + Windows IOCP server, binary framing protocol |
| v4.0.0  | Full rewrite to Apache Arrow Flight; gRPC/HTTP2; TLS built-in |
| v4.1.0  | Added C++ interfaces (`IRecordBatchSource`, `IConnectionPool`, `IWriteSerializer`); C# interfaces (`IDasFlightClient`, `IFlightQueryResult`, `IDasFlightPool`); shared `ArrowValueConverter`; heavy documentation pass |
| v4.1.1–v4.1.9 | 30+ bugs found and fixed across 9 continuous audit passes |

---

## File map

```
duckdb-arrow-server/
├── CLAUDE.md                   ← this file
├── README.md                   v4.1.8 — architecture, quick start, tuning
├── CHANGELOG.md                full version history with all bug fixes
├── PROTOCOL.md                 Flight RPC surface (DoGet / DoAction / ListActions)
├── BUILD_WINDOWS.md            VS 2017/2019/2022 build guide, vcpkg, TLS
├── DuckDB-Arrow-Server-Design.pptx  12-slide architecture deck
├── CMakeLists.txt              cmake 3.14+; find_package(Arrow + ArrowFlight + DuckDB)
│
├── include/
│   ├── interfaces.hpp          IRecordBatchSource, IConnectionPool, IWriteSerializer, WriteResult
│   ├── connection_pool.hpp     ConnectionPool : (conceptually IConnectionPool) + RAII Handle
│   ├── duck_bridge.hpp         DuckDbRecordBatchSource : IRecordBatchSource + RecordBatchReader
│   ├── flight_server.hpp       DuckFlightServer : FlightServerBase; ServerConfig; ServerStats
│   └── write_serializer.hpp    WriteSerializer : IWriteSerializer; DDL detection; batch txns
│
├── src/
│   ├── main.cpp                CLI parsing; signal handlers; arrow_ok(); server startup
│   ├── flight_server.cpp       DoGet / DoAction / ListActions / stats() / cleanup
│   ├── duck_bridge.cpp         DuckDbRecordBatchSource::Make() and read_next()
│   └── connection_pool.cpp     Stub (implementation is header-only)
│
└── client/
    ├── DuckArrowClient.csproj  net462; Apache.Arrow 14 + Apache.Arrow.Flight 14 + Grpc.Core 2.46.6
    ├── Interfaces.cs           IDasFlightClient, IFlightQueryResult, IDasFlightPool + ILease
    ├── DasFlightClient.cs      Concrete IDasFlightClient; Task.Run sync wrappers; Interlocked dispose
    ├── DasFlightPool.cs        Concrete IDasFlightPool; ConcurrentBag; SemaphoreSlim
    ├── FlightQueryResult.cs    Concrete IFlightQueryResult; ArrowTypeMapper (shared)
    ├── ArrowValueConverter.cs  Shared Box(IArrowArray, int) → object; Date32/64 → DateTimeOffset
    ├── DasException.cs         [Serializable] + deserialization ctor for net462
    ├── ArrowStreamReader.cs    Legacy IPC-bytes reader (kept for non-Flight interop)
    ├── DataTableExtensions.cs  ToDataTable() on ArrowStreamReader (delegates to shared helpers)
    └── Example.cs              9 annotated examples; SalesReportService DI pattern
```

---

## Key design decisions (with rationale)

### C++ server

**`DuckDbRecordBatchSource` holds the `ConnectionPool::Handle` for the stream's lifetime**  
DuckDB cursors are attached to the connection that created them. Releasing the
connection while reading batches would corrupt the cursor. The Handle is moved
into the source object in `Make()` and released when the `RecordBatchStream`
destructor runs (after gRPC finishes sending the last batch).

**No `ARROW_ASSIGN_OR_RAISE` inside `DoGet`'s try/catch**  
`ARROW_ASSIGN_OR_RAISE` expands to a bare `return status` — it never throws and
therefore bypasses any surrounding `try/catch`. `DuckDbRecordBatchSource::Make`
is called with an explicit `if (!source_result.ok())` check so that Arrow/DuckDB
errors still increment `stat_errors_` and reach the structured catch block.

**`arrow_ok()` lambda instead of `ARROW_CHECK_OK`**  
`ARROW_CHECK_OK` calls `std::abort()`. `arrow_ok()` throws `std::runtime_error`,
which is caught by the outer catch block and printed as a clean "Fatal:" message.

**`std::atomic<bool> stop_` initialised in the initialiser list (before `writer_thread_`)**  
C++ initialises members in declaration order. `stop_` is declared before
`writer_thread_`, so `stop_(false)` in the initialiser list guarantees the atomic
is set before the thread starts reading it. Putting `stop_.store(false)` in the
constructor *body* was too late — the thread had already started.

**Constructor cleanup lambda for `DuckFlightServer`**  
C++ only calls the destructor for fully-constructed objects. If any step in the
constructor throws, `db_` and `writer_conn_` (plain C handles, not RAII) would
leak. A `cleanup` lambda captures `this` and is called inside `catch(...)` before
rethrowing. The destructor handles the success path.

**`g_server` is `std::atomic<DuckFlightServer*>`**  
On Windows, `SetConsoleCtrlHandler` runs on a system thread. On POSIX, signal
handlers have strict async-signal-safety requirements. A plain pointer would be
a data race. Stored to `nullptr` in both the normal exit path (after `Serve()`
returns) and the exception path (first line of the `catch` block) to prevent a
second signal from calling `Shutdown()` on a partially-destroyed object.

**`strtol` with `errno` + range check instead of `atoi`**  
On 64-bit Linux `long` is 64-bit, `int` is 32-bit. `atoi` silently truncates.
`strtol` with `errno == ERANGE || n < INT_MIN || n > INT_MAX` catches overflow.

**POSIX signal handler uses `write()` not `std::cout`**  
`std::cout` holds internal locks. If the main thread holds the lock when the
signal arrives, the handler deadlocks. `write(STDERR_FILENO, ...)` is listed as
async-signal-safe by POSIX.1-2008.

### C# client

**All sync wrappers use `Task.Run(() => Async()).GetAwaiter().GetResult()`**  
`SynchronizationContext` in WPF/WinForms captures the current context. Blocking
with `.GetResult()` directly on an async method from the UI thread deadlocks
(continuation tries to resume on the blocked thread). `Task.Run` moves execution
to the thread pool, breaking the context capture.

**`_disposed` fields are `int`, guarded by `Interlocked.CompareExchange`**  
`bool` is not guaranteed atomic in .NET. `Interlocked.CompareExchange(ref _disposed, 1, 0)`
atomically sets the flag to 1 only if it was 0, ensuring only one thread proceeds
with disposal even with concurrent callers.

**`Date32` / `Date64` → `DateTimeOffset`, not `DateTime`**  
`ArrowTypeMapper.ToClrType` maps both to `typeof(DateTimeOffset)` for DataTable
column creation. `ArrowValueConverter.Box` must return the same type or DataTable
assignment throws `InvalidCastException`. Arrow's `GetDateTime()` returns `DateTime?`
(Kind=Unspecified), converted to `DateTimeOffset` via
`new DateTimeOffset(v.Value, TimeSpan.Zero)`.

**`DasException` is `[Serializable]` with a protected deserialization constructor**  
Required for exceptions crossing AppDomain boundaries on .NET Framework 4.6.2
(WCF, remoting, distributed tracing).

**`DoActionFirstResult` drains the full gRPC stream before returning**  
Returning early from the `while (await MoveNext())` loop leaves the gRPC call in
an incomplete state, causing server-side resource leaks and warnings. The first
body is stored, the loop continues to drain, then the stored body is returned.

**`DasFlightPool` constructor wraps client creation in `try/catch`**  
If the k-th `DasFlightClient` constructor throws, clients 0..k-1 are in `_idle`
but the pool is only partially constructed — its destructor never runs. The catch
block disposes all clients already created and the `SemaphoreSlim` before rethrowing.

---

## All bugs fixed across audit passes (summary)

### Critical / would crash or corrupt data
1. `execute_dml_run` set promises before COMMIT — data loss + `future_error` crash
2. `stop_` was plain `bool` — data race (→ `std::atomic<bool>`)
3. Double-free on EOS: `arr->release()` + `duckdb_destroy_arrow_array` (→ removed manual call)
4. UI-thread deadlock: sync wrappers used `.GetResult()` directly (→ `Task.Run`)
5. `ARROW_ASSIGN_OR_RAISE` in `DoGet` try/catch bypassed catch entirely (→ explicit ok() check)
6. `DuckFlightServer` constructor leaked `db_`/`writer_conn_` on exception (→ cleanup lambda)
7. `action.body` null dereference in "execute" handler (→ null guard)
8. `stop_` initialised in constructor body after thread started (→ moved to initialiser list)
9. `g_server` plain pointer accessed from signal/console handler thread (→ `std::atomic`)
10. `RecordBatch` objects leaked on `RpcException` in `QueryAsync` (→ dispose in catch)
11. `OperationCanceledException` in `QueryAsync` also leaked batches (→ broad `catch (Exception)`)
12. `DasFlightPool` partial constructor leaked `DasFlightClient` objects (→ try/catch in ctor)

### Medium / incorrect behavior
13. `stat_queries_read_` incremented before success (→ moved after stream set)
14. `Ping()` never verified "pong" body (→ equality check)
15. `ftell` return -1 not checked (→ `sz < 0` guard)
16. `fread` partial read not detected (→ compare return value to expected size)
17. `DasFlightClient._disposed` not thread-safe (→ `int` + `Interlocked`)
18. `DasFlightPool._disposed` not thread-safe (→ same pattern)
19. `Lease._disposed` not thread-safe (→ same pattern)
20. `DasFlightPool.Return()` `_sem.Release()` could throw leaving pool starved (→ try/catch)
21. `DasFlightPool` size=0 → `SemaphoreSlim(0,0)` throws (→ validate `size >= 1`)
22. `stats()` returned configured port (may be 0) not actual bound port (→ `actual_port_` atomic updated after `Init()`)
23. `g_server` not cleared on exception path (→ first line of catch block)
24. `DoActionFirstResult` returned early without draining gRPC stream (→ continue loop)
25. `Date32`/`Date64` returned `DateTime` but DataTable column typed `DateTimeOffset` (→ `new DateTimeOffset(v, Zero)`)
26. `parse_int` silently truncated on 64-bit Linux (→ `errno`/`ERANGE`/range check)
27. `DependencyInjectionPattern` example leaked client on exception (→ `using`)
28. `ConcurrentQueriesAsync` leaked results on `Task.WhenAll` failure (→ dispose in catch)

### Minor / compile-time or portability
29. `System.Data.dll` not referenced in SDK-style csproj (→ `<Reference Include="System.Data"/>`)
30. `RecordBatch.ColumnCount` does not exist in Arrow 14 (→ `Schema.FieldsList.Count`)
31. Arrow 14 `GetValue()` returns `T?` not `T` — DataTable type mismatch (→ `.HasValue ? v.Value : null`)
32. `System.Memory` needed for `ReadOnlySpan<byte>.ToArray()` (→ transitive dep, remove explicit listing)
33. `public interface ILease` inside interface — C# 8 syntax, invalid in 7.3 (→ removed `public`)
34. `DasException` not `[Serializable]` (→ added attribute + deserialization ctor)
35. `<chrono>` missing from `write_serializer.hpp` (→ added)
36. `<cctype>` and `<cstring>` missing from `write_serializer.hpp` (→ added)
37. `DAS_VERSION` / README / PROTOCOL stale version strings (→ updated multiple times)
38. `raw_schema` leaked on `duckdb_query_arrow_schema` failure (→ `duckdb_destroy_arrow_schema`)
39. `Microsoft.NETFramework.ReferenceAssemblies` not needed on Windows (→ removed)

---

## Build instructions

### Server (Windows, vcpkg)
```powershell
vcpkg install "arrow[flight,parquet]:x64-windows" duckdb:x64-windows

# VS 2017
cmake -B build -G "Visual Studio 15 2017" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake
cmake --build build --config Release -j

.\build\Release\duckdb_flight_server.exe --db C:\data\analytics.duckdb
```

### Client (.NET 4.6.2)
```xml
<PackageReference Include="Apache.Arrow"        Version="14.0.1" />
<PackageReference Include="Apache.Arrow.Flight" Version="14.0.1" />
<PackageReference Include="Grpc.Core"           Version="2.46.6" />
<Reference Include="System.Data" />
```

```csharp
using (IDasFlightClient client = new DasFlightClient("server", 17777))
using (IFlightQueryResult result = client.Query("SELECT * FROM sales"))
{
    DataTable dt = result.ToDataTable();
    myDataGridView.DataSource = dt;
}
```

---

## Flight RPC surface

| RPC | Purpose |
|-----|---------|
| `DoGet(ticket=SQL)` | Execute SELECT; stream Arrow batches to client |
| `DoAction("execute", sql)` | Execute DML/DDL; block until committed |
| `DoAction("ping", "")` | Liveness check; returns "pong" |
| `DoAction("stats", "")` | Returns JSON: `queries_read`, `queries_write`, `errors`, `reader_pool_size`, `port` |
| `ListActions()` | Enumerate supported actions |

---

## VS 2017 compatibility rules (critical)

These constraints were hard-won across multiple bugs:

1. **`std::atomic` NSDMI is broken** — do NOT write `std::atomic<bool> stop_ = false;`
   in the class body. Initialise every atomic in the **constructor initialiser list**
   (`stop_(false)`) or constructor body (`.store(0)`). The initialiser list is
   preferred when ordering matters (e.g. `stop_` before `writer_thread_`).

2. **Use `/std:c++14` explicitly** — add to MSVC compile options.

3. **`_WIN32_WINNT=0x0601`** — Windows 7+ minimum target.

4. **No digit separators in template args** — `10'000` in function default params is fine;
   watch for subtler uses.

5. **Lambdas with auto parameters** — not available in C++14; use explicit types.

---

## NuGet package policy

- **Single NuGet entry for Apache.Arrow** — `System.Memory`, `System.Buffers`,
  `System.Threading.Tasks.Extensions`, `Microsoft.Bcl.AsyncInterfaces` are ALL
  transitive dependencies pulled automatically. Do not list them explicitly.
- **`<Reference Include="System.Data" />`** — NOT a NuGet package. SDK-style projects
  targeting net462 do not auto-add GAC DLLs. Required for `DataTable`, `DataRow`.
- **`Grpc.Core 2.46.6`** — last gRPC release supporting .NET Framework. "Deprecated"
  means no new features; security patches continue. `Grpc.Net.Client` requires .NET 5+.

---

## What to work on next (if continuing)

- **Upgrade to .NET 6+** — switch `Grpc.Core` → `Grpc.Net.Client` (pure managed);
  use `async IAsyncEnumerable<RecordBatch>` streaming API instead of materialising all batches.
- **Authentication** — gRPC interceptors for Bearer token auth (already enabled by
  the Flight framework; just needs `ServerMiddleware` on the C++ side).
- **DuckDB authentication** — per-connection user/password if the database file has
  credentials configured.
- **Metrics endpoint** — expose `DoAction("stats")` results as Prometheus metrics.
- **PPTX slide deck** — `DuckDB-Arrow-Server-Design.pptx` has 12 slides covering the
  full architecture; update slide 12 (version history) to reflect v4.1.x fixes.
