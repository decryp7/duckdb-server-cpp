# DuckDB Arrow Flight Server — Protocol  (v4.1.8)

## Overview

The server implements the **Apache Arrow Flight** RPC protocol over **gRPC / HTTP/2**.
There is no custom wire format — clients use any Arrow Flight library.

Default port: **17777** (plaintext) or configurable TLS.

---

## RPC surface

### DoGet  — read queries

| Field   | Value |
|---------|-------|
| Ticket  | UTF-8 SQL string (SELECT / WITH / EXPLAIN / SHOW) |
| Returns | Arrow IPC record batch stream |

The server borrows a read connection from the pool, opens a DuckDB Arrow cursor,
and streams record batches directly to the Flight transport.  Peak server RAM is
bounded by the largest single batch (~64 MB typical) regardless of total result size.

### DoAction("execute")  — write queries

| Field       | Value |
|-------------|-------|
| action.type | `"execute"` |
| action.body | UTF-8 DML/DDL SQL (INSERT / UPDATE / DELETE / CREATE / DROP / ...) |
| Returns     | Empty result stream on success |
| On error    | gRPC `INTERNAL` status with message |

Write requests are queued to the **WriteSerializer** which batches concurrent DML
into one `BEGIN … COMMIT` transaction per window.  DDL is detected and executed
individually outside transactions.

### DoAction("ping")

| Field       | Value |
|-------------|-------|
| action.body | empty |
| Returns     | Single result; body = `"pong"` |

### DoAction("stats")

| Field       | Value |
|-------------|-------|
| action.body | empty |
| Returns     | Single result; body = UTF-8 JSON |

```json
{
  "queries_read":      1000,
  "queries_write":     22,
  "errors":            0,
  "reader_pool_size":  16,
  "port":              17777
}
```

### ListActions

Returns the three action descriptors above.  Used by Flight clients that
want to inspect server capabilities at runtime.

---

## TLS

Start the server with `--tls-cert server.crt --tls-key server.key` to enable TLS.
Clients pass `SslCredentials` (Grpc.Core) or the equivalent in their Flight library.

---

## Client compatibility

Any Arrow Flight client library can connect — the server exposes standard Flight RPCs.

| Client | Package |
|--------|---------|
| .NET 4.6.2 | `Apache.Arrow.Flight 14.x` + `Grpc.Core 2.46.x` |
| .NET 6+ | `Apache.Arrow.Flight 14.x` + `Grpc.Net.Client` |
| Python | `pyarrow.flight` |
| Java | `org.apache.arrow:arrow-flight` |
| Go | `github.com/apache/arrow/go/arrow/flight` |
| Rust | `arrow-flight` crate |

---

## Migration from v3.x custom protocol

The previous versions used a custom length-prefixed binary protocol over IOCP TCP.
That protocol is completely replaced.  All previous client code (`DasConnection`,
`Protocol.cs`, `DasConnectionPool`) is removed.  Use `DasFlightClient` instead.

| Old (v3.x) | New (v4.0) |
|---|---|
| `DasConnection.Query(sql)` | `DasFlightClient.Query(sql)` |
| `DasConnection.Execute(sql)` | `DasFlightClient.Execute(sql)` |
| `ArrowStreamReader` | `FlightQueryResult` |
| `reader.ToDataTable()` | `result.ToDataTable()` |
| `reader.ToRows()` | `result.ToRows()` |
| `DasConnectionPool` | not needed (HTTP/2 mux) |
| Custom `STATUS_STREAM` | built into Flight |
| `MSG_STATS` custom byte | `DoAction("stats")` |
