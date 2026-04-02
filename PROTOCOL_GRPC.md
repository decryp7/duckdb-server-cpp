# Custom gRPC Protocol — Design Rationale

## Why not Apache Arrow Flight?

Apache Arrow Flight is the standard protocol for streaming Arrow data over gRPC.
However, it has fundamental compatibility issues with our requirements:

| Requirement | Arrow Flight Support |
|---|---|
| .NET Framework 4.6.2 (VS2017) | FlightClient needs Grpc.Net.Client (.NET 5+) |
| .NET Framework 4.6.2 server | FlightServer needs ASP.NET Core (.NET 5+) |
| Grpc.Core (last .NET Framework gRPC) | Flight protocol types are `internal` in NuGet |
| Cross-language (C++ ↔ C#) | Only works if both sides use the official Flight libraries |

We tried three approaches before arriving at this solution:

1. **Use Apache.Arrow.Flight directly** — Failed: `FlightClient` and `FlightServer`
   require `Grpc.Net.Client` / ASP.NET Core, which need .NET 5+.

2. **Raw gRPC with manual Flight proto encoding** — Worked for C# ↔ C#, but the
   IPC message serialization (full streams vs split header/body) was incompatible
   with standard Flight clients (Python, C++, Go).

3. **Custom gRPC protocol** (current) — Clean, simple, fully cross-language compatible.

## The Custom Protocol

Defined in `proto/duckdb_service.proto`:

```protobuf
service DuckDbService {
  rpc Query (QueryRequest) returns (stream QueryResponse);
  rpc Execute (ExecuteRequest) returns (ExecuteResponse);
  rpc Ping (PingRequest) returns (PingResponse);
  rpc GetStats (StatsRequest) returns (StatsResponse);
}
```

### Key design decisions

**1. Arrow IPC inside protobuf bytes**

Query results are serialized as Apache Arrow IPC streams and sent as `bytes`
fields in `QueryResponse` messages. This gives us:
- Columnar data format (zero-copy reads on the client)
- Standard Arrow IPC (any Arrow library can parse it)
- No dependency on the Flight wire format

```
Server: DuckDB query → Arrow RecordBatch → ArrowStreamWriter → bytes
Client: bytes → ArrowStreamReader → RecordBatch → DataTable / raw access
```

**2. Unary RPCs for Execute/Ping/Stats**

Unlike Flight (which uses server-streaming for all actions), we use proper
unary (request-response) RPCs for Execute, Ping, and Stats. This is simpler
and more efficient — no stream setup overhead for single-response operations.

**3. Typed error responses**

Execute returns `ExecuteResponse { success, error }` instead of using
gRPC status codes for business logic errors. This makes error handling
explicit and avoids confusion between transport errors and DuckDB errors.

**4. Single proto file, multiple languages**

The same `duckdb_service.proto` file generates code for all languages:

| Language | Codegen tool | Generated output |
|---|---|---|
| C# | Hand-written (VS2017 compat) | `shared/DuckDbProto.cs` |
| C++ | `protoc --cpp_out --grpc_out` | Generated `.pb.h` + `_grpc.pb.h` |
| Python | `grpcio-tools` | `duckdb_service_pb2.py` + `_grpc.py` |
| Go | `protoc-gen-go-grpc` | `duckdb_service.pb.go` + `_grpc.pb.go` |
| Java | `protoc-gen-grpc-java` | Generated Java classes |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Clients (any language with gRPC + protobuf)                │
│                                                             │
│  C# (.NET 4.6.2)    C++ (protoc)    Python (grpcio-tools)  │
│  DasFlightClient     generated       generated              │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  gRPC / HTTP/2  (proto/duckdb_service.proto)                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Server (C# or C++)                                         │
│  DuckDbService implementation                               │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌────────┐  ┌──────┐  │
│  │ Query        │  │ Execute      │  │ Ping   │  │Stats │  │
│  │ (streaming)  │  │ (unary)      │  │(unary) │  │(unary│  │
│  │              │  │              │  │        │  │      │  │
│  │ Arrow IPC    │  │ WriteSerializer│ │ "pong" │  │ JSON │  │
│  │ streaming    │  │ (batched DML)│  │        │  │      │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┘  └──────┘  │
│         │                  │                                 │
│         └────────┬─────────┘                                 │
│                  ▼                                            │
│         DuckDB (single database)                              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Query (SELECT)

```
Client                          Server
  │                               │
  │── QueryRequest {sql} ────────►│
  │                               │ Execute SQL via ConnectionPool
  │                               │ Build Arrow schema + batches
  │                               │ Serialize via ArrowStreamWriter
  │◄── QueryResponse {ipc_data} ──│ (chunk 1: schema + batch)
  │◄── QueryResponse {ipc_data} ──│ (chunk 2: batch)
  │◄── QueryResponse {ipc_data} ──│ (chunk N: batch)
  │◄── stream end ────────────────│
  │                               │
  │ ArrowStreamReader(concat(chunks)) → RecordBatches
```

### Execute (DML/DDL)

```
Client                          Server
  │                               │
  │── ExecuteRequest {sql} ──────►│
  │                               │ Route to WriteSerializer
  │                               │ Batch with other concurrent writes
  │                               │ BEGIN → execute → COMMIT
  │◄── ExecuteResponse ───────────│ {success: true}
  │    or                         │ {success: false, error: "..."}
```

## Why not just use JSON/REST?

| | Custom gRPC | REST/JSON |
|---|---|---|
| Streaming | Native (HTTP/2 server-streaming) | Chunked transfer or polling |
| Data format | Arrow IPC (columnar, zero-copy) | JSON (text, parsing overhead) |
| Type safety | Proto schema enforced | Manual validation |
| Code generation | Automatic for all languages | Manual client SDKs |
| Performance | Binary protocol, multiplexed | Text protocol, connection per request |

## File Map

```
proto/
  duckdb_service.proto     Canonical protocol definition

shared/                    C# protocol types (shared by server + client)
  DuckDbShared.csproj
  DuckDbProto.cs           Hand-written proto messages + gRPC methods

server/                    C# server implementation
  DuckFlightServer.cs      Implements DuckDbService RPCs

client/                    C# client implementation
  DasFlightClient.cs       Calls DuckDbService RPCs
```
