using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Grpc.Core;
using Google.Protobuf;

namespace DuckArrowServer
{
    /// <summary>
    /// Arrow Flight server backed by a DuckDB database.
    /// Implements the raw gRPC FlightServiceBase for Grpc.Core hosting
    /// (Apache.Arrow.Flight.Server.FlightServer is for ASP.NET Core only).
    ///
    /// Handles three types of requests:
    ///   - DoGet:   Run a SELECT query and stream Arrow batches to the client.
    ///   - DoAction: Run a command (execute SQL, ping, or get stats).
    ///   - ListActions: Tell the client what actions are available.
    /// </summary>
    public sealed class DuckFlightServer : IDisposable
    {
        private readonly ServerConfig config;
        private readonly IConnectionPool readPool;
        private readonly IWriteSerializer writer;

        private long queriesRead;
        private long queriesWrite;
        private long errors;

        private readonly int batchSize;

        public DuckFlightServer(ServerConfig config, IConnectionPool readPool, IWriteSerializer writer)
        {
            this.config = config;
            this.readPool = readPool;
            this.writer = writer;
            this.batchSize = config.BatchSize > 0 ? config.BatchSize : 8192;
        }

        /// <summary>Get a snapshot of live server metrics.</summary>
        public ServerStats GetStats()
        {
            return new ServerStats
            {
                QueriesRead = Interlocked.Read(ref queriesRead),
                QueriesWrite = Interlocked.Read(ref queriesWrite),
                Errors = Interlocked.Read(ref errors),
                ReaderPoolSize = readPool.Size,
                Port = config.Port
            };
        }

        /// <summary>
        /// Create the gRPC ServerServiceDefinition for Grpc.Core hosting.
        /// Manually maps the Flight proto RPCs to our handler methods.
        /// </summary>
        public ServerServiceDefinition BuildGrpcService()
        {
            // The Flight proto defines these RPCs. We implement DoGet, DoAction, ListActions.
            // Others return UNIMPLEMENTED.
            var builder = ServerServiceDefinition.CreateBuilder();

            // DoGet: client sends a Ticket, server streams FlightData
            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "DoGet",
                    ByteArrayMarshaller,
                    ByteArrayMarshaller),
                HandleDoGetRaw);

            // DoAction: client sends Action, server streams Result
            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "DoAction",
                    ByteArrayMarshaller,
                    ByteArrayMarshaller),
                HandleDoActionRaw);

            // ListActions: client sends Empty, server streams ActionType
            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "ListActions",
                    ByteArrayMarshaller,
                    ByteArrayMarshaller),
                HandleListActionsRaw);

            return builder.Build();
        }

        // ── Raw byte marshaller for proto messages ───────────────────────────

        private static readonly Marshaller<byte[]> ByteArrayMarshaller = new Marshaller<byte[]>(
            bytes => bytes,
            bytes => bytes);

        // ── DoGet handler ────────────────────────────────────────────────────

        private async Task HandleDoGetRaw(byte[] request, IServerStreamWriter<byte[]> responseStream, ServerCallContext context)
        {
            // Deserialize the Ticket proto message to get the SQL.
            var ticket = Apache.Arrow.Flight.Protocol.Ticket.Parser.ParseFrom(request);
            string sql = ticket.Ticket_.ToStringUtf8();

            try
            {
                using (var handle = readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var schema = RecordBatchBuilder.BuildSchema(reader);

                        // Build schema message and send it first.
                        await SendSchema(responseStream, schema).ConfigureAwait(false);

                        // Stream data batches.
                        RecordBatch batch;
                        while ((batch = RecordBatchBuilder.ReadNextBatch(reader, schema, batchSize)) != null)
                        {
                            try
                            {
                                await SendRecordBatch(responseStream, batch, schema).ConfigureAwait(false);
                            }
                            finally
                            {
                                batch.Dispose();
                            }
                        }
                    }
                }

                Interlocked.Increment(ref queriesRead);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        // ── DoAction handler ─────────────────────────────────────────────────

        private async Task HandleDoActionRaw(byte[] request, IServerStreamWriter<byte[]> responseStream, ServerCallContext context)
        {
            var action = Apache.Arrow.Flight.Protocol.Action.Parser.ParseFrom(request);
            string actionType = action.Type;
            byte[] bodyBytes = action.Body.ToByteArray();

            if (actionType == "execute")
            {
                if (bodyBytes == null || bodyBytes.Length == 0)
                {
                    Interlocked.Increment(ref errors);
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        "execute action requires a non-empty SQL body"));
                }

                string sql = Encoding.UTF8.GetString(bodyBytes);
                var result = writer.Submit(sql);
                if (!result.Ok)
                {
                    Interlocked.Increment(ref errors);
                    throw new RpcException(new Status(StatusCode.Internal, result.Error));
                }

                Interlocked.Increment(ref queriesWrite);
                return;
            }

            if (actionType == "ping")
            {
                await SendActionResult(responseStream, "pong").ConfigureAwait(false);
                return;
            }

            if (actionType == "stats")
            {
                var s = GetStats();
                string json = string.Format(
                    "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2}," +
                    "\"reader_pool_size\":{3},\"port\":{4}}}",
                    s.QueriesRead, s.QueriesWrite, s.Errors, s.ReaderPoolSize, s.Port);
                await SendActionResult(responseStream, json).ConfigureAwait(false);
                return;
            }

            throw new RpcException(new Status(StatusCode.Unimplemented,
                "Unknown action '" + actionType + "'. Supported: execute, ping, stats."));
        }

        // ── ListActions handler ──────────────────────────────────────────────

        private async Task HandleListActionsRaw(byte[] request, IServerStreamWriter<byte[]> responseStream, ServerCallContext context)
        {
            await SendActionType(responseStream, "execute",
                "Execute DML or DDL SQL. Body = UTF-8 SQL. Returns empty stream on success.")
                .ConfigureAwait(false);
            await SendActionType(responseStream, "ping",
                "Liveness check. Returns 'pong'.")
                .ConfigureAwait(false);
            await SendActionType(responseStream, "stats",
                "Server metrics as JSON.")
                .ConfigureAwait(false);
        }

        // ── Proto serialization helpers ──────────────────────────────────────

        private static async Task SendActionResult(IServerStreamWriter<byte[]> stream, string body)
        {
            var result = new Apache.Arrow.Flight.Protocol.Result
            {
                Body = ByteString.CopyFromUtf8(body)
            };
            await stream.WriteAsync(result.ToByteArray()).ConfigureAwait(false);
        }

        private static async Task SendActionType(IServerStreamWriter<byte[]> stream, string type, string description)
        {
            var actionType = new Apache.Arrow.Flight.Protocol.ActionType
            {
                Type = type,
                Description = description
            };
            await stream.WriteAsync(actionType.ToByteArray()).ConfigureAwait(false);
        }

        private static async Task SendSchema(IServerStreamWriter<byte[]> stream, Schema schema)
        {
            // Serialize the schema as an IPC message wrapped in FlightData.
            var flightData = new Apache.Arrow.Flight.Protocol.FlightData
            {
                DataHeader = ByteString.CopyFrom(SerializeSchemaAsIpc(schema))
            };
            await stream.WriteAsync(flightData.ToByteArray()).ConfigureAwait(false);
        }

        private static async Task SendRecordBatch(IServerStreamWriter<byte[]> stream, RecordBatch batch, Schema schema)
        {
            // Serialize the record batch as IPC and wrap in FlightData.
            byte[] ipcBytes = SerializeBatchAsIpc(batch, schema);
            var flightData = new Apache.Arrow.Flight.Protocol.FlightData
            {
                DataHeader = ByteString.CopyFrom(ipcBytes)
            };
            await stream.WriteAsync(flightData.ToByteArray()).ConfigureAwait(false);
        }

        private static byte[] SerializeSchemaAsIpc(Schema schema)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(ms, schema, leaveOpen: true);
                // WriteStart sends the schema message.
                writer.WriteStartAsync().Wait();
                writer.WriteEndAsync().Wait();
                writer.Dispose();
                return ms.ToArray();
            }
        }

        private static byte[] SerializeBatchAsIpc(RecordBatch batch, Schema schema)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(ms, schema, leaveOpen: true);
                writer.WriteRecordBatchAsync(batch).Wait();
                writer.Dispose();
                return ms.ToArray();
            }
        }

        // ── Dispose ──────────────────────────────────────────────────────────

        private int disposed;

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            writer.Dispose();
            readPool.Dispose();
        }
    }
}
