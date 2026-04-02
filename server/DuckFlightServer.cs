using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// Arrow Flight server backed by a DuckDB database.
    /// Uses raw gRPC with manual proto encoding/decoding because
    /// Apache.Arrow.Flight.Protocol types are internal in the NuGet package.
    ///
    /// IMPORTANT: The IPC serialization embeds full Arrow IPC streams inside
    /// FlightData.data_header (not individual IPC messages). This means the C#
    /// server is compatible with the C# client (DasFlightClient) but NOT with
    /// standard Flight clients (Python pyarrow.flight, C++ Arrow Flight, etc.).
    /// For cross-language interop, use the C++ server instead.
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
        /// Maps Flight proto RPCs to our handler methods.
        /// </summary>
        public ServerServiceDefinition BuildGrpcService()
        {
            var builder = ServerServiceDefinition.CreateBuilder();

            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "DoGet",
                    RawMarshaller, RawMarshaller),
                HandleDoGet);

            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "DoAction",
                    RawMarshaller, RawMarshaller),
                HandleDoAction);

            builder.AddMethod(
                new Method<byte[], byte[]>(
                    MethodType.ServerStreaming,
                    "arrow.flight.protocol.FlightService",
                    "ListActions",
                    RawMarshaller, RawMarshaller),
                HandleListActions);

            return builder.Build();
        }

        private static readonly Marshaller<byte[]> RawMarshaller = new Marshaller<byte[]>(
            bytes => bytes,
            bytes => bytes);

        // ── DoGet: Read queries ──────────────────────────────────────────────

        private async Task HandleDoGet(
            byte[] request,
            IServerStreamWriter<byte[]> responseStream,
            ServerCallContext context)
        {
            var ticket = FlightProto.ParseTicket(request);
            string sql = ticket.TicketBytes != null
                ? Encoding.UTF8.GetString(ticket.TicketBytes)
                : "";

            try
            {
                using (var handle = readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var schema = RecordBatchBuilder.BuildSchema(reader);

                        // Send schema as the first message.
                        byte[] schemaIpc = SerializeSchemaIpc(schema);
                        await responseStream.WriteAsync(
                            FlightProto.WriteFlightData(schemaIpc)).ConfigureAwait(false);

                        // Stream data batches.
                        RecordBatch batch;
                        while ((batch = RecordBatchBuilder.ReadNextBatch(reader, schema, batchSize)) != null)
                        {
                            try
                            {
                                byte[] batchIpc = SerializeBatchIpc(batch, schema);
                                await responseStream.WriteAsync(
                                    FlightProto.WriteFlightData(batchIpc)).ConfigureAwait(false);
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

        // ── DoAction: Execute / Ping / Stats ─────────────────────────────────

        private async Task HandleDoAction(
            byte[] request,
            IServerStreamWriter<byte[]> responseStream,
            ServerCallContext context)
        {
            var action = FlightProto.ParseAction(request);
            string actionType = action.Type ?? "";

            if (actionType == "execute")
            {
                if (action.Body == null || action.Body.Length == 0)
                {
                    Interlocked.Increment(ref errors);
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        "execute action requires a non-empty SQL body"));
                }

                string sql = Encoding.UTF8.GetString(action.Body);
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
                byte[] pongBytes = Encoding.UTF8.GetBytes("pong");
                await responseStream.WriteAsync(
                    FlightProto.WriteResult(pongBytes)).ConfigureAwait(false);
                return;
            }

            if (actionType == "stats")
            {
                var s = GetStats();
                string json = string.Format(
                    "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2}," +
                    "\"reader_pool_size\":{3},\"port\":{4}}}",
                    s.QueriesRead, s.QueriesWrite, s.Errors, s.ReaderPoolSize, s.Port);
                byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
                await responseStream.WriteAsync(
                    FlightProto.WriteResult(jsonBytes)).ConfigureAwait(false);
                return;
            }

            throw new RpcException(new Status(StatusCode.Unimplemented,
                "Unknown action '" + actionType + "'. Supported: execute, ping, stats."));
        }

        // ── ListActions ──────────────────────────────────────────────────────

        private async Task HandleListActions(
            byte[] request,
            IServerStreamWriter<byte[]> responseStream,
            ServerCallContext context)
        {
            await responseStream.WriteAsync(FlightProto.WriteActionType("execute",
                "Execute DML or DDL SQL. Body = UTF-8 SQL. Returns empty stream on success."))
                .ConfigureAwait(false);
            await responseStream.WriteAsync(FlightProto.WriteActionType("ping",
                "Liveness check. Returns 'pong'."))
                .ConfigureAwait(false);
            await responseStream.WriteAsync(FlightProto.WriteActionType("stats",
                "Server metrics as JSON."))
                .ConfigureAwait(false);
        }

        // ── IPC serialization ────────────────────────────────────────────────

        private static byte[] SerializeSchemaIpc(Schema schema)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var w = new Apache.Arrow.Ipc.ArrowStreamWriter(ms, schema, leaveOpen: true);
                w.WriteStartAsync().Wait();
                w.WriteEndAsync().Wait();
                w.Dispose();
                return ms.ToArray();
            }
        }

        private static byte[] SerializeBatchIpc(RecordBatch batch, Schema schema)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var w = new Apache.Arrow.Ipc.ArrowStreamWriter(ms, schema, leaveOpen: true);
                w.WriteRecordBatchAsync(batch).Wait();
                w.Dispose();
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
