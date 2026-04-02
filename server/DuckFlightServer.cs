using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDbProto;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// DuckDB gRPC server implementing the generated DuckDbService.
    /// Code is auto-generated from proto/duckdb_service.proto via Grpc.Tools.
    ///
    /// Compatible with any client generated from the same proto file
    /// (C#, C++, Python, Go, Java, etc.).
    /// </summary>
    public sealed class DuckFlightServer : DuckDbService.DuckDbServiceBase, IDisposable
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

        /// <summary>
        /// Create the gRPC ServerServiceDefinition using the generated BindService.
        /// </summary>
        public ServerServiceDefinition BuildGrpcService()
        {
            return DuckDbService.BindService(this);
        }

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

        // ── Query: stream Arrow IPC results ──────────────────────────────────

        public override async Task Query(
            QueryRequest request,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            string sql = request.Sql ?? "";

            try
            {
                using (var handle = readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var schema = RecordBatchBuilder.BuildSchema(reader);

                        // Write each batch as a complete Arrow IPC stream chunk.
                        bool batchSent = false;
                        RecordBatch batch;
                        while ((batch = RecordBatchBuilder.ReadNextBatch(reader, schema, batchSize)) != null)
                        {
                            byte[] chunk;
                            try
                            {
                                chunk = SerializeToIpc(batch, schema);
                            }
                            finally
                            {
                                batch.Dispose();
                            }

                            await responseStream.WriteAsync(new QueryResponse
                            {
                                IpcData = Google.Protobuf.ByteString.CopyFrom(chunk)
                            }).ConfigureAwait(false);
                            batchSent = true;
                        }

                        // If no batches were sent (empty result), send schema-only chunk
                        // so the client still receives the column schema.
                        if (!batchSent)
                        {
                            byte[] schemaChunk = SerializeSchemaOnlyIpc(schema);
                            await responseStream.WriteAsync(new QueryResponse
                            {
                                IpcData = Google.Protobuf.ByteString.CopyFrom(schemaChunk)
                            }).ConfigureAwait(false);
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

        // ── Execute: DML/DDL ─────────────────────────────────────────────────

        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;

            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = "SQL statement is required" });
            }

            var result = writer.Submit(sql);
            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = result.Error });
            }

            Interlocked.Increment(ref queriesWrite);
            return Task.FromResult(new ExecuteResponse { Success = true });
        }

        // ── Ping ─────────────────────────────────────────────────────────────

        public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new PingResponse { Message = "pong" });
        }

        // ── Stats ────────────────────────────────────────────────────────────

        public override Task<StatsResponse> GetStats(StatsRequest request, ServerCallContext context)
        {
            var s = GetStats();
            return Task.FromResult(new StatsResponse
            {
                QueriesRead = s.QueriesRead,
                QueriesWrite = s.QueriesWrite,
                Errors = s.Errors,
                ReaderPoolSize = s.ReaderPoolSize,
                Port = s.Port
            });
        }

        // ── IPC serialization ────────────────────────────────────────────────

        private static byte[] SerializeToIpc(RecordBatch batch, Schema schema)
        {
            using (var ms = new MemoryStream())
            {
                var w = new ArrowStreamWriter(ms, schema, leaveOpen: true);
                w.WriteRecordBatchAsync(batch).Wait();
                w.WriteEndAsync().Wait();
                w.Dispose();
                return ms.ToArray();
            }
        }

        private static byte[] SerializeSchemaOnlyIpc(Schema schema)
        {
            using (var ms = new MemoryStream())
            {
                var w = new ArrowStreamWriter(ms, schema, leaveOpen: true);
                w.WriteStartAsync().Wait();
                w.WriteEndAsync().Wait();
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
