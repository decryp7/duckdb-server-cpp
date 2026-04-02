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
    /// DuckDB gRPC server using the custom DuckDbService protocol.
    /// Uses Arrow IPC for streaming query results.
    ///
    /// Compatible with any client generated from proto/duckdb_service.proto
    /// (C#, C++, Python, Go, Java, etc.).
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

        /// <summary>
        /// Create the gRPC ServerServiceDefinition using typed marshallers.
        /// </summary>
        public ServerServiceDefinition BuildGrpcService()
        {
            return ServerServiceDefinition.CreateBuilder()
                .AddMethod(DuckDbService.QueryMethod, HandleQuery)
                .AddMethod(DuckDbService.ExecuteMethod, HandleExecute)
                .AddMethod(DuckDbService.PingMethod, HandlePing)
                .AddMethod(DuckDbService.StatsMethod, HandleGetStats)
                .Build();
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

        private async Task HandleQuery(
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

                        // Write the entire result as an Arrow IPC stream.
                        // Each QueryResponse chunk contains a piece of the IPC stream.
                        using (var ms = new MemoryStream())
                        {
                            var ipcWriter = new ArrowStreamWriter(ms, schema, leaveOpen: true);

                            // Write schema.
                            await ipcWriter.WriteStartAsync().ConfigureAwait(false);

                            // Write batches.
                            RecordBatch batch;
                            while ((batch = RecordBatchBuilder.ReadNextBatch(reader, schema, batchSize)) != null)
                            {
                                try
                                {
                                    await ipcWriter.WriteRecordBatchAsync(batch).ConfigureAwait(false);
                                }
                                finally
                                {
                                    batch.Dispose();
                                }

                                // Flush what we have so far as a chunk.
                                await ipcWriter.WriteEndAsync().ConfigureAwait(false);
                                ipcWriter.Dispose();

                                byte[] chunk = ms.ToArray();
                                ms.SetLength(0);

                                await responseStream.WriteAsync(new QueryResponse { IpcData = chunk })
                                    .ConfigureAwait(false);

                                // Start a new IPC stream for the next chunk.
                                ipcWriter = new ArrowStreamWriter(ms, schema, leaveOpen: true);
                                await ipcWriter.WriteStartAsync().ConfigureAwait(false);
                            }

                            // Final empty stream (schema only) for queries with no remaining batches.
                            await ipcWriter.WriteEndAsync().ConfigureAwait(false);
                            ipcWriter.Dispose();

                            byte[] finalChunk = ms.ToArray();
                            if (finalChunk.Length > 0)
                            {
                                await responseStream.WriteAsync(new QueryResponse { IpcData = finalChunk })
                                    .ConfigureAwait(false);
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

        // ── Execute: DML/DDL ─────────────────────────────────────────────────

        private Task<ExecuteResponse> HandleExecute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;

            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse
                {
                    Success = false,
                    Error = "SQL statement is required"
                });
            }

            var result = writer.Submit(sql);
            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse
                {
                    Success = false,
                    Error = result.Error
                });
            }

            Interlocked.Increment(ref queriesWrite);
            return Task.FromResult(new ExecuteResponse { Success = true });
        }

        // ── Ping ─────────────────────────────────────────────────────────────

        private Task<PingResponse> HandlePing(PingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new PingResponse { Message = "pong" });
        }

        // ── Stats ────────────────────────────────────────────────────────────

        private Task<StatsResponse> HandleGetStats(StatsRequest request, ServerCallContext context)
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
