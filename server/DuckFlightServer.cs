using System;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// DuckDB gRPC server implementing the generated DuckDbService.
    /// Uses plain DataReader for query results — no Arrow dependency.
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

        // ── Query: stream rows ───────────────────────────────────────────────

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
                        int colCount = reader.FieldCount;

                        // First response: column schema
                        var schemaResponse = new QueryResponse();
                        for (int c = 0; c < colCount; c++)
                        {
                            schemaResponse.Columns.Add(new ColumnInfo
                            {
                                Name = reader.GetName(c),
                                Type = reader.GetDataTypeName(c)
                            });
                        }
                        await responseStream.WriteAsync(schemaResponse).ConfigureAwait(false);

                        // Shared null value — avoid allocating one per null cell.
                        var nullValue = new Value { IsNull = true };

                        // Stream rows in batches
                        var batch = new QueryResponse();
                        int rowsInBatch = 0;

                        // Pre-allocate object array for GetValues (avoids per-cell GetValue boxing)
                        var values = new object[colCount];

                        while (reader.Read())
                        {
                            reader.GetValues(values); // bulk read — one call instead of N

                            var row = new Row();
                            for (int c = 0; c < colCount; c++)
                            {
                                if (values[c] == null || values[c] is DBNull)
                                {
                                    row.Values.Add(nullValue);
                                }
                                else
                                {
                                    row.Values.Add(new Value { Text = values[c].ToString() });
                                }
                            }
                            batch.Rows.Add(row);
                            rowsInBatch++;

                            if (rowsInBatch >= batchSize)
                            {
                                await responseStream.WriteAsync(batch).ConfigureAwait(false);
                                batch = new QueryResponse();
                                rowsInBatch = 0;
                            }
                        }

                        if (rowsInBatch > 0)
                        {
                            await responseStream.WriteAsync(batch).ConfigureAwait(false);
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

        // ── Execute ──────────────────────────────────────────────────────────

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

        // ── GetStats ─────────────────────────────────────────────────────────

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
