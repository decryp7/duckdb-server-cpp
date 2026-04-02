using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckDbServer
{
    /// <summary>
    /// DuckDB gRPC server — the main service class.
    ///
    /// Responsibilities (kept minimal — delegates to specialized classes):
    ///   - Query:      read from sharded pool, build columnar responses, cache results
    ///   - Execute:    fan-out writes to all shards, invalidate cache
    ///   - BulkInsert: fan-out bulk writes to all shards
    ///   - Ping/Stats: simple responses
    ///
    /// Dependencies (injected via constructor):
    ///   - ShardedDuckDb: manages database shards (read/write routing)
    ///   - IQueryCache:   caches query results (bypass DuckDB on cache hit)
    /// </summary>
    public sealed class DuckDbServer : DuckDbService.DuckDbServiceBase, IDisposable
    {
        private readonly ServerConfig config;
        private readonly ShardedDuckDb shardedDb;
        private readonly IQueryCache queryCache;
        private readonly int batchSize;

        // Metrics
        private long queriesRead;
        private long queriesWrite;
        private long errors;
        private long cacheHits;

        public DuckDbServer(ServerConfig config, ShardedDuckDb shardedDb, IQueryCache queryCache)
        {
            this.config = config;
            this.shardedDb = shardedDb;
            this.queryCache = queryCache;
            this.batchSize = config.BatchSize > 0 ? config.BatchSize : 8192;
        }

        public ServerServiceDefinition BuildGrpcService()
        {
            return DuckDbService.BindService(this);
        }

        // ── Query ────────────────────────────────────────────────────────────

        public override async Task Query(
            QueryRequest request,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            string sql = request.Sql ?? "";

            // 1. Check cache
            List<QueryResponse> cached;
            if (queryCache.TryGet(sql, out cached))
            {
                Interlocked.Increment(ref cacheHits);
                Interlocked.Increment(ref queriesRead);
                foreach (var resp in cached)
                    await responseStream.WriteAsync(resp).ConfigureAwait(false);
                return;
            }

            // 2. Execute query on a shard
            try
            {
                var shard = shardedDb.NextForRead();
                var responses = await ExecuteQuery(shard, sql, responseStream, context);

                // 3. Cache the result
                if (responses != null)
                    queryCache.Put(sql, responses);

                Interlocked.Increment(ref queriesRead);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        /// <summary>
        /// Execute a SQL query on a shard and stream results.
        /// Returns the list of responses for caching.
        /// </summary>
        private async Task<List<QueryResponse>> ExecuteQuery(
            ShardedDuckDb.Shard shard, string sql,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            var responses = new List<QueryResponse>();

            using (var handle = shard.Pool.Borrow())
            using (var cmd = handle.Connection.CreateCommand())
            {
                cmd.CommandText = sql;
                using (var reader = cmd.ExecuteReader())
                {
                    int colCount = reader.FieldCount;

                    // Map columns
                    var colTypes = new ColumnType[colCount];
                    var meta = new List<ColumnMeta>(colCount);
                    for (int c = 0; c < colCount; c++)
                    {
                        colTypes[c] = TypeMapper.FromClrType(reader.GetFieldType(c));
                        meta.Add(new ColumnMeta
                        {
                            Name = reader.GetName(c),
                            Type = colTypes[c]
                        });
                    }

                    // Build and stream batches
                    var builders = new ColumnBuilder[colCount];
                    for (int c = 0; c < colCount; c++)
                        builders[c] = new ColumnBuilder(colTypes[c], batchSize);

                    var values = new object[colCount];
                    int rowsInBatch = 0;
                    bool firstBatch = true;

                    while (reader.Read())
                    {
                        if (context.CancellationToken.IsCancellationRequested) return null;

                        reader.GetValues(values);
                        for (int c = 0; c < colCount; c++)
                            builders[c].Add(values[c], rowsInBatch);
                        rowsInBatch++;

                        if (rowsInBatch >= batchSize)
                        {
                            var resp = BuildResponse(builders, colCount, rowsInBatch,
                                firstBatch ? meta : null);
                            await responseStream.WriteAsync(resp).ConfigureAwait(false);
                            responses.Add(resp);
                            firstBatch = false;
                            rowsInBatch = 0;
                            for (int c = 0; c < colCount; c++) builders[c].Reset();
                        }
                    }

                    // Final batch
                    if (rowsInBatch > 0 || firstBatch)
                    {
                        var resp = BuildResponse(builders, colCount, rowsInBatch,
                            firstBatch ? meta : null);
                        await responseStream.WriteAsync(resp).ConfigureAwait(false);
                        responses.Add(resp);
                    }
                }
            }

            return responses;
        }

        private static QueryResponse BuildResponse(
            ColumnBuilder[] builders, int colCount, int rowCount, List<ColumnMeta> meta)
        {
            var response = new QueryResponse { RowCount = rowCount };
            if (meta != null) response.Columns.AddRange(meta);
            for (int c = 0; c < colCount; c++)
                response.Data.Add(builders[c].Build());
            return response;
        }

        // ── Execute ──────────────────────────────────────────────────────────

        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;
            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = "SQL required" });
            }

            var result = shardedDb.WriteToAll(sql);
            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = result.Error });
            }

            queryCache.Invalidate();
            Interlocked.Increment(ref queriesWrite);
            return Task.FromResult(new ExecuteResponse { Success = true });
        }

        // ── BulkInsert ───────────────────────────────────────────────────────

        public override Task<BulkInsertResponse> BulkInsert(
            BulkInsertRequest request, ServerCallContext context)
        {
            string table = request.Table;
            int rowCount = request.RowCount;

            if (string.IsNullOrEmpty(table) || rowCount == 0 || request.Columns.Count == 0)
                return Task.FromResult(new BulkInsertResponse
                    { Success = false, Error = "table, columns, and row_count are required" });

            try
            {
                string sql = BulkInsertSqlBuilder.Build(table, request.Columns, request.Data, rowCount);
                var result = shardedDb.WriteToAll(sql);
                if (!result.Ok)
                {
                    Interlocked.Increment(ref errors);
                    return Task.FromResult(new BulkInsertResponse { Success = false, Error = result.Error });
                }

                queryCache.Invalidate();
                Interlocked.Increment(ref queriesWrite);
                return Task.FromResult(new BulkInsertResponse
                    { Success = true, RowsInserted = rowCount });
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new BulkInsertResponse { Success = false, Error = ex.Message });
            }
        }

        // ── Ping / GetStats ──────────────────────────────────────────────────

        public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new PingResponse { Message = "pong" });
        }

        public override Task<StatsResponse> GetStats(StatsRequest request, ServerCallContext context)
        {
            return Task.FromResult(new StatsResponse
            {
                QueriesRead = Interlocked.Read(ref queriesRead),
                QueriesWrite = Interlocked.Read(ref queriesWrite),
                Errors = Interlocked.Read(ref errors),
                ReaderPoolSize = shardedDb.TotalPoolSize,
                Port = config.Port
            });
        }

        // ── Dispose ──────────────────────────────────────────────────────────

        private int disposed;

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            shardedDb.Dispose();
        }
    }

    /// <summary>
    /// Builds a multi-row INSERT SQL statement from columnar protobuf data.
    /// </summary>
    internal static class BulkInsertSqlBuilder
    {
        public static string Build(
            string table,
            Google.Protobuf.Collections.RepeatedField<ColumnMeta> columns,
            Google.Protobuf.Collections.RepeatedField<ColumnData> data,
            int rowCount)
        {
            int colCount = columns.Count;
            var sb = new System.Text.StringBuilder(rowCount * colCount * 10);

            for (int r = 0; r < rowCount; r++)
            {
                sb.Append(r == 0 ? "INSERT INTO " + table + " VALUES (" : ", (");

                for (int c = 0; c < colCount; c++)
                {
                    if (c > 0) sb.Append(", ");
                    var cd = data[c];
                    bool isNull = false;
                    for (int n = 0; n < cd.NullIndices.Count; n++)
                        if (cd.NullIndices[n] == r) { isNull = true; break; }

                    if (isNull) { sb.Append("NULL"); continue; }

                    switch (columns[c].Type)
                    {
                        case ColumnType.TypeBoolean:
                            sb.Append(r < cd.BoolValues.Count && cd.BoolValues[r] ? "true" : "false"); break;
                        case ColumnType.TypeInt32: case ColumnType.TypeInt8:
                        case ColumnType.TypeInt16: case ColumnType.TypeUint8:
                        case ColumnType.TypeUint16:
                            sb.Append(r < cd.Int32Values.Count ? cd.Int32Values[r] : 0); break;
                        case ColumnType.TypeInt64: case ColumnType.TypeUint32:
                        case ColumnType.TypeUint64:
                            sb.Append(r < cd.Int64Values.Count ? cd.Int64Values[r] : 0); break;
                        case ColumnType.TypeFloat:
                            sb.Append((r < cd.FloatValues.Count ? cd.FloatValues[r] : 0f)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        case ColumnType.TypeDouble: case ColumnType.TypeDecimal:
                            sb.Append((r < cd.DoubleValues.Count ? cd.DoubleValues[r] : 0d)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        default:
                            sb.Append("'");
                            sb.Append(r < cd.StringValues.Count ? cd.StringValues[r].Replace("'", "''") : "");
                            sb.Append("'"); break;
                    }
                }
                sb.Append(")");
            }

            return sb.ToString();
        }
    }
}
