using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckDbServer
{
    /// <summary>
    /// DuckDB gRPC server -- the main service class that implements the
    /// <c>DuckDbService</c> protobuf service definition.
    ///
    /// <para><b>Thread Safety:</b> This class is fully thread-safe. All mutable state
    /// (metric counters) is updated via <see cref="Interlocked"/> operations. The
    /// underlying <see cref="ShardedDuckDb"/> and <see cref="IQueryCache"/> are also
    /// thread-safe, so concurrent gRPC handlers may execute without external locking.</para>
    ///
    /// <para><b>Responsibilities</b> (kept minimal -- delegates to specialized classes):</para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Query:</b> Borrows a read connection from the sharded pool via round-robin,
    ///     executes the SQL, builds columnar protobuf responses in configurable batch sizes,
    ///     streams them to the client, and caches the complete result set for future hits.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Execute:</b> Fans out DML/DDL writes to ALL shards via
    ///     <see cref="ShardedDuckDb.WriteToAll"/>, then invalidates the entire query cache
    ///     to prevent stale reads.
    ///   </description></item>
    ///   <item><description>
    ///     <b>BulkInsert:</b> Converts columnar protobuf data into a multi-row INSERT
    ///     statement via <see cref="BulkInsertSqlBuilder"/>, then fans out to all shards.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Ping/Stats:</b> Lightweight health-check and metrics endpoints.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Dependencies</b> (injected via constructor):</para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="ShardedDuckDb"/>: Manages database shards, connection pools, and
    ///     write serializers. Provides read routing (round-robin) and write fan-out.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="IQueryCache"/>: Caches serialized query responses keyed by SQL text.
    ///     On cache hit, responses are replayed directly to the gRPC stream without touching
    ///     DuckDB, yielding near-zero latency for repeated queries.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Gotchas:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     Disposing this class also disposes the <see cref="ShardedDuckDb"/>. The caller
    ///     must NOT dispose the ShardedDuckDb separately to avoid double-dispose errors.
    ///   </description></item>
    ///   <item><description>
    ///     The query timeout uses <c>Task.WhenAny</c> with <c>Task.Delay</c>. If the query
    ///     times out, the underlying DuckDB query continues running in the background until
    ///     it finishes or the connection is reclaimed. This is a deliberate trade-off: killing
    ///     a DuckDB query requires closing the connection, which is expensive.
    ///   </description></item>
    /// </list>
    /// </summary>
    public sealed class DuckDbServer : DuckDbService.DuckDbServiceBase, IDisposable
    {
        /// <summary>Server configuration snapshot, immutable after construction.</summary>
        private readonly ServerConfig config;

        /// <summary>Sharded database manager that routes reads and writes.</summary>
        private readonly ShardedDuckDb shardedDb;

        /// <summary>Query result cache. On hit, bypasses DuckDB entirely.</summary>
        private readonly IQueryCache queryCache;

        /// <summary>
        /// Number of rows to accumulate before flushing a <see cref="QueryResponse"/>
        /// message to the gRPC stream. Larger values reduce framing overhead but increase
        /// memory usage and latency-to-first-byte.
        /// </summary>
        private readonly int batchSize;

        // ── Metrics ─────────────────────────────────────────────────────────
        // All counters are updated atomically via Interlocked and read via
        // Interlocked.Read for visibility across threads.

        /// <summary>Total number of successfully served read queries (including cache hits).</summary>
        private long queriesRead;

        /// <summary>Total number of successfully executed write statements.</summary>
        private long queriesWrite;

        /// <summary>Total number of failed operations (query timeouts, SQL errors, etc.).</summary>
        private long errors;

        /// <summary>Total number of query cache hits (subset of queriesRead).</summary>
        private long cacheHits;

        /// <summary>
        /// Constructs a new <see cref="DuckDbServer"/> with the given configuration and
        /// dependencies.
        /// </summary>
        /// <param name="config">
        /// Server configuration. The <see cref="ServerConfig.BatchSize"/> controls how many
        /// rows are accumulated per streamed <see cref="QueryResponse"/> message. If zero or
        /// negative, defaults to 8192.
        /// </param>
        /// <param name="shardedDb">
        /// The sharded database manager. Ownership transfers to this instance; it will be
        /// disposed when <see cref="Dispose"/> is called.
        /// </param>
        /// <param name="queryCache">
        /// The query cache implementation. May be a no-op implementation for testing.
        /// </param>
        public DuckDbServer(ServerConfig config, ShardedDuckDb shardedDb, IQueryCache queryCache)
        {
            this.config = config;
            this.shardedDb = shardedDb;
            this.queryCache = queryCache;
            // Default to 8192 rows per batch if not configured. This balances throughput
            // (fewer gRPC messages) against memory usage (each batch is held in memory
            // until streamed).
            this.batchSize = config.BatchSize > 0 ? config.BatchSize : 8192;
        }

        /// <summary>
        /// Creates the gRPC <see cref="ServerServiceDefinition"/> that maps incoming
        /// RPC calls to the handler methods on this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="ServerServiceDefinition"/> ready to be added to a
        /// <see cref="Grpc.Core.Server.Services"/> collection.
        /// </returns>
        public ServerServiceDefinition BuildGrpcService()
        {
            return DuckDbService.BindService(this);
        }

        // ── Query ────────────────────────────────────────────────────────────

        /// <summary>
        /// Handles a streaming read query from a gRPC client.
        ///
        /// <para><b>Flow:</b></para>
        /// <list type="number">
        ///   <item><description>
        ///     <b>Cache check:</b> If the exact SQL string is in the query cache and the
        ///     cached entry has not expired (TTL), replay the cached <see cref="QueryResponse"/>
        ///     messages directly to the stream. This path never touches DuckDB.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Shard selection:</b> Pick the next shard via round-robin
        ///     (<see cref="ShardedDuckDb.NextForRead"/>). All shards hold identical data,
        ///     so any shard can serve any read.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Query execution with timeout:</b> Execute the SQL on the selected shard.
        ///     If <see cref="ServerConfig.QueryTimeoutSeconds"/> is positive, wrap the query
        ///     task with <c>Task.WhenAny(queryTask, Task.Delay(timeout))</c>. If the delay
        ///     wins the race, throw <see cref="StatusCode.DeadlineExceeded"/>. Note: the
        ///     underlying DuckDB query is NOT cancelled -- it runs to completion in the
        ///     background, consuming its connection until done.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Cache population:</b> If the query completed successfully and was not
        ///     cancelled, store the list of <see cref="QueryResponse"/> objects in the cache
        ///     for future hits.
        ///   </description></item>
        /// </list>
        /// </summary>
        /// <param name="request">The incoming query request containing the SQL string.</param>
        /// <param name="responseStream">
        /// The server-side streaming writer. Each <see cref="QueryResponse"/> contains up to
        /// <see cref="batchSize"/> rows of columnar data. The first response also includes
        /// column metadata (names and types).
        /// </param>
        /// <param name="context">
        /// gRPC call context providing the client's cancellation token and deadline.
        /// </param>
        /// <exception cref="RpcException">
        /// Thrown with <see cref="StatusCode.DeadlineExceeded"/> on timeout, or
        /// <see cref="StatusCode.Internal"/> on any other failure.
        /// </exception>
        public override async Task Query(
            QueryRequest request,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            string sql = request.Sql ?? "";

            // 1. Check cache -- this is the fast path. On hit, we skip DuckDB entirely
            //    and replay the previously-serialized protobuf messages.
            List<QueryResponse> cached;
            if (queryCache.TryGet(sql, out cached))
            {
                Interlocked.Increment(ref cacheHits);
                Interlocked.Increment(ref queriesRead);
                foreach (var resp in cached)
                    await responseStream.WriteAsync(resp).ConfigureAwait(false);
                return;
            }

            // 2. Execute query on a shard (with timeout)
            try
            {
                // Pick next shard via atomic round-robin. The shard provides a connection
                // pool for reads and a write serializer for writes.
                var shard = shardedDb.NextForRead();

                // Apply query timeout if configured. The pattern uses Task.WhenAny to race
                // the actual query against a delay timer. This avoids CancellationToken
                // overhead on every query and is simpler than cooperative cancellation in
                // DuckDB's ADO.NET provider.
                //
                // GOTCHA: If the timeout fires, the queryTask continues running in the
                // background. The borrowed connection is held until the query completes.
                // Under sustained timeout conditions, the connection pool can be starved.
                Task<List<QueryResponse>> queryTask = ExecuteQuery(shard, sql, responseStream, context);
                if (config.QueryTimeoutSeconds > 0)
                {
                    var completed = await Task.WhenAny(queryTask,
                        Task.Delay(TimeSpan.FromSeconds(config.QueryTimeoutSeconds)));
                    if (completed != queryTask)
                    {
                        Interlocked.Increment(ref errors);
                        throw new RpcException(new Status(StatusCode.DeadlineExceeded,
                            "Query timed out after " + config.QueryTimeoutSeconds + " seconds"));
                    }
                }
                var responses = await queryTask;

                // 3. Cache the result. A null response means the query was cancelled
                //    (client disconnected), so we skip caching.
                if (responses != null)
                    queryCache.Put(sql, responses);

                Interlocked.Increment(ref queriesRead);
            }
            catch (OperationCanceledException) { throw; } // Let gRPC handle client cancellation
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        /// <summary>
        /// Executes a SQL query on the given shard's connection pool, reads the result set
        /// row-by-row, accumulates rows into columnar batches using <see cref="ColumnBuilder"/>,
        /// and streams each batch to the client as a <see cref="QueryResponse"/> message.
        ///
        /// <para><b>Streaming Protocol:</b></para>
        /// <list type="bullet">
        ///   <item><description>
        ///     The <b>first</b> <see cref="QueryResponse"/> includes column metadata (names
        ///     and types) in <c>Columns</c>. Subsequent responses omit metadata to save bandwidth.
        ///   </description></item>
        ///   <item><description>
        ///     Each response contains up to <see cref="batchSize"/> rows of columnar data
        ///     in packed arrays (one <see cref="ColumnData"/> per column).
        ///   </description></item>
        ///   <item><description>
        ///     For empty result sets, exactly one response is sent with <c>RowCount=0</c>
        ///     and column metadata, so the client always knows the schema.
        ///   </description></item>
        /// </list>
        ///
        /// <para><b>Cancellation:</b> Checks <c>context.CancellationToken</c> between rows.
        /// Returns <c>null</c> if cancelled, signalling the caller to skip caching.</para>
        /// </summary>
        /// <param name="shard">The shard to read from (selected by round-robin in the caller).</param>
        /// <param name="sql">The SQL query string to execute.</param>
        /// <param name="responseStream">gRPC server stream writer for sending batches.</param>
        /// <param name="context">gRPC context carrying the client's cancellation token.</param>
        /// <returns>
        /// The list of all <see cref="QueryResponse"/> messages that were streamed, suitable
        /// for caching. Returns <c>null</c> if the query was cancelled by the client.
        /// </returns>
        private async Task<List<QueryResponse>> ExecuteQuery(
            ShardedDuckDb.Shard shard, string sql,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            var responses = new List<QueryResponse>();

            // Borrow a connection from the pool. The using-block returns it when done.
            // The connection was pre-configured with per-connection PRAGMAs in ConnectionPool.
            using (var handle = shard.Pool.Borrow())
            using (var cmd = handle.Connection.CreateCommand())
            {
                cmd.CommandText = sql;
                using (var reader = cmd.ExecuteReader())
                {
                    int colCount = reader.FieldCount;

                    // Map each column's CLR type (from DuckDB.NET) to our protobuf ColumnType.
                    // This mapping is done once per query, not per row.
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

                    // Create one ColumnBuilder per column. Each builder accumulates typed
                    // values into a packed list (e.g., List<int> for INT32 columns).
                    // Pre-allocated with batchSize capacity to avoid resizing.
                    var builders = new ColumnBuilder[colCount];
                    for (int c = 0; c < colCount; c++)
                        builders[c] = new ColumnBuilder(colTypes[c], batchSize);

                    // Reusable array for reading all column values in one call.
                    // GetValues is faster than calling GetValue per column because it
                    // avoids repeated boundary checks in the ADO.NET provider.
                    var values = new object[colCount];
                    int rowsInBatch = 0;
                    bool firstBatch = true;

                    while (reader.Read())
                    {
                        // Check for client cancellation between rows. This is a lightweight
                        // volatile read, not a kernel call.
                        if (context.CancellationToken.IsCancellationRequested) return null;

                        // Read all column values for this row into the reusable array.
                        reader.GetValues(values);
                        for (int c = 0; c < colCount; c++)
                            builders[c].Add(values[c], rowsInBatch);
                        rowsInBatch++;

                        // When the batch is full, build the protobuf response, stream it,
                        // and reset the builders for the next batch. Builders reuse their
                        // internal lists (Clear does not deallocate).
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

                    // Final (possibly partial) batch. Also handles empty result sets:
                    // if no rows were read (rowsInBatch == 0) and this is the first batch,
                    // we still send one response so the client receives the column metadata.
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

        /// <summary>
        /// Assembles a single <see cref="QueryResponse"/> protobuf message from the
        /// accumulated column builders.
        /// </summary>
        /// <param name="builders">
        /// Array of column builders, one per column, containing the accumulated values
        /// for this batch.
        /// </param>
        /// <param name="colCount">Number of columns (length of <paramref name="builders"/>).</param>
        /// <param name="rowCount">Number of rows in this batch (may be less than batchSize for the final batch).</param>
        /// <param name="meta">
        /// Column metadata (names and types) to include in this response, or <c>null</c> to
        /// omit metadata. Only the first response in a stream includes metadata to avoid
        /// redundant serialization.
        /// </param>
        /// <returns>A fully populated <see cref="QueryResponse"/> ready for streaming or caching.</returns>
        private static QueryResponse BuildResponse(
            ColumnBuilder[] builders, int colCount, int rowCount, List<ColumnMeta> meta)
        {
            var response = new QueryResponse { RowCount = rowCount };
            // Include column metadata only in the first batch message.
            if (meta != null) response.Columns.AddRange(meta);
            // Build each column's packed protobuf ColumnData from the builder's internal lists.
            for (int c = 0; c < colCount; c++)
                response.Data.Add(builders[c].Build());
            return response;
        }

        // ── QueryArrow ────────────────────────────────────────────────────────

        /// <summary>
        /// Arrow IPC streaming is not supported in the C# server. Returns UNIMPLEMENTED
        /// so clients can fall back to the columnar Query RPC.
        /// </summary>
        public override Task QueryArrow(QueryRequest request,
            IServerStreamWriter<ArrowResponse> responseStream,
            ServerCallContext context)
        {
            throw new RpcException(new Status(StatusCode.Unimplemented,
                "QueryArrow not available in C# server. Use Rust server for Arrow IPC."));
        }

        // ── Execute ──────────────────────────────────────────────────────────

        /// <summary>
        /// Returns true if the SQL statement is a simple INSERT (starts with the
        /// keyword INSERT after optional leading whitespace). INSERT statements can
        /// bypass the WriteSerializer and execute directly on the bulk connections
        /// because DuckDB guarantees that concurrent appends do not conflict.
        /// </summary>
        private static bool IsSimpleInsert(string sql)
        {
            if (sql == null || sql.Length < 6) return false;
            int i = 0;
            while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;
            if (i + 6 > sql.Length) return false;
            return sql.Substring(i, 6).Equals("INSERT", StringComparison.OrdinalIgnoreCase)
                && (i + 6 >= sql.Length || char.IsWhiteSpace(sql[i + 6]));
        }

        /// <summary>
        /// Handles a write (DML or DDL) request from a gRPC client.
        ///
        /// <para><b>Flow:</b></para>
        /// <list type="number">
        ///   <item><description>Validate that the SQL string is non-empty.</description></item>
        ///   <item><description>
        ///     For INSERT statements, bypass the WriteSerializer and execute directly on
        ///     each shard's BulkConnection via <see cref="ShardedDuckDb.BulkExecuteAll"/>.
        ///     DuckDB guarantees concurrent appends don't conflict, so this is safe and
        ///     avoids the serializer's queue/batch overhead.
        ///   </description></item>
        ///   <item><description>
        ///     For all other DML/DDL, fan out via <see cref="ShardedDuckDb.WriteToAll"/>.
        ///     Each shard's <see cref="IWriteSerializer"/> queues the statement and batches
        ///     it with other concurrent writes into a single transaction for throughput.
        ///   </description></item>
        ///   <item><description>
        ///     On success, invalidate the entire query cache. This is a broad invalidation
        ///     (not per-table) because determining which cached queries are affected by an
        ///     arbitrary DML/DDL statement is complex and error-prone.
        ///   </description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> This method is synchronous (returns a completed Task) because
        /// <see cref="IWriteSerializer.Submit"/> blocks until the write transaction commits.
        /// This is intentional: the gRPC server thread pool handles concurrency, and blocking
        /// here simplifies error handling and ordering guarantees.</para>
        /// </summary>
        /// <param name="request">The execute request containing the SQL statement.</param>
        /// <param name="context">gRPC call context (unused but required by the base class).</param>
        /// <returns>An <see cref="ExecuteResponse"/> indicating success or failure with error details.</returns>
        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;
            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = "SQL required" });
            }

            // INSERT statements can bypass WriteSerializer and execute directly on
            // bulk connections. DuckDB guarantees concurrent appends don't conflict.
            WriteResult result;
            if (IsSimpleInsert(sql))
                result = shardedDb.BulkExecuteAll(sql);
            else
                result = shardedDb.WriteToAll(sql);

            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = result.Error });
            }

            // Invalidate the entire cache after any write to prevent stale reads.
            queryCache.Invalidate();
            Interlocked.Increment(ref queriesWrite);
            return Task.FromResult(new ExecuteResponse { Success = true });
        }

        // ── BulkInsert ───────────────────────────────────────────────────────

        /// <summary>
        /// Handles a bulk insert request using DuckDB's Appender API for maximum performance.
        ///
        /// <para><b>Performance:</b> The Appender API bypasses the SQL parser, planner, and
        /// optimizer entirely. It writes directly to DuckDB's columnar storage, achieving
        /// 10-100x faster inserts compared to the SQL INSERT approach.</para>
        ///
        /// <para><b>Thread safety:</b> Each shard has a dedicated BulkConnection protected by
        /// a lock. DuckDB allows concurrent appends without conflicts, but the connection
        /// itself is not thread-safe. The lock serializes BulkInsert calls per shard while
        /// allowing concurrent inserts across different shards.</para>
        ///
        /// <para><b>Fan-out:</b> Data is appended to ALL shards (same write-all strategy as
        /// Execute) to maintain data consistency across shards.</para>
        /// </summary>
        public override Task<BulkInsertResponse> BulkInsert(
            BulkInsertRequest request, ServerCallContext context)
        {
            string table = request.Table;
            int rowCount = request.RowCount;
            int colCount = request.Columns.Count;

            if (string.IsNullOrEmpty(table) || rowCount == 0 || colCount == 0)
                return Task.FromResult(new BulkInsertResponse
                    { Success = false, Error = "table, columns, and row_count are required" });

            if (request.Data.Count != colCount)
                return Task.FromResult(new BulkInsertResponse
                    { Success = false, Error = "data column count does not match columns metadata" });

            // Sorted insert: use SQL with ORDER BY for better zonemap effectiveness.
            // Sort columns are validated against column metadata to prevent SQL injection.
            if (request.SortColumns.Count > 0)
            {
                // Validate sort columns exist in the column list
                var colNames = new System.Collections.Generic.HashSet<string>();
                for (int c = 0; c < request.Columns.Count; c++)
                    colNames.Add(request.Columns[c].Name);
                foreach (var sc in request.SortColumns)
                {
                    if (!colNames.Contains(sc))
                        return Task.FromResult(new BulkInsertResponse
                            { Success = false, Error = "sort_columns: unknown column '" + sc + "'" });
                }

                try
                {
                    string sql = BulkInsertSqlBuilder.BuildSorted(table, request.Columns, request.Data, rowCount, request.SortColumns);
                    var result = shardedDb.WriteToAll(sql);
                    if (!result.Ok)
                    {
                        Interlocked.Increment(ref errors);
                        return Task.FromResult(new BulkInsertResponse { Success = false, Error = result.Error });
                    }
                    queryCache.Invalidate();
                    Interlocked.Increment(ref queriesWrite);
                    return Task.FromResult(new BulkInsertResponse { Success = true, RowsInserted = rowCount });
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errors);
                    return Task.FromResult(new BulkInsertResponse { Success = false, Error = ex.Message });
                }
            }

            try
            {
                // Fan-out: append to ALL shards using the Appender API.
                for (int s = 0; s < shardedDb.ShardCount; s++)
                {
                    var shard = shardedDb.GetShard(s);
                    lock (shard.BulkLock)
                    {
                        AppendToShard(shard.BulkConnection, table, request, rowCount, colCount);
                    }
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

        /// <summary>
        /// Appends rows to a single shard using the DuckDB Appender API.
        /// The Appender bypasses SQL parsing entirely — writes go directly to storage.
        /// </summary>
        private static void AppendToShard(
            DuckDB.NET.Data.DuckDBConnection conn, string table,
            BulkInsertRequest request, int rowCount, int colCount)
        {
            using (var appender = conn.CreateAppender(table))
            {
                for (int r = 0; r < rowCount; r++)
                {
                    var row = appender.CreateRow();
                    for (int c = 0; c < colCount; c++)
                    {
                        var cd = request.Data[c];

                        // Check null
                        bool isNull = false;
                        for (int n = 0; n < cd.NullIndices.Count; n++)
                            if (cd.NullIndices[n] == r) { isNull = true; break; }

                        if (isNull)
                        {
                            row.AppendValue(DBNull.Value);
                            continue;
                        }

                        switch (request.Columns[c].Type)
                        {
                            case ColumnType.TypeBoolean:
                                row.AppendValue(r < cd.BoolValues.Count && cd.BoolValues[r]);
                                break;
                            case ColumnType.TypeInt8:
                            case ColumnType.TypeInt16:
                            case ColumnType.TypeInt32:
                            case ColumnType.TypeUint8:
                            case ColumnType.TypeUint16:
                                row.AppendValue(r < cd.Int32Values.Count ? cd.Int32Values[r] : 0);
                                break;
                            case ColumnType.TypeInt64:
                            case ColumnType.TypeUint32:
                            case ColumnType.TypeUint64:
                                row.AppendValue(r < cd.Int64Values.Count ? cd.Int64Values[r] : 0L);
                                break;
                            case ColumnType.TypeFloat:
                                row.AppendValue(r < cd.FloatValues.Count ? cd.FloatValues[r] : 0f);
                                break;
                            case ColumnType.TypeDouble:
                            case ColumnType.TypeDecimal:
                                row.AppendValue(r < cd.DoubleValues.Count ? cd.DoubleValues[r] : 0d);
                                break;
                            default:
                                row.AppendValue(r < cd.StringValues.Count ? cd.StringValues[r] : "");
                                break;
                        }
                    }
                    row.EndRow();
                }
            } // appender.Dispose() flushes remaining rows
        }

        // ── Ping / GetStats ──────────────────────────────────────────────────

        /// <summary>
        /// Health-check endpoint. Returns "pong" if the server is alive.
        /// Does not touch DuckDB -- purely a connectivity test for load balancers
        /// and monitoring systems.
        /// </summary>
        /// <param name="request">Empty ping request.</param>
        /// <param name="context">gRPC call context (unused).</param>
        /// <returns>A <see cref="PingResponse"/> with <c>Message = "pong"</c>.</returns>
        public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new PingResponse { Message = "pong" });
        }

        /// <summary>
        /// Returns a snapshot of live server metrics. All counters are read atomically
        /// via <see cref="Interlocked.Read"/> to ensure visibility across threads, but
        /// the snapshot as a whole is NOT atomic (counters may advance between reads).
        /// This is acceptable for monitoring purposes.
        /// </summary>
        /// <param name="request">Empty stats request.</param>
        /// <param name="context">gRPC call context (unused).</param>
        /// <returns>
        /// A <see cref="StatsResponse"/> containing read/write query counts, error count,
        /// connection pool size, and the server's listening port.
        /// </returns>
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

        /// <summary>
        /// Guard flag for idempotent disposal. 0 = alive, 1 = disposed.
        /// Uses <see cref="Interlocked.CompareExchange"/> for thread-safe one-shot disposal.
        /// </summary>
        private int disposed;

        /// <summary>
        /// Disposes the server and all owned resources (sharded database, connection pools,
        /// writer threads). This method is idempotent -- calling it multiple times is safe.
        ///
        /// <para><b>Gotcha:</b> This disposes the <see cref="ShardedDuckDb"/> instance that
        /// was injected in the constructor. The caller must NOT dispose it separately, or a
        /// double-dispose exception will occur.</para>
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            shardedDb.Dispose();
        }
    }

    /// <summary>
    /// Builds a multi-row <c>INSERT INTO table VALUES (...), (...), ...</c> SQL statement
    /// from columnar protobuf data.
    ///
    /// <para><b>Thread Safety:</b> This class is stateless and all methods are static,
    /// so it is inherently thread-safe.</para>
    ///
    /// <para><b>Performance:</b> Pre-allocates the <see cref="System.Text.StringBuilder"/>
    /// with an estimated capacity of <c>rowCount * colCount * 10</c> bytes to minimize
    /// reallocations. For large bulk inserts (10,000+ rows), this avoids significant GC
    /// pressure from repeated buffer doubling.</para>
    ///
    /// <para><b>NULL Handling:</b> Each <see cref="ColumnData"/> carries a
    /// <c>NullIndices</c> list of row indices where the value is NULL. The builder
    /// performs a linear scan of this list for each cell. For columns with many NULLs,
    /// this is O(N*M) where N is rows and M is null count. A HashSet would be faster
    /// but would allocate more memory per column.</para>
    /// </summary>
    internal static class BulkInsertSqlBuilder
    {
        /// <summary>
        /// Constructs a multi-row INSERT SQL string from columnar protobuf arrays.
        /// </summary>
        /// <param name="table">
        /// Target table name. Inserted directly into SQL (the caller is responsible for
        /// ensuring this is a valid identifier).
        /// </param>
        /// <param name="columns">
        /// Column metadata defining the type of each column. Used to determine how to
        /// format each value (numeric literal, boolean literal, or quoted string).
        /// </param>
        /// <param name="data">
        /// Columnar data arrays. One <see cref="ColumnData"/> per column, each containing
        /// packed arrays of typed values and a <c>NullIndices</c> list.
        /// </param>
        /// <param name="rowCount">
        /// Expected number of rows. The caller guarantees that each <see cref="ColumnData"/>
        /// has at least this many values (or the value is NULL as indicated by NullIndices).
        /// </param>
        /// <returns>
        /// A complete SQL INSERT statement, e.g.:
        /// <c>INSERT INTO my_table VALUES (1, 'hello'), (2, 'world')</c>
        /// </returns>
        public static string Build(
            string table,
            Google.Protobuf.Collections.RepeatedField<ColumnMeta> columns,
            Google.Protobuf.Collections.RepeatedField<ColumnData> data,
            int rowCount)
        {
            int colCount = columns.Count;
            // Estimate ~10 chars per cell for initial capacity.
            var sb = new System.Text.StringBuilder(rowCount * colCount * 10);

            for (int r = 0; r < rowCount; r++)
            {
                // First row starts with "INSERT INTO table VALUES (", subsequent rows
                // are appended as ", (" to form the multi-row VALUES clause.
                sb.Append(r == 0 ? "INSERT INTO " + table + " VALUES (" : ", (");

                for (int c = 0; c < colCount; c++)
                {
                    if (c > 0) sb.Append(", ");
                    var cd = data[c];

                    // Check if this cell is NULL by scanning the NullIndices list.
                    // Linear scan is acceptable because most columns have few NULLs.
                    bool isNull = false;
                    for (int n = 0; n < cd.NullIndices.Count; n++)
                        if (cd.NullIndices[n] == r) { isNull = true; break; }

                    if (isNull) { sb.Append("NULL"); continue; }

                    // Format the value based on its protobuf column type.
                    // Numeric types are appended as literals; strings are single-quoted
                    // with internal single quotes doubled for SQL escaping.
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
                            // InvariantCulture ensures decimal point is always '.', never ','
                            // (important for locales like de-DE where the decimal separator is ',').
                            sb.Append((r < cd.FloatValues.Count ? cd.FloatValues[r] : 0f)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        case ColumnType.TypeDouble: case ColumnType.TypeDecimal:
                            sb.Append((r < cd.DoubleValues.Count ? cd.DoubleValues[r] : 0d)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        default:
                            // String/blob/unknown types: wrap in single quotes, escape embedded quotes.
                            sb.Append("'");
                            sb.Append(r < cd.StringValues.Count ? cd.StringValues[r].Replace("'", "''") : "");
                            sb.Append("'"); break;
                    }
                }
                sb.Append(")");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Constructs a sorted INSERT SQL using a subquery with ORDER BY.
        /// This produces SQL like:
        /// <c>INSERT INTO t SELECT * FROM (VALUES (1,'a'), (2,'b')) AS _t(col1, col2) ORDER BY col1</c>
        /// Sorting before insert improves DuckDB zonemap effectiveness for selective reads.
        /// </summary>
        public static string BuildSorted(
            string table,
            Google.Protobuf.Collections.RepeatedField<ColumnMeta> columns,
            Google.Protobuf.Collections.RepeatedField<ColumnData> data,
            int rowCount,
            Google.Protobuf.Collections.RepeatedField<string> sortColumns)
        {
            int colCount = columns.Count;
            var sb = new System.Text.StringBuilder(rowCount * colCount * 10 + 200);

            // Build: INSERT INTO table SELECT * FROM (VALUES (...), (...)) AS _t(col1, col2) ORDER BY sort_col
            sb.Append("INSERT INTO \"").Append(table.Replace("\"", "\"\"")).Append("\" SELECT * FROM (VALUES ");

            for (int r = 0; r < rowCount; r++)
            {
                if (r > 0) sb.Append(", ");
                sb.Append("(");
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
                        case ColumnType.TypeInt32: case ColumnType.TypeInt8: case ColumnType.TypeInt16:
                        case ColumnType.TypeUint8: case ColumnType.TypeUint16:
                            sb.Append(r < cd.Int32Values.Count ? cd.Int32Values[r] : 0); break;
                        case ColumnType.TypeInt64: case ColumnType.TypeUint32: case ColumnType.TypeUint64:
                            sb.Append(r < cd.Int64Values.Count ? cd.Int64Values[r] : 0); break;
                        case ColumnType.TypeFloat:
                            sb.Append((r < cd.FloatValues.Count ? cd.FloatValues[r] : 0f)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        case ColumnType.TypeDouble: case ColumnType.TypeDecimal:
                            sb.Append((r < cd.DoubleValues.Count ? cd.DoubleValues[r] : 0d)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
                        default:
                            sb.Append("'").Append(r < cd.StringValues.Count ? cd.StringValues[r].Replace("'", "''") : "").Append("'"); break;
                    }
                }
                sb.Append(")");
            }

            sb.Append(") AS _t(");
            for (int c = 0; c < colCount; c++)
            {
                if (c > 0) sb.Append(", ");
                sb.Append('"').Append(columns[c].Name.Replace("\"", "\"\"")).Append('"');
            }
            sb.Append(") ORDER BY ");
            for (int s = 0; s < sortColumns.Count; s++)
            {
                if (s > 0) sb.Append(", ");
                sb.Append('"').Append(sortColumns[s].Replace("\"", "\"\"")).Append('"');
            }

            return sb.ToString();
        }
    }
}
