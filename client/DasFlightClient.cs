using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Thread-safe gRPC client for the DuckDB server.
    /// Implements <see cref="IDasFlightClient"/>.
    ///
    /// Uses typed protobuf values — no string parsing overhead for
    /// numeric types (int, long, double, bool).
    /// </summary>
    public sealed class DasFlightClient : IDasFlightClient
    {
        private readonly Channel channel;
        private readonly DuckDbService.DuckDbServiceClient grpcClient;
        private int disposed;

        public DasFlightClient(string host = "localhost", int port = 17777)
            : this(new Channel(host + ":" + port, ChannelCredentials.Insecure))
        { }

        public DasFlightClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials))
        { }

        private DasFlightClient(Channel channel)
        {
            this.channel = channel;
            this.grpcClient = new DuckDbService.DuckDbServiceClient(channel);
        }

        // ── Query ────────────────────────────────────────────────────────────

        public IFlightQueryResult Query(string sql, CancellationToken ct = default)
        {
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();

            try
            {
                var request = new QueryRequest { Sql = sql };
                var columns = new List<ColumnInfo>();
                var allRows = new List<Row>();

                using (var call = grpcClient.Query(request, cancellationToken: ct))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    {
                        var response = call.ResponseStream.Current;
                        if (response.Columns.Count > 0)
                            columns.AddRange(response.Columns);
                        if (response.Rows.Count > 0)
                            allRows.AddRange(response.Rows);
                    }
                }

                return new QueryResult(columns, allRows);
            }
            catch (RpcException ex)
            {
                throw new DasException("Query failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Execute ──────────────────────────────────────────────────────────

        public void Execute(string sql, CancellationToken ct = default)
        {
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            try
            {
                var response = await grpcClient.ExecuteAsync(new ExecuteRequest { Sql = sql }, cancellationToken: ct)
                    .ResponseAsync.ConfigureAwait(false);
                if (!response.Success)
                    throw new DasException("Execute failed: " + response.Error);
            }
            catch (RpcException ex)
            {
                throw new DasException("Execute failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Ping ─────────────────────────────────────────────────────────────

        public void Ping()
        {
            EnsureNotDisposed();
            try
            {
                var response = Task.Run(async () =>
                    await grpcClient.PingAsync(new PingRequest()).ResponseAsync.ConfigureAwait(false))
                    .GetAwaiter().GetResult();
                if (response.Message != "pong")
                    throw new DasException("Ping: unexpected '" + response.Message + "'");
            }
            catch (RpcException ex)
            {
                throw new DasException("Ping failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Stats ────────────────────────────────────────────────────────────

        public string GetStats()
        {
            EnsureNotDisposed();
            try
            {
                var r = Task.Run(async () =>
                    await grpcClient.GetStatsAsync(new StatsRequest()).ResponseAsync.ConfigureAwait(false))
                    .GetAwaiter().GetResult();
                return string.Format(
                    "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                    r.QueriesRead, r.QueriesWrite, r.Errors, r.ReaderPoolSize, r.Port);
            }
            catch (RpcException ex)
            {
                throw new DasException("GetStats failed: " + ex.Status.Detail, ex);
            }
        }

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(DasFlightClient));
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            try { channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort */ }
        }
    }

    // ── QueryResult ──────────────────────────────────────────────────────────

    /// <summary>
    /// Query result with typed value extraction.
    /// Values are stored as protobuf TypedValue (oneof) — no string parsing needed
    /// for int/long/double/bool columns.
    /// </summary>
    public sealed class QueryResult : IFlightQueryResult
    {
        private readonly List<ColumnInfo> columns;
        private readonly List<Row> rows;

        public QueryResult(List<ColumnInfo> columns, List<Row> rows)
        {
            this.columns = columns;
            this.rows = rows;
        }

        public int RowCount { get { return rows.Count; } }
        public int ColumnCount { get { return columns.Count; } }
        public string GetColumnName(int index) { return columns[index].Name; }
        public string GetColumnType(int index) { return columns[index].Type.ToString(); }

        /// <summary>
        /// Extract the CLR value from a TypedValue.
        /// Returns the native type (int, long, double, bool, string) — no parsing.
        /// </summary>
        private static object ExtractValue(TypedValue tv)
        {
            switch (tv.KindCase)
            {
                case TypedValue.KindOneofCase.IsNull:    return null;
                case TypedValue.KindOneofCase.BoolValue:   return tv.BoolValue;
                case TypedValue.KindOneofCase.Int32Value:   return tv.Int32Value;
                case TypedValue.KindOneofCase.Int64Value:   return tv.Int64Value;
                case TypedValue.KindOneofCase.DoubleValue:  return tv.DoubleValue;
                case TypedValue.KindOneofCase.StringValue:  return tv.StringValue;
                case TypedValue.KindOneofCase.BlobValue:    return tv.BlobValue.ToByteArray();
                default:                                     return null;
            }
        }

        public List<Dictionary<string, object>> ToRows()
        {
            var result = new List<Dictionary<string, object>>(rows.Count);
            foreach (var row in rows)
            {
                var dict = new Dictionary<string, object>(columns.Count);
                for (int c = 0; c < columns.Count && c < row.Values.Count; c++)
                    dict[columns[c].Name] = ExtractValue(row.Values[c]);
                result.Add(dict);
            }
            return result;
        }

        public DataTable ToDataTable(string tableName = "Result")
        {
            var dt = new DataTable(tableName);

            // Create typed columns based on proto ColumnType.
            foreach (var col in columns)
                dt.Columns.Add(col.Name, MapToClrType(col.Type));

            foreach (var row in rows)
            {
                var dr = dt.NewRow();
                for (int c = 0; c < columns.Count && c < row.Values.Count; c++)
                {
                    var val = ExtractValue(row.Values[c]);
                    dr[c] = val ?? (object)DBNull.Value;
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

        /// <summary>
        /// Map proto ColumnType to CLR type for DataTable column creation.
        /// </summary>
        private static Type MapToClrType(ColumnType ct)
        {
            switch (ct)
            {
                case ColumnType.TypeBoolean:  return typeof(bool);
                case ColumnType.TypeInt8:
                case ColumnType.TypeInt16:
                case ColumnType.TypeInt32:
                case ColumnType.TypeUint8:
                case ColumnType.TypeUint16:   return typeof(int);
                case ColumnType.TypeInt64:
                case ColumnType.TypeUint32:
                case ColumnType.TypeUint64:   return typeof(long);
                case ColumnType.TypeFloat:
                case ColumnType.TypeDouble:
                case ColumnType.TypeDecimal:  return typeof(double);
                case ColumnType.TypeBlob:     return typeof(byte[]);
                default:                      return typeof(string);
            }
        }

        public void Dispose() { /* plain data, nothing to dispose */ }
    }
}
