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
    /// Uses the custom DuckDbService protocol (proto/duckdb_service.proto).
    /// No Arrow dependency — results are returned as protobuf rows.
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

                        // First response has column metadata
                        if (response.Columns.Count > 0)
                            columns.AddRange(response.Columns);

                        // Subsequent responses have row data
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
                var request = new ExecuteRequest { Sql = sql };
                var response = await grpcClient.ExecuteAsync(request, cancellationToken: ct)
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
                    await grpcClient.PingAsync(new PingRequest())
                        .ResponseAsync.ConfigureAwait(false))
                    .GetAwaiter().GetResult();

                if (response.Message != "pong")
                    throw new DasException("Ping: unexpected response '" + response.Message + "'");
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
                var response = Task.Run(async () =>
                    await grpcClient.GetStatsAsync(new StatsRequest())
                        .ResponseAsync.ConfigureAwait(false))
                    .GetAwaiter().GetResult();

                return string.Format(
                    "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                    response.QueriesRead, response.QueriesWrite, response.Errors,
                    response.ReaderPoolSize, response.Port);
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

    // ── Simple query result (replaces Arrow-based FlightQueryResult) ─────────

    /// <summary>
    /// Query result backed by protobuf rows. No Arrow dependency.
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

        public string GetColumnType(int index) { return columns[index].Type; }

        public List<Dictionary<string, object>> ToRows()
        {
            var result = new List<Dictionary<string, object>>(rows.Count);
            foreach (var row in rows)
            {
                var dict = new Dictionary<string, object>(columns.Count);
                for (int c = 0; c < columns.Count && c < row.Values.Count; c++)
                {
                    var val = row.Values[c];
                    dict[columns[c].Name] = val.IsNull ? null : (object)val.Text;
                }
                result.Add(dict);
            }
            return result;
        }

        public DataTable ToDataTable(string tableName = "Result")
        {
            var dt = new DataTable(tableName);
            foreach (var col in columns)
                dt.Columns.Add(col.Name, typeof(string));

            foreach (var row in rows)
            {
                var dr = dt.NewRow();
                for (int c = 0; c < columns.Count && c < row.Values.Count; c++)
                {
                    var val = row.Values[c];
                    dr[c] = val.IsNull ? (object)DBNull.Value : val.Text;
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

        public void Dispose() { /* nothing to dispose — plain data */ }
    }
}
