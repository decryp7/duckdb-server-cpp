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
    /// Reads columnar data — packed arrays instead of per-cell objects.
    /// </summary>
    public sealed class DasFlightClient : IDasFlightClient
    {
        private readonly Channel channel;
        private readonly DuckDbService.DuckDbServiceClient grpcClient;
        private int disposed;

        // gRPC channel options for high performance
        private static readonly ChannelOption[] HighPerfOptions = new[]
        {
            new ChannelOption(ChannelOptions.MaxConcurrentStreams, 200),
            new ChannelOption(ChannelOptions.MaxReceiveMessageLength, 64 * 1024 * 1024),
            new ChannelOption(ChannelOptions.MaxSendMessageLength, 64 * 1024 * 1024),
            new ChannelOption("grpc.keepalive_time_ms", 30000),
            new ChannelOption("grpc.keepalive_timeout_ms", 10000),
            new ChannelOption("grpc.keepalive_permit_without_calls", 1),
            new ChannelOption("grpc.http2.max_pings_without_data", 0),
        };

        public DasFlightClient(string host = "localhost", int port = 17777)
            : this(new Channel(host + ":" + port, ChannelCredentials.Insecure, HighPerfOptions)) { }

        public DasFlightClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials, HighPerfOptions)) { }

        private DasFlightClient(Channel channel)
        {
            this.channel = channel;
            this.grpcClient = new DuckDbService.DuckDbServiceClient(channel);
        }

        public IFlightQueryResult Query(string sql, CancellationToken ct = default)
        {
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            try
            {
                var columns = new List<ColumnMeta>();
                var batches = new List<ColumnarBatch>();

                using (var call = grpcClient.Query(new QueryRequest { Sql = sql }, cancellationToken: ct))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    {
                        var resp = call.ResponseStream.Current;
                        if (resp.Columns.Count > 0)
                            columns.AddRange(resp.Columns);
                        if (resp.RowCount > 0)
                            batches.Add(new ColumnarBatch(resp.Data, resp.RowCount));
                    }
                }

                return new ColumnarQueryResult(columns, batches);
            }
            catch (RpcException ex)
            {
                throw new DasException("Query failed: " + ex.Status.Detail, ex);
            }
        }

        public void Execute(string sql, CancellationToken ct = default)
        {
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            try
            {
                var r = await grpcClient.ExecuteAsync(new ExecuteRequest { Sql = sql }, cancellationToken: ct)
                    .ResponseAsync.ConfigureAwait(false);
                if (!r.Success) throw new DasException("Execute failed: " + r.Error);
            }
            catch (RpcException ex) { throw new DasException("Execute failed: " + ex.Status.Detail, ex); }
        }

        public void Ping()
        {
            EnsureNotDisposed();
            try
            {
                var r = Task.Run(async () => await grpcClient.PingAsync(new PingRequest()).ResponseAsync.ConfigureAwait(false)).GetAwaiter().GetResult();
                if (r.Message != "pong") throw new DasException("Ping: unexpected '" + r.Message + "'");
            }
            catch (RpcException ex) { throw new DasException("Ping failed: " + ex.Status.Detail, ex); }
        }

        public string GetStats()
        {
            EnsureNotDisposed();
            try
            {
                var r = Task.Run(async () => await grpcClient.GetStatsAsync(new StatsRequest()).ResponseAsync.ConfigureAwait(false)).GetAwaiter().GetResult();
                return string.Format("{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                    r.QueriesRead, r.QueriesWrite, r.Errors, r.ReaderPoolSize, r.Port);
            }
            catch (RpcException ex) { throw new DasException("GetStats failed: " + ex.Status.Detail, ex); }
        }

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref disposed) != 0) throw new ObjectDisposedException(nameof(DasFlightClient));
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            try { channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); } catch { }
        }
    }

    // ── Columnar batch ───────────────────────────────────────────────────────

    internal sealed class ColumnarBatch
    {
        public readonly IList<ColumnData> Data;
        public readonly int RowCount;

        public ColumnarBatch(IList<ColumnData> data, int rowCount)
        {
            Data = data;
            RowCount = rowCount;
        }
    }

    // ── Columnar query result ────────────────────────────────────────────────

    public sealed class ColumnarQueryResult : IFlightQueryResult
    {
        private readonly List<ColumnMeta> columns;
        private readonly List<ColumnarBatch> batches;
        private readonly int totalRows;

        internal ColumnarQueryResult(List<ColumnMeta> columns, List<ColumnarBatch> batches)
        {
            this.columns = columns;
            this.batches = batches;
            int total = 0;
            foreach (var b in batches) total += b.RowCount;
            this.totalRows = total;
        }

        public int RowCount { get { return totalRows; } }
        public int ColumnCount { get { return columns.Count; } }
        public string GetColumnName(int index) { return columns[index].Name; }
        public string GetColumnType(int index) { return columns[index].Type.ToString(); }

        /// <summary>
        /// Extract a value from a column at a row index within a batch.
        /// Reads directly from the packed array — no per-cell object overhead.
        /// </summary>
        private static object GetColumnValue(ColumnData cd, ColumnType type, int row)
        {
            // Check null
            for (int i = 0; i < cd.NullIndices.Count; i++)
                if (cd.NullIndices[i] == row) return null;

            switch (type)
            {
                case ColumnType.TypeBoolean:
                    return row < cd.BoolValues.Count ? (object)cd.BoolValues[row] : null;
                case ColumnType.TypeInt32:
                case ColumnType.TypeInt8:
                case ColumnType.TypeInt16:
                case ColumnType.TypeUint8:
                case ColumnType.TypeUint16:
                    return row < cd.Int32Values.Count ? (object)cd.Int32Values[row] : null;
                case ColumnType.TypeInt64:
                case ColumnType.TypeUint32:
                case ColumnType.TypeUint64:
                    return row < cd.Int64Values.Count ? (object)cd.Int64Values[row] : null;
                case ColumnType.TypeFloat:
                    return row < cd.FloatValues.Count ? (object)cd.FloatValues[row] : null;
                case ColumnType.TypeDouble:
                case ColumnType.TypeDecimal:
                    return row < cd.DoubleValues.Count ? (object)cd.DoubleValues[row] : null;
                case ColumnType.TypeBlob:
                    return row < cd.BlobValues.Count ? (object)cd.BlobValues[row].ToByteArray() : null;
                default:
                    return row < cd.StringValues.Count ? (object)cd.StringValues[row] : null;
            }
        }

        public List<Dictionary<string, object>> ToRows()
        {
            var result = new List<Dictionary<string, object>>(totalRows);
            foreach (var batch in batches)
            {
                for (int r = 0; r < batch.RowCount; r++)
                {
                    var dict = new Dictionary<string, object>(columns.Count);
                    for (int c = 0; c < columns.Count && c < batch.Data.Count; c++)
                        dict[columns[c].Name] = GetColumnValue(batch.Data[c], columns[c].Type, r);
                    result.Add(dict);
                }
            }
            return result;
        }

        public DataTable ToDataTable(string tableName = "Result")
        {
            var dt = new DataTable(tableName);
            foreach (var col in columns)
                dt.Columns.Add(col.Name, MapClrType(col.Type));

            foreach (var batch in batches)
            {
                for (int r = 0; r < batch.RowCount; r++)
                {
                    var dr = dt.NewRow();
                    for (int c = 0; c < columns.Count && c < batch.Data.Count; c++)
                    {
                        var val = GetColumnValue(batch.Data[c], columns[c].Type, r);
                        dr[c] = val ?? (object)DBNull.Value;
                    }
                    dt.Rows.Add(dr);
                }
            }
            return dt;
        }

        private static Type MapClrType(ColumnType ct)
        {
            switch (ct)
            {
                case ColumnType.TypeBoolean: return typeof(bool);
                case ColumnType.TypeInt8: case ColumnType.TypeInt16:
                case ColumnType.TypeInt32: case ColumnType.TypeUint8:
                case ColumnType.TypeUint16: return typeof(int);
                case ColumnType.TypeInt64: case ColumnType.TypeUint32:
                case ColumnType.TypeUint64: return typeof(long);
                case ColumnType.TypeFloat: return typeof(float);
                case ColumnType.TypeDouble: case ColumnType.TypeDecimal: return typeof(double);
                case ColumnType.TypeBlob: return typeof(byte[]);
                default: return typeof(string);
            }
        }

        public void Dispose() { }
    }
}
