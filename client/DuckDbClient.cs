using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckDbClient
{
    /// <summary>
    /// Thread-safe gRPC client for the DuckDB server. Provides synchronous and
    /// asynchronous methods for queries, writes, health checks, and statistics.
    ///
    /// <para><b>Thread Safety:</b> This class is fully thread-safe. The underlying gRPC
    /// <see cref="Channel"/> and <see cref="DuckDbService.DuckDbServiceClient"/> handle
    /// concurrent calls internally. The disposal flag uses <see cref="Interlocked"/>
    /// for thread-safe one-shot shutdown.</para>
    ///
    /// <para><b>Columnar Data Model:</b> Query results are received as columnar batches
    /// (packed arrays per column) rather than row-oriented data. This matches DuckDB's
    /// internal storage format and minimizes serialization/deserialization overhead.
    /// The <see cref="ColumnarQueryResult"/> class provides convenience methods
    /// (<see cref="ColumnarQueryResult.ToRows"/>, <see cref="ColumnarQueryResult.ToDataTable"/>)
    /// for converting to row-oriented formats when needed.</para>
    ///
    /// <para><b>Connection Lifecycle:</b> One client instance per application is typical.
    /// The underlying gRPC channel manages HTTP/2 connections, multiplexing, and reconnection
    /// automatically. Disposing the client shuts down the channel with a 5-second grace period.</para>
    ///
    /// <para><b>Error Handling:</b> All gRPC errors are caught and wrapped in
    /// <see cref="DuckDbException"/> for a clean API surface. The original
    /// <see cref="RpcException"/> is available as <see cref="Exception.InnerException"/>.</para>
    /// </summary>
    public sealed class DuckDbClient : IDuckDbClient
    {
        /// <summary>The underlying gRPC channel (manages HTTP/2 connections).</summary>
        private readonly Channel channel;

        /// <summary>The generated gRPC client stub for calling server methods.</summary>
        private readonly DuckDbService.DuckDbServiceClient grpcClient;

        /// <summary>Disposal flag. 0 = alive, 1 = disposed.</summary>
        private int disposed;

        /// <summary>
        /// gRPC channel options tuned for high-throughput analytics workloads.
        /// These match the server-side options for optimal performance.
        /// </summary>
        private static readonly ChannelOption[] HighPerfOptions = new[]
        {
            // Allow up to 200 concurrent RPC streams over a single HTTP/2 connection.
            new ChannelOption(ChannelOptions.MaxConcurrentStreams, 200),
            // 64 MB max message size for large query results and bulk inserts.
            new ChannelOption(ChannelOptions.MaxReceiveMessageLength, 64 * 1024 * 1024),
            new ChannelOption(ChannelOptions.MaxSendMessageLength, 64 * 1024 * 1024),
            // Keepalive: ping every 30s, timeout after 10s. Prevents load balancers
            // from closing idle connections.
            new ChannelOption("grpc.keepalive_time_ms", 30000),
            new ChannelOption("grpc.keepalive_timeout_ms", 10000),
            new ChannelOption("grpc.keepalive_permit_without_calls", 1),
            new ChannelOption("grpc.http2.max_pings_without_data", 0),
        };

        /// <summary>
        /// Creates a client connected to the specified host and port using insecure (plaintext)
        /// credentials. Suitable for localhost or VPN-protected networks.
        /// </summary>
        /// <param name="host">Server hostname or IP address. Default: "localhost".</param>
        /// <param name="port">Server gRPC port. Default: 19100.</param>
        public DuckDbClient(string host = "localhost", int port = 19100)
            : this(new Channel(host + ":" + port, ChannelCredentials.Insecure, HighPerfOptions)) { }

        /// <summary>
        /// Creates a client connected to the specified host and port with custom credentials
        /// (e.g., SSL/TLS for encrypted connections).
        /// </summary>
        /// <param name="host">Server hostname or IP address.</param>
        /// <param name="port">Server gRPC port.</param>
        /// <param name="credentials">gRPC channel credentials (e.g., <see cref="SslCredentials"/>).</param>
        public DuckDbClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials, HighPerfOptions)) { }

        /// <summary>
        /// Internal constructor that initializes the gRPC client from a pre-configured channel.
        /// </summary>
        /// <param name="channel">The gRPC channel to use for all RPCs.</param>
        private DuckDbClient(Channel channel)
        {
            this.channel = channel;
            this.grpcClient = new DuckDbService.DuckDbServiceClient(channel);
        }

        /// <summary>
        /// Executes a read query synchronously by blocking on the async implementation.
        /// Uses <c>Task.Run</c> to avoid deadlocks in synchronization contexts (e.g., WinForms, ASP.NET).
        /// </summary>
        /// <param name="sql">The SQL query string to execute.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>The query result containing column metadata and row data.</returns>
        /// <exception cref="DuckDbException">If the query fails or the gRPC call errors.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public IQueryResult Query(string sql, CancellationToken ct = default)
        {
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a read query asynchronously. Opens a server-streaming RPC and reads
        /// all response batches into a <see cref="ColumnarQueryResult"/>.
        ///
        /// <para><b>Streaming protocol:</b> The server sends multiple <see cref="QueryResponse"/>
        /// messages, each containing up to batch-size rows of columnar data. The first
        /// response includes column metadata (names and types). The client accumulates all
        /// batches and returns a single <see cref="ColumnarQueryResult"/>.</para>
        /// </summary>
        /// <param name="sql">The SQL query string to execute.</param>
        /// <param name="ct">Optional cancellation token for aborting the query.</param>
        /// <returns>A task that resolves to the complete query result.</returns>
        /// <exception cref="DuckDbException">If the query fails or the gRPC call errors.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public async Task<IQueryResult> QueryAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            try
            {
                var columns = new List<ColumnMeta>();
                var batches = new List<ColumnarBatch>();

                // Open a server-streaming call and read all response batches.
                using (var call = grpcClient.Query(new QueryRequest { Sql = sql }, cancellationToken: ct))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    {
                        var resp = call.ResponseStream.Current;
                        // Column metadata is included only in the first response.
                        if (resp.Columns.Count > 0)
                            columns.AddRange(resp.Columns);
                        // Accumulate data batches for the result.
                        if (resp.RowCount > 0)
                            batches.Add(new ColumnarBatch(resp.Data, resp.RowCount));
                    }
                }

                return new ColumnarQueryResult(columns, batches);
            }
            catch (RpcException ex)
            {
                throw new DuckDbException("Query failed: " + ex.Status.Detail, ex);
            }
        }

        /// <summary>
        /// Executes a write (DML or DDL) statement synchronously.
        /// </summary>
        /// <param name="sql">The SQL statement to execute (e.g., INSERT, CREATE TABLE).</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <exception cref="DuckDbException">If the execute fails or the gRPC call errors.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public void Execute(string sql, CancellationToken ct = default)
        {
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a write (DML or DDL) statement asynchronously.
        /// </summary>
        /// <param name="sql">The SQL statement to execute.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <exception cref="DuckDbException">If the server returns <c>Success = false</c> or the gRPC call fails.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            try
            {
                var r = await grpcClient.ExecuteAsync(new ExecuteRequest { Sql = sql }, cancellationToken: ct)
                    .ResponseAsync.ConfigureAwait(false);
                if (!r.Success) throw new DuckDbException("Execute failed: " + r.Error);
            }
            catch (RpcException ex) { throw new DuckDbException("Execute failed: " + ex.Status.Detail, ex); }
        }

        /// <summary>
        /// Sends a health-check ping to the server and verifies the response.
        /// Throws <see cref="DuckDbException"/> if the server is unreachable or returns
        /// an unexpected response.
        /// </summary>
        /// <exception cref="DuckDbException">If the ping fails or returns an unexpected message.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public void Ping()
        {
            EnsureNotDisposed();
            try
            {
                var r = Task.Run(async () => await grpcClient.PingAsync(new PingRequest()).ResponseAsync.ConfigureAwait(false)).GetAwaiter().GetResult();
                if (r.Message != "pong") throw new DuckDbException("Ping: unexpected '" + r.Message + "'");
            }
            catch (RpcException ex) { throw new DuckDbException("Ping failed: " + ex.Status.Detail, ex); }
        }

        /// <summary>
        /// Retrieves server statistics as a JSON string. Includes query counts, error counts,
        /// pool size, and port information.
        /// </summary>
        /// <returns>A JSON string with server metrics.</returns>
        /// <exception cref="DuckDbException">If the gRPC call fails.</exception>
        /// <exception cref="ObjectDisposedException">If the client has been disposed.</exception>
        public string GetStats()
        {
            EnsureNotDisposed();
            try
            {
                var r = Task.Run(async () => await grpcClient.GetStatsAsync(new StatsRequest()).ResponseAsync.ConfigureAwait(false)).GetAwaiter().GetResult();
                return string.Format("{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                    r.QueriesRead, r.QueriesWrite, r.Errors, r.ReaderPoolSize, r.Port);
            }
            catch (RpcException ex) { throw new DuckDbException("GetStats failed: " + ex.Status.Detail, ex); }
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if the client has been disposed.
        /// Uses <see cref="Thread.VolatileRead"/> for a lightweight visibility check
        /// without a full memory barrier.
        /// </summary>
        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref disposed) != 0) throw new ObjectDisposedException(nameof(DuckDbClient));
        }

        /// <summary>
        /// Shuts down the gRPC channel with a 5-second grace period. Idempotent -- safe
        /// to call multiple times. Exceptions during shutdown are swallowed because
        /// the channel is being abandoned anyway.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            try { channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); } catch { }
        }
    }

    // ── Columnar batch ───────────────────────────────────────────────────────

    /// <summary>
    /// A single batch of columnar data received from the server. Contains one
    /// <see cref="ColumnData"/> per column and a row count. Multiple batches may
    /// comprise a single query result.
    ///
    /// <para><b>Thread Safety:</b> NOT thread-safe. Batches are created during
    /// query execution and then read-only.</para>
    /// </summary>
    internal sealed class ColumnarBatch
    {
        /// <summary>Columnar data arrays, one per column. Indexed by column ordinal.</summary>
        public readonly IList<ColumnData> Data;

        /// <summary>Number of rows in this batch (may be less than the server's batch size for the final batch).</summary>
        public readonly int RowCount;

        /// <summary>
        /// Creates a new columnar batch.
        /// </summary>
        /// <param name="data">Column data arrays from the server response.</param>
        /// <param name="rowCount">Number of rows in this batch.</param>
        public ColumnarBatch(IList<ColumnData> data, int rowCount)
        {
            Data = data;
            RowCount = rowCount;
        }
    }

    // ── Columnar query result ────────────────────────────────────────────────

    /// <summary>
    /// Represents the complete result of a query, composed of column metadata and
    /// one or more columnar batches. Provides methods to convert the columnar data
    /// into row-oriented formats (<see cref="ToRows"/>, <see cref="ToDataTable"/>).
    ///
    /// <para><b>Thread Safety:</b> NOT thread-safe. Query results are intended for
    /// single-threaded consumption after the query completes.</para>
    ///
    /// <para><b>Memory:</b> All batches are held in memory simultaneously. For very
    /// large result sets (millions of rows), memory usage is proportional to the total
    /// number of rows. Consider streaming processing for such cases.</para>
    /// </summary>
    public sealed class ColumnarQueryResult : IQueryResult
    {
        /// <summary>Column metadata (names and types) from the first server response.</summary>
        private readonly List<ColumnMeta> columns;

        /// <summary>All data batches received from the server.</summary>
        private readonly List<ColumnarBatch> batches;

        /// <summary>Total row count across all batches (pre-computed for O(1) access).</summary>
        private readonly int totalRows;

        /// <summary>
        /// Creates a new query result from column metadata and data batches.
        /// Pre-computes the total row count.
        /// </summary>
        /// <param name="columns">Column metadata from the server.</param>
        /// <param name="batches">Data batches containing packed columnar arrays.</param>
        internal ColumnarQueryResult(List<ColumnMeta> columns, List<ColumnarBatch> batches)
        {
            this.columns = columns;
            this.batches = batches;
            // Pre-compute total rows to avoid repeated iteration.
            int total = 0;
            foreach (var b in batches) total += b.RowCount;
            this.totalRows = total;
        }

        /// <summary>Total number of rows across all batches.</summary>
        public int RowCount { get { return totalRows; } }

        /// <summary>Number of columns in the result.</summary>
        public int ColumnCount { get { return columns.Count; } }

        /// <summary>Gets the name of a column by its zero-based index.</summary>
        /// <param name="index">Zero-based column index.</param>
        /// <returns>The column name as defined by the server.</returns>
        public string GetColumnName(int index) { return columns[index].Name; }

        /// <summary>Gets the protobuf type name of a column by its zero-based index.</summary>
        /// <param name="index">Zero-based column index.</param>
        /// <returns>The column type as a string (e.g., "TypeInt32", "TypeString").</returns>
        public string GetColumnType(int index) { return columns[index].Type.ToString(); }

        /// <summary>
        /// Extracts a single value from a column's packed array at a given row index.
        /// Checks the <c>NullIndices</c> list first; if the row is NULL, returns <c>null</c>.
        /// Otherwise, reads from the appropriate typed array.
        ///
        /// <para><b>Null check:</b> Uses a linear scan of <c>NullIndices</c>. For columns
        /// with many NULLs, this is O(M) per value. A HashSet would be faster but would
        /// require allocation per column per batch.</para>
        ///
        /// <para><b>Boxing:</b> Numeric values are boxed to <c>object</c> when returned.
        /// This is unavoidable for the dictionary-based <see cref="ToRows"/> API but does
        /// create GC pressure for large result sets.</para>
        /// </summary>
        /// <param name="cd">The column's packed data array.</param>
        /// <param name="type">The column's protobuf type (determines which array to read).</param>
        /// <param name="row">The zero-based row index within the batch.</param>
        /// <returns>The value as a boxed CLR object, or <c>null</c> for NULL values.</returns>
        private static object GetColumnValue(ColumnData cd, ColumnType type, int row)
        {
            // Linear scan of null indices. Returns null if this row is marked as NULL.
            for (int i = 0; i < cd.NullIndices.Count; i++)
                if (cd.NullIndices[i] == row) return null;

            // Read from the appropriate typed array based on the column type.
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
                    // Convert protobuf ByteString to byte[] for the CLR API.
                    return row < cd.BlobValues.Count ? (object)cd.BlobValues[row].ToByteArray() : null;
                default:
                    return row < cd.StringValues.Count ? (object)cd.StringValues[row] : null;
            }
        }

        /// <summary>
        /// Converts the columnar result into a list of row dictionaries, where each
        /// dictionary maps column name to its value (or <c>null</c> for NULL).
        ///
        /// <para><b>Performance:</b> This method materializes ALL rows into dictionaries,
        /// which involves boxing every numeric value and allocating a dictionary per row.
        /// For large result sets, prefer <see cref="ToDataTable"/> or direct columnar access.</para>
        /// </summary>
        /// <returns>
        /// A list of dictionaries, one per row. Each dictionary has one entry per column.
        /// NULL values are represented as <c>null</c> dictionary values.
        /// </returns>
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

        /// <summary>
        /// Converts the columnar result into an ADO.NET <see cref="DataTable"/>, suitable
        /// for data binding in WinForms, WPF, or ASP.NET applications.
        ///
        /// <para><b>Type mapping:</b> Column types are mapped to CLR types via
        /// <see cref="MapClrType"/>. Small integer types (INT8, INT16, UINT8, UINT16) are
        /// widened to <c>int</c>. Large integer types (UINT32, UINT64) are stored as <c>long</c>.
        /// DECIMAL is stored as <c>double</c>.</para>
        ///
        /// <para><b>NULL handling:</b> NULL values are represented as <see cref="DBNull.Value"/>
        /// in the DataTable, following ADO.NET conventions.</para>
        /// </summary>
        /// <param name="tableName">The name for the DataTable. Default: "Result".</param>
        /// <returns>A populated <see cref="DataTable"/> with typed columns and all rows.</returns>
        public DataTable ToDataTable(string tableName = "Result")
        {
            var dt = new DataTable(tableName);
            // Define columns with appropriate CLR types.
            foreach (var col in columns)
                dt.Columns.Add(col.Name, MapClrType(col.Type));

            // Populate rows from all batches.
            foreach (var batch in batches)
            {
                for (int r = 0; r < batch.RowCount; r++)
                {
                    var dr = dt.NewRow();
                    for (int c = 0; c < columns.Count && c < batch.Data.Count; c++)
                    {
                        var val = GetColumnValue(batch.Data[c], columns[c].Type, r);
                        // ADO.NET convention: NULL values must be DBNull.Value, not null.
                        dr[c] = val ?? (object)DBNull.Value;
                    }
                    dt.Rows.Add(dr);
                }
            }
            return dt;
        }

        /// <summary>
        /// Maps a protobuf <see cref="ColumnType"/> to the corresponding CLR <see cref="Type"/>
        /// for use in <see cref="DataTable"/> column definitions.
        /// </summary>
        /// <param name="ct">The protobuf column type.</param>
        /// <returns>The CLR type to use for the DataTable column.</returns>
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

        /// <summary>
        /// No-op disposal. This class does not own any unmanaged resources. Implements
        /// <see cref="IDisposable"/> to satisfy the <see cref="IQueryResult"/> interface
        /// contract, allowing callers to use <c>using</c> blocks consistently.
        /// </summary>
        public void Dispose() { }
    }
}
