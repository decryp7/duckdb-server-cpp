using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDbClient
{
    /// <summary>
    /// The result of a read query, containing column metadata and row data.
    /// Implements <see cref="IDisposable"/> for resource cleanup in <c>using</c> blocks,
    /// even though the default implementation (<see cref="ColumnarQueryResult"/>) holds
    /// only managed references.
    ///
    /// <para><b>Thread Safety:</b> Implementations are NOT required to be thread-safe.
    /// Query results are typically consumed on a single thread after the query completes.</para>
    ///
    /// <para><b>Contract:</b></para>
    /// <list type="bullet">
    ///   <item><description><see cref="RowCount"/> and <see cref="ColumnCount"/> MUST return
    ///     consistent values that do not change after construction.</description></item>
    ///   <item><description><see cref="ToRows"/> and <see cref="ToDataTable"/> MUST return
    ///     new collections each time they are called (not cached references).</description></item>
    ///   <item><description>NULL values in <see cref="ToRows"/> MUST be represented as
    ///     <c>null</c> dictionary values. NULL values in <see cref="ToDataTable"/> MUST
    ///     be represented as <see cref="DBNull.Value"/>.</description></item>
    /// </list>
    /// </summary>
    public interface IQueryResult : IDisposable
    {
        /// <summary>Total number of rows across all batches.</summary>
        int RowCount { get; }

        /// <summary>Number of columns in the result.</summary>
        int ColumnCount { get; }

        /// <summary>Gets the name of a column by its zero-based index.</summary>
        /// <param name="index">Zero-based column index.</param>
        /// <returns>The column name as defined by the query.</returns>
        string GetColumnName(int index);

        /// <summary>Gets the type name of a column by its zero-based index.</summary>
        /// <param name="index">Zero-based column index.</param>
        /// <returns>The column type as a string (e.g., "TypeInt32", "TypeString").</returns>
        string GetColumnType(int index);

        /// <summary>
        /// Converts the result to a list of row dictionaries. Each dictionary maps
        /// column names to their values (boxed CLR objects). NULL values are represented
        /// as <c>null</c> dictionary values.
        ///
        /// <para><b>Performance note:</b> This materializes all rows into dictionaries,
        /// boxing every numeric value. For large result sets, prefer
        /// <see cref="ToDataTable"/> or direct columnar access.</para>
        /// </summary>
        /// <returns>A new list of dictionaries, one per row.</returns>
        List<Dictionary<string, object>> ToRows();

        /// <summary>
        /// Converts the result to an ADO.NET <see cref="DataTable"/> for data binding
        /// in WinForms, WPF, or ASP.NET applications.
        ///
        /// <para>NULL values are represented as <see cref="DBNull.Value"/> following
        /// ADO.NET conventions.</para>
        /// </summary>
        /// <param name="tableName">The name for the DataTable. Default: "Result".</param>
        /// <returns>A new <see cref="DataTable"/> with typed columns and all rows.</returns>
        DataTable ToDataTable(string tableName = "Result");
    }

    /// <summary>
    /// A client connection to a DuckDB gRPC server. Provides methods for executing
    /// queries, writes, health checks, and retrieving statistics.
    ///
    /// <para><b>Thread Safety:</b> Implementations MUST be thread-safe. A single
    /// <see cref="IDuckDbClient"/> instance should be shared across the entire application
    /// (one per server endpoint). The underlying gRPC channel handles connection
    /// multiplexing and reconnection automatically.</para>
    ///
    /// <para><b>Error Handling:</b> All methods throw <see cref="DuckDbException"/> on
    /// failure (SQL errors, network errors, timeouts). The original gRPC exception is
    /// available as <see cref="Exception.InnerException"/>.</para>
    ///
    /// <para><b>Disposal:</b> Disposing the client shuts down the gRPC channel. After
    /// disposal, all methods throw <see cref="ObjectDisposedException"/>.</para>
    /// </summary>
    public interface IDuckDbClient : IDisposable
    {
        /// <summary>Executes a read query synchronously and returns the result.</summary>
        /// <param name="sql">The SQL query string.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>The query result containing column metadata and row data.</returns>
        /// <exception cref="DuckDbException">On query failure.</exception>
        IQueryResult Query(string sql, CancellationToken ct = default);

        /// <summary>Executes a read query asynchronously and returns the result.</summary>
        /// <param name="sql">The SQL query string.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>A task resolving to the query result.</returns>
        /// <exception cref="DuckDbException">On query failure.</exception>
        Task<IQueryResult> QueryAsync(string sql, CancellationToken ct = default);

        /// <summary>Executes a write (DML or DDL) statement synchronously.</summary>
        /// <param name="sql">The SQL statement to execute.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <exception cref="DuckDbException">On execution failure.</exception>
        void Execute(string sql, CancellationToken ct = default);

        /// <summary>Executes a write (DML or DDL) statement asynchronously.</summary>
        /// <param name="sql">The SQL statement to execute.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <exception cref="DuckDbException">On execution failure.</exception>
        Task ExecuteAsync(string sql, CancellationToken ct = default);

        /// <summary>
        /// Sends a health-check ping to the server. Throws if the server is unreachable
        /// or returns an unexpected response.
        /// </summary>
        /// <exception cref="DuckDbException">If the ping fails.</exception>
        void Ping();

        /// <summary>
        /// Retrieves server statistics as a JSON-formatted string.
        /// </summary>
        /// <returns>JSON string with query counts, errors, pool size, and port.</returns>
        /// <exception cref="DuckDbException">If the stats call fails.</exception>
        string GetStats();
    }

    /// <summary>
    /// A borrowed client from a pool. Disposing returns the client to its pool.
    /// Follows the same RAII pattern as <see cref="DuckDbServer.IConnectionHandle"/>.
    ///
    /// <para><b>Thread Safety:</b> NOT thread-safe. Each lease should be used by a
    /// single thread. The client itself (<see cref="Client"/>) is thread-safe.</para>
    /// </summary>
    public interface IPoolLease : IDisposable
    {
        /// <summary>The borrowed client instance. Valid until this lease is disposed.</summary>
        IDuckDbClient Client { get; }
    }

    /// <summary>
    /// Optional pool of <see cref="IDuckDbClient"/> instances. Useful when connecting
    /// to multiple server endpoints or when client-side connection pooling is desired.
    ///
    /// <para><b>Thread Safety:</b> Implementations MUST be thread-safe. <see cref="Borrow"/>
    /// may be called concurrently from multiple threads.</para>
    /// </summary>
    public interface IDuckDbPool : IDisposable
    {
        /// <summary>Borrows a client from the pool. Blocks if none are available.</summary>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>A lease that returns the client when disposed.</returns>
        IPoolLease Borrow(CancellationToken ct = default);

        /// <summary>Borrows a client from the pool asynchronously.</summary>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>A task resolving to a lease that returns the client when disposed.</returns>
        Task<IPoolLease> BorrowAsync(CancellationToken ct = default);
    }
}
