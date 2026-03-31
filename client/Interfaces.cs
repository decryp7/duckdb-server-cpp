using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;

namespace DuckArrowClient
{
    // ─────────────────────────────────────────────────────────────────────────
    /// <summary>
    /// The result of a read query (Arrow Flight <c>DoGet</c>).
    ///
    /// <para>
    /// Provides the query schema and the ordered sequence of record batches
    /// returned by the server, plus convenience converters for common .NET
    /// data access patterns.
    /// </para>
    ///
    /// <para>
    /// Implementations are typically eagerly-materialised (all batches already
    /// in memory when the object is returned to the caller).  Dispose the result
    /// to release the underlying Arrow buffers.
    /// </para>
    ///
    /// <para><b>Design note — why expose batches instead of rows?</b></para>
    /// <para>
    /// Arrow's columnar layout allows CPU-efficient vectorised operations over
    /// entire columns.  Exposing <see cref="Batches"/> lets callers work in the
    /// native Arrow model.  <see cref="ToRows"/> and <see cref="ToDataTable"/>
    /// are provided as convenience wrappers for code that needs row-oriented access.
    /// </para>
    /// </summary>
    // ─────────────────────────────────────────────────────────────────────────
    public interface IFlightQueryResult : IDisposable
    {
        /// <summary>Arrow schema describing the columns of this result.</summary>
        Schema Schema { get; }

        /// <summary>Total number of data rows across all batches.</summary>
        int RowCount { get; }

        /// <summary>
        /// The underlying record batches in order.
        /// Each batch is a contiguous block of rows sharing the same schema.
        /// </summary>
        IReadOnlyList<RecordBatch> Batches { get; }

        /// <summary>
        /// Convert the result to a list of row dictionaries.
        ///
        /// <para>
        /// Each dictionary maps column name → boxed CLR value.
        /// Null cells are represented as <c>null</c> dictionary values.
        /// </para>
        ///
        /// <para>
        /// Suitable for small result sets and unit tests.  For large results,
        /// iterate over <see cref="Batches"/> and process columns directly.
        /// </para>
        /// </summary>
        /// <returns>One dictionary per row, in result order.</returns>
        List<Dictionary<string, object>> ToRows();

        /// <summary>
        /// Convert the result to an ADO.NET <see cref="DataTable"/>.
        ///
        /// <para>
        /// Suitable for binding to WinForms <c>DataGridView</c>, Crystal Reports,
        /// RDLC reports, and any other .NET 4.6.2 data-bound control.
        /// </para>
        /// </summary>
        /// <param name="tableName">Name assigned to the returned table.</param>
        /// <returns>A fully-populated DataTable with typed columns.</returns>
        DataTable ToDataTable(string tableName = "Result");
    }

    // ─────────────────────────────────────────────────────────────────────────
    /// <summary>
    /// A client connection to a DuckDB Arrow Flight server.
    ///
    /// <para>
    /// Implementations must be fully thread-safe.  A single
    /// <see cref="IDasFlightClient"/> instance should serve all concurrent callers
    /// in the application — gRPC / HTTP/2 multiplexing handles concurrency
    /// without the need for per-caller connections.
    /// </para>
    ///
    /// <para><b>Dependency injection / testing</b></para>
    /// <para>
    /// Code that depends on database access should accept <c>IDasFlightClient</c>
    /// rather than the concrete <see cref="DasFlightClient"/>.  This allows unit
    /// tests to inject a mock that returns pre-built <see cref="IFlightQueryResult"/>
    /// objects without starting a real server.
    /// </para>
    ///
    /// <example>
    /// <code>
    /// public class ReportService
    /// {
    ///     private readonly IDasFlightClient _db;
    ///     public ReportService(IDasFlightClient db) { _db = db; }
    ///
    ///     public DataTable GetSales()
    ///     {
    ///         using (var result = _db.Query("SELECT * FROM sales"))
    ///             return result.ToDataTable();
    ///     }
    /// }
    /// </code>
    /// </example>
    /// </summary>
    // ─────────────────────────────────────────────────────────────────────────
    public interface IDasFlightClient : IDisposable
    {
        // ── Read queries ──────────────────────────────────────────────────────

        /// <summary>
        /// Execute a SELECT / analytical query synchronously.
        ///
        /// <para>
        /// Sends a Flight <c>DoGet</c> RPC with <paramref name="sql"/> as the
        /// ticket payload and materialises all returned batches before returning.
        /// </para>
        /// </summary>
        /// <param name="sql">UTF-8 SELECT / WITH / EXPLAIN / SHOW statement.</param>
        /// <param name="ct">Cancellation token for the RPC call.</param>
        /// <returns>
        /// An <see cref="IFlightQueryResult"/> holding the schema and all batches.
        /// Must be disposed by the caller.
        /// </returns>
        /// <exception cref="DasException">Server-side execution error.</exception>
        IFlightQueryResult Query(string sql, CancellationToken ct = default);

        /// <summary>Async version of <see cref="Query"/>.</summary>
        Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default);

        // ── Write / DDL ───────────────────────────────────────────────────────

        /// <summary>
        /// Execute a DML or DDL statement synchronously.
        ///
        /// <para>
        /// Sends a Flight <c>DoAction("execute")</c> RPC.  The server batches
        /// concurrent DML requests into one transaction; DDL is executed outside
        /// any batch.  Blocks until the statement's transaction commits.
        /// </para>
        /// </summary>
        /// <param name="sql">UTF-8 INSERT / UPDATE / DELETE / CREATE / DROP / … statement.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <exception cref="DasException">DuckDB execution error.</exception>
        void Execute(string sql, CancellationToken ct = default);

        /// <summary>Async version of <see cref="Execute"/>.</summary>
        Task ExecuteAsync(string sql, CancellationToken ct = default);

        // ── Liveness and diagnostics ──────────────────────────────────────────

        /// <summary>
        /// Send a ping and verify the server responds with "pong".
        /// </summary>
        /// <exception cref="DasException">
        /// Network failure or unexpected response.
        /// </exception>
        void Ping();

        /// <summary>
        /// Retrieve live server metrics as a JSON string.
        ///
        /// <para>
        /// Example response:
        /// <code>
        /// {"queries_read":1000,"queries_write":22,"errors":0,"reader_pool_size":16,"port":17777}
        /// </code>
        /// Parse with <c>Newtonsoft.Json</c> or <c>System.Text.Json</c> as preferred.
        /// </para>
        /// </summary>
        /// <returns>UTF-8 JSON object string.</returns>
        string GetStats();
    }

    // ─────────────────────────────────────────────────────────────────────────
    /// <summary>
    /// A pool of <see cref="IDasFlightClient"/> instances.
    ///
    /// <para>
    /// <b>In most applications you do not need this pool.</b>  A single
    /// <see cref="IDasFlightClient"/> handles hundreds of concurrent callers
    /// via HTTP/2 multiplexing.  Use <see cref="IDasFlightPool"/> only when you
    /// need hard channel-count limits or per-channel TLS session isolation.
    /// </para>
    /// </summary>
    // ─────────────────────────────────────────────────────────────────────────
    /// <summary>
    /// Represents a borrowed client from a <see cref="IDasFlightPool"/>.
    /// Disposing the lease returns the client to the pool for reuse.
    /// </summary>
    public interface IPoolLease : IDisposable
    {
        /// <summary>The borrowed client. Valid until this lease is disposed.</summary>
        IDasFlightClient Client { get; }
    }

    public interface IDasFlightPool : IDisposable
    {
        /// <summary>
        /// Borrow a client, blocking until one is available.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A lease that returns the client on disposal.</returns>
        IPoolLease Borrow(CancellationToken ct = default);

        /// <summary>Async version of <see cref="Borrow"/>.</summary>
        Task<IPoolLease> BorrowAsync(CancellationToken ct = default);
    }
}
