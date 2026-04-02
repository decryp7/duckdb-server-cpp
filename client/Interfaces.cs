using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace DuckArrowClient
{
    /// <summary>
    /// The result of a read query. Contains column metadata and row data.
    /// </summary>
    public interface IFlightQueryResult : IDisposable
    {
        /// <summary>Total number of rows.</summary>
        int RowCount { get; }

        /// <summary>Number of columns.</summary>
        int ColumnCount { get; }

        /// <summary>Get the name of a column by index.</summary>
        string GetColumnName(int index);

        /// <summary>Get the type name of a column by index.</summary>
        string GetColumnType(int index);

        /// <summary>
        /// Convert to a list of row dictionaries (column name → string value).
        /// Null values are represented as null dictionary values.
        /// </summary>
        List<Dictionary<string, object>> ToRows();

        /// <summary>
        /// Convert to an ADO.NET DataTable for data binding.
        /// </summary>
        DataTable ToDataTable(string tableName = "Result");
    }

    /// <summary>
    /// A client connection to a DuckDB gRPC server.
    /// Thread-safe — one instance per application.
    /// </summary>
    public interface IDasFlightClient : IDisposable
    {
        IFlightQueryResult Query(string sql, CancellationToken ct = default);
        Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default);

        void Execute(string sql, CancellationToken ct = default);
        Task ExecuteAsync(string sql, CancellationToken ct = default);

        void Ping();
        string GetStats();
    }

    /// <summary>
    /// A borrowed client from a pool. Disposing returns the client.
    /// </summary>
    public interface IPoolLease : IDisposable
    {
        IDasFlightClient Client { get; }
    }

    /// <summary>
    /// Optional pool of client instances.
    /// </summary>
    public interface IDasFlightPool : IDisposable
    {
        IPoolLease Borrow(CancellationToken ct = default);
        Task<IPoolLease> BorrowAsync(CancellationToken ct = default);
    }
}
