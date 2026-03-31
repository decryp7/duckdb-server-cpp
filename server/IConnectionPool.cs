using System;

namespace DuckArrowServer
{
    /// <summary>
    /// A thread-safe pool of database connections.
    /// Callers borrow a connection, use it, and return it by disposing the handle.
    /// </summary>
    public interface IConnectionPool : IDisposable
    {
        /// <summary>
        /// Borrow an idle connection. Blocks until one is available or timeout.
        /// </summary>
        /// <returns>A handle that returns the connection when disposed.</returns>
        IConnectionHandle Borrow();

        /// <summary>Total number of connections (idle + borrowed).</summary>
        int Size { get; }
    }

    /// <summary>
    /// A borrowed database connection. Disposing returns it to the pool.
    /// </summary>
    public interface IConnectionHandle : IDisposable
    {
        /// <summary>The underlying DuckDB connection.</summary>
        DuckDB.NET.Data.DuckDBConnection Connection { get; }
    }
}
