using System;

namespace DuckDbServer
{
    /// <summary>
    /// A thread-safe pool of pre-opened DuckDB database connections for read queries.
    ///
    /// <para><b>Contract:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     Implementations MUST be thread-safe. <see cref="Borrow"/> may be called
    ///     concurrently from any number of gRPC handler threads.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="Borrow"/> MUST block if all connections are currently in use, up to
    ///     the implementation-defined timeout. It MUST throw <see cref="TimeoutException"/>
    ///     if no connection becomes available within the timeout period.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="Borrow"/> MUST throw <see cref="ObjectDisposedException"/> if the
    ///     pool has been disposed.
    ///   </description></item>
    ///   <item><description>
    ///     The returned <see cref="IConnectionHandle"/> MUST return the connection to the
    ///     pool when disposed. Callers MUST use a <c>using</c> block to guarantee return.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="IDisposable.Dispose"/> MUST close all connections (idle and, after
    ///     waiting for return, borrowed). After disposal, <see cref="Borrow"/> MUST throw.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Usage pattern:</b></para>
    /// <code>
    /// using (var handle = pool.Borrow())
    /// using (var cmd = handle.Connection.CreateCommand())
    /// {
    ///     cmd.CommandText = "SELECT * FROM my_table";
    ///     using (var reader = cmd.ExecuteReader()) { ... }
    /// }
    /// // Connection is automatically returned to the pool here.
    /// </code>
    /// </summary>
    public interface IConnectionPool : IDisposable
    {
        /// <summary>
        /// Borrows an idle connection from the pool. Blocks until one is available or
        /// the implementation-defined timeout expires.
        /// </summary>
        /// <returns>
        /// An <see cref="IConnectionHandle"/> that wraps the borrowed connection. The
        /// caller MUST dispose this handle to return the connection to the pool. Failure
        /// to dispose leaks the connection permanently, eventually starving the pool.
        /// </returns>
        /// <exception cref="ObjectDisposedException">If the pool has been disposed.</exception>
        /// <exception cref="TimeoutException">If no connection is available within the timeout.</exception>
        IConnectionHandle Borrow();

        /// <summary>
        /// Total number of connections in the pool (idle + currently borrowed).
        /// This value is fixed at construction time and does not change.
        /// </summary>
        int Size { get; }
    }

    /// <summary>
    /// A borrowed database connection handle. Implements the RAII (Resource Acquisition
    /// Is Initialization) pattern: disposing the handle returns the connection to its
    /// owning pool.
    ///
    /// <para><b>Contract:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="Connection"/> MUST return the underlying DuckDB connection while
    ///     the handle is alive, and throw <see cref="ObjectDisposedException"/> after disposal.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="IDisposable.Dispose"/> MUST be idempotent. Calling it multiple times
    ///     MUST NOT return the connection more than once.
    ///   </description></item>
    ///   <item><description>
    ///     Disposing the handle does NOT close the connection; it returns it to the pool
    ///     for reuse by other borrowers.
    ///   </description></item>
    /// </list>
    /// </summary>
    public interface IConnectionHandle : IDisposable
    {
        /// <summary>
        /// The underlying DuckDB connection. Valid only while this handle has not been disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">If the handle has been disposed.</exception>
        DuckDB.NET.Data.DuckDBConnection Connection { get; }
    }
}
