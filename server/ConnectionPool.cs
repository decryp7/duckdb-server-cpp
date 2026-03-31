using System;
using System.Collections.Generic;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Thread-safe fixed-size pool of DuckDB connections for read queries.
    /// Mirrors the C++ ConnectionPool class.
    ///
    /// DuckDB supports many concurrent readers but only one writer at a time.
    /// This pool manages the reader connections with RAII-style handles.
    /// </summary>
    public sealed class ConnectionPool : IDisposable
    {
        private readonly Queue<DuckDBConnection> _idle;
        private readonly object _lock = new object();
        private readonly int _borrowTimeoutMs;
        private readonly string _connectionString;
        private bool _disposed;

        /// <summary>Total number of connections (idle + borrowed).</summary>
        public int Size { get; }

        /// <summary>
        /// Construct a pool of DuckDB read connections.
        /// </summary>
        /// <param name="connectionString">DuckDB connection string (e.g. "Data Source=:memory:" or "Data Source=C:\data\db.duckdb").</param>
        /// <param name="poolSize">Number of connections to create.</param>
        /// <param name="borrowTimeoutMs">Max milliseconds to wait for an idle connection.</param>
        public ConnectionPool(string connectionString, int poolSize, int borrowTimeoutMs = 10000)
        {
            if (poolSize < 1)
                throw new ArgumentOutOfRangeException(nameof(poolSize), poolSize,
                    "Pool size must be at least 1.");

            _connectionString = connectionString;
            _borrowTimeoutMs = borrowTimeoutMs;
            Size = poolSize;
            _idle = new Queue<DuckDBConnection>(poolSize);

            // Pre-create all connections. If any fails, dispose those already created.
            try
            {
                for (int i = 0; i < poolSize; i++)
                {
                    var conn = new DuckDBConnection(connectionString);
                    conn.Open();
                    _idle.Enqueue(conn);
                }
            }
            catch
            {
                lock (_lock)
                {
                    while (_idle.Count > 0)
                    {
                        try { _idle.Dequeue().Dispose(); }
                        catch { /* best-effort cleanup */ }
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Borrow an idle connection, blocking until one is available.
        /// </summary>
        /// <returns>A Handle that returns the connection on disposal.</returns>
        /// <exception cref="TimeoutException">No connection available within timeout.</exception>
        public Handle Borrow()
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(_borrowTimeoutMs);

            lock (_lock)
            {
                while (_idle.Count == 0)
                {
                    int remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0)
                        throw new TimeoutException(
                            string.Format(
                                "ConnectionPool: timed out waiting for an idle connection (pool_size={0}). Consider increasing --readers.",
                                Size));

                    Monitor.Wait(_lock, remaining);
                }

                var conn = _idle.Dequeue();
                return new Handle(this, conn);
            }
        }

        private void Release(DuckDBConnection conn)
        {
            if (conn == null) return;

            lock (_lock)
            {
                if (_disposed)
                {
                    conn.Dispose();
                    return;
                }
                _idle.Enqueue(conn);
                Monitor.Pulse(_lock);
            }
        }

        /// <summary>
        /// RAII wrapper that holds a borrowed connection.
        /// Returns the connection to the pool when disposed.
        /// </summary>
        public sealed class Handle : IDisposable
        {
            private readonly ConnectionPool _pool;
            private DuckDBConnection _conn;

            internal Handle(ConnectionPool pool, DuckDBConnection conn)
            {
                _pool = pool;
                _conn = conn;
            }

            /// <summary>The borrowed DuckDB connection. Valid for the Handle's lifetime.</summary>
            public DuckDBConnection Connection
            {
                get
                {
                    if (_conn == null)
                        throw new ObjectDisposedException(nameof(Handle));
                    return _conn;
                }
            }

            public void Dispose()
            {
                var conn = _conn;
                if (conn == null) return;
                _conn = null;
                _pool.Release(conn);
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;
                while (_idle.Count > 0)
                {
                    try { _idle.Dequeue().Dispose(); }
                    catch { /* best-effort */ }
                }
            }
        }
    }
}
