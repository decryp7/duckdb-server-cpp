using System;
using System.Collections.Generic;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Thread-safe fixed-size pool of DuckDB connections for read queries.
    ///
    /// How it works:
    ///   1. On creation, opens N connections and puts them in a queue.
    ///   2. When a caller needs a connection, it calls Borrow().
    ///   3. Borrow() waits until a connection is free, then gives it out.
    ///   4. The caller gets a Handle. When the Handle is disposed, the
    ///      connection goes back into the queue for the next caller.
    /// </summary>
    public sealed class ConnectionPool : IConnectionPool
    {
        private readonly Queue<DuckDBConnection> idle;
        private readonly object lockObj = new object();
        private readonly int borrowTimeoutMs;
        private bool disposed;

        /// <summary>Total number of connections (idle + borrowed).</summary>
        public int Size { get; }

        /// <summary>
        /// Create a pool of DuckDB read connections.
        /// </summary>
        /// <param name="connectionString">DuckDB connection string.</param>
        /// <param name="poolSize">How many connections to create.</param>
        /// <param name="borrowTimeoutMs">How long to wait before giving up (milliseconds).</param>
        public ConnectionPool(string connectionString, int poolSize, int borrowTimeoutMs = 10000)
        {
            if (poolSize < 1)
                throw new ArgumentOutOfRangeException(nameof(poolSize), "Must be at least 1.");

            this.borrowTimeoutMs = borrowTimeoutMs;
            Size = poolSize;
            idle = new Queue<DuckDBConnection>(poolSize);

            CreateConnections(connectionString, poolSize);
        }

        /// <summary>
        /// Borrow an idle connection. Blocks until one is available.
        /// </summary>
        public IConnectionHandle Borrow()
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(borrowTimeoutMs);

            lock (lockObj)
            {
                // Wait until a connection is free.
                while (idle.Count == 0)
                {
                    int remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0)
                        throw new TimeoutException(
                            "ConnectionPool: timed out waiting for a connection. " +
                            "Pool size is " + Size + ". Consider increasing --readers.");

                    Monitor.Wait(lockObj, remaining);
                }

                var conn = idle.Dequeue();
                return new Handle(this, conn);
            }
        }

        public void Dispose()
        {
            lock (lockObj)
            {
                if (disposed) return;
                disposed = true;
                while (idle.Count > 0)
                    SafeDispose(idle.Dequeue());
            }
        }

        // ── Private helpers ──────────────────────────────────────────────────

        private void CreateConnections(string connectionString, int count)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var conn = new DuckDBConnection(connectionString);
                    conn.Open();
                    idle.Enqueue(conn);
                }
            }
            catch
            {
                // If one fails, clean up the ones we already created.
                while (idle.Count > 0)
                    SafeDispose(idle.Dequeue());
                throw;
            }
        }

        private void ReturnConnection(DuckDBConnection conn)
        {
            if (conn == null) return;

            lock (lockObj)
            {
                if (disposed)
                {
                    conn.Dispose();
                    return;
                }
                idle.Enqueue(conn);
                Monitor.Pulse(lockObj); // Wake up one waiting caller.
            }
        }

        private static void SafeDispose(IDisposable obj)
        {
            try { obj?.Dispose(); }
            catch { /* best-effort cleanup */ }
        }

        // ── Handle (RAII wrapper) ────────────────────────────────────────────

        /// <summary>
        /// Holds a borrowed connection. When you dispose the handle,
        /// the connection goes back to the pool automatically.
        /// </summary>
        private sealed class Handle : IConnectionHandle
        {
            private readonly ConnectionPool pool;
            private DuckDBConnection conn;

            internal Handle(ConnectionPool pool, DuckDBConnection conn)
            {
                this.pool = pool;
                this.conn = conn;
            }

            public DuckDBConnection Connection
            {
                get
                {
                    if (conn == null)
                        throw new ObjectDisposedException(nameof(Handle));
                    return conn;
                }
            }

            public void Dispose()
            {
                var c = conn;
                if (c == null) return;
                conn = null;
                pool.ReturnConnection(c);
            }
        }
    }
}
