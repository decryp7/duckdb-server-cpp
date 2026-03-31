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
    ///   5. Dispose() waits for all borrowed connections to be returned
    ///      before destroying them (prevents use-after-dispose).
    /// </summary>
    public sealed class ConnectionPool : IConnectionPool
    {
        private readonly Queue<DuckDBConnection> idle;
        private readonly object lockObj = new object();
        private readonly int borrowTimeoutMs;
        private readonly int poolSize;
        private bool disposed;

        /// <summary>Total number of connections (idle + borrowed).</summary>
        public int Size { get { return poolSize; } }

        /// <summary>
        /// Create a pool of DuckDB read connections.
        /// </summary>
        public ConnectionPool(DatabaseManager dbManager, int poolSize, int borrowTimeoutMs = 10000)
        {
            if (poolSize < 1)
                throw new ArgumentOutOfRangeException(nameof(poolSize), "Must be at least 1.");

            this.borrowTimeoutMs = borrowTimeoutMs;
            this.poolSize = poolSize;
            idle = new Queue<DuckDBConnection>(poolSize);

            CreateConnections(dbManager, poolSize);
        }

        /// <summary>
        /// Borrow an idle connection. Blocks until one is available.
        /// </summary>
        public IConnectionHandle Borrow()
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(borrowTimeoutMs);

            lock (lockObj)
            {
                while (idle.Count == 0)
                {
                    if (disposed)
                        throw new ObjectDisposedException(nameof(ConnectionPool));

                    int remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0)
                        throw new TimeoutException(
                            "ConnectionPool: timed out waiting for a connection. " +
                            "Pool size is " + poolSize + ". Consider increasing --readers.");

                    Monitor.Wait(lockObj, remaining);
                }

                var conn = idle.Dequeue();
                return new Handle(this, conn);
            }
        }

        /// <summary>
        /// Wait for all borrowed connections to be returned, then dispose them.
        /// This prevents use-after-dispose when in-flight gRPC handlers still
        /// hold borrowed connections.
        /// </summary>
        public void Dispose()
        {
            lock (lockObj)
            {
                if (disposed) return;
                disposed = true;

                // Wake any threads waiting in Borrow() so they see disposed=true.
                Monitor.PulseAll(lockObj);

                // Wait up to 30 seconds total for all borrowed connections to return.
                // If a Handle is leaked (caller bug), we give up and dispose what we have.
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (idle.Count < poolSize)
                {
                    int remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0) break; // give up waiting for leaked handles
                    Monitor.Wait(lockObj, remaining);
                }

                // Dispose all connections we have (idle + any that were returned).
                while (idle.Count > 0)
                    SafeDispose(idle.Dequeue());
            }
        }

        // ── Private helpers ──────────────────────────────────────────────────

        private void CreateConnections(DatabaseManager dbManager, int count)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var conn = dbManager.CreateConnection();
                    idle.Enqueue(conn);
                }
            }
            catch
            {
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
                    // Pool is shutting down. Put back so Dispose() can count and clean up.
                    idle.Enqueue(conn);
                    Monitor.PulseAll(lockObj); // Wake Dispose() waiting for all connections.
                    return;
                }
                idle.Enqueue(conn);
                Monitor.Pulse(lockObj);
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
                var c = Interlocked.Exchange(ref conn, null);
                if (c == null) return;
                pool.ReturnConnection(c);
            }
        }
    }
}
