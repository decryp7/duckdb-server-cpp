using System;
using System.Collections.Concurrent;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckDbServer
{
    /// <summary>
    /// High-performance thread-safe connection pool for DuckDB read queries.
    ///
    /// Uses ConcurrentBag + SemaphoreSlim instead of Queue + Monitor
    /// for lower lock contention under high concurrency (100+ threads).
    ///
    /// ConcurrentBag is lock-free for same-thread take/add (thread-local lists).
    /// SemaphoreSlim is lighter than Monitor for signalling.
    /// </summary>
    public sealed class ConnectionPool : IConnectionPool
    {
        private readonly ConcurrentBag<DuckDBConnection> idle;
        private readonly SemaphoreSlim semaphore;
        private readonly int borrowTimeoutMs;
        private readonly int poolSize;
        private int disposed; // 0=alive, 1=disposed

        public int Size { get { return poolSize; } }

        public ConnectionPool(DatabaseManager dbManager, int poolSize, int borrowTimeoutMs = 10000)
        {
            if (poolSize < 1)
                throw new ArgumentOutOfRangeException(nameof(poolSize), "Must be at least 1.");

            this.borrowTimeoutMs = borrowTimeoutMs;
            this.poolSize = poolSize;
            idle = new ConcurrentBag<DuckDBConnection>();
            semaphore = new SemaphoreSlim(poolSize, poolSize);

            CreateConnections(dbManager, poolSize);
        }

        /// <summary>
        /// Borrow a connection. Blocks up to borrowTimeoutMs if all are busy.
        /// Uses SemaphoreSlim.Wait instead of Monitor.Wait for less contention.
        /// </summary>
        public IConnectionHandle Borrow()
        {
            if (Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(ConnectionPool));

            if (!semaphore.Wait(borrowTimeoutMs))
                throw new TimeoutException(
                    "ConnectionPool: timed out waiting for a connection. " +
                    "Pool size is " + poolSize + ". Consider increasing --readers.");

            // Semaphore acquired — a connection must be available in the bag.
            DuckDBConnection conn;
            if (!idle.TryTake(out conn))
            {
                semaphore.Release();
                throw new InvalidOperationException("ConnectionPool: internal inconsistency.");
            }

            return new Handle(this, conn);
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;

            // Wait up to 30 seconds for all borrowed connections to return.
            int returned = 0;
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (returned < poolSize)
            {
                DuckDBConnection conn;
                if (idle.TryTake(out conn))
                {
                    SafeDispose(conn);
                    returned++;
                }
                else if (DateTime.UtcNow > deadline)
                {
                    break; // give up waiting for leaked handles
                }
                else
                {
                    Thread.Sleep(50); // brief wait for handles to be returned
                }
            }

            semaphore.Dispose();
        }

        private void CreateConnections(DatabaseManager dbManager, int count)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var conn = dbManager.CreateConnection();
                    // Apply performance PRAGMAs on EVERY connection (not just primary).
                    // DuckDB settings are per-connection, not inherited from cache=shared.
                    ApplyConnectionPragmas(conn);
                    idle.Add(conn);
                }
            }
            catch
            {
                DuckDBConnection conn;
                while (idle.TryTake(out conn))
                    SafeDispose(conn);
                throw;
            }
        }

        /// <summary>
        /// Apply performance settings on each pool connection.
        /// These are per-connection settings that improve query throughput:
        ///   - threads=1: eliminates internal thread contention (pool provides parallelism)
        ///   - preserve_insertion_order=false: 1.5-3x faster scans without ORDER BY
        ///   - enable_object_cache: caches Parquet/table metadata
        /// </summary>
        private static void ApplyConnectionPragmas(DuckDBConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SET threads=1";
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SET preserve_insertion_order=false";
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "PRAGMA enable_object_cache";
                    cmd.ExecuteNonQuery();
                }
            }
            catch { /* best-effort — PRAGMAs are optimization hints */ }
        }

        private void ReturnConnection(DuckDBConnection conn)
        {
            if (conn == null) return;

            if (Thread.VolatileRead(ref disposed) != 0)
            {
                SafeDispose(conn);
                return;
            }

            idle.Add(conn);
            semaphore.Release();
        }

        private static void SafeDispose(IDisposable obj)
        {
            try { obj?.Dispose(); }
            catch { /* best-effort */ }
        }

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
