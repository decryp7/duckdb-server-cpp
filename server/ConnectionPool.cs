using System;
using System.Collections.Concurrent;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckDbServer
{
    /// <summary>
    /// High-performance thread-safe connection pool for DuckDB read queries.
    ///
    /// <para><b>Thread Safety:</b> This class is fully thread-safe. All operations
    /// use lock-free or low-contention synchronization primitives.</para>
    ///
    /// <para><b>Design Rationale -- ConcurrentBag + SemaphoreSlim:</b></para>
    /// <para>A naive pool implementation uses <c>Queue&lt;T&gt;</c> + <c>Monitor</c>,
    /// but this creates a single contention point under high concurrency (100+ threads).
    /// This pool instead uses:</para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="ConcurrentBag{T}"/>: Provides lock-free Take/Add for same-thread
    ///     operations via thread-local lists. When a thread borrows and returns a connection,
    ///     it often gets the same connection back from its thread-local list, which improves
    ///     CPU cache locality. Cross-thread steals use lightweight spinlocks.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="SemaphoreSlim"/>: Acts as a counting gate that limits concurrent
    ///     borrows to <c>poolSize</c>. Lighter than <c>Monitor</c> because it does not
    ///     require entering/leaving a critical section -- it only signals availability.
    ///     Uses kernel-mode wait only when contended; uncontended Wait/Release are
    ///     pure user-mode spinlock operations.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>RAII Pattern:</b> Borrowing returns an <see cref="IConnectionHandle"/> that
    /// implements <see cref="IDisposable"/>. Disposing the handle returns the connection to
    /// the pool. This ensures connections are always returned, even when exceptions occur,
    /// as long as the caller uses a <c>using</c> block.</para>
    ///
    /// <para><b>Invariant:</b> The semaphore count + number of borrowed handles always equals
    /// <c>poolSize</c>. The ConcurrentBag count equals the semaphore count (every available
    /// semaphore slot has a corresponding idle connection in the bag).</para>
    /// </summary>
    public sealed class ConnectionPool : IConnectionPool
    {
        /// <summary>
        /// Bag of idle (available) connections. Lock-free for same-thread operations.
        /// The bag size always matches the semaphore's current count.
        /// </summary>
        private readonly ConcurrentBag<DuckDBConnection> idle;

        /// <summary>
        /// Counting semaphore that limits concurrent borrows to <see cref="poolSize"/>.
        /// Initialized with <c>count = poolSize</c>. Each Borrow decrements it; each
        /// Return increments it. Callers block on Wait when all connections are borrowed.
        /// </summary>
        private readonly SemaphoreSlim semaphore;

        /// <summary>
        /// Maximum time in milliseconds to wait for a connection before throwing
        /// <see cref="TimeoutException"/>. Default is 10 seconds. Under sustained load,
        /// timeouts indicate the pool is too small (increase <c>--readers</c>).
        /// </summary>
        private readonly int borrowTimeoutMs;

        /// <summary>Total number of connections in the pool (idle + borrowed).</summary>
        private readonly int poolSize;

        /// <summary>
        /// Disposal guard. 0 = alive, 1 = disposed. Checked via
        /// <see cref="Thread.VolatileRead"/> to ensure visibility across threads without
        /// a full memory barrier.
        /// </summary>
        private int disposed;

        /// <summary>Total number of connections in the pool (idle + borrowed).</summary>
        public int Size { get { return poolSize; } }

        /// <summary>
        /// Creates a new connection pool with <paramref name="poolSize"/> pre-opened connections.
        /// All connections are created and opened eagerly during construction (fail-fast).
        /// </summary>
        /// <param name="dbManager">
        /// The database manager used to create connections. All connections share the same
        /// underlying DuckDB database via <c>cache=shared</c> (for in-memory) or file path.
        /// </param>
        /// <param name="poolSize">
        /// Number of connections to create. Must be >= 1. Each connection consumes a DuckDB
        /// thread and some memory for query processing buffers.
        /// </param>
        /// <param name="borrowTimeoutMs">
        /// Maximum time to wait for an available connection in <see cref="Borrow"/>.
        /// Default is 10,000 ms (10 seconds).
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">If <paramref name="poolSize"/> is less than 1.</exception>
        public ConnectionPool(DatabaseManager dbManager, int poolSize, int borrowTimeoutMs = 10000)
        {
            if (poolSize < 1)
                throw new ArgumentOutOfRangeException(nameof(poolSize), "Must be at least 1.");

            this.borrowTimeoutMs = borrowTimeoutMs;
            this.poolSize = poolSize;
            idle = new ConcurrentBag<DuckDBConnection>();
            semaphore = new SemaphoreSlim(poolSize, poolSize);

            // Eagerly create all connections. If any creation fails, all successfully
            // created connections are disposed before re-throwing the exception.
            CreateConnections(dbManager, poolSize);
        }

        /// <summary>
        /// Borrows a connection from the pool. Blocks up to <see cref="borrowTimeoutMs"/>
        /// if all connections are currently borrowed by other threads.
        ///
        /// <para><b>Flow:</b></para>
        /// <list type="number">
        ///   <item><description>Check disposal flag (volatile read, no barrier).</description></item>
        ///   <item><description>Wait on the semaphore. If the pool has an available slot, this
        ///     returns immediately (user-mode fast path). Otherwise, the calling thread blocks
        ///     until a connection is returned or the timeout expires.</description></item>
        ///   <item><description>Take a connection from the ConcurrentBag. This should always
        ///     succeed because the semaphore guarantees a connection is available. If it fails,
        ///     it indicates an internal invariant violation.</description></item>
        ///   <item><description>Return a <see cref="Handle"/> that wraps the connection.
        ///     When the handle is disposed, the connection is returned to the pool.</description></item>
        /// </list>
        /// </summary>
        /// <returns>
        /// An <see cref="IConnectionHandle"/> wrapping the borrowed connection. The caller
        /// MUST dispose this handle (preferably via a <c>using</c> block) to return the
        /// connection to the pool. Failure to dispose leaks the connection permanently.
        /// </returns>
        /// <exception cref="ObjectDisposedException">If the pool has been disposed.</exception>
        /// <exception cref="TimeoutException">
        /// If no connection becomes available within <see cref="borrowTimeoutMs"/>. This
        /// typically means the pool is undersized for the workload; increase <c>--readers</c>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// If the semaphore and bag are out of sync (should never happen in correct usage).
        /// </exception>
        public IConnectionHandle Borrow()
        {
            if (Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(ConnectionPool));

            // Block until a semaphore slot is available (connection returned to pool)
            // or the timeout expires. SemaphoreSlim.Wait is preferable to Monitor.Wait
            // because it avoids the lock acquisition overhead.
            if (!semaphore.Wait(borrowTimeoutMs))
                throw new TimeoutException(
                    "ConnectionPool: timed out waiting for a connection. " +
                    "Pool size is " + poolSize + ". Consider increasing --readers.");

            // Semaphore acquired -- a connection MUST be available in the bag.
            // The invariant is: semaphore count == bag count at all times.
            DuckDBConnection conn;
            if (!idle.TryTake(out conn))
            {
                // Should never happen. Release the semaphore to avoid permanent leak.
                semaphore.Release();
                throw new InvalidOperationException("ConnectionPool: internal inconsistency.");
            }

            return new Handle(this, conn);
        }

        /// <summary>
        /// Disposes the pool by closing all connections. Waits up to 30 seconds for
        /// borrowed connections to be returned via their handles.
        ///
        /// <para><b>Gotcha:</b> If a handle is never disposed (leaked), the pool will
        /// wait up to the deadline and then abandon the leaked connections. The semaphore
        /// is still disposed, so any late handle disposal will silently discard the
        /// connection via <see cref="ReturnConnection"/>'s disposed check.</para>
        /// </summary>
        public void Dispose()
        {
            // Idempotent: only the first caller performs disposal.
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;

            // Wait up to 30 seconds for all borrowed connections to be returned.
            // As handles are disposed, they add connections back to the bag.
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
                    break; // Give up waiting for leaked handles after 30 seconds.
                }
                else
                {
                    Thread.Sleep(50); // Brief wait for handles to be returned by other threads.
                }
            }

            semaphore.Dispose();
        }

        /// <summary>
        /// Creates <paramref name="count"/> connections and adds them to the idle bag.
        /// Each connection has per-connection performance PRAGMAs applied.
        ///
        /// <para><b>Error handling:</b> If any connection fails to open, all previously
        /// created connections are disposed before the exception propagates. This prevents
        /// connection leaks during partial construction.</para>
        /// </summary>
        /// <param name="dbManager">Database manager for creating connections.</param>
        /// <param name="count">Number of connections to create.</param>
        private void CreateConnections(DatabaseManager dbManager, int count)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var conn = dbManager.CreateConnection();
                    // Apply performance PRAGMAs on EVERY connection (not just primary).
                    // DuckDB settings are per-connection, not inherited from cache=shared.
                    // Without this, each pool connection would use DuckDB's default settings
                    // (e.g., threads=nCPU), causing excessive thread contention.
                    ApplyConnectionPragmas(conn);
                    idle.Add(conn);
                }
            }
            catch
            {
                // Clean up any connections that were successfully created before the failure.
                DuckDBConnection conn;
                while (idle.TryTake(out conn))
                    SafeDispose(conn);
                throw;
            }
        }

        /// <summary>
        /// Applies per-connection performance settings via DuckDB SET/PRAGMA commands.
        ///
        /// <para><b>Why per-connection?</b> In DuckDB, SET commands are scoped to the
        /// connection, not the database. Even with <c>cache=shared</c> (in-memory mode),
        /// each connection needs its own settings.</para>
        ///
        /// <para><b>Settings applied:</b></para>
        /// <list type="bullet">
        ///   <item><description>
        ///     <c>SET threads=1</c>: Each pool connection uses a single DuckDB internal
        ///     thread. Parallelism comes from the pool itself (N connections = N concurrent
        ///     queries). This eliminates internal thread contention within DuckDB and avoids
        ///     over-subscription when many queries run concurrently.
        ///   </description></item>
        ///   <item><description>
        ///     <c>SET preserve_insertion_order=false</c>: Allows DuckDB to scan rows in
        ///     storage order rather than insertion order, yielding 1.5-3x faster table scans
        ///     for queries without ORDER BY. Analytics queries rarely need insertion order.
        ///   </description></item>
        ///   <item><description>
        ///     <c>PRAGMA enable_object_cache</c>: Caches Parquet metadata and table schema
        ///     information, reducing repeated I/O for metadata lookups.
        ///   </description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> Failures are silently caught. These are optimization hints,
        /// not correctness requirements. If a PRAGMA fails (e.g., unsupported in an older
        /// DuckDB version), the connection still works with default settings.</para>
        /// </summary>
        /// <param name="conn">The newly created connection to configure.</param>
        private static void ApplyConnectionPragmas(DuckDBConnection conn)
        {
            // Each PRAGMA is wrapped individually so failure of one doesn't skip the rest.
            TryPragma(conn, "SET threads=1");
            TryPragma(conn, "SET preserve_insertion_order=false");
            TryPragma(conn, "PRAGMA enable_object_cache");
        }

        private static void TryPragma(DuckDBConnection conn, string sql)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
            }
            catch { /* best-effort — PRAGMAs are optimization hints, not requirements */ }
        }

        /// <summary>
        /// Returns a borrowed connection to the pool. Called by <see cref="Handle.Dispose"/>.
        ///
        /// <para><b>Disposed pool check:</b> If the pool has been disposed while a handle
        /// was outstanding, the connection is closed immediately instead of being returned
        /// to the bag. This prevents use-after-dispose scenarios where a returned connection
        /// would sit in the bag with no one to ever close it.</para>
        /// </summary>
        /// <param name="conn">The connection to return. May be null (no-op).</param>
        private void ReturnConnection(DuckDBConnection conn)
        {
            if (conn == null) return;

            // If the pool was disposed while this connection was borrowed, close it
            // immediately instead of returning it to the bag.
            if (Thread.VolatileRead(ref disposed) != 0)
            {
                SafeDispose(conn);
                return;
            }

            // Return the connection to the bag, then release the semaphore to unblock
            // any thread waiting in Borrow(). Order matters: Add before Release ensures
            // the connection is in the bag when the semaphore signals availability.
            idle.Add(conn);
            semaphore.Release();
        }

        /// <summary>
        /// Disposes an object, swallowing any exceptions. Used during cleanup when
        /// multiple disposals must complete even if some fail.
        /// </summary>
        /// <param name="obj">The object to dispose (may be null).</param>
        private static void SafeDispose(IDisposable obj)
        {
            try { obj?.Dispose(); }
            catch { /* best-effort -- disposal should not throw, but some providers do */ }
        }

        /// <summary>
        /// RAII handle that wraps a borrowed connection. Disposing the handle returns
        /// the connection to the pool.
        ///
        /// <para><b>Thread Safety:</b> The <see cref="Dispose"/> method uses
        /// <see cref="Interlocked.Exchange"/> to ensure the connection is returned
        /// exactly once, even if Dispose is called concurrently from multiple threads
        /// (which should not happen in normal usage but is safe regardless).</para>
        ///
        /// <para><b>Gotcha:</b> Accessing <see cref="Connection"/> after disposal throws
        /// <see cref="ObjectDisposedException"/>. The underlying DuckDB connection is
        /// NOT closed -- it is returned to the pool for reuse.</para>
        /// </summary>
        private sealed class Handle : IConnectionHandle
        {
            /// <summary>Reference to the owning pool, used to return the connection on disposal.</summary>
            private readonly ConnectionPool pool;

            /// <summary>
            /// The borrowed connection. Set to null atomically on disposal to prevent
            /// double-return. The null check in <see cref="Connection"/> enforces
            /// use-after-dispose detection.
            /// </summary>
            private DuckDBConnection conn;

            internal Handle(ConnectionPool pool, DuckDBConnection conn)
            {
                this.pool = pool;
                this.conn = conn;
            }

            /// <summary>
            /// The underlying DuckDB connection. Valid only while this handle has not
            /// been disposed.
            /// </summary>
            /// <exception cref="ObjectDisposedException">If the handle has been disposed.</exception>
            public DuckDBConnection Connection
            {
                get
                {
                    if (conn == null)
                        throw new ObjectDisposedException(nameof(Handle));
                    return conn;
                }
            }

            /// <summary>
            /// Returns the connection to the pool. Idempotent -- safe to call multiple times.
            /// Uses <see cref="Interlocked.Exchange"/> to atomically null out the connection
            /// reference, ensuring exactly one return.
            /// </summary>
            public void Dispose()
            {
                // Atomically take ownership of the connection reference.
                var c = Interlocked.Exchange(ref conn, null);
                if (c == null) return; // Already disposed.
                pool.ReturnConnection(c);
            }
        }
    }
}
