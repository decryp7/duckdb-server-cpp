using System;
using DuckDB.NET.Data;

namespace DuckDbServer
{
    /// <summary>
    /// Manages the DuckDB database lifecycle, applies performance PRAGMAs, and
    /// creates new connections that share the same underlying database.
    ///
    /// <para><b>Thread Safety:</b> The <see cref="CreateConnection"/> method is thread-safe
    /// (guarded by a volatile disposal check). The primary connection is used only during
    /// construction for applying PRAGMAs and is not accessed concurrently.</para>
    ///
    /// <para><b>Why this class exists:</b></para>
    /// <para>In DuckDB.NET, each <c>new DuckDBConnection("Data Source=:memory:")</c>
    /// creates a <b>separate</b> in-memory database. Without special handling, readers
    /// and the writer would have completely independent databases with no shared tables
    /// or data.</para>
    ///
    /// <para><b>In-memory mode (<c>cache=shared</c>):</b></para>
    /// <para>For in-memory databases, this class uses the connection string
    /// <c>"Data Source=:memory:?cache=shared"</c>. The <c>cache=shared</c> parameter
    /// tells DuckDB.NET's internal <c>ConnectionCache</c> to reuse the same underlying
    /// DuckDB database handle for all connections with the same connection string. This
    /// means all pool connections and the writer connection share the SAME in-memory
    /// database, enabling readers to see tables and data created by the writer.</para>
    ///
    /// <para><b>File-based mode:</b></para>
    /// <para>For file-based databases, all connections naturally point to the same file,
    /// so no special handling is needed. The connection string is simply
    /// <c>"Data Source=/path/to/db.duckdb"</c>.</para>
    ///
    /// <para><b>Primary connection:</b></para>
    /// <para>The primary connection is opened during construction and kept alive for the
    /// lifetime of this manager. For <c>cache=shared</c> mode, this connection acts as
    /// the anchor that keeps the shared database alive. If all connections were closed,
    /// the in-memory database would be destroyed. The primary connection prevents this.</para>
    /// </summary>
    public sealed class DatabaseManager : IDisposable
    {
        /// <summary>
        /// The primary (anchor) connection. Kept open for the lifetime of this manager.
        /// For <c>cache=shared</c> in-memory mode, this connection prevents the shared
        /// database from being destroyed when all pool connections are temporarily idle.
        /// Also used during construction to apply database-level PRAGMAs.
        /// </summary>
        private readonly DuckDBConnection primaryConnection;

        /// <summary>
        /// The connection string used to create all connections. All connections using the
        /// same string share the same underlying database (via DuckDB.NET's ConnectionCache
        /// for in-memory mode, or via the file system for file-based mode).
        /// </summary>
        private readonly string connectionString;

        /// <summary>
        /// Disposal guard. 0 = alive, 1 = disposed. Uses <see cref="System.Threading.Interlocked"/>
        /// for thread-safe compare-and-swap in <see cref="Dispose"/>.
        /// </summary>
        private int disposed;

        /// <summary>
        /// Opens the database, applies performance settings, and prepares for connection
        /// creation.
        /// </summary>
        /// <param name="dbPath">
        /// The database path. Use <c>":memory:"</c> or empty string for an in-memory
        /// database (will use <c>cache=shared</c>). Use a file path for persistent storage.
        /// </param>
        /// <param name="config">
        /// Server configuration providing memory limit, thread count, and other DuckDB
        /// tuning parameters.
        /// </param>
        /// <remarks>
        /// The primary connection is opened synchronously during construction. If the
        /// database file does not exist, DuckDB creates it. If the file is locked by
        /// another process, the constructor throws.
        /// </remarks>
        public DatabaseManager(string dbPath, ServerConfig config, int shardCount = 1)
        {
            connectionString = BuildConnectionString(dbPath);

            // Open the primary connection first. For cache=shared mode, this creates
            // the shared in-memory database. For file mode, this opens/creates the file.
            primaryConnection = new DuckDBConnection(connectionString);
            primaryConnection.Open();

            // Apply database-level performance settings via the primary connection.
            ApplyPerformanceSettings(config, shardCount);
        }

        /// <summary>
        /// Creates a new connection to the SAME database that the primary connection uses.
        ///
        /// <para><b>For in-memory mode:</b> The new connection shares the same database via
        /// <c>cache=shared</c>. Tables, data, and metadata are visible across all connections.</para>
        ///
        /// <para><b>For file mode:</b> The new connection opens the same database file.
        /// DuckDB handles concurrent access internally.</para>
        ///
        /// <para><b>Ownership:</b> The caller owns the returned connection and is responsible
        /// for disposing it. This class does NOT track created connections.</para>
        /// </summary>
        /// <returns>A new, opened <see cref="DuckDBConnection"/> to the shared database.</returns>
        /// <exception cref="ObjectDisposedException">If this manager has been disposed.</exception>
        public DuckDBConnection CreateConnection()
        {
            if (System.Threading.Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(DatabaseManager));

            var conn = new DuckDBConnection(connectionString);
            conn.Open();
            return conn;
        }

        /// <summary>
        /// Disposes the primary connection. Idempotent -- safe to call multiple times.
        ///
        /// <para><b>Gotcha:</b> For <c>cache=shared</c> in-memory mode, disposing the
        /// primary connection while other connections are still open is safe -- the shared
        /// database stays alive as long as at least one connection exists. However, the
        /// caller should ensure all pool connections and the writer connection are disposed
        /// BEFORE this manager to maintain a clean shutdown order.</para>
        /// </summary>
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            primaryConnection.Dispose();
        }

        // ── Private helpers ──────────────────────────────────────────────────

        /// <summary>
        /// Applies database-level DuckDB performance settings via SET/PRAGMA commands
        /// on the primary connection.
        ///
        /// <para><b>Note:</b> Some settings (like <c>memory_limit</c> and
        /// <c>checkpoint_threshold</c>) are database-wide and affect all connections.
        /// Others (like <c>threads</c>) are per-connection; they are applied here for
        /// the primary connection and separately in <see cref="ConnectionPool"/> for
        /// pool connections.</para>
        /// </summary>
        /// <param name="config">Server configuration with tuning parameters.</param>
        private void ApplyPerformanceSettings(ServerConfig config, int shardCount)
        {
            // Auto-calculate per-shard memory limit when not explicitly set and running
            // multiple shards. Divides 80% of system RAM evenly across shards.
            if (string.IsNullOrEmpty(config.MemoryLimit) && shardCount > 1)
            {
                var autoLimit = "'" + (80 / shardCount) + "%'";
                ExecutePragma("SET memory_limit=" + autoLimit);
            }

            // Memory limit: controls the maximum RAM DuckDB uses for buffer management
            // and query processing. When exceeded, DuckDB spills to disk (if file-based)
            // or returns an error (if in-memory). Default: 80% of system RAM.
            // Recommended: set explicitly in production to prevent DuckDB from competing
            // with the .NET GC and gRPC buffers for memory.
            if (!string.IsNullOrEmpty(config.MemoryLimit))
                ExecutePragma("SET memory_limit='" + config.MemoryLimit + "'");

            // Thread count per connection. For server workloads with many concurrent
            // connections, setting threads=1 per connection eliminates DuckDB's internal
            // thread pool contention. The connection pool provides parallelism instead
            // (N connections = N parallel queries). Without this, DuckDB would create
            // nCPU threads per connection, leading to severe over-subscription.
            // Always threads=1 on the primary connection. The connection pool and
            // write serializer both apply threads=1 on their own connections too.
            // Pool-level parallelism (N connections) replaces DuckDB's internal threading.
            if (config.DuckDbThreads > 0)
                ExecutePragma("SET threads=" + config.DuckDbThreads);
            else
                ExecutePragma("SET threads=1");

            // Object cache: caches Parquet file metadata, table schema information,
            // and other metadata objects. Reduces I/O for repeated queries against
            // the same tables. Negligible memory overhead.
            ExecutePragma("PRAGMA enable_object_cache");

            // Disable insertion order preservation. By default, DuckDB preserves the
            // order rows were inserted, which adds overhead to both writes (maintaining
            // row IDs) and reads (scanning in insertion order instead of storage order).
            // Analytics queries rarely need insertion order and benefit from 1.5-3x
            // faster table scans when this is disabled.
            ExecutePragma("SET preserve_insertion_order=false");

            // Increase checkpoint threshold from the default 16MB to 256MB.
            // Checkpoints flush the WAL (write-ahead log) to the main database file.
            // Frequent checkpoints during write-heavy workloads cause significant I/O
            // overhead. A 256MB threshold reduces checkpoint frequency by ~16x, yielding
            // 2-5x faster sustained write throughput. The trade-off is increased recovery
            // time after a crash (more WAL to replay).
            ExecutePragma("SET checkpoint_threshold='256MB'");

            // Disable the progress bar. In server mode, there is no terminal to display
            // progress, and the progress tracking adds CPU overhead to long-running queries.
            ExecutePragma("PRAGMA enable_progress_bar=false");

            // Late materialization: limits the number of rows materialized before filtering,
            // improving performance for selective queries on wide tables.

            // Allocator flush threshold: controls when DuckDB returns unused memory to the OS.
            // A 128MB threshold prevents excessive memory retention while avoiding frequent
            // allocation/deallocation overhead.
            ExecutePragma("SET allocator_flush_threshold='128MB'");

            // Temp directory: configurable spill-to-disk location for queries that exceed
            // the memory limit.
            if (!string.IsNullOrEmpty(config.TempDirectory))
                ExecutePragma("SET temp_directory='" + config.TempDirectory + "'");
        }

        /// <summary>
        /// Executes a PRAGMA or SET command on the primary connection. Logs warnings
        /// on failure but does not throw, because PRAGMAs are optimization hints that
        /// should not prevent the server from starting.
        /// </summary>
        /// <param name="sql">The PRAGMA or SET SQL command to execute.</param>
        private void ExecutePragma(string sql)
        {
            try
            {
                using (var cmd = primaryConnection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                // Log but don't fail -- PRAGMAs are optimization hints. An older DuckDB
                // version may not support all PRAGMAs; the server should still start.
                Console.Error.WriteLine("[das] Warning: " + sql + " failed: " + ex.Message);
            }
        }

        /// <summary>
        /// Builds the ADO.NET connection string for DuckDB.
        ///
        /// <para><b>In-memory (<c>cache=shared</c>):</b> Appends <c>?cache=shared</c> to
        /// the <c>:memory:</c> data source. This is a DuckDB.NET-specific feature that
        /// routes all connections with the same string to the same underlying DuckDB
        /// database handle via an internal connection cache. Without this, each connection
        /// would create an independent in-memory database.</para>
        ///
        /// <para><b>File-based:</b> Simply sets <c>Data Source=</c> to the file path.
        /// DuckDB handles file locking and concurrent access internally.</para>
        /// </summary>
        /// <param name="dbPath">The user-configured database path.</param>
        /// <returns>A connection string suitable for <see cref="DuckDBConnection"/>.</returns>
        private static string BuildConnectionString(string dbPath)
        {
            if (string.IsNullOrEmpty(dbPath) || dbPath == ":memory:")
                return "Data Source=:memory:?cache=shared";

            return "Data Source=" + dbPath;
        }
    }
}
