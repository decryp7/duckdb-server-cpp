using System;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDbServer
{
    /// <summary>
    /// Sharded DuckDB with a read-all / write-all replication strategy.
    ///
    /// <para><b>Thread Safety:</b> This class is fully thread-safe. The shard array is
    /// immutable after construction, and the round-robin counter uses
    /// <see cref="Interlocked.Increment(ref long)"/> for lock-free atomic updates.</para>
    ///
    /// <para><b>Read Path (round-robin):</b> Every shard holds a complete copy of all data.
    /// Read queries are distributed across shards using an atomic round-robin counter.
    /// With N shards, read throughput scales to approximately N times single-shard throughput,
    /// because each shard's connection pool and DuckDB instance operate independently.</para>
    ///
    /// <para><b>Write Path (fan-out):</b> Every write (DML or DDL) is submitted to ALL shards.
    /// This keeps data consistent across all shards. Write throughput is limited to single-shard
    /// speed (the slowest shard determines the commit latency), but this is acceptable because
    /// the primary bottleneck in analytics workloads is read throughput, not write throughput.</para>
    ///
    /// <para><b>Architecture Diagram:</b></para>
    /// <code>
    ///   Shard 0: [full data] &lt;-- reads (round-robin) + writes (fan-out)
    ///   Shard 1: [full data] &lt;-- reads (round-robin) + writes (fan-out)
    ///   Shard 2: [full data] &lt;-- reads (round-robin) + writes (fan-out)
    ///   Shard 3: [full data] &lt;-- reads (round-robin) + writes (fan-out)
    /// </code>
    ///
    /// <para><b>Database Paths:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     For <c>:memory:</c> databases, each shard creates an independent in-memory database.
    ///     Data consistency is maintained by executing the same writes on all shards.
    ///   </description></item>
    ///   <item><description>
    ///     For file-based databases, each shard gets its own file (e.g., <c>data_0.db</c>,
    ///     <c>data_1.db</c>). This avoids DuckDB's single-writer file lock contention.
    ///   </description></item>
    /// </list>
    /// </summary>
    public sealed class ShardedDuckDb : IDisposable
    {
        /// <summary>
        /// Fixed-size array of shards. Immutable after construction (the array reference
        /// and its contents never change).
        /// </summary>
        private readonly Shard[] shards;

        /// <summary>
        /// Atomic round-robin counter for read shard selection. Starts at -1 so the first
        /// <see cref="Interlocked.Increment(ref long)"/> yields 0 (first shard).
        /// This counter grows unboundedly but is masked with <c>0x7FFFFFFFFFFFFFFF</c>
        /// before modulo to prevent negative indices when it wraps past <see cref="long.MaxValue"/>.
        /// </summary>
        private long nextReadShard;

        /// <summary>Number of shards (always >= 1).</summary>
        public int ShardCount { get { return shards.Length; } }

        /// <summary>
        /// Total number of read connections across all shards. Equal to
        /// <c>readersPerShard * shardCount</c>. Reported in the stats endpoint.
        /// </summary>
        public int TotalPoolSize { get; }

        /// <summary>
        /// Constructs the sharded database, creating one <see cref="DatabaseManager"/>,
        /// one <see cref="ConnectionPool"/>, and one <see cref="WriteSerializer"/> per shard.
        /// </summary>
        /// <param name="config">
        /// Server configuration. Key fields:
        /// <list type="bullet">
        ///   <item><description><c>Shards</c>: Number of shards (clamped to >= 1).</description></item>
        ///   <item><description><c>ReaderPoolSize</c>: Total read connections, divided evenly across shards.</description></item>
        ///   <item><description><c>DbPath</c>: Base database path. Per-shard paths are derived via <see cref="GetShardPath"/>.</description></item>
        ///   <item><description><c>WriteBatchMs</c> / <c>WriteBatchMax</c>: Write batching parameters forwarded to each shard's <see cref="WriteSerializer"/>.</description></item>
        /// </list>
        /// </param>
        /// <remarks>
        /// If <c>config.ReaderPoolSize</c> is 8 and <c>config.Shards</c> is 4, each shard
        /// gets 2 read connections (8 / 4 = 2). The total pool size is recalculated as
        /// <c>readersPerShard * shardCount</c> to account for integer division truncation.
        /// </remarks>
        public ShardedDuckDb(ServerConfig config)
        {
            int shardCount = Math.Max(1, config.Shards);
            // Divide total readers evenly across shards. Integer division may drop remainder.
            int readersPerShard = Math.Max(1, config.ReaderPoolSize / shardCount);

            shards = new Shard[shardCount];
            nextReadShard = -1; // Pre-decrement so first Increment yields 0.

            for (int i = 0; i < shardCount; i++)
            {
                // Compute the database path for this shard (e.g., "data_0.db", "data_1.db").
                string dbPath = GetShardPath(config.DbPath, i, shardCount);

                // Each shard gets its own DatabaseManager (owns the primary DuckDB connection),
                // ConnectionPool (for read parallelism), and WriteSerializer (for batched writes).
                var dbManager = new DatabaseManager(dbPath, config, shardCount);
                var pool = new ConnectionPool(dbManager, readersPerShard);
                var writer = new WriteSerializer(dbManager, config.WriteBatchMs, config.WriteBatchMax);

                // Dedicated connection for BulkInsert Appender API (10-100x faster than SQL INSERT).
                // Separate from writer to avoid blocking the WriteSerializer's background thread.
                var bulkConn = dbManager.CreateConnection();

                shards[i] = new Shard
                {
                    DbManager = dbManager,
                    Pool = pool,
                    Writer = writer,
                    BulkConnection = bulkConn,
                };
            }

            TotalPoolSize = readersPerShard * shardCount;

            Console.WriteLine("[das] Sharded DuckDB (read-all / write-all):");
            Console.WriteLine("      shards   = {0}", shardCount);
            Console.WriteLine("      readers  = {0} per shard, {1} total", readersPerShard, TotalPoolSize);
            Console.WriteLine("      strategy = reads round-robin, writes fan-out to all");
        }

        /// <summary>
        /// Selects the next shard for a READ query using atomic round-robin.
        ///
        /// <para><b>Algorithm:</b> Atomically increments <see cref="nextReadShard"/> and
        /// takes the result modulo shard count. The bitmask <c>0x7FFFFFFFFFFFFFFF</c>
        /// clears the sign bit to prevent negative array indices when the counter
        /// overflows past <see cref="long.MaxValue"/>. This is a standard lock-free
        /// technique for wrapping counters.</para>
        ///
        /// <para><b>Why round-robin instead of random?</b> Round-robin distributes load
        /// perfectly evenly across shards, which maximizes cache locality within each
        /// shard's connection pool (same-thread ConcurrentBag affinity).</para>
        /// </summary>
        /// <returns>The next shard to use for reading. All shards hold identical data.</returns>
        public Shard NextForRead()
        {
            long idx = Interlocked.Increment(ref nextReadShard);
            // Mask sign bit to handle long overflow, then modulo for shard selection.
            return shards[(int)((idx & 0x7FFFFFFFFFFFFFFFL) % shards.Length)];
        }

        /// <summary>
        /// Executes a write (DML or DDL) on ALL shards to maintain data consistency.
        ///
        /// <para><b>Single-shard fast path:</b> When there is only one shard, the write
        /// is submitted directly without Task.Run overhead.</para>
        ///
        /// <para><b>Multi-shard fan-out:</b> Submits the write to all shards in parallel
        /// via <see cref="Task.Run"/>. Each shard's <see cref="IWriteSerializer.Submit"/>
        /// blocks until its transaction commits, so Task.WaitAll blocks until ALL shards
        /// have committed.</para>
        ///
        /// <para><b>Error handling:</b> If any shard fails, returns the first error
        /// encountered. Other shards may have committed successfully, leaving the system
        /// in an inconsistent state. This is a deliberate trade-off: full distributed
        /// transactions (2PC) would add significant complexity and latency. In practice,
        /// errors are typically SQL syntax errors that fail on all shards identically.</para>
        /// </summary>
        /// <param name="sql">The DML or DDL statement to execute on all shards.</param>
        /// <returns>
        /// <see cref="WriteResult"/> with <c>Ok = true</c> if all shards succeeded, or
        /// the first error encountered.
        /// </returns>
        public WriteResult WriteToAll(string sql)
        {
            if (shards.Length == 1)
            {
                // Fast path: single shard, no Task.Run overhead or closure allocation.
                return shards[0].Writer.Submit(sql);
            }

            // Fan-out: submit to all shards in parallel using the thread pool.
            var tasks = new Task<WriteResult>[shards.Length];
            for (int i = 0; i < shards.Length; i++)
            {
                int idx = i; // Capture loop variable for the closure (C# 7.3 gotcha).
                tasks[i] = Task.Run(() => shards[idx].Writer.Submit(sql));
            }

            // Wait for all shards to commit (or fail).
            Task.WaitAll(tasks);

            // Return first error, or success if all succeeded.
            for (int i = 0; i < tasks.Length; i++)
            {
                if (!tasks[i].Result.Ok)
                    return tasks[i].Result;
            }
            return tasks[0].Result;
        }

        /// <summary>
        /// Execute SQL directly on each shard's BulkConnection, bypassing WriteSerializer.
        /// Used for INSERT statements that can safely run concurrently — DuckDB guarantees
        /// concurrent appends don't conflict.
        ///
        /// <para><b>Thread Safety:</b> Each shard's BulkConnection is protected by its
        /// <see cref="Shard.BulkLock"/>. Multiple concurrent BulkExecuteAll calls will
        /// serialize per-shard but can execute across shards in parallel.</para>
        /// </summary>
        /// <param name="sql">The INSERT SQL statement to execute on all shards.</param>
        /// <returns>
        /// <see cref="WriteResult"/> with <c>Ok = true</c> if all shards succeeded, or
        /// the first error encountered.
        /// </returns>
        public WriteResult BulkExecuteAll(string sql)
        {
            for (int i = 0; i < shards.Length; i++)
            {
                var shard = shards[i];
                lock (shard.BulkLock)
                {
                    try
                    {
                        using (var cmd = shard.BulkConnection.CreateCommand())
                        {
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception ex)
                    {
                        return WriteResult.Failure(ex.Message);
                    }
                }
            }
            return WriteResult.Success();
        }

        /// <summary>
        /// Executes a write on ALL shards using pre-built SQL. This is a convenience
        /// alias for <see cref="WriteToAll"/> used by the BulkInsert handler where the
        /// SQL has already been constructed by <see cref="BulkInsertSqlBuilder"/>.
        /// </summary>
        /// <param name="sql">Pre-built INSERT SQL statement.</param>
        /// <returns>The result from <see cref="WriteToAll"/>.</returns>
        public WriteResult WriteToAllRaw(string sql)
        {
            return WriteToAll(sql);
        }

        /// <summary>
        /// Retrieves a specific shard by index. The index is taken modulo shard count,
        /// so out-of-range indices wrap around safely.
        /// </summary>
        /// <param name="index">Zero-based shard index (wraps via modulo).</param>
        /// <returns>The shard at the wrapped index.</returns>
        public Shard GetShard(int index)
        {
            return shards[index % shards.Length];
        }

        /// <summary>
        /// Disposes all shards in order. Each shard's Writer, Pool, and DbManager are
        /// disposed in that order (reverse of dependency: the writer thread must stop
        /// before the pool connections are closed, and pool connections must be closed
        /// before the database manager's primary connection).
        /// </summary>
        public void Dispose()
        {
            foreach (var shard in shards)
            {
                // Dispose in dependency order: writer first (stops background thread),
                // then bulk connection, then pool, then database manager.
                shard.Writer.Dispose();
                if (shard.BulkConnection != null)
                {
                    try { shard.BulkConnection.Dispose(); } catch { }
                }
                shard.Pool.Dispose();
                shard.DbManager.Dispose();
            }
        }

        /// <summary>
        /// Computes the database file path for a specific shard.
        ///
        /// <para><b>Naming convention:</b></para>
        /// <list type="bullet">
        ///   <item><description>Single shard: uses <paramref name="basePath"/> unchanged.</description></item>
        ///   <item><description>In-memory: always returns <c>":memory:"</c> (each shard gets its own in-memory DB).</description></item>
        ///   <item><description>File with extension: inserts shard index before the extension, e.g., <c>"data.db"</c> becomes <c>"data_0.db"</c>, <c>"data_1.db"</c>.</description></item>
        ///   <item><description>File without extension: appends shard index, e.g., <c>"mydb"</c> becomes <c>"mydb_0"</c>, <c>"mydb_1"</c>.</description></item>
        /// </list>
        /// </summary>
        /// <param name="basePath">The user-configured database path (may be <c>":memory:"</c> or a file path).</param>
        /// <param name="shardIndex">Zero-based shard index.</param>
        /// <param name="shardCount">Total number of shards.</param>
        /// <returns>The database path for this specific shard.</returns>
        private static string GetShardPath(string basePath, int shardIndex, int shardCount)
        {
            // Single shard: no need to modify the path.
            if (shardCount <= 1) return basePath;
            // In-memory databases: each shard creates a separate in-memory DB.
            if (string.IsNullOrEmpty(basePath) || basePath == ":memory:") return ":memory:";

            // Insert shard index before the file extension (if any).
            int dot = basePath.LastIndexOf('.');
            if (dot >= 0)
                return basePath.Substring(0, dot) + "_" + shardIndex + basePath.Substring(dot);
            return basePath + "_" + shardIndex;
        }

        /// <summary>
        /// Represents a single database shard with its own database manager, connection
        /// pool, and write serializer. Each shard operates independently and holds a
        /// full copy of all data.
        ///
        /// <para><b>Thread Safety:</b> The Shard itself is a plain data holder. Thread
        /// safety is provided by the contained <see cref="IConnectionPool"/> and
        /// <see cref="IWriteSerializer"/> implementations.</para>
        /// </summary>
        public sealed class Shard
        {
            /// <summary>Manages the DuckDB database lifecycle and creates connections for this shard.</summary>
            public DatabaseManager DbManager;

            /// <summary>Thread-safe connection pool for read queries on this shard.</summary>
            public IConnectionPool Pool;

            /// <summary>
            /// Dedicated connection for DuckDB Appender API (BulkInsert).
            /// Protected by BulkLock since DuckDB connections are not thread-safe.
            /// Separate from the writer connection to avoid blocking the WriteSerializer.
            /// DuckDB allows concurrent appends without conflicts.
            /// </summary>
            public DuckDB.NET.Data.DuckDBConnection BulkConnection;

            /// <summary>Lock protecting BulkConnection access. Only one BulkInsert at a time per shard.</summary>
            public readonly object BulkLock = new object();

            /// <summary>Write serializer that batches concurrent writes into transactions on this shard.</summary>
            public IWriteSerializer Writer;
        }
    }
}
