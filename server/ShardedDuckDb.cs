using System;
using System.Threading;

namespace DuckDbServer
{
    /// <summary>
    /// Sharded DuckDB: multiple independent database instances with round-robin dispatch.
    ///
    /// Why sharding helps:
    ///   Each DuckDB instance has its own internal lock for writes and its own
    ///   thread pool. With a single instance, 100 concurrent queries compete
    ///   for one lock. With 4 shards, each shard handles ~25 queries — 4x less
    ///   contention, 4x more effective parallelism.
    ///
    /// Architecture:
    ///   Shard 0: DatabaseManager + ConnectionPool + WriteSerializer
    ///   Shard 1: DatabaseManager + ConnectionPool + WriteSerializer
    ///   ...
    ///   Shard N: DatabaseManager + ConnectionPool + WriteSerializer
    ///
    /// Queries are dispatched round-robin (atomic counter mod shard count).
    /// For :memory: databases, each shard is fully independent.
    /// For file databases, each shard uses a separate .duckdb file:
    ///   data.duckdb → data_0.duckdb, data_1.duckdb, data_2.duckdb, ...
    /// </summary>
    public sealed class ShardedDuckDb : IDisposable
    {
        private readonly Shard[] shards;
        private long nextShard; // atomic round-robin counter

        public int ShardCount { get { return shards.Length; } }
        public int TotalPoolSize { get; }

        public ShardedDuckDb(ServerConfig config)
        {
            int shardCount = Math.Max(1, config.Shards);
            int readersPerShard = Math.Max(1, config.ReaderPoolSize / shardCount);

            shards = new Shard[shardCount];
            nextShard = 0;

            for (int i = 0; i < shardCount; i++)
            {
                string dbPath = GetShardPath(config.DbPath, i, shardCount);

                var dbManager = new DatabaseManager(dbPath, config);
                var pool = new ConnectionPool(dbManager, readersPerShard);
                var writer = new WriteSerializer(dbManager, config.WriteBatchMs, config.WriteBatchMax);

                shards[i] = new Shard
                {
                    DbManager = dbManager,
                    Pool = pool,
                    Writer = writer,
                };
            }

            TotalPoolSize = readersPerShard * shardCount;

            Console.WriteLine("[das] Sharded DuckDB: {0} shards × {1} readers = {2} total connections",
                shardCount, readersPerShard, TotalPoolSize);
        }

        /// <summary>
        /// Get the next shard (round-robin).
        /// Uses atomic increment for lock-free dispatch.
        /// </summary>
        public Shard Next()
        {
            long idx = Interlocked.Increment(ref nextShard);
            return shards[(int)(idx % shards.Length)];
        }

        /// <summary>
        /// Get a specific shard by index (for targeted operations).
        /// </summary>
        public Shard GetShard(int index)
        {
            return shards[index % shards.Length];
        }

        public void Dispose()
        {
            foreach (var shard in shards)
            {
                shard.Writer.Dispose();
                shard.Pool.Dispose();
                shard.DbManager.Dispose();
            }
        }

        /// <summary>
        /// Generate shard-specific database path.
        /// :memory: → :memory: (each shard is independent in-memory DB)
        /// data.duckdb → data_0.duckdb, data_1.duckdb, etc.
        /// </summary>
        private static string GetShardPath(string basePath, int shardIndex, int shardCount)
        {
            if (shardCount <= 1) return basePath;
            if (string.IsNullOrEmpty(basePath) || basePath == ":memory:") return ":memory:";

            // data.duckdb → data_0.duckdb
            int dot = basePath.LastIndexOf('.');
            if (dot >= 0)
                return basePath.Substring(0, dot) + "_" + shardIndex + basePath.Substring(dot);
            return basePath + "_" + shardIndex;
        }

        public sealed class Shard
        {
            public DatabaseManager DbManager;
            public IConnectionPool Pool;
            public IWriteSerializer Writer;
        }
    }
}
