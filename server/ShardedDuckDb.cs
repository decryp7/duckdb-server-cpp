using System;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDbServer
{
    /// <summary>
    /// Sharded DuckDB with read-all / write-one strategy.
    ///
    /// READ:  Round-robin across all shards. Each shard has a full data copy.
    ///        N shards = N× read throughput.
    ///
    /// WRITE: Fan-out to ALL shards. Every shard executes the same write.
    ///        Data stays consistent across all shards.
    ///        Write throughput = single shard speed (but reads scale).
    ///
    /// Architecture:
    ///   Shard 0: [full data] ← reads (round-robin) + writes (fan-out)
    ///   Shard 1: [full data] ← reads (round-robin) + writes (fan-out)
    ///   Shard 2: [full data] ← reads (round-robin) + writes (fan-out)
    ///   Shard 3: [full data] ← reads (round-robin) + writes (fan-out)
    ///
    /// For :memory: databases, each shard is independent (no shared state).
    /// For file databases, each shard uses a separate file.
    /// </summary>
    public sealed class ShardedDuckDb : IDisposable
    {
        private readonly Shard[] shards;
        private long nextReadShard; // atomic round-robin for reads

        public int ShardCount { get { return shards.Length; } }
        public int TotalPoolSize { get; }

        public ShardedDuckDb(ServerConfig config)
        {
            int shardCount = Math.Max(1, config.Shards);
            int readersPerShard = Math.Max(1, config.ReaderPoolSize / shardCount);

            shards = new Shard[shardCount];
            nextReadShard = -1;

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

            Console.WriteLine("[das] Sharded DuckDB (read-all / write-all):");
            Console.WriteLine("      shards   = {0}", shardCount);
            Console.WriteLine("      readers  = {0} per shard, {1} total", readersPerShard, TotalPoolSize);
            Console.WriteLine("      strategy = reads round-robin, writes fan-out to all");
        }

        /// <summary>
        /// Get the next shard for READING (round-robin).
        /// Each shard has a full copy — any shard can serve any read.
        /// </summary>
        public Shard NextForRead()
        {
            long idx = Interlocked.Increment(ref nextReadShard);
            return shards[(int)((idx & 0x7FFFFFFFFFFFFFFFL) % shards.Length)];
        }

        /// <summary>
        /// Execute a WRITE on ALL shards (fan-out).
        /// Every shard gets the same write so data stays consistent.
        /// Returns the result from the first shard (or first error).
        /// </summary>
        public WriteResult WriteToAll(string sql)
        {
            if (shards.Length == 1)
            {
                // Fast path: single shard, no fan-out overhead
                return shards[0].Writer.Submit(sql);
            }

            // Fan-out: submit to all shards in parallel
            var tasks = new Task<WriteResult>[shards.Length];
            for (int i = 0; i < shards.Length; i++)
            {
                int idx = i; // capture for closure
                tasks[i] = Task.Run(() => shards[idx].Writer.Submit(sql));
            }

            Task.WaitAll(tasks);

            // Return first error, or success if all succeeded
            for (int i = 0; i < tasks.Length; i++)
            {
                if (!tasks[i].Result.Ok)
                    return tasks[i].Result;
            }
            return tasks[0].Result;
        }

        /// <summary>
        /// Execute a WRITE on ALL shards using pre-built SQL.
        /// Used by BulkInsert where the SQL is already constructed.
        /// </summary>
        public WriteResult WriteToAllRaw(string sql)
        {
            return WriteToAll(sql);
        }

        /// <summary>
        /// Get a specific shard by index.
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

        private static string GetShardPath(string basePath, int shardIndex, int shardCount)
        {
            if (shardCount <= 1) return basePath;
            if (string.IsNullOrEmpty(basePath) || basePath == ":memory:") return ":memory:";

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
