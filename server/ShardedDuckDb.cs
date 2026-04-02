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
        /// Get the shard for a SQL statement using hash-based routing.
        ///
        /// Extracts the table name from the SQL and hashes it to a shard index.
        /// This ensures all operations on the same table go to the same shard:
        ///   CREATE TABLE foo → shard 2
        ///   INSERT INTO foo  → shard 2
        ///   SELECT FROM foo  → shard 2
        ///
        /// For queries without a clear table (e.g. SELECT 1+1), falls back
        /// to hashing the full SQL string.
        /// </summary>
        public Shard ForSql(string sql)
        {
            string key = ExtractTableName(sql) ?? sql;
            int hash = GetStableHash(key);
            return shards[Math.Abs(hash) % shards.Length];
        }

        /// <summary>
        /// Get a specific shard by index.
        /// </summary>
        public Shard GetShard(int index)
        {
            return shards[index % shards.Length];
        }

        /// <summary>
        /// Extract the primary table name from a SQL statement.
        /// Handles: SELECT ... FROM table, INSERT INTO table, CREATE TABLE table,
        /// DROP TABLE table, UPDATE table, DELETE FROM table
        /// Returns null if no table name found.
        /// </summary>
        private static string ExtractTableName(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return null;

            // Normalize: trim, collapse whitespace, uppercase for matching
            string upper = sql.TrimStart().ToUpperInvariant();

            // Try common patterns
            string[] afterKeywords = {
                "FROM ", "INTO ", "TABLE ", "UPDATE ", "JOIN "
            };

            foreach (var kw in afterKeywords)
            {
                int idx = upper.IndexOf(kw, StringComparison.Ordinal);
                if (idx >= 0)
                {
                    int start = idx + kw.Length;
                    // Skip whitespace after keyword
                    while (start < sql.Length && char.IsWhiteSpace(sql[start])) start++;
                    // Skip optional "IF NOT EXISTS" / "IF EXISTS"
                    if (upper.Substring(start).StartsWith("IF "))
                    {
                        int nextSpace = sql.IndexOf(' ', start + 3);
                        if (nextSpace > 0)
                        {
                            start = nextSpace + 1;
                            while (start < sql.Length && char.IsWhiteSpace(sql[start])) start++;
                            // Skip "EXISTS" or "NOT EXISTS"
                            if (upper.Substring(start).StartsWith("EXISTS ") ||
                                upper.Substring(start).StartsWith("NOT "))
                            {
                                nextSpace = sql.IndexOf(' ', start);
                                if (nextSpace > 0)
                                {
                                    start = nextSpace + 1;
                                    while (start < sql.Length && char.IsWhiteSpace(sql[start])) start++;
                                    // If "NOT EXISTS", skip "EXISTS" too
                                    if (upper.Substring(start).StartsWith("EXISTS "))
                                    {
                                        nextSpace = sql.IndexOf(' ', start);
                                        if (nextSpace > 0) start = nextSpace + 1;
                                        while (start < sql.Length && char.IsWhiteSpace(sql[start])) start++;
                                    }
                                }
                            }
                        }
                    }

                    // Read table name (stops at whitespace, (, ;, or end)
                    int end = start;
                    while (end < sql.Length && !char.IsWhiteSpace(sql[end])
                           && sql[end] != '(' && sql[end] != ';')
                        end++;

                    if (end > start)
                        return sql.Substring(start, end - start).ToLowerInvariant();
                }
            }

            return null;
        }

        /// <summary>
        /// Stable hash that doesn't change across .NET versions.
        /// (String.GetHashCode is not guaranteed stable across runtimes.)
        /// </summary>
        private static int GetStableHash(string s)
        {
            unchecked
            {
                int hash = 5381;
                foreach (char c in s)
                    hash = ((hash << 5) + hash) + c;
                return hash;
            }
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
