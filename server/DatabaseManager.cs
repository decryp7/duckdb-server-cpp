using System;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Manages the DuckDB database lifecycle and creates connections.
    ///
    /// Why this exists:
    ///   In DuckDB.NET, each "new DuckDBConnection(Data Source=:memory:)"
    ///   creates a SEPARATE in-memory database. Readers and writers would
    ///   not share tables or data.
    ///
    ///   For in-memory mode, we use "Data Source=:memory:?cache=shared"
    ///   which makes all connections with that string share the SAME
    ///   underlying DuckDB database handle via DuckDB.NET's ConnectionCache.
    ///
    ///   For file-based databases, all connections naturally share the same
    ///   file, so no special handling is needed.
    /// </summary>
    public sealed class DatabaseManager : IDisposable
    {
        private readonly DuckDBConnection primaryConnection;
        private readonly string connectionString;
        private int disposed; // 0=alive, 1=disposed; use Interlocked

        /// <summary>
        /// Open the database and apply performance settings.
        /// All subsequent connections share this database.
        /// </summary>
        public DatabaseManager(string dbPath, ServerConfig config)
        {
            connectionString = BuildConnectionString(dbPath);

            primaryConnection = new DuckDBConnection(connectionString);
            primaryConnection.Open();

            ApplyPerformanceSettings(config);
        }

        /// <summary>
        /// Create a new connection to the SAME database.
        /// The caller owns the returned connection and must dispose it.
        /// </summary>
        public DuckDBConnection CreateConnection()
        {
            if (System.Threading.Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(DatabaseManager));

            var conn = new DuckDBConnection(connectionString);
            conn.Open();
            return conn;
        }

        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            primaryConnection.Dispose();
        }

        // ── Private helpers ──────────────────────────────────────────────────

        /// <summary>
        /// Apply DuckDB PRAGMA settings for performance.
        /// These are database-level settings shared across all connections.
        /// </summary>
        private void ApplyPerformanceSettings(ServerConfig config)
        {
            // Memory limit: controls how much RAM DuckDB uses for query processing.
            if (!string.IsNullOrEmpty(config.MemoryLimit))
                ExecutePragma("SET memory_limit='" + config.MemoryLimit + "'");

            // Thread count: controls DuckDB's internal parallelism.
            if (config.DuckDbThreads > 0)
                ExecutePragma("SET threads=" + config.DuckDbThreads);

            // Enable object cache for faster metadata lookups.
            ExecutePragma("PRAGMA enable_object_cache");

            // Disable insertion order preservation for faster bulk inserts.
            // DuckDB normally preserves the order rows were inserted, which
            // adds overhead. Analytics queries don't need insertion order.
            ExecutePragma("SET preserve_insertion_order=false");

            // Increase checkpoint threshold: default 16MB triggers frequent checkpoints.
            // 256MB reduces checkpoint frequency during write-heavy workloads (2-5x faster).
            ExecutePragma("SET checkpoint_threshold='256MB'");

            // Enable partial block reading for faster scans on large tables.
            ExecutePragma("PRAGMA enable_progress_bar=false");
        }

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
                // Log but don't fail — PRAGMAs are optimization hints.
                Console.Error.WriteLine("[das] Warning: " + sql + " failed: " + ex.Message);
            }
        }

        private static string BuildConnectionString(string dbPath)
        {
            if (string.IsNullOrEmpty(dbPath) || dbPath == ":memory:")
                return "Data Source=:memory:?cache=shared";

            return "Data Source=" + dbPath;
        }
    }
}
