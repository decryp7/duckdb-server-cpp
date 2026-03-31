using System;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Manages the DuckDB database lifecycle and creates connections.
    ///
    /// Why this exists:
    ///   In DuckDB.NET, each "new DuckDBConnection(Data Source=:memory:)"
    ///   creates a SEPARATE in-memory database. That means the writer
    ///   connection and reader connections would not share tables or data.
    ///
    ///   This class opens the database ONCE and creates all connections
    ///   from the same underlying database handle, so they share state.
    ///
    ///   For file-based databases this is also correct — all connections
    ///   point to the same file and share the DuckDB instance.
    /// </summary>
    public sealed class DatabaseManager : IDisposable
    {
        private readonly DuckDBConnection primaryConnection;
        private readonly string connectionString;
        private bool disposed;

        /// <summary>
        /// Open the database. All subsequent connections will share this database.
        /// </summary>
        public DatabaseManager(string dbPath)
        {
            connectionString = BuildConnectionString(dbPath);

            // Open the primary connection. This creates (or opens) the database.
            // For :memory: mode, this is THE database instance. All other
            // connections must be created from the same underlying DuckDB handle.
            primaryConnection = new DuckDBConnection(connectionString);
            primaryConnection.Open();
        }

        /// <summary>
        /// Create a new connection to the same database.
        /// The caller owns the returned connection and must dispose it.
        /// </summary>
        public DuckDBConnection CreateConnection()
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(DatabaseManager));

            var conn = new DuckDBConnection(connectionString);
            conn.Open();
            return conn;
        }

        /// <summary>
        /// The connection string used for all connections.
        /// </summary>
        public string ConnectionString
        {
            get { return connectionString; }
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            primaryConnection.Dispose();
        }

        private static string BuildConnectionString(string dbPath)
        {
            if (string.IsNullOrEmpty(dbPath) || dbPath == ":memory:")
                return "Data Source=:memory:";
            return "Data Source=" + dbPath;
        }
    }
}
