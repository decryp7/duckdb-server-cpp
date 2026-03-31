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
    ///   and reader connections would not share tables or data.
    ///
    ///   This class opens the database ONCE and creates all additional
    ///   connections via DuplicateConnection(), which calls duckdb_connect
    ///   on the same underlying database handle. All connections share
    ///   the same tables and data.
    ///
    ///   For file-based databases this also works correctly.
    /// </summary>
    public sealed class DatabaseManager : IDisposable
    {
        private readonly DuckDBConnection primaryConnection;
        private bool disposed;

        /// <summary>
        /// Open the database. All subsequent connections share this database.
        /// </summary>
        public DatabaseManager(string dbPath)
        {
            string connectionString = BuildConnectionString(dbPath);
            primaryConnection = new DuckDBConnection(connectionString);
            primaryConnection.Open();
        }

        /// <summary>
        /// Create a new connection to the SAME database.
        /// Uses DuplicateConnection() which shares the underlying DuckDB handle.
        /// The caller owns the returned connection and must dispose it.
        /// </summary>
        public DuckDBConnection CreateConnection()
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(DatabaseManager));

            // DuplicateConnection() calls duckdb_connect on the same duckdb_database
            // handle, so all connections share the same in-memory (or file) database.
            var conn = primaryConnection.DuplicateConnection();
            conn.Open();
            return conn;
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
