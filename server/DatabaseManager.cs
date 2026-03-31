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
        private bool disposed;

        /// <summary>
        /// Open the database. All subsequent connections share this database.
        /// </summary>
        public DatabaseManager(string dbPath)
        {
            connectionString = BuildConnectionString(dbPath);

            // Open the primary connection to create the database.
            // For :memory:?cache=shared, this registers the shared instance.
            primaryConnection = new DuckDBConnection(connectionString);
            primaryConnection.Open();
        }

        /// <summary>
        /// Create a new connection to the SAME database.
        /// For :memory: mode, uses cache=shared so all connections share state.
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
            // For in-memory databases, use cache=shared so all connections
            // share the same underlying DuckDB database handle.
            if (string.IsNullOrEmpty(dbPath) || dbPath == ":memory:")
                return "Data Source=:memory:?cache=shared";

            return "Data Source=" + dbPath;
        }
    }
}
