using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace DuckDbClient
{
    /// <summary>
    /// Example usage of the <see cref="DuckDbClient"/> library. Demonstrates basic queries,
    /// DataTable binding, write operations, server statistics, concurrent queries, and
    /// dependency injection patterns.
    ///
    /// <para><b>Prerequisites:</b> A DuckDB gRPC server must be running on localhost:17777.
    /// Start the server with: <c>DuckDbServer.exe --db :memory: --port 17777</c></para>
    ///
    /// <para><b>Error handling:</b> Each example is wrapped in a try/catch by
    /// <see cref="RunExample"/>. If the server is not running, examples are skipped
    /// with an error message rather than crashing.</para>
    /// </summary>
    internal static class Example
    {
        // ── 1. Basic query ───────────────────────────────────────────────────

        /// <summary>
        /// Demonstrates a basic query with health check. Shows how to:
        /// <list type="bullet">
        ///   <item><description>Create a client connected to localhost:17777.</description></item>
        ///   <item><description>Verify server connectivity with <see cref="IDuckDbClient.Ping"/>.</description></item>
        ///   <item><description>Execute a SELECT query and iterate over rows.</description></item>
        ///   <item><description>Access column values by name from the row dictionary.</description></item>
        /// </list>
        /// </summary>
        public static void BasicQuery()
        {
            // Create a client. The using block ensures the gRPC channel is shut down.
            using (IDuckDbClient client = new DuckDbClient("localhost", 17777))
            {
                // Verify the server is reachable before executing queries.
                client.Ping();

                // Execute a query and read results. The using block disposes the result.
                using (IQueryResult result = client.Query(
                    "SELECT 42 AS answer, 'hello' AS greeting"))
                {
                    Console.WriteLine("Total rows: " + result.RowCount);
                    // ToRows() converts columnar data to row dictionaries.
                    foreach (var row in result.ToRows())
                        foreach (var kv in row)
                            Console.WriteLine("  {0} = {1}", kv.Key, kv.Value ?? "<null>");
                }
            }
        }

        // ── 2. DataTable binding ─────────────────────────────────────────────

        /// <summary>
        /// Demonstrates converting query results to an ADO.NET <see cref="DataTable"/>,
        /// which can be used for data binding in WinForms, WPF, or ASP.NET applications.
        /// Queries 1,000 rows to show that streaming works for non-trivial result sets.
        /// </summary>
        public static void DataTableBinding()
        {
            // Default constructor connects to localhost:17777.
            using (IDuckDbClient client = new DuckDbClient())
            {
                using (IQueryResult result = client.Query(
                    "SELECT range AS id, 'row_' || range AS label, random() AS score " +
                    "FROM range(0, 1000)"))
                {
                    // ToDataTable creates typed columns (int, string, double)
                    // and populates all rows.
                    DataTable dt = result.ToDataTable("Scores");
                    Console.WriteLine("Rows={0}  Cols={1}", dt.Rows.Count, dt.Columns.Count);
                }
            }
        }

        // ── 3. Write operations ──────────────────────────────────────────────

        /// <summary>
        /// Demonstrates DDL (CREATE TABLE) and DML (INSERT) operations, followed by
        /// a query to verify the written data. Shows the typical write-then-read pattern.
        /// </summary>
        public static void WriteOperations()
        {
            using (IDuckDbClient client = new DuckDbClient())
            {
                // DDL: Create a table (idempotent with IF NOT EXISTS).
                client.Execute("CREATE TABLE IF NOT EXISTS events (id INT, name TEXT)");
                // DML: Insert rows. On the server, these may be batched into a single
                // transaction by the WriteSerializer for better throughput.
                client.Execute("INSERT INTO events VALUES (1, 'login')");
                client.Execute("INSERT INTO events VALUES (2, 'purchase')");

                // Read back the inserted data.
                using (IQueryResult result = client.Query(
                    "SELECT * FROM events ORDER BY id"))
                {
                    foreach (var row in result.ToRows())
                        Console.WriteLine("{0}: {1}", row["id"], row["name"]);
                }
            }
        }

        // ── 4. Stats ─────────────────────────────────────────────────────────

        /// <summary>
        /// Retrieves and displays server statistics (query counts, errors, pool size).
        /// Useful for monitoring and debugging.
        /// </summary>
        public static void PrintStats()
        {
            using (IDuckDbClient client = new DuckDbClient())
            {
                string json = client.GetStats();
                Console.WriteLine("Server stats: " + json);
            }
        }

        // ── 5. Concurrent queries ────────────────────────────────────────────

        /// <summary>
        /// Demonstrates executing 20 queries concurrently using <see cref="Task.WhenAll"/>.
        /// Shows that a single <see cref="DuckDbClient"/> instance can safely handle
        /// multiple concurrent queries (the gRPC channel multiplexes over HTTP/2).
        ///
        /// <para><b>Error handling:</b> If any query fails, all successfully completed
        /// results are disposed before re-throwing the exception to prevent resource leaks.</para>
        /// </summary>
        public static async Task ConcurrentQueriesAsync()
        {
            using (IDuckDbClient client = new DuckDbClient())
            {
                // Launch 20 concurrent queries.
                var tasks = new List<Task<IQueryResult>>();
                for (int i = 0; i < 20; i++)
                {
                    string sql = string.Format("SELECT {0} * {0} AS sq", i);
                    tasks.Add(client.QueryAsync(sql));
                }

                IQueryResult[] results = null;
                try
                {
                    results = await Task.WhenAll(tasks);
                }
                catch
                {
                    // Clean up any successfully completed results to avoid leaks.
                    foreach (var t in tasks)
                        if (t.Status == TaskStatus.RanToCompletion)
                            t.Result?.Dispose();
                    throw;
                }
                // Print results and dispose each one.
                foreach (var r in results)
                {
                    Console.WriteLine("sq=" + r.ToRows()[0]["sq"]);
                    r.Dispose();
                }
            }
        }

        // ── 6. DI pattern ────────────────────────────────────────────────────

        /// <summary>
        /// Demonstrates the dependency injection pattern: inject <see cref="IDuckDbClient"/>
        /// into service classes. This enables unit testing with mock implementations and
        /// decouples business logic from the gRPC transport.
        /// </summary>
        public static void DependencyInjectionPattern()
        {
            using (IDuckDbClient client = new DuckDbClient("localhost", 17777))
            {
                // Inject the client into a service class via constructor injection.
                var service = new SalesReportService(client);
                DataTable report = service.GetTopProducts(10);
                Console.WriteLine("Products: " + report.Rows.Count);
            }
        }

        // ── Main ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Runs all examples sequentially. Each example is isolated in its own
        /// try/catch so that a failure in one does not prevent others from running.
        /// </summary>
        static void Main()
        {
            RunExample("1. Basic query", BasicQuery);
            RunExample("2. DataTable", DataTableBinding);
            RunExample("3. Writes", WriteOperations);
            RunExample("4. Stats", PrintStats);
            // Wrap async method in Task.Run to avoid deadlock in the console sync context.
            RunExample("5. Concurrent", () => Task.Run(ConcurrentQueriesAsync).Wait());
            RunExample("6. DI pattern", DependencyInjectionPattern);

            Console.WriteLine();
            Console.WriteLine("All examples completed.");
        }

        /// <summary>
        /// Runs a single example with error handling. Prints the example name, executes
        /// the action, and catches any exceptions (typically server-not-running errors).
        /// </summary>
        /// <param name="name">Display name for the example.</param>
        /// <param name="action">The example code to execute.</param>
        private static void RunExample(string name, Action action)
        {
            Console.WriteLine("=== " + name + " ===");
            try
            {
                action();
            }
            catch (Exception ex)
            {
                // Display the most specific error message available.
                Console.WriteLine("  Skipped: " + (ex.InnerException?.Message ?? ex.Message));
            }
            Console.WriteLine();
        }
    }

    /// <summary>
    /// Example service class that takes <see cref="IDuckDbClient"/> as a constructor
    /// dependency. Demonstrates how to build testable business logic on top of the
    /// DuckDB client library.
    ///
    /// <para><b>Testing:</b> In unit tests, pass a mock <see cref="IDuckDbClient"/>
    /// instead of a real one. This avoids the need for a running server during tests.</para>
    /// </summary>
    internal sealed class SalesReportService
    {
        /// <summary>Injected DuckDB client. NOT owned by this class (do not dispose).</summary>
        private readonly IDuckDbClient db;

        /// <summary>
        /// Creates a new service with the given DuckDB client.
        /// </summary>
        /// <param name="db">The DuckDB client to use for queries. Must not be null.</param>
        public SalesReportService(IDuckDbClient db) { this.db = db; }

        /// <summary>
        /// Generates a sample report of top products by revenue.
        /// Uses DuckDB's <c>range()</c> function to generate synthetic data.
        /// </summary>
        /// <param name="n">Number of products to include in the report.</param>
        /// <returns>A <see cref="DataTable"/> with "product" and "revenue" columns, sorted by revenue descending.</returns>
        public DataTable GetTopProducts(int n)
        {
            string sql = string.Format(
                "SELECT 'product_' || range AS product, random() * 1000 AS revenue " +
                "FROM range(0, {0}) ORDER BY revenue DESC", n);

            using (IQueryResult result = db.Query(sql))
                return result.ToDataTable("TopProducts");
        }
    }
}
