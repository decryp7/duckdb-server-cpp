using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Annotated usage examples for the DuckDB Arrow Flight client library.
    ///
    /// <para>
    /// All examples use the <see cref="IDasFlightClient"/> interface rather than
    /// the concrete <see cref="DasFlightClient"/> class.  This makes it trivial
    /// to swap the real client for a mock in unit tests.
    /// </para>
    /// </summary>
    internal static class Example
    {
        // ── 1. Basic synchronous query ─────────────────────────────────────────
        /// <summary>
        /// Demonstrate a simple SELECT query: open a client, send SQL, read results.
        /// </summary>
        public static void BasicQuery()
        {
            // DasFlightClient is the concrete implementation of IDasFlightClient.
            // One instance per application — HTTP/2 handles all concurrency.
            using (IDasFlightClient client = new DasFlightClient("localhost", 17777))
            {
                // Liveness check: sends DoAction("ping"), expects "pong".
                client.Ping();

                // DoGet: sends SQL as the ticket, receives Arrow record batches.
                using (IFlightQueryResult result = client.Query(
                    "SELECT 42 AS answer, 'hello' AS greeting, NOW() AS ts"))
                {
                    Console.WriteLine("Total rows: " + result.RowCount);

                    // ToRows() converts Arrow batches to row dictionaries.
                    // Suitable for small results and debugging.
                    foreach (var row in result.ToRows())
                        foreach (var kv in row)
                            Console.WriteLine("  {0} = {1}", kv.Key, kv.Value ?? "<null>");
                }
            }
        }

        // ── 2. ADO.NET DataTable — bind to DataGridView / RDLC reports ─────────
        /// <summary>
        /// Show how to convert a query result to a DataTable for data binding.
        /// </summary>
        public static void DataTableBinding()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                using (IFlightQueryResult result = client.Query(
                    "SELECT range AS id, 'row_' || range AS label, random() AS score " +
                    "FROM range(0, 1000)"))
                {
                    // ToDataTable() produces a typed DataTable:
                    //   id    → typeof(long)
                    //   label → typeof(string)
                    //   score → typeof(double)
                    DataTable dt = result.ToDataTable("Scores");
                    Console.WriteLine("Rows={0}  Cols={1}", dt.Rows.Count, dt.Columns.Count);

                    // Bind directly to a WinForms DataGridView:
                    // myDataGridView.DataSource = dt;
                }
            }
        }

        // ── 3. Write operations (DML + DDL) ───────────────────────────────────
        /// <summary>
        /// Demonstrate INSERT, UPDATE, and CREATE TABLE via DoAction("execute").
        /// The server batches concurrent DML automatically; DDL runs individually.
        /// </summary>
        public static void WriteOperations()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                // DDL: detected by keyword, executed outside any batch transaction.
                client.Execute("CREATE TABLE IF NOT EXISTS events (id INT, name TEXT)");

                // DML: queued and batched with other concurrent writes.
                client.Execute("INSERT INTO events VALUES (1, 'login')");
                client.Execute("INSERT INTO events VALUES (2, 'purchase')");

                // Verify with a read query.
                using (IFlightQueryResult result = client.Query(
                    "SELECT * FROM events ORDER BY id"))
                {
                    foreach (var row in result.ToRows())
                        Console.WriteLine("{0}: {1}", row["id"], row["name"]);
                }
            }
        }

        // ── 4. Server statistics ───────────────────────────────────────────────
        /// <summary>
        /// Retrieve live server metrics via DoAction("stats").
        /// </summary>
        public static void PrintStats()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                string json = client.GetStats();
                Console.WriteLine("Server stats: " + json);
                // {"queries_read":340,"queries_write":22,"errors":0,
                //  "reader_pool_size":16,"port":17777}
            }
        }

        // ── 5. TLS connection ─────────────────────────────────────────────────
        /// <summary>
        /// Connect with TLS using a CA certificate for server verification.
        /// Start server with: --tls-cert server.crt --tls-key server.key
        /// </summary>
        public static void TlsConnection()
        {
            string caPem = System.IO.File.ReadAllText("ca.crt");
            var credentials = new SslCredentials(caPem);

            using (IDasFlightClient client =
                new DasFlightClient("myserver.local", 17777, credentials))
            {
                client.Ping();
                using (IFlightQueryResult result = client.Query("SELECT 1 AS secure"))
                    Console.WriteLine("TLS rows: " + result.RowCount);
            }
        }

        // ── 6. Concurrent async queries ────────────────────────────────────────
        /// <summary>
        /// One DasFlightClient handles all concurrent async callers — no pool needed.
        /// HTTP/2 multiplexes the RPCs over a single TCP connection.
        /// </summary>
        public static async Task ConcurrentQueriesAsync()
        {
            // Share one client across all tasks.
            using (IDasFlightClient client = new DasFlightClient())
            {
                var tasks = new List<Task<IFlightQueryResult>>();
                for (int i = 0; i < 20; i++)
                {
                    string sql = string.Format("SELECT {0} * {0} AS sq", i);
                    tasks.Add(client.QueryAsync(sql));
                }

                IFlightQueryResult[] results = null;
                try
                {
                    results = await Task.WhenAll(tasks);
                }
                catch
                {
                    // Dispose any results from tasks that succeeded before
                    // re-throwing so Arrow column buffers are not leaked.
                    foreach (var t in tasks)
                        if (t.Status == TaskStatus.RanToCompletion)
                            t.Result?.Dispose();
                    throw;
                }
                foreach (var r in results)
                {
                    Console.WriteLine("sq=" + r.ToRows()[0]["sq"]);
                    r.Dispose();
                }
            }
        }

        // ── 7. Dependency injection pattern ───────────────────────────────────
        /// <summary>
        /// Show how to use <see cref="IDasFlightClient"/> for dependency injection.
        /// The service is testable with a mock that does not need a real server.
        /// </summary>
        public static void DependencyInjectionPattern()
        {
            // `using` ensures the client is disposed even if GetTopProducts throws.
            using (IDasFlightClient client = new DasFlightClient("localhost", 17777))
            {
                var service = new SalesReportService(client);
                DataTable report = service.GetTopProducts(10);
                Console.WriteLine("Products: " + report.Rows.Count);
            }
        }

        // ── 8. Raw Arrow batch access — zero-allocation analytics ──────────────
        /// <summary>
        /// Access Arrow column buffers directly for high-performance analytics
        /// without converting to row-oriented types.
        /// </summary>
        public static void RawArrowBatches()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                // Create a test table for this example.
                client.Execute("CREATE TABLE IF NOT EXISTS sales (amount DOUBLE, region TEXT)");
                client.Execute("INSERT INTO sales SELECT random() * 100, 'region_' || (range % 5) FROM range(0, 10000)");
            }

            using (IDasFlightClient client = new DasFlightClient())
            using (IFlightQueryResult result = client.Query(
                "SELECT amount, region FROM sales LIMIT 1000000"))
            {
                int amountIdx = result.Schema.GetFieldIndex("amount");

                double total = 0.0;
                // Iterate batches — each batch is a contiguous columnar buffer.
                foreach (RecordBatch batch in result.Batches)
                {
                    // Cast to the typed array — no per-cell boxing overhead.
                    var amounts = (DoubleArray)batch.Column(amountIdx);
                    for (int row = 0; row < amounts.Length; row++)
                    {
                        if (!amounts.IsNull(row))
                            total += amounts.GetValue(row).GetValueOrDefault();
                    }
                }
                Console.WriteLine("Total sales: " + total);
            }
        }

        // ── 9. Pool usage (rarely needed — see docs on IDasFlightPool) ─────────
        /// <summary>
        /// Demonstrate the optional connection pool for workloads that need
        /// hard channel-count limits or TLS session isolation.
        /// </summary>
        public static async Task PooledQuery()
        {
            using (IDasFlightPool pool = new DasFlightPool("localhost", 17777, size: 4))
            using (IPoolLease lease = await pool.BorrowAsync())
            using (IFlightQueryResult result =
                await lease.Client.QueryAsync("SELECT current_date AS today"))
            {
                Console.WriteLine("Today: " + result.ToRows()[0]["today"]);
            } // lease.Dispose() returns the client to the pool
        }

        static void Main()
        {
            RunExample("1. Basic query", BasicQuery);
            RunExample("2. DataTable", DataTableBinding);
            RunExample("3. Writes", WriteOperations);
            RunExample("4. Stats", PrintStats);
            RunExample("5. Raw batches", RawArrowBatches);
            RunExample("6. Concurrent", () => Task.Run(ConcurrentQueriesAsync).Wait());
            RunExample("7. Pool", () => Task.Run(PooledQuery).Wait());

            Console.WriteLine();
            Console.WriteLine("All examples completed.");
        }

        private static void RunExample(string name, Action action)
        {
            Console.WriteLine("=== " + name + " ===");
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Skipped: " + (ex.InnerException?.Message ?? ex.Message));
            }
            Console.WriteLine();
        }
    }

    // ── Supporting class for Example 7 ────────────────────────────────────────

    /// <summary>
    /// Example service class that depends on <see cref="IDasFlightClient"/>.
    /// Because it accepts the interface, unit tests can inject a mock without
    /// starting a real DuckDB Flight server.
    /// </summary>
    internal sealed class SalesReportService
    {
        private readonly IDasFlightClient _db;

        /// <param name="db">Flight client injected at construction time.</param>
        public SalesReportService(IDasFlightClient db) { _db = db; }

        /// <summary>
        /// Return the top <paramref name="n"/> products by revenue as a DataTable.
        /// </summary>
        public DataTable GetTopProducts(int n)
        {
            string sql = string.Format(
                "SELECT product, SUM(amount) AS revenue " +
                "FROM sales GROUP BY product ORDER BY revenue DESC LIMIT {0}", n);

            using (IFlightQueryResult result = _db.Query(sql))
                return result.ToDataTable("TopProducts");
        }
    }
}
