using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace DuckArrowClient
{
    internal static class Example
    {
        // ── 1. Basic query ───────────────────────────────────────────────────
        public static void BasicQuery()
        {
            using (IDasFlightClient client = new DasFlightClient("localhost", 17777))
            {
                client.Ping();

                using (IFlightQueryResult result = client.Query(
                    "SELECT 42 AS answer, 'hello' AS greeting"))
                {
                    Console.WriteLine("Total rows: " + result.RowCount);
                    foreach (var row in result.ToRows())
                        foreach (var kv in row)
                            Console.WriteLine("  {0} = {1}", kv.Key, kv.Value ?? "<null>");
                }
            }
        }

        // ── 2. DataTable binding ─────────────────────────────────────────────
        public static void DataTableBinding()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                using (IFlightQueryResult result = client.Query(
                    "SELECT range AS id, 'row_' || range AS label, random() AS score " +
                    "FROM range(0, 1000)"))
                {
                    DataTable dt = result.ToDataTable("Scores");
                    Console.WriteLine("Rows={0}  Cols={1}", dt.Rows.Count, dt.Columns.Count);
                }
            }
        }

        // ── 3. Write operations ──────────────────────────────────────────────
        public static void WriteOperations()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                client.Execute("CREATE TABLE IF NOT EXISTS events (id INT, name TEXT)");
                client.Execute("INSERT INTO events VALUES (1, 'login')");
                client.Execute("INSERT INTO events VALUES (2, 'purchase')");

                using (IFlightQueryResult result = client.Query(
                    "SELECT * FROM events ORDER BY id"))
                {
                    foreach (var row in result.ToRows())
                        Console.WriteLine("{0}: {1}", row["id"], row["name"]);
                }
            }
        }

        // ── 4. Stats ─────────────────────────────────────────────────────────
        public static void PrintStats()
        {
            using (IDasFlightClient client = new DasFlightClient())
            {
                string json = client.GetStats();
                Console.WriteLine("Server stats: " + json);
            }
        }

        // ── 5. Concurrent queries ────────────────────────────────────────────
        public static async Task ConcurrentQueriesAsync()
        {
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

        // ── 6. DI pattern ────────────────────────────────────────────────────
        public static void DependencyInjectionPattern()
        {
            using (IDasFlightClient client = new DasFlightClient("localhost", 17777))
            {
                var service = new SalesReportService(client);
                DataTable report = service.GetTopProducts(10);
                Console.WriteLine("Products: " + report.Rows.Count);
            }
        }

        // ── Main ─────────────────────────────────────────────────────────────

        static void Main()
        {
            RunExample("1. Basic query", BasicQuery);
            RunExample("2. DataTable", DataTableBinding);
            RunExample("3. Writes", WriteOperations);
            RunExample("4. Stats", PrintStats);
            RunExample("5. Concurrent", () => Task.Run(ConcurrentQueriesAsync).Wait());
            RunExample("6. DI pattern", DependencyInjectionPattern);

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

    internal sealed class SalesReportService
    {
        private readonly IDasFlightClient db;
        public SalesReportService(IDasFlightClient db) { this.db = db; }

        public DataTable GetTopProducts(int n)
        {
            string sql = string.Format(
                "SELECT 'product_' || range AS product, random() * 1000 AS revenue " +
                "FROM range(0, {0}) ORDER BY revenue DESC", n);

            using (IFlightQueryResult result = db.Query(sql))
                return result.ToDataTable("TopProducts");
        }
    }
}
