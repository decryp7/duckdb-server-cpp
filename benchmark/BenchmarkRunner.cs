using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckArrowClient;

namespace DuckArrowBenchmark
{
    /// <summary>
    /// Runs benchmark scenarios against a DuckDB Arrow Flight server.
    /// Measures throughput, latency, and maximum concurrency.
    /// </summary>
    public sealed class BenchmarkRunner
    {
        private readonly string host;
        private readonly int port;
        private readonly int warmupOps;

        public BenchmarkRunner(string host = "localhost", int port = 17777, int warmupOps = 10)
        {
            this.host = host;
            this.port = port;
            this.warmupOps = warmupOps;
        }

        // ── Scenario 1: Concurrent Readers ───────────────────────────────────

        /// <summary>
        /// Test how many concurrent SELECT queries the server can handle.
        /// Each reader runs a query that returns `rowCount` rows.
        /// </summary>
        public BenchmarkResult RunConcurrentReaders(int concurrency, int opsPerClient, int rowCount = 1000)
        {
            string sql = string.Format("SELECT range AS id, random() AS value FROM range(0, {0})", rowCount);

            using (var client = new DasFlightClient(host, port))
            {
                // Warmup: let the server JIT and stabilize.
                RunWarmup(client, sql);

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                var tasks = new Task[concurrency];
                for (int i = 0; i < concurrency; i++)
                {
                    int clientId = i;
                    tasks[i] = Task.Run(() => ReaderWorker(client, sql, opsPerClient, tracker));
                }

                Task.WaitAll(tasks);
                stopwatch.Stop();

                return tracker.BuildResult(
                    string.Format("Concurrent Readers ({0} rows)", rowCount),
                    concurrency,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void ReaderWorker(
            IDasFlightClient client, string sql, int ops, LatencyTracker tracker)
        {
            for (int i = 0; i < ops; i++)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using (var result = client.Query(sql))
                    {
                        // Force materialization by reading row count.
                        int rows = result.RowCount;
                    }
                    sw.Stop();
                    tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                }
                catch
                {
                    sw.Stop();
                    tracker.RecordError();
                }
            }
        }

        // ── Scenario 2: Concurrent Writers ───────────────────────────────────

        /// <summary>
        /// Test how many concurrent INSERT/UPDATE operations the server can handle.
        /// </summary>
        public BenchmarkResult RunConcurrentWriters(int concurrency, int opsPerClient)
        {
            using (var client = new DasFlightClient(host, port))
            {
                // Create the test table.
                client.Execute("CREATE TABLE IF NOT EXISTS bench_write (id INTEGER, value DOUBLE, label TEXT)");
                client.Execute("DELETE FROM bench_write");

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                var tasks = new Task[concurrency];
                for (int i = 0; i < concurrency; i++)
                {
                    int clientId = i;
                    tasks[i] = Task.Run(() => WriterWorker(client, clientId, opsPerClient, tracker));
                }

                Task.WaitAll(tasks);
                stopwatch.Stop();

                // Verify: count rows to confirm writes landed.
                using (var result = client.Query("SELECT COUNT(*) AS cnt FROM bench_write"))
                {
                    var rows = result.ToRows();
                    Console.WriteLine("  Rows written: " + rows[0]["cnt"]);
                }

                return tracker.BuildResult(
                    "Concurrent Writers",
                    concurrency,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void WriterWorker(
            IDasFlightClient client, int clientId, int ops, LatencyTracker tracker)
        {
            for (int i = 0; i < ops; i++)
            {
                string sql = string.Format(
                    "INSERT INTO bench_write VALUES ({0}, {1}.{2}, 'client_{0}_op_{3}')",
                    clientId * 100000 + i, clientId, i, i);

                var sw = Stopwatch.StartNew();
                try
                {
                    client.Execute(sql);
                    sw.Stop();
                    tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                }
                catch
                {
                    sw.Stop();
                    tracker.RecordError();
                }
            }
        }

        // ── Scenario 3: Mixed Read/Write ─────────────────────────────────────

        /// <summary>
        /// Test mixed workload: some threads read, some threads write.
        /// </summary>
        public BenchmarkResult RunMixedWorkload(int readers, int writers, int opsPerClient, int rowCount = 100)
        {
            string readSql = string.Format("SELECT range AS id FROM range(0, {0})", rowCount);

            using (var client = new DasFlightClient(host, port))
            {
                client.Execute("CREATE TABLE IF NOT EXISTS bench_mixed (id INTEGER, data TEXT)");
                client.Execute("DELETE FROM bench_mixed");

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                int totalClients = readers + writers;
                var tasks = new Task[totalClients];

                // Start reader threads.
                for (int i = 0; i < readers; i++)
                {
                    tasks[i] = Task.Run(() => ReaderWorker(client, readSql, opsPerClient, tracker));
                }

                // Start writer threads.
                for (int i = 0; i < writers; i++)
                {
                    int clientId = i;
                    tasks[readers + i] = Task.Run(() => MixedWriterWorker(client, clientId, opsPerClient, tracker));
                }

                Task.WaitAll(tasks);
                stopwatch.Stop();

                return tracker.BuildResult(
                    string.Format("Mixed ({0}R + {1}W)", readers, writers),
                    totalClients,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void MixedWriterWorker(
            IDasFlightClient client, int clientId, int ops, LatencyTracker tracker)
        {
            for (int i = 0; i < ops; i++)
            {
                string sql = string.Format(
                    "INSERT INTO bench_mixed VALUES ({0}, 'mixed_{1}')",
                    clientId * 100000 + i, i);

                var sw = Stopwatch.StartNew();
                try
                {
                    client.Execute(sql);
                    sw.Stop();
                    tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                }
                catch
                {
                    sw.Stop();
                    tracker.RecordError();
                }
            }
        }

        // ── Scenario 4: Large Result Sets ────────────────────────────────────

        /// <summary>
        /// Test streaming large result sets. Measures how fast the server
        /// can push data to the client.
        /// </summary>
        public BenchmarkResult RunLargeResultSet(int rowCount, int iterations)
        {
            string sql = string.Format(
                "SELECT range AS id, random() AS val1, random() AS val2, " +
                "'row_' || range AS label FROM range(0, {0})", rowCount);

            using (var client = new DasFlightClient(host, port))
            {
                RunWarmup(client, "SELECT 1");

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                for (int i = 0; i < iterations; i++)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        using (var result = client.Query(sql))
                        {
                            int rows = result.RowCount;
                        }
                        sw.Stop();
                        tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                    }
                    catch
                    {
                        sw.Stop();
                        tracker.RecordError();
                    }
                }

                stopwatch.Stop();

                return tracker.BuildResult(
                    string.Format("Large Result Set ({0:N0} rows)", rowCount),
                    1,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        // ── Scenario 5: Find Maximum Concurrency ─────────────────────────────

        /// <summary>
        /// Ramp up concurrency until the error rate exceeds a threshold.
        /// Returns results at each concurrency level.
        /// </summary>
        public List<BenchmarkResult> FindMaxConcurrency(
            bool isReader,
            int startConcurrency,
            int maxConcurrency,
            int step,
            int opsPerClient,
            double maxErrorRate = 0.05)
        {
            var results = new List<BenchmarkResult>();

            Console.WriteLine();
            Console.WriteLine("=== Finding Max " + (isReader ? "Reader" : "Writer") + " Concurrency ===");
            Console.WriteLine(string.Format("  Ramping from {0} to {1}, step {2}, max error rate {3:P0}",
                startConcurrency, maxConcurrency, step, maxErrorRate));
            Console.WriteLine();

            for (int concurrency = startConcurrency; concurrency <= maxConcurrency; concurrency += step)
            {
                Console.Write(string.Format("  Testing {0} concurrent {1}s... ",
                    concurrency, isReader ? "reader" : "writer"));

                BenchmarkResult result;
                if (isReader)
                    result = RunConcurrentReaders(concurrency, opsPerClient, 100);
                else
                    result = RunConcurrentWriters(concurrency, opsPerClient);

                double errorRate = result.TotalOperations > 0
                    ? (double)result.ErrorCount / result.TotalOperations
                    : 0;

                Console.WriteLine(string.Format("{0:F1} ops/s, {1:F1} ms avg, {2} errors ({3:P1})",
                    result.OpsPerSecond, result.AvgLatencyMs, result.ErrorCount, errorRate));

                results.Add(result);

                if (errorRate > maxErrorRate)
                {
                    Console.WriteLine(string.Format(
                        "  >> Error rate {0:P1} exceeds threshold {1:P0}. Stopping.",
                        errorRate, maxErrorRate));
                    break;
                }
            }

            return results;
        }

        // ── Scenario 6: Sustained Throughput ─────────────────────────────────

        /// <summary>
        /// Run at a fixed concurrency for a set duration to measure
        /// sustained throughput and stability.
        /// </summary>
        public BenchmarkResult RunSustainedThroughput(
            int concurrency, int durationSeconds, bool isReader, int rowCount = 100)
        {
            string readSql = string.Format("SELECT range AS id FROM range(0, {0})", rowCount);

            using (var client = new DasFlightClient(host, port))
            {
                if (!isReader)
                {
                    client.Execute("CREATE TABLE IF NOT EXISTS bench_sustained (id INTEGER, data TEXT)");
                    client.Execute("DELETE FROM bench_sustained");
                }

                var tracker = new LatencyTracker();
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
                var stopwatch = Stopwatch.StartNew();

                var tasks = new Task[concurrency];
                for (int i = 0; i < concurrency; i++)
                {
                    int clientId = i;
                    if (isReader)
                        tasks[i] = Task.Run(() => SustainedReaderWorker(client, readSql, tracker, cts.Token));
                    else
                        tasks[i] = Task.Run(() => SustainedWriterWorker(client, clientId, tracker, cts.Token));
                }

                Task.WaitAll(tasks);
                stopwatch.Stop();

                string name = string.Format("Sustained {0} ({1}s)",
                    isReader ? "Read" : "Write", durationSeconds);

                return tracker.BuildResult(name, concurrency, stopwatch.ElapsedMilliseconds);
            }
        }

        private static void SustainedReaderWorker(
            IDasFlightClient client, string sql, LatencyTracker tracker, CancellationToken ct)
        {
            int i = 0;
            while (!ct.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using (var result = client.Query(sql))
                    {
                        int rows = result.RowCount;
                    }
                    sw.Stop();
                    tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                }
                catch (OperationCanceledException) { break; }
                catch
                {
                    sw.Stop();
                    tracker.RecordError();
                }
                i++;
            }
        }

        private static void SustainedWriterWorker(
            IDasFlightClient client, int clientId, LatencyTracker tracker, CancellationToken ct)
        {
            int i = 0;
            while (!ct.IsCancellationRequested)
            {
                string sql = string.Format(
                    "INSERT INTO bench_sustained VALUES ({0}, 'sustained_{1}')",
                    clientId * 1000000 + i, i);

                var sw = Stopwatch.StartNew();
                try
                {
                    client.Execute(sql);
                    sw.Stop();
                    tracker.RecordSuccess(sw.Elapsed.TotalMilliseconds);
                }
                catch (OperationCanceledException) { break; }
                catch
                {
                    sw.Stop();
                    tracker.RecordError();
                }
                i++;
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private void RunWarmup(IDasFlightClient client, string sql)
        {
            for (int i = 0; i < warmupOps; i++)
            {
                try
                {
                    using (var result = client.Query(sql)) { }
                }
                catch { /* ignore warmup errors */ }
            }
        }
    }
}
