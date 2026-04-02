using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDbClient;

namespace DuckDbBenchmark
{
    /// <summary>
    /// Runs benchmark scenarios against a DuckDB gRPC server.
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

        public BenchmarkResult RunConcurrentReaders(int concurrency, int opsPerClient, int rowCount = 1000)
        {
            string sql = string.Format("SELECT range AS id, random() AS value FROM range(0, {0})", rowCount);

            string description = string.Format(
                "{0} threads each run {1} SELECT queries returning {2} rows.\n" +
                "  │ SQL: SELECT range AS id, random() AS value FROM range(0, {2})\n" +
                "  │ Tests: gRPC multiplexing, connection pool contention, DuckDB read parallelism",
                concurrency, opsPerClient, rowCount);

            Console.WriteLine("  Starting: {0} concurrent readers, {1} ops each, {2} rows per query...",
                concurrency, opsPerClient, rowCount);

            using (var client = new DuckDbClient.DuckDbClient(host, port))
            {
                RunWarmup(client, sql);

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                var tasks = new Task[concurrency];
                for (int i = 0; i < concurrency; i++)
                    tasks[i] = Task.Run(() => ReaderWorker(client, sql, opsPerClient, tracker));

                Task.WaitAll(tasks);
                stopwatch.Stop();

                return tracker.BuildResult(
                    string.Format("Concurrent Readers x{0} ({1} rows)", concurrency, rowCount),
                    description,
                    concurrency,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void ReaderWorker(
            IDuckDbClient client, string sql, int ops, LatencyTracker tracker)
        {
            for (int i = 0; i < ops; i++)
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
        }

        // ── Scenario 2: Concurrent Writers ───────────────────────────────────

        public BenchmarkResult RunConcurrentWriters(int concurrency, int opsPerClient)
        {
            string description = string.Format(
                "{0} threads each run {1} INSERT statements into the same table.\n" +
                "  │ SQL: INSERT INTO bench_write VALUES (id, value, label)\n" +
                "  │ Tests: WriteSerializer batching, multi-row INSERT merging, single-writer throughput\n" +
                "  │ Total inserts: {2}",
                concurrency, opsPerClient, concurrency * opsPerClient);

            Console.WriteLine("  Starting: {0} concurrent writers, {1} INSERTs each ({2} total)...",
                concurrency, opsPerClient, concurrency * opsPerClient);

            using (var client = new DuckDbClient.DuckDbClient(host, port))
            {
                client.Execute("DROP TABLE IF EXISTS bench_write");
                client.Execute("CREATE TABLE bench_write (id INTEGER, value DOUBLE, label TEXT)");

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

                using (var result = client.Query("SELECT COUNT(*) AS cnt FROM bench_write"))
                {
                    var rows = result.ToRows();
                    Console.WriteLine("  Verified: " + rows[0]["cnt"] + " rows written to bench_write");
                }

                return tracker.BuildResult(
                    string.Format("Concurrent Writers x{0}", concurrency),
                    description,
                    concurrency,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void WriterWorker(
            IDuckDbClient client, int clientId, int ops, LatencyTracker tracker)
        {
            for (int i = 0; i < ops; i++)
            {
                int id = clientId * 100000 + i;
                double value = clientId + (i * 0.001);
                string sql = string.Format(
                    "INSERT INTO bench_write VALUES ({0}, {1}, 'client_{2}_op_{3}')",
                    id, value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    clientId, i);

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

        public BenchmarkResult RunMixedWorkload(int readers, int writers, int opsPerClient, int rowCount = 100)
        {
            string readSql = string.Format("SELECT range AS id FROM range(0, {0})", rowCount);

            string description = string.Format(
                "{0} reader threads + {1} writer threads running simultaneously.\n" +
                "  │ Readers: SELECT range AS id FROM range(0, {4}) ({2} ops each)\n" +
                "  │ Writers: INSERT INTO bench_mixed VALUES (...) ({2} ops each)\n" +
                "  │ Tests: read/write contention, connection pool under mixed load\n" +
                "  │ Total operations: {3}",
                readers, writers, opsPerClient, (readers + writers) * opsPerClient, rowCount);

            Console.WriteLine("  Starting: {0} readers + {1} writers, {2} ops each...",
                readers, writers, opsPerClient);

            using (var client = new DuckDbClient.DuckDbClient(host, port))
            {
                client.Execute("DROP TABLE IF EXISTS bench_mixed");
                client.Execute("CREATE TABLE bench_mixed (id INTEGER, data TEXT)");

                var tracker = new LatencyTracker();
                var stopwatch = Stopwatch.StartNew();

                int totalClients = readers + writers;
                var tasks = new Task[totalClients];

                for (int i = 0; i < readers; i++)
                    tasks[i] = Task.Run(() => ReaderWorker(client, readSql, opsPerClient, tracker));

                for (int i = 0; i < writers; i++)
                {
                    int clientId = i;
                    tasks[readers + i] = Task.Run(() => MixedWriterWorker(client, clientId, opsPerClient, tracker));
                }

                Task.WaitAll(tasks);
                stopwatch.Stop();

                return tracker.BuildResult(
                    string.Format("Mixed {0}R + {1}W", readers, writers),
                    description,
                    totalClients,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        private static void MixedWriterWorker(
            IDuckDbClient client, int clientId, int ops, LatencyTracker tracker)
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

        public BenchmarkResult RunLargeResultSet(int rowCount, int iterations)
        {
            string sql = string.Format(
                "SELECT range AS id, random() AS val1, random() AS val2, " +
                "'row_' || range AS label FROM range(0, {0})", rowCount);

            double estimatedMb = rowCount * 36.0 / 1024 / 1024; // ~36 bytes per row estimate

            string description = string.Format(
                "Stream {0:N0} rows x 4 columns (INT, DOUBLE, DOUBLE, VARCHAR) x {1} iterations.\n" +
                "  │ Estimated data: ~{2:F0} MB per query\n" +
                "  │ SQL: SELECT range, random(), random(), 'row_'||range FROM range(0, {0})\n" +
                "  │ Tests: Arrow IPC serialization speed, gRPC streaming throughput, memory handling",
                rowCount, iterations, estimatedMb);

            Console.WriteLine("  Starting: streaming {0:N0} rows ({1:F0} MB est.) x {2} iterations...",
                rowCount, estimatedMb, iterations);

            using (var client = new DuckDbClient.DuckDbClient(host, port))
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
                    string.Format("Large Result ({0:N0} rows)", rowCount),
                    description,
                    1,
                    stopwatch.ElapsedMilliseconds);
            }
        }

        // ── Scenario 5: Find Maximum Concurrency ─────────────────────────────

        public List<BenchmarkResult> FindMaxConcurrency(
            bool isReader,
            int startConcurrency,
            int maxConcurrency,
            int step,
            int opsPerClient,
            double maxErrorRate = 0.05)
        {
            var results = new List<BenchmarkResult>();
            string kind = isReader ? "Reader" : "Writer";

            Console.WriteLine();
            Console.WriteLine("=== Finding Maximum {0} Concurrency ===", kind);
            Console.WriteLine("  Strategy: Increase parallel threads from {0} to {1} (step {2})",
                startConcurrency, maxConcurrency, step);
            Console.WriteLine("  Each level: {0} ops per thread, stop when error rate > {1:P0}",
                opsPerClient, maxErrorRate);
            if (isReader)
                Console.WriteLine("  Operation: SELECT 100 rows per query (DoGet RPC)");
            else
                Console.WriteLine("  Operation: INSERT INTO bench_write (DoAction RPC)");
            Console.WriteLine();

            for (int concurrency = startConcurrency; concurrency <= maxConcurrency; concurrency += step)
            {
                Console.Write("  {0,3} threads: ", concurrency);

                BenchmarkResult result;
                if (isReader)
                    result = RunConcurrentReaders(concurrency, opsPerClient, 100);
                else
                    result = RunConcurrentWriters(concurrency, opsPerClient);

                double errorRate = result.TotalOperations > 0
                    ? (double)result.ErrorCount / result.TotalOperations
                    : 0;

                Console.WriteLine("{0,8:F1} ops/s | {1,7:F1} ms avg | {2,7:F1} ms P99 | {3} errors ({4:P1})",
                    result.OpsPerSecond, result.AvgLatencyMs, result.P99LatencyMs, result.ErrorCount, errorRate);

                results.Add(result);

                if (errorRate > maxErrorRate)
                {
                    Console.WriteLine("  >>> Error rate {0:P1} exceeds {1:P0} threshold. Maximum reached.", errorRate, maxErrorRate);
                    break;
                }
            }

            return results;
        }

        // ── Scenario 6: Sustained Throughput ─────────────────────────────────

        public BenchmarkResult RunSustainedThroughput(
            int concurrency, int durationSeconds, bool isReader, int rowCount = 100)
        {
            string readSql = string.Format("SELECT range AS id FROM range(0, {0})", rowCount);
            string kind = isReader ? "Read" : "Write";

            string description = string.Format(
                "{0} threads running continuously for {1} seconds.\n" +
                "  │ Operation: {2}\n" +
                "  │ Tests: long-running stability, memory leaks, throughput consistency",
                concurrency, durationSeconds,
                isReader
                    ? string.Format("SELECT range AS id FROM range(0, {0}) — repeated until time expires", rowCount)
                    : "INSERT INTO bench_sustained VALUES (...) — repeated until time expires");

            Console.WriteLine("  Starting: {0} threads x {1}s sustained {2}...",
                concurrency, durationSeconds, kind.ToLower());

            using (var client = new DuckDbClient.DuckDbClient(host, port))
            {
                if (!isReader)
                {
                    client.Execute("DROP TABLE IF EXISTS bench_sustained");
                    client.Execute("CREATE TABLE bench_sustained (id INTEGER, data TEXT)");
                }

                var tracker = new LatencyTracker();
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds)))
                {
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

                    return tracker.BuildResult(
                        string.Format("Sustained {0} ({1}s, x{2})", kind, durationSeconds, concurrency),
                        description,
                        concurrency,
                        stopwatch.ElapsedMilliseconds);
                }
            }
        }

        private static void SustainedReaderWorker(
            IDuckDbClient client, string sql, LatencyTracker tracker, CancellationToken ct)
        {
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
            }
        }

        private static void SustainedWriterWorker(
            IDuckDbClient client, int clientId, LatencyTracker tracker, CancellationToken ct)
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

        private void RunWarmup(IDuckDbClient client, string sql)
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
