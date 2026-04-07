using System;
using System.Collections.Generic;
using DuckDbClient;

namespace DuckDbBenchmark
{
    internal static class Program
    {
        static int Main(string[] args)
        {
            string host = "localhost";
            int port = 19100;
            bool quick = false;
            bool full = false;

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--host":
                        if (i + 1 < args.Length) host = args[++i];
                        break;
                    case "--port":
                        if (i + 1 < args.Length) port = int.Parse(args[++i]);
                        break;
                    case "--quick":
                        quick = true;
                        break;
                    case "--full":
                        full = true;
                        break;
                    case "--help":
                        PrintUsage();
                        return 0;
                }
            }

            Console.WriteLine("==========================================================================");
            Console.WriteLine("  DuckDB gRPC Server v5.4 — Performance Benchmark");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();
            Console.WriteLine("  Target server  : {0}:{1}", host, port);
            Console.WriteLine("  Mode           : {0}", full ? "FULL (all tests + max concurrency + sustained)" : quick ? "QUICK (smoke test)" : "STANDARD");
            Console.WriteLine("  Protocol       : custom gRPC (proto/duckdb_service.proto)");
            Console.WriteLine("  Encoding       : columnar protobuf (packed arrays per column)");
            Console.WriteLine("  Client         : single shared DuckDbClient (HTTP/2 multiplexing)");
            Console.WriteLine();
            Console.WriteLine("  Server features:");
            Console.WriteLine("    - Sharding:    N independent DuckDB instances (read-all/write-all)");
            Console.WriteLine("    - Caching:     LRU query cache with TTL (cache hit = 0.1ms)");
            Console.WriteLine("    - Batching:    concurrent writes batched into transactions");
            Console.WriteLine("    - DuckDB:      threads=1/conn, preserve_insertion_order=false");
            Console.WriteLine("    - Fast path:   INSERT via Execute bypasses write queue");
            Console.WriteLine("    - Appender:    BulkInsert uses DuckDB Appender API (10-100x)");
            Console.WriteLine();
            Console.WriteLine("  What is measured:");
            Console.WriteLine("    - Throughput:  operations per second (higher = better)");
            Console.WriteLine("    - Latency:     time per operation in ms (lower = better)");
            Console.WriteLine("    - P50/P95/P99: percentile latencies (tail latency)");
            Console.WriteLine("    - Errors:      failed operations (should be 0)");
            Console.WriteLine();
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            var runner = new BenchmarkRunner(host, port);

            if (!VerifyServer(runner, host, port))
                return 1;

            var allResults = new List<BenchmarkResult>();

            if (quick)
                RunQuickBenchmark(runner, allResults);
            else if (full)
                RunFullBenchmark(runner, allResults);
            else
                RunStandardBenchmark(runner, allResults);

            PrintSummary(allResults);
            return 0;
        }

        // ── Quick ────────────────────────────────────────────────────────────

        private static void RunQuickBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  QUICK BENCHMARK — Smoke test (4 scenarios)");
            Console.WriteLine("  Purpose: verify server works and get a rough performance baseline");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            Console.WriteLine("--- Test 1: Single reader ---");
            Console.WriteLine("  1 thread sends 50 SELECT queries, each returning 100 rows.");
            Console.WriteLine("  Measures: baseline single-query latency without contention.");
            results.Add(runner.RunConcurrentReaders(1, 50, 100));
            results.Last().Print();

            Console.WriteLine("--- Test 2: 10 concurrent readers ---");
            Console.WriteLine("  10 threads each send 25 SELECT queries (250 total), 100 rows each.");
            Console.WriteLine("  Measures: read throughput under moderate parallelism.");
            results.Add(runner.RunConcurrentReaders(10, 25, 100));
            results.Last().Print();

            Console.WriteLine("--- Test 3: Single writer ---");
            Console.WriteLine("  1 thread sends 50 INSERT statements into bench_write.");
            Console.WriteLine("  Measures: baseline single-writer latency.");
            results.Add(runner.RunConcurrentWriters(1, 50));
            results.Last().Print();

            Console.WriteLine("--- Test 4: 10 concurrent writers ---");
            Console.WriteLine("  10 threads each send 25 INSERT statements (250 total).");
            Console.WriteLine("  Measures: write batching effectiveness under contention.");
            results.Add(runner.RunConcurrentWriters(10, 25));
            results.Last().Print();

            Console.WriteLine("--- Test 5: Cache hit performance ---");
            Console.WriteLine("  10 threads send pre-cached query 50 times each.");
            Console.WriteLine("  Measures: QueryCache throughput (should be ~0.1ms/op).");
            results.Add(runner.RunCacheHitTest(10, 50, 100));
            results.Last().Print();

            Console.WriteLine("--- Test 6: INSERT fast path ---");
            Console.WriteLine("  10 threads send 25 INSERTs via concurrent append path.");
            Console.WriteLine("  Measures: INSERT bypass of WriteSerializer queue.");
            results.Add(runner.RunConcurrentInserts(10, 25));
            results.Last().Print();
        }

        // ── Standard ─────────────────────────────────────────────────────────

        private static void RunStandardBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            // --- Readers ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  CONCURRENT READERS — Scaling test");
            Console.WriteLine("  Each thread sends 20 SELECT queries returning 1000 rows (2 columns).");
            Console.WriteLine("  Scaling: 1 → 5 → 10 → 25 → 50 → 100 → 150 → 200 → 250 → 300 threads");
            Console.WriteLine("  Purpose: find the throughput curve and saturation point for reads.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            int[] readerLevels = { 1, 5, 10, 25, 50, 100, 150, 200, 250, 300 };
            foreach (int level in readerLevels)
            {
                var result = runner.RunConcurrentReaders(level, 20, 1000);
                result.Print();
                results.Add(result);
            }

            // --- Writers ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  CONCURRENT WRITERS — Scaling test");
            Console.WriteLine("  Each thread sends 20 INSERT statements into bench_write.");
            Console.WriteLine("  The server's WriteSerializer batches these into transactions and");
            Console.WriteLine("  merges compatible INSERTs into multi-row statements automatically.");
            Console.WriteLine("  Scaling: 1 → 5 → 10 → 25 → 50 → 100 → 150 → 200 → 250 → 300 threads");
            Console.WriteLine("  Purpose: find the throughput curve for writes with batching.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            int[] writerLevels = { 1, 5, 10, 25, 50, 100, 150, 200, 250, 300 };
            foreach (int level in writerLevels)
            {
                var result = runner.RunConcurrentWriters(level, 20);
                result.Print();
                results.Add(result);
            }

            // --- Mixed ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  MIXED READ/WRITE — Contention test");
            Console.WriteLine("  Reader and writer threads run simultaneously on the same server.");
            Console.WriteLine("  Readers: SELECT 100 rows | Writers: INSERT INTO bench_mixed");
            Console.WriteLine("  Purpose: measure performance degradation when reads and writes compete.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunMixedWorkload(readers: 50, writers: 10, opsPerClient: 20));
            results.Last().Print();

            results.Add(runner.RunMixedWorkload(readers: 100, writers: 50, opsPerClient: 15));
            results.Last().Print();

            results.Add(runner.RunMixedWorkload(readers: 150, writers: 150, opsPerClient: 10));
            results.Last().Print();

            results.Add(runner.RunMixedWorkload(readers: 50, writers: 250, opsPerClient: 10));
            results.Last().Print();

            // --- Large results ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  LARGE RESULT SETS — Streaming throughput test");
            Console.WriteLine("  Single client streams increasingly large query results.");
            Console.WriteLine("  Query: SELECT id, val1, val2, label FROM range(0, N)");
            Console.WriteLine("  Columns: INT64 + DOUBLE + DOUBLE + VARCHAR (~36 bytes/row)");
            Console.WriteLine("  Purpose: measure Arrow IPC serialization and gRPC streaming speed.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunLargeResultSet(rowCount: 100000, iterations: 5));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 1000000, iterations: 3));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 5000000, iterations: 2));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 12000000, iterations: 2));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 24000000, iterations: 1));
            results.Last().Print();

            // --- Cache performance ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  CACHE HIT PERFORMANCE — QueryCache throughput test");
            Console.WriteLine("  Pre-cache a query, then measure throughput of cache-only responses.");
            Console.WriteLine("  Purpose: verify cache hit latency is near-zero (~0.1ms).");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunCacheHitTest(1, 100, 100));
            results.Last().Print();

            results.Add(runner.RunCacheHitTest(10, 100, 100));
            results.Last().Print();

            results.Add(runner.RunCacheHitTest(50, 50, 100));
            results.Last().Print();

            // --- INSERT fast path vs UPDATE serialized ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  INSERT FAST PATH vs UPDATE SERIALIZED");
            Console.WriteLine("  INSERT: bypasses WriteSerializer, executes directly on bulk connection.");
            Console.WriteLine("  UPDATE: goes through WriteSerializer batch queue.");
            Console.WriteLine("  Purpose: measure the performance gain from concurrent append fast path.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunConcurrentInserts(1, 50));
            results.Last().Print();

            results.Add(runner.RunConcurrentInserts(10, 25));
            results.Last().Print();

            results.Add(runner.RunConcurrentInserts(50, 20));
            results.Last().Print();

            results.Add(runner.RunConcurrentUpdates(1, 50));
            results.Last().Print();

            results.Add(runner.RunConcurrentUpdates(10, 25));
            results.Last().Print();

            // --- BulkInsert (Appender API) ---
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  BULKINSERT (APPENDER API) — Batch insertion throughput");
            Console.WriteLine("  Uses the BulkInsert RPC which bypasses SQL parsing entirely.");
            Console.WriteLine("  Purpose: measure Appender API throughput for batch data loading.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunBulkInsert(rowCount: 1000, iterations: 10));
            results.Last().Print();

            results.Add(runner.RunBulkInsert(rowCount: 10000, iterations: 5));
            results.Last().Print();

            results.Add(runner.RunBulkInsert(rowCount: 100000, iterations: 3));
            results.Last().Print();
        }

        // ── Full ─────────────────────────────────────────────────────────────

        private static void RunFullBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            RunStandardBenchmark(runner, results);

            // --- Max concurrency ---
            var readerResults = runner.FindMaxConcurrency(
                isReader: true, startConcurrency: 10, maxConcurrency: 300,
                step: 10, opsPerClient: 10, maxErrorRate: 0.05);
            results.AddRange(readerResults);

            var writerResults = runner.FindMaxConcurrency(
                isReader: false, startConcurrency: 10, maxConcurrency: 300,
                step: 10, opsPerClient: 10, maxErrorRate: 0.05);
            results.AddRange(writerResults);

            // --- Sustained ---
            Console.WriteLine();
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  SUSTAINED THROUGHPUT — Stability test (60 seconds each)");
            Console.WriteLine("  Threads run continuously for 60 seconds without stopping.");
            Console.WriteLine("  Purpose: detect memory leaks, GC pressure, and throughput degradation.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            var sr = runner.RunSustainedThroughput(concurrency: 50, durationSeconds: 60, isReader: true);
            sr.Print();
            results.Add(sr);

            var sw = runner.RunSustainedThroughput(concurrency: 50, durationSeconds: 60, isReader: false);
            sw.Print();
            results.Add(sw);

            var sm = runner.RunSustainedThroughput(concurrency: 100, durationSeconds: 60, isReader: true);
            sm.Print();
            results.Add(sm);

            Console.WriteLine();
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  LARGE RESULT SET — Maximum data transfer");
            Console.WriteLine("  Stream 24 million rows through the Flight protocol.");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();

            results.Add(runner.RunLargeResultSet(rowCount: 24000000, iterations: 1));
            results.Last().Print();
        }

        // ── Summary ──────────────────────────────────────────────────────────

        private static void PrintSummary(List<BenchmarkResult> results)
        {
            Console.WriteLine();
            Console.WriteLine("==========================================================================");
            Console.WriteLine("  SUMMARY TABLE");
            Console.WriteLine("==========================================================================");
            Console.WriteLine();
            Console.WriteLine(string.Format(
                "  {0,-40} {1,5} {2,10} {3,10} {4,10} {5,5}",
                "Scenario", "Conc", "Ops/s", "Avg(ms)", "P99(ms)", "Err"));
            Console.WriteLine("  " + new string('-', 88));

            foreach (var r in results)
            {
                Console.WriteLine(string.Format(
                    "  {0,-40} {1,5} {2,10:F1} {3,10:F2} {4,10:F2} {5,5}",
                    Truncate(r.ScenarioName, 40),
                    r.ConcurrentClients,
                    r.OpsPerSecond,
                    r.AvgLatencyMs,
                    r.P99LatencyMs,
                    r.ErrorCount));
            }

            Console.WriteLine();

            BenchmarkResult bestRead = null;
            BenchmarkResult bestWrite = null;
            foreach (var r in results)
            {
                if (r.ScenarioName.Contains("Reader") || r.ScenarioName.Contains("Read"))
                {
                    if (bestRead == null || r.OpsPerSecond > bestRead.OpsPerSecond)
                        bestRead = r;
                }
                if (r.ScenarioName.Contains("Writer") || r.ScenarioName.Contains("Write"))
                {
                    if (bestWrite == null || r.OpsPerSecond > bestWrite.OpsPerSecond)
                        bestWrite = r;
                }
            }

            Console.WriteLine("  KEY FINDINGS:");
            if (bestRead != null)
                Console.WriteLine(string.Format(
                    "    Peak read throughput  : {0:F1} ops/s at {1} concurrent threads",
                    bestRead.OpsPerSecond, bestRead.ConcurrentClients));
            if (bestWrite != null)
                Console.WriteLine(string.Format(
                    "    Peak write throughput : {0:F1} ops/s at {1} concurrent threads",
                    bestWrite.OpsPerSecond, bestWrite.ConcurrentClients));
            Console.WriteLine();
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static bool VerifyServer(BenchmarkRunner runner, string host, int port)
        {
            Console.Write("  Connecting to {0}:{1}... ", host, port);
            try
            {
                using (var client = new DuckDbClient.DuckDbClient(host, port))
                {
                    client.Ping();
                    string stats = client.GetStats();
                    Console.WriteLine("OK");
                    Console.WriteLine("  Server stats: " + stats);
                }
                Console.WriteLine();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("FAILED");
                Console.Error.WriteLine();
                Console.Error.WriteLine("  Could not connect to " + host + ":" + port);
                Console.Error.WriteLine("  Error: " + ex.Message);
                Console.Error.WriteLine();
                Console.Error.WriteLine("  Make sure the server is running:");
                Console.Error.WriteLine("    DuckDbServer.exe --port " + port);
                return false;
            }
        }

        private static string Truncate(string s, int maxLen)
        {
            if (s.Length <= maxLen) return s;
            return s.Substring(0, maxLen - 3) + "...";
        }

        private static void PrintUsage()
        {
            Console.WriteLine("DuckDB gRPC Server — Performance Benchmark");
            Console.WriteLine();
            Console.WriteLine("Usage: DuckDbBenchmark.exe [options]");
            Console.WriteLine();
            Console.WriteLine("  --host <addr>   Server address (default: localhost)");
            Console.WriteLine("  --port <n>      Server port (default: 19100)");
            Console.WriteLine("  --quick         Fast smoke test (4 scenarios, ~30s)");
            Console.WriteLine("  --full          Full suite + max concurrency + sustained (~15min)");
            Console.WriteLine("  --help          Show this message");
            Console.WriteLine();
            Console.WriteLine("Modes:");
            Console.WriteLine("  (default)       Standard suite: scaling readers/writers 1-300,");
            Console.WriteLine("                  mixed workloads, large results up to 24M rows");
            Console.WriteLine("  --quick         4 quick tests to verify server works");
            Console.WriteLine("  --full          Standard + find max concurrency + 60s sustained");
            Console.WriteLine();
            Console.WriteLine("The server must be running before starting the benchmark.");
        }
    }

    internal static class ListExtensions
    {
        public static T Last<T>(this List<T> list)
        {
            return list[list.Count - 1];
        }
    }
}
