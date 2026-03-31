using System;
using System.Collections.Generic;

namespace DuckArrowBenchmark
{
    /// <summary>
    /// DuckDB Arrow Flight Server Benchmark Tool
    ///
    /// Runs a suite of performance tests against a running server:
    ///   1. Concurrent readers at various levels
    ///   2. Concurrent writers at various levels
    ///   3. Mixed read/write workloads
    ///   4. Large result set streaming
    ///   5. Find maximum reader concurrency
    ///   6. Find maximum writer concurrency
    ///   7. Sustained throughput over time
    ///
    /// Usage:
    ///   DuckArrowBenchmark.exe [--host HOST] [--port PORT] [--quick] [--full]
    ///
    /// The server must be running before starting the benchmark.
    /// </summary>
    internal static class Program
    {
        static int Main(string[] args)
        {
            string host = "localhost";
            int port = 17777;
            bool quick = false;
            bool full = false;

            // Parse arguments.
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

            Console.WriteLine("==========================================================");
            Console.WriteLine("  DuckDB Arrow Flight Server Benchmark");
            Console.WriteLine("  Target: " + host + ":" + port);
            Console.WriteLine("  Mode: " + (full ? "full" : quick ? "quick" : "standard"));
            Console.WriteLine("==========================================================");
            Console.WriteLine();

            var runner = new BenchmarkRunner(host, port);

            // Verify server is reachable.
            if (!VerifyServer(runner, host, port))
                return 1;

            var allResults = new List<BenchmarkResult>();

            if (quick)
                RunQuickBenchmark(runner, allResults);
            else if (full)
                RunFullBenchmark(runner, allResults);
            else
                RunStandardBenchmark(runner, allResults);

            // Print summary table.
            PrintSummary(allResults);
            return 0;
        }

        // ── Quick: fast smoke test ───────────────────────────────────────────

        private static void RunQuickBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            Console.WriteLine("=== Quick Benchmark ===");
            Console.WriteLine();

            results.Add(runner.RunConcurrentReaders(1, 50, 100));
            results.Last().Print();

            results.Add(runner.RunConcurrentReaders(4, 25, 100));
            results.Last().Print();

            results.Add(runner.RunConcurrentWriters(1, 50));
            results.Last().Print();

            results.Add(runner.RunConcurrentWriters(4, 25));
            results.Last().Print();
        }

        // ── Standard: typical performance profile ────────────────────────────

        private static void RunStandardBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            // --- Readers at increasing concurrency ---
            Console.WriteLine("=== Concurrent Readers ===");
            Console.WriteLine();

            int[] readerLevels = { 1, 2, 4, 8, 16, 32 };
            foreach (int level in readerLevels)
            {
                var result = runner.RunConcurrentReaders(level, 50, 1000);
                result.Print();
                results.Add(result);
            }

            // --- Writers at increasing concurrency ---
            Console.WriteLine();
            Console.WriteLine("=== Concurrent Writers ===");
            Console.WriteLine();

            int[] writerLevels = { 1, 2, 4, 8, 16, 32 };
            foreach (int level in writerLevels)
            {
                var result = runner.RunConcurrentWriters(level, 50);
                result.Print();
                results.Add(result);
            }

            // --- Mixed workloads ---
            Console.WriteLine();
            Console.WriteLine("=== Mixed Read/Write Workloads ===");
            Console.WriteLine();

            results.Add(runner.RunMixedWorkload(readers: 8, writers: 2, opsPerClient: 30));
            results.Last().Print();

            results.Add(runner.RunMixedWorkload(readers: 4, writers: 4, opsPerClient: 30));
            results.Last().Print();

            results.Add(runner.RunMixedWorkload(readers: 2, writers: 8, opsPerClient: 30));
            results.Last().Print();

            // --- Large result sets ---
            Console.WriteLine();
            Console.WriteLine("=== Large Result Sets ===");
            Console.WriteLine();

            results.Add(runner.RunLargeResultSet(rowCount: 10000, iterations: 10));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 100000, iterations: 5));
            results.Last().Print();

            results.Add(runner.RunLargeResultSet(rowCount: 1000000, iterations: 3));
            results.Last().Print();
        }

        // ── Full: everything + max concurrency + sustained ───────────────────

        private static void RunFullBenchmark(BenchmarkRunner runner, List<BenchmarkResult> results)
        {
            // Run the standard suite first.
            RunStandardBenchmark(runner, results);

            // --- Find max reader concurrency ---
            var readerResults = runner.FindMaxConcurrency(
                isReader: true,
                startConcurrency: 4,
                maxConcurrency: 256,
                step: 4,
                opsPerClient: 20,
                maxErrorRate: 0.05);
            results.AddRange(readerResults);

            // --- Find max writer concurrency ---
            var writerResults = runner.FindMaxConcurrency(
                isReader: false,
                startConcurrency: 4,
                maxConcurrency: 256,
                step: 4,
                opsPerClient: 20,
                maxErrorRate: 0.05);
            results.AddRange(writerResults);

            // --- Sustained throughput: 30 seconds each ---
            Console.WriteLine();
            Console.WriteLine("=== Sustained Throughput (30 seconds each) ===");
            Console.WriteLine();

            var sustainedRead = runner.RunSustainedThroughput(
                concurrency: 8, durationSeconds: 30, isReader: true);
            sustainedRead.Print();
            results.Add(sustainedRead);

            var sustainedWrite = runner.RunSustainedThroughput(
                concurrency: 8, durationSeconds: 30, isReader: false);
            sustainedWrite.Print();
            results.Add(sustainedWrite);

            // --- High concurrency readers ---
            Console.WriteLine();
            Console.WriteLine("=== High Concurrency Readers ===");
            Console.WriteLine();

            int[] highLevels = { 64, 128, 256 };
            foreach (int level in highLevels)
            {
                var result = runner.RunConcurrentReaders(level, 10, 100);
                result.Print();
                results.Add(result);
            }
        }

        // ── Summary table ────────────────────────────────────────────────────

        private static void PrintSummary(List<BenchmarkResult> results)
        {
            Console.WriteLine();
            Console.WriteLine("==========================================================");
            Console.WriteLine("  SUMMARY");
            Console.WriteLine("==========================================================");
            Console.WriteLine();
            Console.WriteLine(string.Format(
                "{0,-35} {1,6} {2,10} {3,10} {4,10} {5,6}",
                "Scenario", "Conc", "Ops/s", "Avg(ms)", "P99(ms)", "Errs"));
            Console.WriteLine(new string('-', 85));

            foreach (var r in results)
            {
                Console.WriteLine(string.Format(
                    "{0,-35} {1,6} {2,10:F1} {3,10:F2} {4,10:F2} {5,6}",
                    Truncate(r.ScenarioName, 35),
                    r.ConcurrentClients,
                    r.OpsPerSecond,
                    r.AvgLatencyMs,
                    r.P99LatencyMs,
                    r.ErrorCount));
            }

            Console.WriteLine();

            // Find peak throughput for reads and writes.
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

            if (bestRead != null)
            {
                Console.WriteLine(string.Format(
                    "  Peak read throughput:  {0:F1} ops/s at {1} concurrent clients",
                    bestRead.OpsPerSecond, bestRead.ConcurrentClients));
            }
            if (bestWrite != null)
            {
                Console.WriteLine(string.Format(
                    "  Peak write throughput: {0:F1} ops/s at {1} concurrent clients",
                    bestWrite.OpsPerSecond, bestWrite.ConcurrentClients));
            }
            Console.WriteLine();
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static bool VerifyServer(BenchmarkRunner runner, string host, int port)
        {
            Console.Write("Connecting to server... ");
            try
            {
                using (var client = new DasFlightClient(host, port))
                {
                    client.Ping();
                }
                Console.WriteLine("OK");
                Console.WriteLine();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("FAILED");
                Console.Error.WriteLine("Could not connect to " + host + ":" + port);
                Console.Error.WriteLine("Error: " + ex.Message);
                Console.Error.WriteLine();
                Console.Error.WriteLine("Make sure the server is running:");
                Console.Error.WriteLine("  DuckArrowServer.exe --port " + port);
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
            Console.WriteLine("DuckDB Arrow Flight Server Benchmark");
            Console.WriteLine();
            Console.WriteLine("Usage: DuckArrowBenchmark.exe [options]");
            Console.WriteLine();
            Console.WriteLine("  --host <addr>   Server address (default: localhost)");
            Console.WriteLine("  --port <n>      Server port (default: 17777)");
            Console.WriteLine("  --quick         Fast smoke test (4 scenarios)");
            Console.WriteLine("  --full          Full suite including max concurrency search");
            Console.WriteLine("  --help          Show this message");
            Console.WriteLine();
            Console.WriteLine("The server must be running before starting the benchmark.");
        }
    }

    // Extension method for List (C# 7.3 doesn't have LINQ .Last() without System.Linq).
    internal static class ListExtensions
    {
        public static T Last<T>(this List<T> list)
        {
            return list[list.Count - 1];
        }
    }
}
