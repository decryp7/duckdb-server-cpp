using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DuckArrowBenchmark
{
    /// <summary>
    /// Results from a single benchmark scenario.
    /// </summary>
    public sealed class BenchmarkResult
    {
        public string ScenarioName { get; set; }
        public int ConcurrentClients { get; set; }
        public int TotalOperations { get; set; }
        public int SuccessCount { get; set; }
        public int ErrorCount { get; set; }
        public long ElapsedMs { get; set; }
        public double OpsPerSecond { get; set; }
        public double AvgLatencyMs { get; set; }
        public double MinLatencyMs { get; set; }
        public double MaxLatencyMs { get; set; }
        public double P50LatencyMs { get; set; }
        public double P95LatencyMs { get; set; }
        public double P99LatencyMs { get; set; }

        public void Print()
        {
            Console.WriteLine("──────────────────────────────────────────────────");
            Console.WriteLine("  Scenario     : " + ScenarioName);
            Console.WriteLine("  Concurrency  : " + ConcurrentClients);
            Console.WriteLine("  Operations   : " + TotalOperations);
            Console.WriteLine("  Successes    : " + SuccessCount);
            Console.WriteLine("  Errors       : " + ErrorCount);
            Console.WriteLine("  Elapsed      : " + ElapsedMs + " ms");
            Console.WriteLine("  Throughput   : " + OpsPerSecond.ToString("F1") + " ops/sec");
            Console.WriteLine("  Avg latency  : " + AvgLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  Min latency  : " + MinLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  Max latency  : " + MaxLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  P50 latency  : " + P50LatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  P95 latency  : " + P95LatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  P99 latency  : " + P99LatencyMs.ToString("F2") + " ms");
            Console.WriteLine("──────────────────────────────────────────────────");
        }
    }

    /// <summary>
    /// Tracks latency for each operation in a thread-safe way.
    /// </summary>
    public sealed class LatencyTracker
    {
        private readonly List<double> latencies = new List<double>();
        private readonly object lockObj = new object();
        private int successCount;
        private int errorCount;

        public void RecordSuccess(double latencyMs)
        {
            lock (lockObj) { latencies.Add(latencyMs); }
            Interlocked.Increment(ref successCount);
        }

        public void RecordError()
        {
            Interlocked.Increment(ref errorCount);
        }

        public BenchmarkResult BuildResult(string scenarioName, int concurrency, long elapsedMs)
        {
            var result = new BenchmarkResult
            {
                ScenarioName = scenarioName,
                ConcurrentClients = concurrency,
                SuccessCount = successCount,
                ErrorCount = errorCount,
                ElapsedMs = elapsedMs
            };

            result.TotalOperations = result.SuccessCount + result.ErrorCount;

            if (elapsedMs > 0)
                result.OpsPerSecond = (double)result.TotalOperations / elapsedMs * 1000.0;

            lock (lockObj)
            {
                if (latencies.Count > 0)
                {
                    latencies.Sort();
                    result.MinLatencyMs = latencies[0];
                    result.MaxLatencyMs = latencies[latencies.Count - 1];
                    result.AvgLatencyMs = Average(latencies);
                    result.P50LatencyMs = Percentile(latencies, 0.50);
                    result.P95LatencyMs = Percentile(latencies, 0.95);
                    result.P99LatencyMs = Percentile(latencies, 0.99);
                }
            }

            return result;
        }

        private static double Average(List<double> sorted)
        {
            double sum = 0;
            foreach (var v in sorted) sum += v;
            return sum / sorted.Count;
        }

        private static double Percentile(List<double> sorted, double p)
        {
            int index = (int)(p * (sorted.Count - 1));
            if (index < 0) index = 0;
            if (index >= sorted.Count) index = sorted.Count - 1;
            return sorted[index];
        }
    }
}
