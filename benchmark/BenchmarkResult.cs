using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDbBenchmark
{
    /// <summary>
    /// Results from a single benchmark scenario.
    /// </summary>
    public sealed class BenchmarkResult
    {
        public string ScenarioName { get; set; }
        public string Description { get; set; }
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
        public string ErrorDetails { get; set; }

        public void Print()
        {
            Console.WriteLine("  ┌─────────────────────────────────────────────────────");
            Console.WriteLine("  │ " + ScenarioName);
            if (!string.IsNullOrEmpty(Description))
                Console.WriteLine("  │ " + Description);
            Console.WriteLine("  ├─────────────────────────────────────────────────────");
            Console.WriteLine("  │ Concurrency  : " + ConcurrentClients + " parallel threads");
            Console.WriteLine("  │ Operations   : " + TotalOperations + " total (" + SuccessCount + " ok, " + ErrorCount + " failed)");
            Console.WriteLine("  │ Elapsed      : " + FormatDuration(ElapsedMs));
            Console.WriteLine("  │ Throughput   : " + OpsPerSecond.ToString("F1") + " operations/second");
            Console.WriteLine("  │ Latency avg  : " + AvgLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  │ Latency min  : " + MinLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  │ Latency max  : " + MaxLatencyMs.ToString("F2") + " ms");
            Console.WriteLine("  │ Latency P50  : " + P50LatencyMs.ToString("F2") + " ms (half of requests faster than this)");
            Console.WriteLine("  │ Latency P95  : " + P95LatencyMs.ToString("F2") + " ms (95% of requests faster than this)");
            Console.WriteLine("  │ Latency P99  : " + P99LatencyMs.ToString("F2") + " ms (99% of requests faster than this)");
            if (ErrorCount > 0 && !string.IsNullOrEmpty(ErrorDetails))
            {
                Console.WriteLine("  │ Error breakdown:");
                foreach (var line in ErrorDetails.Split('\n'))
                {
                    if (!string.IsNullOrEmpty(line.Trim()))
                        Console.WriteLine("  │   " + line.Trim());
                }
            }
            Console.WriteLine("  └─────────────────────────────────────────────────────");
            Console.WriteLine();
        }

        private static string FormatDuration(long ms)
        {
            if (ms < 1000) return ms + " ms";
            if (ms < 60000) return (ms / 1000.0).ToString("F1") + " seconds";
            return (ms / 60000.0).ToString("F1") + " minutes";
        }
    }

    /// <summary>
    /// Tracks latency for each operation in a thread-safe way.
    /// </summary>
    public sealed class LatencyTracker
    {
        private readonly List<double> latencies = new List<double>();
        private readonly object lockObj = new object();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> errorMessages
            = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
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

        public void RecordError(string message)
        {
            Interlocked.Increment(ref errorCount);
            if (!string.IsNullOrEmpty(message))
            {
                // Truncate long messages and track frequency
                string key = message.Length > 80 ? message.Substring(0, 80) : message;
                errorMessages.AddOrUpdate(key, 1, (k, v) => v + 1);
            }
        }

        public BenchmarkResult BuildResult(string scenarioName, string description, int concurrency, long elapsedMs)
        {
            var result = new BenchmarkResult
            {
                ScenarioName = scenarioName,
                Description = description,
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

            // Attach error details if any
            if (errorMessages.Count > 0)
            {
                var sb = new System.Text.StringBuilder();
                foreach (var kvp in errorMessages)
                    sb.AppendFormat("    [{0}x] {1}\n", kvp.Value, kvp.Key);
                result.ErrorDetails = sb.ToString();
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
