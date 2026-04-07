using System;

namespace DuckDbServer
{
    /// <summary>
    /// Configuration for the DuckDB gRPC server. All fields have sensible defaults
    /// that auto-scale to the host machine.
    ///
    /// <para><b>Thread Safety:</b> This class is NOT thread-safe. It is populated once
    /// during startup (from CLI arguments) and then treated as immutable for the lifetime
    /// of the server. No locking is needed because the config is fully initialized before
    /// any server threads are created.</para>
    /// </summary>
    public sealed class ServerConfig
    {
        /// <summary>
        /// DuckDB database file path, or <c>":memory:"</c> for an in-process database.
        ///
        /// <para><b>In-memory mode:</b> Creates a transient database that exists only for the
        /// lifetime of the server process. Data is lost on shutdown. All shards create
        /// independent in-memory databases; consistency is maintained by fan-out writes.</para>
        ///
        /// <para><b>File mode:</b> Creates or opens a persistent DuckDB database file. For
        /// multi-shard configurations, each shard gets a separate file (e.g., <c>data_0.db</c>,
        /// <c>data_1.db</c>).</para>
        ///
        /// <para><b>CLI flag:</b> <c>--db &lt;path&gt;</c></para>
        /// </summary>
        public string DbPath { get; set; } = ":memory:";

        /// <summary>
        /// Network address to bind the gRPC server to.
        ///
        /// <para><b>Common values:</b></para>
        /// <list type="bullet">
        ///   <item><description><c>"0.0.0.0"</c> (default): Listen on all network interfaces (accessible from other machines).</description></item>
        ///   <item><description><c>"127.0.0.1"</c> or <c>"localhost"</c>: Listen only on loopback (local access only).</description></item>
        ///   <item><description>A specific IP address: Listen only on that interface.</description></item>
        /// </list>
        ///
        /// <para><b>CLI flag:</b> <c>--host &lt;addr&gt;</c></para>
        /// </summary>
        public string Host { get; set; } = "0.0.0.0";

        /// <summary>
        /// TCP port for the gRPC server to listen on.
        ///
        /// <para><b>Default:</b> 19100. Choose a port not used by other services.
        /// Ports below 1024 require root/administrator privileges on most systems.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--port &lt;n&gt;</c></para>
        /// </summary>
        public int Port { get; set; } = 19100;

        /// <summary>
        /// Total number of DuckDB read connections in the pool (across all shards).
        /// Each connection can serve one concurrent read query.
        ///
        /// <para><b>Default (0):</b> Auto-configures to <c>2 x logical CPU count</c>
        /// (e.g., 16 connections on an 8-core machine). This provides enough concurrency
        /// for most workloads without over-subscribing CPU or memory.</para>
        ///
        /// <para><b>Performance impact:</b> More connections = more concurrent queries,
        /// but also more memory usage (each DuckDB connection has its own buffer manager).
        /// Too many connections can cause memory pressure and GC overhead.</para>
        ///
        /// <para><b>Recommended:</b> 2x-4x CPU count for read-heavy workloads. If queries
        /// are I/O-bound (scanning large Parquet files), higher values help. If queries are
        /// CPU-bound, diminishing returns above 2x CPU count.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--readers &lt;n&gt;</c></para>
        /// </summary>
        public int ReaderPoolSize { get; set; } = 0;

        /// <summary>
        /// Write batch accumulation window in milliseconds. After the first write request
        /// arrives, the writer thread waits this long for additional requests before
        /// executing the batch.
        ///
        /// <para><b>Trade-off:</b> Longer windows accumulate more requests per transaction
        /// (higher throughput via amortized transaction overhead), but increase write latency
        /// (each caller blocks longer).</para>
        ///
        /// <para><b>Recommended:</b> 1-10 ms. At 5 ms (default), a burst of 100 concurrent
        /// INSERTs is typically merged into a single transaction, yielding ~10x throughput
        /// improvement over individual commits.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--batch-ms &lt;ms&gt;</c></para>
        /// </summary>
        public int WriteBatchMs { get; set; } = 5;

        /// <summary>
        /// Maximum number of DML statements per write transaction. Limits memory usage
        /// and transaction size.
        ///
        /// <para><b>Performance impact:</b> Larger batches amortize transaction overhead
        /// better, but increase the blast radius of a single failing statement (the entire
        /// batch must be retried individually). Very large batches may also exceed DuckDB's
        /// WAL size limits.</para>
        ///
        /// <para><b>Recommended:</b> 256-1024. Default: 512.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--batch-max &lt;n&gt;</c></para>
        /// </summary>
        public int WriteBatchMax { get; set; } = 512;

        /// <summary>
        /// Number of rows per columnar batch when streaming query results to the client.
        ///
        /// <para><b>Trade-off:</b></para>
        /// <list type="bullet">
        ///   <item><description><b>Larger (e.g., 65536):</b> Fewer gRPC messages = less framing
        ///     overhead = higher throughput for large result sets. But increases memory usage
        ///     and latency-to-first-byte.</description></item>
        ///   <item><description><b>Smaller (e.g., 1024):</b> Lower latency-to-first-byte and
        ///     memory usage, but more gRPC messages and framing overhead.</description></item>
        /// </list>
        ///
        /// <para><b>Recommended:</b> 4096-16384. Default: 8192 (a good balance for most
        /// analytics queries returning 1K-1M rows).</para>
        ///
        /// <para><b>CLI flag:</b> <c>--batch-size &lt;n&gt;</c></para>
        /// </summary>
        public int BatchSize { get; set; } = 8192;

        /// <summary>
        /// DuckDB memory limit string (e.g., <c>"4GB"</c>, <c>"512MB"</c>).
        /// Controls the maximum RAM DuckDB uses for buffer management and query processing.
        ///
        /// <para><b>Default (empty):</b> DuckDB uses its own default, which is approximately
        /// 80% of system RAM. This can be problematic in containerized environments or when
        /// running alongside other services.</para>
        ///
        /// <para><b>Recommended:</b> Set explicitly in production. Leave 2-4 GB for the
        /// .NET runtime, GC, gRPC buffers, and OS. For a 16 GB machine, <c>"10GB"</c> is
        /// a reasonable starting point.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--memory-limit &lt;s&gt;</c></para>
        /// </summary>
        public string MemoryLimit { get; set; } = "";

        /// <summary>
        /// DuckDB internal thread count per connection. Controls how many threads DuckDB
        /// uses for parallel query execution within a single query.
        ///
        /// <para><b>Default (0):</b> Auto-configures to 1 thread per connection. This is
        /// optimal for server workloads where parallelism comes from the connection pool
        /// (N concurrent queries, each single-threaded), not from DuckDB's internal
        /// parallelism.</para>
        ///
        /// <para><b>When to increase:</b> If the server handles very few concurrent queries
        /// but each query scans large tables, increasing threads allows DuckDB to parallelize
        /// within a single query. Typical values: 2-4 for mixed workloads.</para>
        ///
        /// <para><b>Gotcha:</b> With N connections and T threads per connection, DuckDB may
        /// create up to N*T threads, leading to severe CPU over-subscription if both are large.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--threads &lt;n&gt;</c></para>
        /// </summary>
        public int DuckDbThreads { get; set; } = 0;

        /// <summary>
        /// Number of DuckDB shards (independent database instances). Each shard has its
        /// own connection pool, writer thread, and DuckDB instance.
        ///
        /// <para><b>How it works:</b> Reads are distributed round-robin across shards.
        /// Writes are fanned out to ALL shards. This means N shards provide approximately
        /// N times the read throughput, but write throughput is unchanged.</para>
        ///
        /// <para><b>Default (1):</b> Single database, no sharding overhead.</para>
        ///
        /// <para><b>Recommended for maximum read throughput:</b> Set to <c>nCPU / 2</c>.
        /// With 16 CPUs and 8 shards, each shard gets 2 connections (16 / 8 = 2), and
        /// the total read parallelism is 16 concurrent queries across 8 independent DuckDB
        /// instances.</para>
        ///
        /// <para><b>Performance impact:</b> Sharding eliminates DuckDB's internal lock
        /// contention that occurs when many connections share a single database instance.
        /// The improvement is most noticeable for short queries (< 10 ms) where lock overhead
        /// dominates execution time.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--shards &lt;n&gt;</c></para>
        /// </summary>
        public int Shards { get; set; } = 1;

        /// <summary>
        /// Query timeout in seconds. Queries that take longer than this are aborted with
        /// a <c>DEADLINE_EXCEEDED</c> gRPC status code.
        ///
        /// <para><b>Default:</b> 30 seconds. Set to 0 to disable timeouts (not recommended
        /// in production -- a single runaway query can hold a connection indefinitely).</para>
        ///
        /// <para><b>Gotcha:</b> The timeout does NOT cancel the underlying DuckDB query.
        /// The query continues running in the background until it finishes, consuming its
        /// connection. Under sustained timeout conditions, the connection pool can be starved.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--timeout &lt;n&gt;</c></para>
        /// </summary>
        public int QueryTimeoutSeconds { get; set; } = 30;

        /// <summary>Path to temp directory for DuckDB spill-to-disk. Empty = DuckDB default.</summary>
        public string TempDirectory { get; set; } = "";

        /// <summary>
        /// Path to a TLS certificate PEM file for encrypted gRPC connections.
        ///
        /// <para><b>Default (empty):</b> Plaintext gRPC (no encryption). Suitable for
        /// localhost or VPN-protected networks.</para>
        ///
        /// <para><b>Usage:</b> Must be set together with <see cref="TlsKeyPath"/>. Both
        /// must be provided, or both must be empty.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--tls-cert &lt;path&gt;</c></para>
        /// </summary>
        public string TlsCertPath { get; set; } = "";

        /// <summary>
        /// Path to a TLS private key PEM file for encrypted gRPC connections.
        ///
        /// <para><b>Default (empty):</b> Plaintext gRPC (no encryption).</para>
        ///
        /// <para><b>Usage:</b> Must be set together with <see cref="TlsCertPath"/>. Both
        /// must be provided, or both must be empty.</para>
        ///
        /// <para><b>CLI flag:</b> <c>--tls-key &lt;path&gt;</c></para>
        /// </summary>
        public string TlsKeyPath { get; set; } = "";
    }

    /// <summary>
    /// Snapshot of live server metrics. A value type (struct) because it is returned
    /// by value and does not need heap allocation.
    ///
    /// <para><b>Thread Safety:</b> Individual fields are NOT atomically consistent
    /// with each other. Each counter is read via <see cref="System.Threading.Interlocked.Read"/>
    /// in the caller, but the snapshot as a whole is not atomic. This is acceptable
    /// for monitoring purposes.</para>
    /// </summary>
    public struct ServerStats
    {
        /// <summary>Total number of successfully completed read queries (including cache hits).</summary>
        public long QueriesRead;

        /// <summary>Total number of successfully completed write operations.</summary>
        public long QueriesWrite;

        /// <summary>Total number of failed operations (query errors, timeouts, etc.).</summary>
        public long Errors;

        /// <summary>Total number of read connections across all shards.</summary>
        public int ReaderPoolSize;

        /// <summary>The gRPC port the server is listening on.</summary>
        public int Port;
    }

    /// <summary>
    /// Result of a write (DML/DDL) operation. A value type (struct) to avoid heap
    /// allocation for the common success case.
    ///
    /// <para><b>Usage pattern:</b></para>
    /// <code>
    /// var result = writer.Submit("INSERT INTO t VALUES (1)");
    /// if (!result.Ok) Console.Error.WriteLine(result.Error);
    /// </code>
    /// </summary>
    public struct WriteResult
    {
        /// <summary><c>true</c> if the write succeeded; <c>false</c> if it failed.</summary>
        public bool Ok;

        /// <summary>Error message on failure; <c>null</c> on success.</summary>
        public string Error;

        /// <summary>Creates a success result with <c>Ok = true</c> and <c>Error = null</c>.</summary>
        /// <returns>A successful <see cref="WriteResult"/>.</returns>
        public static WriteResult Success()
        {
            return new WriteResult { Ok = true, Error = null };
        }

        /// <summary>Creates a failure result with <c>Ok = false</c> and the given error message.</summary>
        /// <param name="error">The error message describing why the write failed.</param>
        /// <returns>A failed <see cref="WriteResult"/>.</returns>
        public static WriteResult Failure(string error)
        {
            return new WriteResult { Ok = false, Error = error };
        }
    }
}
