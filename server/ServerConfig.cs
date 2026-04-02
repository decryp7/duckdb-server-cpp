using System;

namespace DuckDbServer
{
    /// <summary>
    /// Configuration for the DuckDB gRPC server.
    /// All fields have sensible defaults that auto-scale to the host machine.
    /// </summary>
    public sealed class ServerConfig
    {
        /// <summary>DuckDB database file path, or ":memory:" for in-process database.</summary>
        public string DbPath { get; set; } = ":memory:";

        /// <summary>Network address to bind. "0.0.0.0" listens on all interfaces.</summary>
        public string Host { get; set; } = "0.0.0.0";

        /// <summary>gRPC listen port.</summary>
        public int Port { get; set; } = 17777;

        /// <summary>
        /// Number of DuckDB read connections in the pool.
        /// Default (0) auto-configures to 2 x logical CPU count.
        /// </summary>
        public int ReaderPoolSize { get; set; } = 0;

        /// <summary>Write batch accumulation window in milliseconds.</summary>
        public int WriteBatchMs { get; set; } = 5;

        /// <summary>Maximum number of DML statements per write transaction.</summary>
        public int WriteBatchMax { get; set; } = 512;

        /// <summary>
        /// Number of rows per Arrow record batch when streaming results.
        /// Larger = fewer gRPC messages = higher throughput for big results.
        /// Smaller = lower latency for first rows.
        /// </summary>
        public int BatchSize { get; set; } = 8192;

        /// <summary>DuckDB memory limit (e.g. "4GB", "512MB"). Empty = DuckDB default (80% of RAM).</summary>
        public string MemoryLimit { get; set; } = "";

        /// <summary>DuckDB thread count per shard. 0 = auto (1 for sharded, nCPU for single).</summary>
        public int DuckDbThreads { get; set; } = 0;

        /// <summary>
        /// Number of DuckDB shards (independent database instances).
        /// Each shard has its own connection pool, writer, and thread pool.
        /// Queries are distributed round-robin across shards.
        /// Default (1) = single database. Set to nCPU/2 for maximum throughput.
        /// </summary>
        public int Shards { get; set; } = 1;

        /// <summary>Path to TLS certificate PEM file. Empty for plaintext.</summary>
        public string TlsCertPath { get; set; } = "";

        /// <summary>Path to TLS private key PEM file. Empty for plaintext.</summary>
        public string TlsKeyPath { get; set; } = "";
    }

    /// <summary>
    /// Snapshot of live server metrics.
    /// </summary>
    public struct ServerStats
    {
        public long QueriesRead;
        public long QueriesWrite;
        public long Errors;
        public int ReaderPoolSize;
        public int Port;
    }

    /// <summary>
    /// Result of a write (DML/DDL) operation.
    /// </summary>
    public struct WriteResult
    {
        public bool Ok;
        public string Error;

        public static WriteResult Success()
        {
            return new WriteResult { Ok = true, Error = null };
        }

        public static WriteResult Failure(string error)
        {
            return new WriteResult { Ok = false, Error = error };
        }
    }
}
