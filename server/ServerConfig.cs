using System;
using System.Threading;

namespace DuckArrowServer
{
    /// <summary>
    /// Configuration parameters for the DuckDB Arrow Flight server.
    /// Mirrors the C++ ServerConfig struct.
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
        /// Number of DuckDB read connections to maintain in the pool.
        /// Default (0) auto-configures to 2 x logical CPU count.
        /// </summary>
        public int ReaderPoolSize { get; set; } = 0;

        /// <summary>Write batch accumulation window in milliseconds.</summary>
        public int WriteBatchMs { get; set; } = 5;

        /// <summary>Maximum number of DML statements per write transaction.</summary>
        public int WriteBatchMax { get; set; } = 512;

        /// <summary>Path to TLS server certificate PEM file. Empty for plaintext.</summary>
        public string TlsCertPath { get; set; } = "";

        /// <summary>Path to TLS server private key PEM file. Empty for plaintext.</summary>
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
}
