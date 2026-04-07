using System;
using System.IO;
using Grpc.Core;

namespace DuckDbServer
{
    /// <summary>
    /// Entry point for the DuckDB gRPC Server (C# edition). Orchestrates the startup
    /// sequence: CLI parsing, TLS validation, pool auto-sizing, signal handling, and
    /// server lifecycle management.
    ///
    /// <para><b>Thread Safety:</b> The <see cref="grpcServer"/> field is marked
    /// <c>volatile</c> because it is written by the main thread and read by the
    /// Ctrl+C handler, which runs on a separate thread pool thread.</para>
    ///
    /// <para><b>Startup Sequence:</b></para>
    /// <list type="number">
    ///   <item><description>Configure gRPC thread pool and completion queues (must happen before any gRPC object creation).</description></item>
    ///   <item><description>Parse CLI arguments into <see cref="ServerConfig"/>.</description></item>
    ///   <item><description>Validate TLS settings (cert and key must both be present or both absent).</description></item>
    ///   <item><description>Auto-size the reader pool if not explicitly configured.</description></item>
    ///   <item><description>Register Ctrl+C handler for graceful shutdown.</description></item>
    ///   <item><description>Create the sharded database, query cache, gRPC service, and start listening.</description></item>
    ///   <item><description>Block on <c>grpcServer.ShutdownTask</c> until Ctrl+C triggers shutdown.</description></item>
    /// </list>
    /// </summary>
    internal static class Program
    {
        /// <summary>Server version string, displayed in the startup banner and <c>--version</c> output.</summary>
        private const string Version = "5.0.0";

        /// <summary>
        /// Reference to the running gRPC server. Volatile because it is written by the
        /// main thread and read by the Ctrl+C handler (<see cref="OnCancelKeyPress"/>).
        /// </summary>
        private static volatile Server grpcServer;

        /// <summary>
        /// Application entry point. Configures gRPC, parses arguments, and starts the server.
        /// </summary>
        /// <param name="args">Command-line arguments (see <see cref="PrintUsage"/> for supported flags).</param>
        /// <returns>
        /// Exit code: 0 on clean shutdown, 1 on error or invalid arguments.
        /// </returns>
        static int Main(string[] args)
        {
            // Configure the gRPC native thread pool and completion queue count.
            // MUST be called BEFORE creating any gRPC Channel, Server, or Client.
            // The thread pool size determines how many native threads handle I/O
            // completion. The completion queue count determines how many event loops
            // process gRPC events. Both are sized to the CPU count for good throughput.
            GrpcEnvironment.SetThreadPoolSize(Environment.ProcessorCount * 2);
            GrpcEnvironment.SetCompletionQueueCount(Environment.ProcessorCount);

            // Step 1: Parse command-line arguments into a config object.
            var config = ParseArguments(args);
            if (config == null) return 1; // Error already printed to stderr.

            // Step 2: Validate TLS settings (both cert and key must be present, or neither).
            if (!ValidateTls(config)) return 1;

            // Step 3: Auto-size the reader pool if not explicitly specified.
            // Default: 2x logical CPU count, minimum 4. This provides enough parallelism
            // for typical workloads without excessive memory usage.
            if (config.ReaderPoolSize <= 0)
                config.ReaderPoolSize = Math.Max(Environment.ProcessorCount * 2, 4);

            // Step 4: Set up Ctrl+C (SIGINT) handler for graceful shutdown.
            Console.CancelKeyPress += OnCancelKeyPress;

            // Step 5: Start the server. This blocks until shutdown.
            try
            {
                return RunServer(config);
            }
            catch (Exception ex)
            {
                // Ensure the gRPC server is shut down even on unexpected exceptions.
                ShutdownGrpcServer();
                Console.Error.WriteLine("[das] Fatal: " + ex.Message);
                return 1;
            }
        }

        // ── Server startup ───────────────────────────────────────────────────

        /// <summary>
        /// Creates the sharded database, query cache, gRPC server, and starts listening.
        /// Blocks until the server is shut down (via Ctrl+C or fatal error).
        ///
        /// <para><b>Resource lifecycle:</b></para>
        /// <list type="number">
        ///   <item><description>Create <see cref="ShardedDuckDb"/> (opens databases, creates pools and writers).</description></item>
        ///   <item><description>Create <see cref="QueryCache"/> (in-memory, 10K entries, 60s TTL).</description></item>
        ///   <item><description>Create <see cref="DuckDbServer"/> (the gRPC service implementation).</description></item>
        ///   <item><description>Build TLS credentials (insecure or SSL based on config).</description></item>
        ///   <item><description>Create and start the gRPC <see cref="Server"/> with performance-tuned channel options.</description></item>
        ///   <item><description>Block on <c>grpcServer.ShutdownTask</c>.</description></item>
        ///   <item><description>On shutdown, dispose <see cref="DuckDbServer"/> (which disposes the sharded DB internally).</description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> <see cref="DuckDbServer.Dispose"/> disposes the
        /// <see cref="ShardedDuckDb"/> internally. The caller must NOT dispose
        /// <c>shardedDb</c> separately, or a double-dispose error will occur.</para>
        /// </summary>
        /// <param name="config">Fully validated server configuration.</param>
        /// <returns>Exit code: 0 on clean shutdown.</returns>
        private static int RunServer(ServerConfig config)
        {
            // Create the database manager first. All connections share this DB.
            // Without this, each DuckDBConnection(":memory:") creates a separate DB.
            DuckDbServer duckDbServer = null;

            ShardedDuckDb shardedDb = null;
            try
            {
                // Create the sharded database (opens databases, creates connection pools
                // and writer threads for each shard).
                shardedDb = new ShardedDuckDb(config);

                // Create the query cache. 10,000 entries with 60-second TTL is suitable
                // for most workloads. Adjust for workloads with many distinct queries.
                IQueryCache queryCache = new QueryCache(maxEntries: 10000, ttlSeconds: 60);

                // Create the gRPC service implementation.
                duckDbServer = new DuckDbServer(config, shardedDb, queryCache);

                // Build TLS credentials (insecure for plaintext, SSL for encrypted).
                var credentials = BuildCredentials(config);

                // gRPC server performance tuning options:
                var channelOptions = new[]
                {
                    // MaxConcurrentStreams: limits the number of simultaneous RPC streams
                    // per HTTP/2 connection. 200 is generous; most clients use fewer.
                    new ChannelOption(ChannelOptions.MaxConcurrentStreams, 200),
                    // MaxReceiveMessageLength / MaxSendMessageLength: 64 MB max for
                    // large bulk insert payloads and large query results.
                    new ChannelOption(ChannelOptions.MaxReceiveMessageLength, 64 * 1024 * 1024),
                    new ChannelOption(ChannelOptions.MaxSendMessageLength, 64 * 1024 * 1024),
                    // Keepalive: send pings every 30s, timeout after 10s with no response.
                    // Prevents load balancers from closing idle connections.
                    new ChannelOption("grpc.keepalive_time_ms", 30000),
                    new ChannelOption("grpc.keepalive_timeout_ms", 10000),
                    // Allow keepalive pings even when there are no active RPCs.
                    new ChannelOption("grpc.keepalive_permit_without_calls", 1),
                    // Allow unlimited pings without data (prevents gRPC from throttling keepalives).
                    new ChannelOption("grpc.http2.max_pings_without_data", 0),
                    // 2 MB HTTP/2 write buffer for batching small messages.
                    new ChannelOption("grpc.http2_write_buffer_size", 2 * 1024 * 1024),
                };

                grpcServer = new Server(channelOptions)
                {
                    Services = { duckDbServer.BuildGrpcService() },
                    Ports = { new ServerPort(config.Host, config.Port, credentials) }
                };

                grpcServer.Start();

                PrintStartupBanner(config);

                // Block until shutdown. Ctrl+C calls ShutdownAsync, which completes
                // the ShutdownTask and unblocks this Wait.
                grpcServer.ShutdownTask.Wait();
            }
            finally
            {
                // DuckDbServer.Dispose() disposes shardedDb internally.
                // If DuckDbServer was never created (constructor threw), dispose shardedDb directly.
                if (duckDbServer != null)
                    duckDbServer.Dispose();
                else if (shardedDb != null)
                    shardedDb.Dispose();
            }

            return 0;
        }

        // ── TLS credentials ──────────────────────────────────────────────────

        /// <summary>
        /// Builds gRPC server credentials based on the TLS configuration.
        ///
        /// <para><b>Insecure mode:</b> If no TLS certificate is configured, returns
        /// <see cref="ServerCredentials.Insecure"/> (plaintext HTTP/2). Suitable for
        /// localhost, VPN-protected networks, or when TLS is handled by a reverse proxy.</para>
        ///
        /// <para><b>TLS mode:</b> Reads the PEM-encoded certificate and private key from
        /// disk and creates an <see cref="SslServerCredentials"/>. The certificate chain
        /// should include intermediate CA certificates if needed by clients.</para>
        /// </summary>
        /// <param name="config">Server configuration with TLS paths.</param>
        /// <returns>
        /// <see cref="ServerCredentials.Insecure"/> for plaintext, or
        /// <see cref="SslServerCredentials"/> for encrypted connections.
        /// </returns>
        private static ServerCredentials BuildCredentials(ServerConfig config)
        {
            if (string.IsNullOrEmpty(config.TlsCertPath))
                return ServerCredentials.Insecure;

            string cert = File.ReadAllText(config.TlsCertPath);
            string key = File.ReadAllText(config.TlsKeyPath);
            return new SslServerCredentials(new[] { new KeyCertificatePair(cert, key) });
        }

        /// <summary>
        /// Validates that TLS cert and key are either both provided or both omitted.
        /// Providing only one is a configuration error.
        /// </summary>
        /// <param name="config">Server configuration to validate.</param>
        /// <returns><c>true</c> if valid, <c>false</c> if invalid (error printed to stderr).</returns>
        private static bool ValidateTls(ServerConfig config)
        {
            bool hasCert = !string.IsNullOrEmpty(config.TlsCertPath);
            bool hasKey = !string.IsNullOrEmpty(config.TlsKeyPath);

            if (hasCert != hasKey)
            {
                Console.Error.WriteLine("[das] --tls-cert and --tls-key must both be set or both omitted");
                return false;
            }
            return true;
        }

        // ── Signal handling ──────────────────────────────────────────────────

        /// <summary>
        /// Ctrl+C (SIGINT) handler. Cancels the default process termination and
        /// initiates a graceful gRPC server shutdown. The <see cref="e.Cancel"/>
        /// property is set to <c>true</c> to prevent the CLR from terminating the
        /// process immediately, allowing the finally block in <see cref="RunServer"/>
        /// to dispose resources cleanly.
        /// </summary>
        /// <param name="sender">Event sender (unused).</param>
        /// <param name="e">Console cancel event args. <c>Cancel</c> is set to true.</param>
        private static void OnCancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true; // Prevent immediate process termination.
            Console.WriteLine("\n[das] Shutting down...");
            ShutdownGrpcServer();
        }

        /// <summary>
        /// Initiates a graceful gRPC server shutdown with a 5-second timeout.
        /// Exceptions are swallowed because this is called during error handling
        /// or signal handling where additional errors are not recoverable.
        /// </summary>
        private static void ShutdownGrpcServer()
        {
            try { grpcServer?.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort -- swallow errors during shutdown */ }
        }

        // ── Startup banner ───────────────────────────────────────────────────

        /// <summary>
        /// Prints the server startup banner with version, address, configuration,
        /// and instructions for stopping. Written to stdout for easy log capture.
        /// </summary>
        /// <param name="config">Server configuration to display.</param>
        private static void PrintStartupBanner(ServerConfig config)
        {
            Console.WriteLine("[das] gRPC server v" + Version + " (C#)");
            Console.WriteLine("      address    = " + config.Host + ":" + config.Port);
            Console.WriteLine("      readers    = " + config.ReaderPoolSize);
            Console.WriteLine("      batch-size = " + config.BatchSize + " rows");
            Console.WriteLine("      tls        = " + (string.IsNullOrEmpty(config.TlsCertPath) ? "off" : "on"));
            Console.WriteLine("      db         = " + config.DbPath);
            if (!string.IsNullOrEmpty(config.MemoryLimit))
                Console.WriteLine("      memory     = " + config.MemoryLimit);
            if (config.DuckDbThreads > 0)
                Console.WriteLine("      threads    = " + config.DuckDbThreads);
            Console.WriteLine();
            Console.WriteLine("Press Ctrl+C to stop.");
        }

        // ── CLI argument parsing ─────────────────────────────────────────────

        /// <summary>
        /// Parses command-line arguments into a <see cref="ServerConfig"/> instance.
        /// Uses a simple <c>--flag value</c> pattern (GNU-style long options without <c>=</c>).
        ///
        /// <para><b>Special flags:</b></para>
        /// <list type="bullet">
        ///   <item><description><c>--version</c>: Prints the version and exits immediately with code 0.</description></item>
        ///   <item><description><c>--help</c>: Prints usage information and exits immediately with code 0.</description></item>
        /// </list>
        ///
        /// <para><b>Error handling:</b> On invalid arguments, prints an error to stderr
        /// and returns <c>null</c> to signal the caller to exit with code 1. On invalid
        /// integer values, calls <see cref="Environment.Exit"/> directly (a simplification
        /// that avoids propagating parse errors through the entire chain).</para>
        /// </summary>
        /// <param name="args">The command-line arguments array from <c>Main</c>.</param>
        /// <returns>
        /// A populated <see cref="ServerConfig"/>, or <c>null</c> if the program should
        /// exit (due to an error or <c>--help</c>/<c>--version</c>).
        /// </returns>
        private static ServerConfig ParseArguments(string[] args)
        {
            var config = new ServerConfig();

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];

                // Handle standalone flags that don't take a value.
                if (arg == "--version")
                {
                    Console.WriteLine("duckdb_flight_server " + Version);
                    Environment.Exit(0);
                }
                if (arg == "--help")
                {
                    PrintUsage();
                    Environment.Exit(0);
                }

                // All remaining flags require a value (--flag value).
                if (i + 1 >= args.Length)
                {
                    Console.Error.WriteLine(arg + " requires a value");
                    return null;
                }
                string value = args[++i]; // Consume the next argument as the value.

                // Map each flag to its config property.
                switch (arg)
                {
                    case "--db":        config.DbPath = value; break;
                    case "--host":      config.Host = value; break;
                    case "--port":      config.Port = ParseInt(value, "--port"); break;
                    case "--readers":   config.ReaderPoolSize = ParsePositive(value, "--readers"); break;
                    case "--batch-ms":  config.WriteBatchMs = ParseInt(value, "--batch-ms"); break;
                    case "--batch-max":    config.WriteBatchMax = ParsePositive(value, "--batch-max"); break;
                    case "--shards":       config.Shards = ParsePositive(value, "--shards"); break;
                    case "--timeout":      config.QueryTimeoutSeconds = ParseInt(value, "--timeout"); break;
                    case "--batch-size":   config.BatchSize = ParsePositive(value, "--batch-size"); break;
                    case "--memory-limit": config.MemoryLimit = value; break;
                    case "--threads":      config.DuckDbThreads = ParsePositive(value, "--threads"); break;
                    case "--temp-dir":     config.TempDirectory = value; break;
                    case "--tls-cert":     config.TlsCertPath = value; break;
                    case "--tls-key":   config.TlsKeyPath = value; break;
                    case "--backup-db": config.BackupDbPath = value; break;
                    default:
                        Console.Error.WriteLine("Unknown option: " + arg);
                        PrintUsage();
                        return null;
                }
            }

            return config;
        }

        /// <summary>
        /// Parses a string as an integer. On failure, prints an error and exits the process.
        /// </summary>
        /// <param name="value">The string value to parse.</param>
        /// <param name="flag">The flag name, used in error messages (e.g., "--port").</param>
        /// <returns>The parsed integer value.</returns>
        private static int ParseInt(string value, string flag)
        {
            int result;
            if (!int.TryParse(value, out result))
            {
                Console.Error.WriteLine(flag + ": '" + value + "' is not a valid integer");
                Environment.Exit(1);
            }
            return result;
        }

        /// <summary>
        /// Parses a string as a positive integer (> 0). On failure, prints an error
        /// and exits the process.
        /// </summary>
        /// <param name="value">The string value to parse.</param>
        /// <param name="flag">The flag name, used in error messages.</param>
        /// <returns>The parsed positive integer value.</returns>
        private static int ParsePositive(string value, string flag)
        {
            int result = ParseInt(value, flag);
            if (result <= 0)
            {
                Console.Error.WriteLine(flag + ": value must be > 0, got " + result);
                Environment.Exit(1);
            }
            return result;
        }

        /// <summary>
        /// Prints command-line usage information to stdout. Shows all supported flags
        /// with their descriptions and default values.
        /// </summary>
        private static void PrintUsage()
        {
            int cpuCount = Environment.ProcessorCount;
            Console.WriteLine("DuckDB gRPC Server v" + Version + " (C#)");
            Console.WriteLine();
            Console.WriteLine("Usage: DuckDbServer.exe [options]");
            Console.WriteLine();
            Console.WriteLine("Database:");
            Console.WriteLine("  --db       <path>   DuckDB file or :memory:  (default: :memory:)");
            Console.WriteLine();
            Console.WriteLine("Network:");
            Console.WriteLine("  --host     <addr>   Bind address             (default: 0.0.0.0)");
            Console.WriteLine("  --port     <n>      gRPC port                (default: 19100)");
            Console.WriteLine("  --tls-cert <path>   TLS certificate PEM");
            Console.WriteLine("  --tls-key  <path>   TLS private key PEM");
            Console.WriteLine();
            Console.WriteLine(string.Format("Concurrency ({0} logical CPUs detected):", cpuCount));
            Console.WriteLine(string.Format("  --readers  <n>      Read connection pool     (default: {0})", cpuCount * 2));
            Console.WriteLine();
            Console.WriteLine("Write batching:");
            Console.WriteLine("  --batch-ms  <ms>    Batch window             (default: 5)");
            Console.WriteLine("  --batch-max <n>     Max writes per batch     (default: 512)");
            Console.WriteLine();
            Console.WriteLine("Performance:");
            Console.WriteLine("  --batch-size <n>    Rows per Arrow batch     (default: 8192)");
            Console.WriteLine("  --memory-limit <s>  DuckDB memory limit      (e.g. 4GB)");
            Console.WriteLine("  --threads <n>       DuckDB thread count      (default: nCPU)");
            Console.WriteLine();
            Console.WriteLine("Other:");
            Console.WriteLine("  --version           Print version and exit");
            Console.WriteLine("  --help              Show this message");
        }
    }
}
