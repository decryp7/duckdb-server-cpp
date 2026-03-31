using System;
using System.IO;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// Entry point for the DuckDB Arrow Flight Server (C# edition).
    /// Parses command-line arguments, sets up signal handling, and starts the server.
    /// </summary>
    internal static class Program
    {
        private const string Version = "4.1.9";
        private static volatile Server grpcServer;

        static int Main(string[] args)
        {
            // Step 1: Parse command-line arguments into config.
            var config = ParseArguments(args);
            if (config == null) return 1; // error already printed

            // Step 2: Validate TLS settings.
            if (!ValidateTls(config)) return 1;

            // Step 3: Auto-size the reader pool if not specified.
            if (config.ReaderPoolSize <= 0)
                config.ReaderPoolSize = Math.Max(Environment.ProcessorCount * 2, 4);

            // Step 4: Set up Ctrl+C handler.
            Console.CancelKeyPress += OnCancelKeyPress;

            // Step 5: Start the server.
            try
            {
                return RunServer(config);
            }
            catch (Exception ex)
            {
                ShutdownGrpcServer();
                Console.Error.WriteLine("[das] Fatal: " + ex.Message);
                return 1;
            }
        }

        // ── Server startup ───────────────────────────────────────────────────

        private static int RunServer(ServerConfig config)
        {
            // Create the database manager first. All connections share this DB.
            // Without this, each DuckDBConnection(":memory:") creates a separate DB.
            DatabaseManager dbManager = null;
            DuckFlightServer flightServer = null;

            try
            {
                dbManager = new DatabaseManager(config.DbPath, config);

                var readPool = new ConnectionPool(dbManager, config.ReaderPoolSize);
                var writer = new WriteSerializer(dbManager, config.WriteBatchMs, config.WriteBatchMax);
                flightServer = new DuckFlightServer(config, readPool, writer);

                var credentials = BuildCredentials(config);
                // Apache.Arrow.Flight.Protocol.FlightService is internal.
                // Use reflection to call BindService on the proto-generated gRPC class.
                var serviceDef = GetFlightServiceDefinition(flightServer);
                grpcServer = new Server
                {
                    Services = { serviceDef },
                    Ports = { new ServerPort(config.Host, config.Port, credentials) }
                };
                grpcServer.Start();

                PrintStartupBanner(config);

                // Block until shutdown (Ctrl+C calls ShutdownAsync).
                grpcServer.ShutdownTask.Wait();
            }
            finally
            {
                // Dispose in reverse order. Handles partial construction.
                flightServer?.Dispose();
                dbManager?.Dispose();
            }

            return 0;
        }

        // ── TLS credentials ──────────────────────────────────────────────────

        private static ServerCredentials BuildCredentials(ServerConfig config)
        {
            if (string.IsNullOrEmpty(config.TlsCertPath))
                return ServerCredentials.Insecure;

            string cert = File.ReadAllText(config.TlsCertPath);
            string key = File.ReadAllText(config.TlsKeyPath);
            return new SslServerCredentials(new[] { new KeyCertificatePair(cert, key) });
        }

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

        private static void OnCancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            Console.WriteLine("\n[das] Shutting down...");
            ShutdownGrpcServer();
        }

        /// <summary>
        /// Apache.Arrow.Flight.Protocol.FlightService is internal.
        /// Use reflection to call its BindService(FlightServiceBase) method
        /// to get the ServerServiceDefinition for Grpc.Core hosting.
        /// </summary>
        private static Grpc.Core.ServerServiceDefinition GetFlightServiceDefinition(
            Apache.Arrow.Flight.Server.FlightServer flightServer)
        {
            var asm = typeof(Apache.Arrow.Flight.Server.FlightServer).Assembly;
            var flightServiceType = asm.GetType("Apache.Arrow.Flight.Protocol.FlightService");
            if (flightServiceType == null)
                throw new InvalidOperationException(
                    "Cannot find Apache.Arrow.Flight.Protocol.FlightService in the assembly.");

            var bindMethod = flightServiceType.GetMethod("BindService",
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static,
                null,
                new[] { flightServer.GetType().BaseType ?? flightServer.GetType() },
                null);

            // Try with the concrete type if base type didn't work.
            if (bindMethod == null)
            {
                foreach (var m in flightServiceType.GetMethods(
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Static))
                {
                    if (m.Name == "BindService")
                    {
                        bindMethod = m;
                        break;
                    }
                }
            }

            if (bindMethod == null)
                throw new InvalidOperationException(
                    "Cannot find BindService method on FlightService.");

            return (Grpc.Core.ServerServiceDefinition)bindMethod.Invoke(null, new object[] { flightServer });
        }

        private static void ShutdownGrpcServer()
        {
            try { grpcServer?.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort */ }
        }

        // ── Startup banner ───────────────────────────────────────────────────

        private static void PrintStartupBanner(ServerConfig config)
        {
            Console.WriteLine("[das] Arrow Flight server v" + Version + " (C#)");
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
        /// Parse command-line arguments. Returns null if the program should exit
        /// (e.g. --help was given, or an error occurred).
        /// </summary>
        private static ServerConfig ParseArguments(string[] args)
        {
            var config = new ServerConfig();

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];

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

                // All remaining flags require a value.
                if (i + 1 >= args.Length)
                {
                    Console.Error.WriteLine(arg + " requires a value");
                    return null;
                }
                string value = args[++i];

                switch (arg)
                {
                    case "--db":        config.DbPath = value; break;
                    case "--host":      config.Host = value; break;
                    case "--port":      config.Port = ParseInt(value, "--port"); break;
                    case "--readers":   config.ReaderPoolSize = ParsePositive(value, "--readers"); break;
                    case "--batch-ms":  config.WriteBatchMs = ParseInt(value, "--batch-ms"); break;
                    case "--batch-max":    config.WriteBatchMax = ParsePositive(value, "--batch-max"); break;
                    case "--batch-size":   config.BatchSize = ParsePositive(value, "--batch-size"); break;
                    case "--memory-limit": config.MemoryLimit = value; break;
                    case "--threads":      config.DuckDbThreads = ParsePositive(value, "--threads"); break;
                    case "--tls-cert":     config.TlsCertPath = value; break;
                    case "--tls-key":   config.TlsKeyPath = value; break;
                    default:
                        Console.Error.WriteLine("Unknown option: " + arg);
                        PrintUsage();
                        return null;
                }
            }

            return config;
        }

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

        private static void PrintUsage()
        {
            int cpuCount = Environment.ProcessorCount;
            Console.WriteLine("DuckDB Arrow Flight Server v" + Version + " (C#)");
            Console.WriteLine();
            Console.WriteLine("Usage: DuckArrowServer.exe [options]");
            Console.WriteLine();
            Console.WriteLine("Database:");
            Console.WriteLine("  --db       <path>   DuckDB file or :memory:  (default: :memory:)");
            Console.WriteLine();
            Console.WriteLine("Network:");
            Console.WriteLine("  --host     <addr>   Bind address             (default: 0.0.0.0)");
            Console.WriteLine("  --port     <n>      gRPC port                (default: 17777)");
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
