using System;
using System.IO;
using System.Threading;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// Entry point for the DuckDB Arrow Flight Server (C# edition).
    /// Mirrors the C++ main.cpp: CLI parsing, signal handling, server startup.
    /// </summary>
    internal static class Program
    {
        private const string Version = "4.1.9";
        private static DuckFlightServer _flightServer;
        private static Server _grpcServer;

        static int Main(string[] args)
        {
            var cfg = new ServerConfig();

            // ── CLI parsing ──────────────────────────────────────────────────
            for (int i = 0; i < args.Length; i++)
            {
                string a = args[i];

                if (a == "--version")
                {
                    Console.WriteLine("duckdb_flight_server " + Version);
                    return 0;
                }
                if (a == "--help")
                {
                    PrintUsage();
                    return 0;
                }

                if (i + 1 >= args.Length)
                {
                    Console.Error.WriteLine(a + " requires a value");
                    return 1;
                }
                string val = args[++i];

                switch (a)
                {
                    case "--db":        cfg.DbPath = val; break;
                    case "--host":      cfg.Host = val; break;
                    case "--port":      cfg.Port = ParseInt(val, "--port"); break;
                    case "--readers":   cfg.ReaderPoolSize = ParsePositive(val, "--readers"); break;
                    case "--batch-ms":  cfg.WriteBatchMs = ParseInt(val, "--batch-ms"); break;
                    case "--batch-max": cfg.WriteBatchMax = ParsePositive(val, "--batch-max"); break;
                    case "--tls-cert":  cfg.TlsCertPath = val; break;
                    case "--tls-key":   cfg.TlsKeyPath = val; break;
                    default:
                        Console.Error.WriteLine("Unknown option: " + a);
                        PrintUsage();
                        return 1;
                }
            }

            // Validate TLS: both or neither.
            if (string.IsNullOrEmpty(cfg.TlsCertPath) != string.IsNullOrEmpty(cfg.TlsKeyPath))
            {
                Console.Error.WriteLine("[das] --tls-cert and --tls-key must both be set or both omitted");
                return 1;
            }

            // ── Signal handling ──────────────────────────────────────────────
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("\n[das] Shutting down...");
                ShutdownServer();
            };

            // ── Start server ─────────────────────────────────────────────────
            try
            {
                _flightServer = new DuckFlightServer(cfg);

                // Build gRPC server credentials.
                ServerCredentials credentials;
                if (!string.IsNullOrEmpty(cfg.TlsCertPath))
                {
                    string cert = File.ReadAllText(cfg.TlsCertPath);
                    string key = File.ReadAllText(cfg.TlsKeyPath);
                    credentials = new SslServerCredentials(
                        new[] { new KeyCertificatePair(cert, key) });
                }
                else
                {
                    credentials = ServerCredentials.Insecure;
                }

                // Create and start the gRPC server.
                _grpcServer = new Server
                {
                    Services = { FlightServer.BindService(_flightServer) },
                    Ports = { new ServerPort(cfg.Host, cfg.Port, credentials) }
                };
                _grpcServer.Start();

                var stats = _flightServer.Stats();
                Console.WriteLine("[das] Arrow Flight server v" + Version + " (C#)");
                Console.WriteLine("      address  = " + cfg.Host + ":" + cfg.Port);
                Console.WriteLine("      readers  = " + stats.ReaderPoolSize);
                Console.WriteLine("      tls      = " + (string.IsNullOrEmpty(cfg.TlsCertPath) ? "off" : "on"));
                Console.WriteLine("      db       = " + cfg.DbPath);
                Console.WriteLine();
                Console.WriteLine("Press Ctrl+C to stop.");

                // Block until shutdown.
                _grpcServer.ShutdownTask.Wait();
                _flightServer.Dispose();
                _flightServer = null;
                return 0;
            }
            catch (Exception ex)
            {
                ShutdownServer();
                Console.Error.WriteLine("[das] Fatal: " + ex.Message);
                return 1;
            }
        }

        private static void ShutdownServer()
        {
            try { _grpcServer?.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort */ }
        }

        // ── CLI helpers ──────────────────────────────────────────────────────

        private static int ParseInt(string value, string flag)
        {
            int n;
            if (!int.TryParse(value, out n))
            {
                Console.Error.WriteLine(flag + ": '" + value + "' is not a valid integer");
                Environment.Exit(1);
            }
            return n;
        }

        private static int ParsePositive(string value, string flag)
        {
            int n = ParseInt(value, flag);
            if (n <= 0)
            {
                Console.Error.WriteLine(flag + ": value must be > 0, got " + n);
                Environment.Exit(1);
            }
            return n;
        }

        private static void PrintUsage()
        {
            int hw = Environment.ProcessorCount;
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
            Console.WriteLine("  --tls-cert <path>   TLS certificate PEM      (enables TLS)");
            Console.WriteLine("  --tls-key  <path>   TLS private key PEM");
            Console.WriteLine();
            Console.WriteLine(string.Format("Concurrency ({0} logical CPUs detected):", hw));
            Console.WriteLine(string.Format("  --readers  <n>      Read connection pool     (default: {0})", hw * 2));
            Console.WriteLine();
            Console.WriteLine("Write batching:");
            Console.WriteLine("  --batch-ms  <ms>    Batch window             (default: 5)");
            Console.WriteLine("  --batch-max <n>     Max writes per batch     (default: 512)");
            Console.WriteLine();
            Console.WriteLine("Other:");
            Console.WriteLine("  --version           Print version and exit");
            Console.WriteLine("  --help              Show this message");
            Console.WriteLine();
            Console.WriteLine("Flight RPC surface:");
            Console.WriteLine("  DoGet(ticket)          ticket = UTF-8 SELECT SQL -> Arrow stream");
            Console.WriteLine("  DoAction(\"execute\")    body   = UTF-8 DML/DDL SQL -> empty stream");
            Console.WriteLine("  DoAction(\"ping\")       -> \"pong\"");
            Console.WriteLine("  DoAction(\"stats\")      -> JSON metrics");
            Console.WriteLine("  ListActions()          -> advertises the three actions above");
        }
    }
}
