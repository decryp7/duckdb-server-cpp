/**
 * @file main_grpc.cpp
 * @brief Entry point for the DuckDB gRPC Server (C++ edition).
 *
 * Responsibilities:
 *   1. Parse command-line arguments into ServerConfig.
 *   2. Set up signal handlers for graceful shutdown (Ctrl+C / SIGTERM).
 *   3. Create DuckGrpcServer (which creates shards, pools, writers).
 *   4. Configure gRPC ServerBuilder with performance tuning options.
 *   5. Start the gRPC server and block until shutdown.
 *
 * Uses proto/duckdb_service.proto — compatible with the C# client,
 * Rust server, and any client generated from the same .proto file.
 *
 * Signal handling:
 *   - Windows: SetConsoleCtrlHandler for CTRL_C, CTRL_BREAK, CTRL_CLOSE.
 *   - Unix: std::signal for SIGINT and SIGTERM.
 *   - Both call grpc::Server::Shutdown() which unblocks Wait().
 *   - Unix handler uses write() instead of std::cout (async-signal-safe).
 *
 * gRPC tuning applied:
 *   - NUM_CQS = hardware_concurrency (one completion queue per CPU core).
 *   - MIN_POLLERS = 2, MAX_POLLERS = 2×cores (bounds polling thread count).
 *   - Max message size = 64MB (default 4MB truncates large results).
 *   - HTTP/2 write buffer = 2MB (batches small writes into larger frames).
 *   - Keepalive = 30s interval, 10s timeout (detects dead connections).
 *   - BDP probe enabled (auto-tunes TCP window based on bandwidth-delay product).
 *   - Max concurrent streams = 200 (HTTP/2 multiplexing limit per connection).
 */

#include "grpc_server.hpp"
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <csignal>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#ifndef _WIN32
#include <unistd.h>
#endif

#define DAS_VERSION "4.1.9"

static std::atomic<grpc::Server*> g_grpc_server(nullptr);

#ifdef _WIN32
#include <windows.h>
static BOOL WINAPI console_handler(DWORD ctrl) {
    if (ctrl == CTRL_C_EVENT || ctrl == CTRL_BREAK_EVENT || ctrl == CTRL_CLOSE_EVENT) {
        std::cout << "\n[das] Shutting down...\n";
        grpc::Server* s = g_grpc_server.load();
        if (s) s->Shutdown();
        return TRUE;
    }
    return FALSE;
}
#else
static void on_signal(int) {
    static const char msg[] = "\n[das] Shutting down...\n";
    (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
    grpc::Server* s = g_grpc_server.load();
    if (s) s->Shutdown();
}
#endif

static void usage(const char* prog) {
    unsigned hw = std::thread::hardware_concurrency();
    if (!hw) hw = 4;
    std::cout
        << "DuckDB gRPC Server v" DAS_VERSION "\n\n"
        << "Usage: " << prog << " [options]\n\n"
        << "Database:\n"
        << "  --db       <path>   DuckDB file or :memory:  (default: :memory:)\n\n"
        << "Network:\n"
        << "  --host     <addr>   Bind address             (default: 0.0.0.0)\n"
        << "  --port     <n>      gRPC port                (default: 17777)\n"
        << "  --tls-cert <path>   TLS certificate PEM\n"
        << "  --tls-key  <path>   TLS private key PEM\n\n"
        << "Concurrency (" << hw << " logical CPUs detected):\n"
        << "  --readers  <n>      Read connection pool     (default: " << hw*2 << ")\n\n"
        << "Write batching:\n"
        << "  --batch-ms  <ms>    Batch window             (default: 5)\n"
        << "  --batch-max <n>     Max writes per batch     (default: 512)\n\n"
        << "Performance:\n"
        << "  --memory-limit <sz> Per-shard memory limit   (default: auto 80%/shards)\n"
        << "  --temp-dir   <path> DuckDB spill-to-disk dir (default: DuckDB default)\n\n"
        << "Other:\n"
        << "  --version           Print version and exit\n"
        << "  --help              Show this message\n\n"
        << "Protocol: custom gRPC (proto/duckdb_service.proto)\n"
        << "  Query(sql)     → stream of Arrow IPC chunks\n"
        << "  Execute(sql)   → success/error\n"
        << "  Ping()         → \"pong\"\n"
        << "  GetStats()     → server metrics\n";
}

int main(int argc, char* argv[]) {
    das::ServerConfig cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--version") { std::cout << "duckdb_grpc_server " DAS_VERSION "\n"; return 0; }
        if (a == "--help")    { usage(argv[0]); return 0; }

        if (i + 1 >= argc) { std::cerr << a << " requires a value\n"; return 1; }
        const char* val = argv[++i];

        auto parse_int = [&](const char* v, const char* flag) -> int {
            char* end;
            errno = 0;
            long n = std::strtol(v, &end, 10);
            if (end == v || *end != '\0') {
                std::cerr << flag << ": '" << v << "' is not a valid integer\n";
                std::exit(1);
            }
            if (errno == ERANGE || n < INT_MIN || n > INT_MAX) {
                std::cerr << flag << ": " << v << " is out of range\n";
                std::exit(1);
            }
            return static_cast<int>(n);
        };
        auto parse_pos = [&](const char* v, const char* flag) -> size_t {
            int n = parse_int(v, flag);
            if (n <= 0) {
                std::cerr << flag << ": value must be > 0, got " << n << "\n";
                std::exit(1);
            }
            return static_cast<size_t>(n);
        };

        if      (a == "--db")        cfg.db_path          = val;
        else if (a == "--host")      cfg.host             = val;
        else if (a == "--port")      cfg.port             = parse_int(val, "--port");
        else if (a == "--readers")   cfg.reader_pool_size = parse_pos(val, "--readers");
        else if (a == "--batch-ms")  cfg.write_batch_ms   = parse_int(val, "--batch-ms");
        else if (a == "--batch-max") cfg.write_batch_max  = parse_pos(val, "--batch-max");
        else if (a == "--shards")    cfg.shards           = parse_int(val, "--shards");
        else if (a == "--tls-cert")      cfg.tls_cert_path    = val;
        else if (a == "--tls-key")       cfg.tls_key_path     = val;
        else if (a == "--memory-limit")  cfg.memory_limit     = val;
        else if (a == "--temp-dir")      cfg.temp_directory   = val;
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    if (cfg.tls_cert_path.empty() != cfg.tls_key_path.empty()) {
        std::cerr << "[das] --tls-cert and --tls-key must both be set or both omitted\n";
        return 1;
    }

#ifdef _WIN32
    SetConsoleCtrlHandler(console_handler, TRUE);
#else
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);
#endif

    try {
        das::DuckGrpcServer service(cfg);

        // Build server address
        std::string address = cfg.host + ":" + std::to_string(cfg.port);

        grpc::ServerBuilder builder;

        // TLS or plaintext
        if (!cfg.tls_cert_path.empty()) {
            // Read cert/key files
            auto read_file = [](const std::string& path) -> std::string {
                FILE* f = std::fopen(path.c_str(), "rb");
                if (!f) throw std::runtime_error("Cannot read: " + path);
                std::fseek(f, 0, SEEK_END);
                long sz = std::ftell(f);
                if (sz < 0) { std::fclose(f); throw std::runtime_error("ftell failed: " + path); }
                std::fseek(f, 0, SEEK_SET);
                std::string content(static_cast<size_t>(sz), '\0');
                size_t got = std::fread(&content[0], 1, static_cast<size_t>(sz), f);
                std::fclose(f);
                if (got != static_cast<size_t>(sz)) throw std::runtime_error("Partial read: " + path);
                return content;
            };

            grpc::SslServerCredentialsOptions ssl_opts;
            grpc::SslServerCredentialsOptions::PemKeyCertPair pair;
            pair.private_key = read_file(cfg.tls_key_path);
            pair.cert_chain = read_file(cfg.tls_cert_path);
            ssl_opts.pem_key_cert_pairs.push_back(pair);

            builder.AddListeningPort(address, grpc::SslServerCredentials(ssl_opts));
        } else {
            builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        }

        builder.RegisterService(&service);

        // ── gRPC performance tuning ──────────────────────────────────────
        // Max message sizes (default 4MB can truncate large results)
        builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
        builder.SetMaxSendMessageSize(64 * 1024 * 1024);

        // Completion queues and polling threads
        unsigned hw = std::thread::hardware_concurrency();
        if (!hw) hw = 4;
        builder.SetSyncServerOption(
            grpc::ServerBuilder::SyncServerOption::NUM_CQS, static_cast<int>(hw));
        builder.SetSyncServerOption(
            grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 2);
        builder.SetSyncServerOption(
            grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, static_cast<int>(hw * 2));

        // HTTP/2 tuning
        builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 200);
        builder.AddChannelArgument(GRPC_ARG_HTTP2_WRITE_BUFFER_SIZE, 2 * 1024 * 1024);
        builder.AddChannelArgument(GRPC_ARG_HTTP2_BDP_PROBE, 1);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        if (!server) {
            std::cerr << "[das] Failed to start gRPC server on " << address << "\n";
            return 1;
        }

        g_grpc_server.store(server.get());

        const das::ServerStats st = service.stats();
        std::cout << "[das] gRPC server v" DAS_VERSION "\n"
                  << "      address  = " << address << "\n"
                  << "      readers  = " << st.reader_pool_size << "\n"
                  << "      tls      = " << (cfg.tls_cert_path.empty() ? "off" : "on") << "\n"
                  << "      db       = " << cfg.db_path << "\n"
                  << "      protocol = duckdb.v1.DuckDbService (proto/duckdb_service.proto)\n";

        server->Wait(); // blocks until Shutdown()
        g_grpc_server.store(nullptr);

    } catch (const std::exception& ex) {
        g_grpc_server.store(nullptr);
        std::cerr << "[das] Fatal: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
