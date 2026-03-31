#include "flight_server.hpp"
#include <arrow/flight/api.h>
#include <atomic>
#include <csignal>
#ifndef _WIN32
#include <unistd.h>  // write(), STDERR_FILENO
#endif
#include <cerrno>         // errno, ERANGE for strtol overflow detection
#include <climits>        // INT_MIN, INT_MAX for parse_int range check
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#define DAS_VERSION "4.1.8"

// std::atomic ensures the pointer write in main() is visible to the
// console handler / signal handler, which run on a different thread (Windows)
// or in a signal context (POSIX).  Plain pointer access without synchronisation
// is a data race under the C++ memory model.
static std::atomic<das::DuckFlightServer*> g_server(nullptr);

#ifdef _WIN32
#include <windows.h>
static BOOL WINAPI console_handler(DWORD ctrl) {
    if (ctrl == CTRL_C_EVENT || ctrl == CTRL_BREAK_EVENT || ctrl == CTRL_CLOSE_EVENT) {
        std::cout << "\n[das] Shutting down…\n";
        { das::DuckFlightServer* s = g_server.load(); if (s) s->Shutdown(); }
        return TRUE;
    }
    return FALSE;
}
#else
static void on_signal(int) {
    // std::cout is NOT async-signal-safe (uses internal locks that may
    // be held by the main thread when the signal arrives, causing deadlock).
    // write() to stderr is async-signal-safe per POSIX.1-2008.
    static const char msg[] = "\n[das] Shutting down...\n";
    (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
    das::DuckFlightServer* s = g_server.load();
    if (s) s->Shutdown();
}
#endif

static void usage(const char* prog) {
    unsigned hw = std::thread::hardware_concurrency();
    if (!hw) hw = 4;
    std::cout
        << "DuckDB Arrow Flight Server v" DAS_VERSION "\n\n"
        << "Usage: " << prog << " [options]\n\n"
        << "Database:\n"
        << "  --db       <path>   DuckDB file or :memory:  (default: :memory:)\n\n"
        << "Network:\n"
        << "  --host     <addr>   Bind address             (default: 0.0.0.0)\n"
        << "  --port     <n>      gRPC port                (default: 17777)\n"
        << "  --tls-cert <path>   TLS certificate PEM      (enables TLS)\n"
        << "  --tls-key  <path>   TLS private key PEM\n\n"
        << "Concurrency (" << hw << " logical CPUs detected):\n"
        << "  --readers  <n>      Read connection pool     (default: " << hw*2 << ")\n\n"
        << "Write batching:\n"
        << "  --batch-ms  <ms>    Batch window             (default: 5)\n"
        << "  --batch-max <n>     Max writes per batch     (default: 512)\n\n"
        << "Other:\n"
        << "  --version           Print version and exit\n"
        << "  --help              Show this message\n\n"
        << "Flight RPC surface:\n"
        << "  DoGet(ticket)          ticket = UTF-8 SELECT SQL → Arrow stream\n"
        << "  DoAction(\"execute\")    body   = UTF-8 DML/DDL SQL → empty stream\n"
        << "  DoAction(\"ping\")       → \"pong\"\n"
        << "  DoAction(\"stats\")      → JSON metrics\n"
        << "  ListActions()          → advertises the three actions above\n";
}

int main(int argc, char* argv[]) {
    das::ServerConfig cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--version") { std::cout << "duckdb_flight_server " DAS_VERSION "\n"; return 0; }
        if (a == "--help")    { usage(argv[0]); return 0; }

        if (i + 1 >= argc) { std::cerr << a << " requires a value\n"; return 1; }
        const char* val = argv[++i];

        // Parse integer arguments with strtol for proper error detection.
        // atoi() returns 0 silently for non-numeric input (e.g. "--port abc" → 0)
        // and wraps silently for negative values cast to size_t.
        auto parse_int = [&](const char* v, const char* flag) -> int {
            char* end;
            errno = 0;
            long n = std::strtol(v, &end, 10);
            // Check for non-numeric input
            if (end == v || *end != '\0') {
                std::cerr << flag << ": '" << v << "' is not a valid integer\n";
                std::exit(1);
            }
            // Check for strtol overflow (LONG_MAX/LONG_MIN) and int range
            // On 64-bit Linux, long is 64-bit while int is 32-bit;
            // silent truncation would produce wrong port/batch-ms values.
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
        else if (a == "--tls-cert")  cfg.tls_cert_path    = val;
        else if (a == "--tls-key")   cfg.tls_key_path     = val;
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    // Validate TLS: both or neither
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

    // Helper: check an arrow::Status and throw std::runtime_error on failure.
    // ARROW_CHECK_OK calls std::abort() on failure, bypassing the catch block.
    auto arrow_ok = [](const arrow::Status& s, const char* ctx) {
        if (!s.ok())
            throw std::runtime_error(std::string(ctx) + ": " + s.ToString());
    };

    try {
        das::DuckFlightServer server(cfg);
        g_server.store(&server);

        // ── Build Flight server options ─────────────────────────────────────
        arrow::flight::Location location;
        if (cfg.tls_cert_path.empty()) {
            arrow_ok(arrow::flight::Location::ForGrpcTcp(
                cfg.host, cfg.port, &location), "ForGrpcTcp");
        } else {
            arrow_ok(arrow::flight::Location::ForGrpcTls(
                cfg.host, cfg.port, &location), "ForGrpcTls");
        }

        arrow::flight::FlightServerOptions opts(location);

        if (!cfg.tls_cert_path.empty()) {
            arrow::flight::CertKeyPair tls;
            // Read PEM files
            auto read_file = [](const std::string& path, std::string& out) -> bool {
                FILE* f = std::fopen(path.c_str(), "rb");
                if (!f) return false;
                if (std::fseek(f, 0, SEEK_END) != 0) { std::fclose(f); return false; }
                long sz = std::ftell(f);
                if (sz < 0) { std::fclose(f); return false; } // ftell returns -1 on error
                if (std::fseek(f, 0, SEEK_SET) != 0) { std::fclose(f); return false; }
                out.resize(static_cast<size_t>(sz));
                size_t got = std::fread(&out[0], 1, static_cast<size_t>(sz), f);
                std::fclose(f);
                return got == static_cast<size_t>(sz); // fail on partial read
            };
            if (!read_file(cfg.tls_cert_path, tls.pem_cert) ||
                !read_file(cfg.tls_key_path,  tls.pem_key)) {
                std::cerr << "[das] Failed to read TLS cert/key files\n";
                return 1;
            }
            opts.tls_certificates.push_back(std::move(tls));
        }

        // ── Start server ────────────────────────────────────────────────────
        arrow_ok(server.Init(opts), "server.Init");
        // arrow_ok throws std::runtime_error on failure, caught by the outer catch block.
        // Cache the actual bound port (may differ from cfg.port if port=0 was used).
        server.set_actual_port(server.port());

        // Use server.stats() for reader count — the constructor may have
        // auto-sized cfg_.reader_pool_size, leaving the local cfg stale.
        const das::ServerStats st = server.stats();
        std::cout << "[das] Arrow Flight server v" DAS_VERSION "\n"
                  << "      address  = " << cfg.host << ":" << server.port() << "\n"
                  << "      readers  = " << st.reader_pool_size << "\n"
                  << "      tls      = " << (cfg.tls_cert_path.empty() ? "off" : "on") << "\n"
                  << "      db       = " << cfg.db_path << "\n";

        // Serve() blocks until Shutdown() is called from signal handler
        arrow_ok(server.Serve(), "server.Serve");

        // Clear g_server before the local `server` variable is destroyed.
        // A second signal arriving during destructor execution would otherwise
        // call Shutdown() on a partially-destroyed object.
        g_server.store(nullptr);

    } catch (const std::exception& ex) {
        // Clear g_server before the server object (on the stack above)
        // is destroyed during stack unwinding.  Without this, a signal
        // arriving during ~DuckFlightServer could call Shutdown() on a
        // partially-destroyed object.
        g_server.store(nullptr);
        std::cerr << "[das] Fatal: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
