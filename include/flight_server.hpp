#pragma once
/**
 * @file flight_server.hpp
 * @brief Arrow Flight server wrapping a DuckDB database.
 *
 * ## Architecture overview
 *
 * @code
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │  DuckFlightServer  (arrow::flight::FlightServerBase)             │
 *   │                                                                  │
 *   │  DoGet("SELECT …")          DoAction("execute", "INSERT …")     │
 *   │       │                              │                           │
 *   │       ▼                              ▼                           │
 *   │  ConnectionPool              WriteSerializer                     │
 *   │  (parallel reads)            (batched writes)                    │
 *   │       │                              │                           │
 *   │       └──────────────┬───────────────┘                           │
 *   │                      ▼                                           │
 *   │               DuckDB (single database file)                      │
 *   └──────────────────────────────────────────────────────────────────┘
 * @endcode
 *
 * ## Flight RPC surface
 *
 * | RPC                        | Purpose                              |
 * |----------------------------|--------------------------------------|
 * | `DoGet(ticket)`            | Execute SELECT; stream Arrow batches |
 * | `DoAction("execute", sql)` | Execute DML/DDL; wait for commit     |
 * | `DoAction("ping", "")`     | Liveness check; returns "pong"       |
 * | `DoAction("stats", "")`    | JSON server metrics                  |
 * | `ListActions()`            | Enumerate supported action types     |
 *
 * ## Threading model
 *
 * Arrow Flight (gRPC) manages its own thread pool.  Each incoming RPC call
 * is dispatched to one of gRPC's worker threads.  Concurrent `DoGet` calls
 * each borrow a distinct connection from `ConnectionPool`; concurrent
 * `DoAction("execute")` calls are serialised by `WriteSerializer`.
 */

#include "connection_pool.hpp"
#include "write_serializer.hpp"
#include <arrow/flight/api.h>
#include <atomic>
#include <memory>
#include <string>

namespace das {

// ─────────────────────────────────────────────────────────────────────────────
/**
 * @brief Configuration parameters for DuckFlightServer.
 *
 * All fields have sensible defaults that auto-scale to the host machine.
 * Construct and adjust the fields you need, then pass to DuckFlightServer.
 *
 * @code
 *   ServerConfig cfg;
 *   cfg.db_path  = "C:/data/analytics.duckdb";
 *   cfg.port     = 17777;
 *   cfg.tls_cert_path = "server.crt";
 *   cfg.tls_key_path  = "server.key";
 *   DuckFlightServer server(cfg);
 * @endcode
 */
// ─────────────────────────────────────────────────────────────────────────────
struct ServerConfig {
    /// DuckDB database file path, or `":memory:"` for an in-process database.
    std::string db_path = ":memory:";

    /// Network address to bind. `"0.0.0.0"` listens on all interfaces.
    std::string host = "0.0.0.0";

    /// gRPC listen port.
    int port = 17777;

    /**
     * @brief Number of DuckDB read connections to maintain in the pool.
     *
     * Each concurrent `DoGet` call consumes one connection.  Setting this
     * too low causes `DoGet` callers to queue; too high wastes memory.
     *
     * **Default (0):** auto-configured to `2 × hardware_concurrency()`.
     * Rule of thumb: set to the number of physical CPU cores on the host.
     */
    size_t reader_pool_size = 0;

    /**
     * @brief Write batch accumulation window in milliseconds.
     *
     * The writer thread waits up to this long for additional write requests
     * before committing the current batch.  Increasing this value raises write
     * throughput at the cost of higher write latency.
     */
    int write_batch_ms = 5;

    /**
     * @brief Maximum number of DML statements per write transaction.
     *
     * Once this many statements accumulate, the batch flushes immediately
     * regardless of the time window.
     */
    size_t write_batch_max = 512;

    /**
     * @brief Path to TLS server certificate PEM file.
     * Leave empty (default) for plaintext gRPC. Both cert and key must be set together.
     */
    std::string tls_cert_path;

    /**
     * @brief Path to TLS server private key PEM file.
     * Leave empty (default) for plaintext gRPC. Both cert and key must be set together.
     */
    std::string tls_key_path;
};

// ─────────────────────────────────────────────────────────────────────────────
/**
 * @brief Snapshot of live server metrics.
 *
 * Obtained via `DuckFlightServer::stats()`.  All counts are cumulative since
 * server start and monotonically increasing.
 */
// ─────────────────────────────────────────────────────────────────────────────
struct ServerStats {
    long long queries_read;    ///< Total `DoGet` calls served.
    long long queries_write;   ///< Total `DoAction("execute")` calls served.
    long long errors;          ///< Total failed RPCs (query errors, pool timeouts, …).
    size_t    reader_pool_size;///< Configured read connection pool size.
    int       port;            ///< Actual listen port (from `server.port()`).
};

// ─────────────────────────────────────────────────────────────────────────────
/**
 * @brief Arrow Flight server backed by a DuckDB database.
 *
 * Subclasses `arrow::flight::FlightServerBase` and overrides `DoGet`,
 * `DoAction`, and `ListActions`.  All other gRPC plumbing (thread pool,
 * TLS handshake, HTTP/2 framing) is handled by the Arrow Flight / gRPC
 * runtime.
 *
 * ### Typical usage
 * @code
 *   ServerConfig cfg;
 *   cfg.db_path = "analytics.duckdb";
 *   DuckFlightServer server(cfg);
 *
 *   arrow::flight::Location loc;
 *   arrow::flight::Location::ForGrpcTcp("0.0.0.0", cfg.port, &loc);
 *   arrow::flight::FlightServerOptions opts(loc);
 *   server.Init(opts);
 *   server.Serve();  // blocks until Shutdown() is called
 * @endcode
 */
// ─────────────────────────────────────────────────────────────────────────────
class DuckFlightServer : public arrow::flight::FlightServerBase {
public:
    /**
     * @brief Construct the server, open the database, and start the writer thread.
     *
     * Does NOT begin accepting connections — call `Init()` then `Serve()`.
     *
     * @param cfg  Configuration (database path, pool size, TLS paths, …).
     * @throws std::runtime_error if the database cannot be opened or any
     *         connection cannot be created.
     */
    explicit DuckFlightServer(const ServerConfig& cfg);

    /**
     * @brief Shut down the writer thread and close all database connections.
     * Safe to call before or after `Serve()` returns.
     */
    ~DuckFlightServer() override;

    // Non-copyable (owns database handles and threads)
    DuckFlightServer(const DuckFlightServer&)            = delete;
    DuckFlightServer& operator=(const DuckFlightServer&) = delete;

    // ── FlightServerBase overrides ────────────────────────────────────────────

    /**
     * @brief Execute a SELECT query and stream Arrow record batches to the client.
     *
     * The `ticket.ticket` bytes are interpreted as a UTF-8 SQL string.
     * A `DuckDbRecordBatchSource` is created and wrapped in a
     * `RecordBatchStream` — batches flow from DuckDB to gRPC with no
     * intermediate copy.
     *
     * The borrowed connection is held for the duration of the stream and
     * returned to the pool when the stream is destroyed (after the last batch
     * is sent).
     *
     * @param ctx     gRPC call context (peer address, metadata, cancellation).
     * @param request Flight ticket; `ticket.ticket` = UTF-8 SELECT SQL.
     * @param stream  Output: set to a `RecordBatchStream` on success.
     * @return `OK` on success; `ExecutionError` on DuckDB failure; `Internal`
     *         on connection pool timeout.
     */
    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Ticket& request,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

    /**
     * @brief Dispatch a named action (execute / ping / stats).
     *
     * | `action.type` | `action.body`   | Returns                     |
     * |---------------|-----------------|----------------------------|
     * | `"execute"`   | UTF-8 DML/DDL   | Empty stream on success    |
     * | `"ping"`      | empty           | Single result: `"pong"`    |
     * | `"stats"`     | empty           | Single result: JSON object |
     *
     * Unknown action types return `NotImplemented`.
     *
     * @param ctx     gRPC call context.
     * @param action  Named action with optional body payload.
     * @param result  Output: populated result stream.
     */
    arrow::Status DoAction(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Action& action,
        std::unique_ptr<arrow::flight::ResultStream>* result) override;

    /**
     * @brief Enumerate supported action types.
     *
     * Returns descriptors for `execute`, `ping`, and `stats`.  Flight clients
     * can call `ListActions` to discover server capabilities at runtime without
     * prior knowledge of the server implementation.
     *
     * @param ctx     gRPC call context.
     * @param actions Output vector of ActionType descriptors.
     */
    arrow::Status ListActions(
        const arrow::flight::ServerCallContext& ctx,
        std::vector<arrow::flight::ActionType>* actions) override;

    // ── Metrics ───────────────────────────────────────────────────────────────

    /**
     * @brief Return a snapshot of live server metrics.
     *
     * Thread-safe: reads from atomics.  Counts are cumulative since start.
     */
    ServerStats stats() const;

    /**
     * @brief Record the actual listening port after Init() completes.
     * FlightServerBase::port() is only valid post-Init(); this method
     * caches it so stats() can return the real port rather than the
     * configured value (which may be 0 for OS-assigned ports).
     */
    void set_actual_port(int port) { actual_port_.store(port); }

private:
    // ── Action handlers ───────────────────────────────────────────────────────

    /// Handle `DoAction("execute", sql)` — route to WriteSerializer.
    arrow::Status handle_execute(
        const std::string& sql,
        std::unique_ptr<arrow::flight::ResultStream>* result);

    /// Handle `DoAction("ping")` — return "pong".
    arrow::Status handle_ping(
        std::unique_ptr<arrow::flight::ResultStream>* result);

    /// Handle `DoAction("stats")` — return JSON metrics.
    arrow::Status handle_stats(
        std::unique_ptr<arrow::flight::ResultStream>* result);

    // ── Utilities ─────────────────────────────────────────────────────────────

    /**
     * @brief Build a single-item ResultStream with the given body string.
     *
     * Used by `handle_ping` and `handle_stats`.  An empty body string produces
     * an empty (zero-item) stream, signalling success for `handle_execute`.
     */
    static std::unique_ptr<arrow::flight::ResultStream>
    make_result(const std::string& body);

    // ── State ─────────────────────────────────────────────────────────────────

    ServerConfig     cfg_;           ///< Server configuration (immutable after construction).
    duckdb_database  db_;            ///< Opened database handle.
    duckdb_connection writer_conn_;  ///< Dedicated write connection (owned).
    std::unique_ptr<ConnectionPool>  read_pool_; ///< Read connection pool (IConnectionPool conceptually).
    std::unique_ptr<IWriteSerializer> writer_;   ///< Write serializer (IWriteSerializer).

    // Atomics initialised in constructor body (VS 2017: no NSDMI for atomics).
    std::atomic<long long> stat_queries_read_;  ///< Cumulative DoGet calls.
    std::atomic<long long> stat_queries_write_; ///< Cumulative execute actions.
    std::atomic<long long> stat_errors_;        ///< Cumulative RPC failures.
    std::atomic<int>       actual_port_;        ///< Actual bound port (set after Init()).
};

} // namespace das
