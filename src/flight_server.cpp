/**
 * @file flight_server.cpp
 * @brief Implementation of DuckFlightServer.
 *
 * See flight_server.hpp for the full design documentation.
 */

#include "flight_server.hpp"
#include "duck_bridge.hpp"

#include <duckdb.h>
#include <arrow/buffer.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace das {

// ─────────────────────────────────────────────────────────────────────────────
// Construction / destruction
// ─────────────────────────────────────────────────────────────────────────────

DuckFlightServer::DuckFlightServer(const ServerConfig& cfg)
    : cfg_(cfg)
    , db_(nullptr)
    , writer_conn_(nullptr)
{
    // VS 2017 compatibility: initialise atomics in constructor body.
    stat_queries_read_.store(0);
    stat_queries_write_.store(0);
    stat_errors_.store(0);
    actual_port_.store(cfg_.port); // updated to real port via set_actual_port() after Init()

    // Auto-size the reader pool to 2× the number of logical CPUs.
    if (cfg_.reader_pool_size == 0) {
        unsigned hw = std::thread::hardware_concurrency();
        cfg_.reader_pool_size = (hw > 0 ? hw : 4) * 2;
    }

    // All resource acquisition is wrapped in a try block so that if any step
    // fails, the cleanup lambda runs and releases everything acquired so far.
    // C++ only calls the destructor for a fully-constructed object; if the
    // constructor throws, partially-acquired resources (db_, writer_conn_) would
    // leak without this explicit cleanup.
    auto cleanup = [this]() noexcept {
        writer_.reset();
        read_pool_.reset();
        if (writer_conn_) { duckdb_disconnect(&writer_conn_); writer_conn_ = nullptr; }
        if (db_)          { duckdb_close(&db_);               db_          = nullptr; }
    };

    try {
        const bool is_memory = cfg_.db_path.empty() || cfg_.db_path == ":memory:";
        const char* path = is_memory ? nullptr : cfg_.db_path.c_str();

        if (duckdb_open(path, &db_) == DuckDBError)
            throw std::runtime_error("DuckFlightServer: cannot open database: " + cfg_.db_path);

        // Allocate the read pool — shares the database handle but does not own it.
        read_pool_ = std::unique_ptr<ConnectionPool>(
            new ConnectionPool(db_, cfg_.reader_pool_size));

        // Create the dedicated write connection.
        if (duckdb_connect(db_, &writer_conn_) == DuckDBError)
            throw std::runtime_error("DuckFlightServer: cannot create writer connection");

        // Start the write serializer (and its background thread).
        writer_ = std::unique_ptr<IWriteSerializer>(
            new WriteSerializer(writer_conn_, cfg_.write_batch_ms, cfg_.write_batch_max));

    } catch (...) {
        cleanup();  // release everything acquired before rethrowing
        throw;
    }

    std::cout << "[das] DuckFlightServer initialised\n"
              << "      db       = " << cfg_.db_path << "\n"
              << "      readers  = " << cfg_.reader_pool_size << "\n"
              << "      batch_ms = " << cfg_.write_batch_ms << "\n";
}

DuckFlightServer::~DuckFlightServer() {
    // Destruction order matters:
    //   1. Stop the writer (joins background thread, drains in-flight writes).
    //   2. Release the read pool (disconnects all reader connections).
    //   3. Disconnect the writer connection.
    //   4. Close the database.
    writer_.reset();
    read_pool_.reset();
    if (writer_conn_) duckdb_disconnect(&writer_conn_);
    if (db_)          duckdb_close(&db_);
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

ServerStats DuckFlightServer::stats() const {
    return {
        stat_queries_read_.load(),
        stat_queries_write_.load(),
        stat_errors_.load(),
        cfg_.reader_pool_size,
        actual_port_.load() // set via set_actual_port() after Init()
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// DoGet — read queries
// ─────────────────────────────────────────────────────────────────────────────

arrow::Status DuckFlightServer::DoGet(
    const arrow::flight::ServerCallContext& /*ctx*/,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream)
{
    // The ticket payload is the raw UTF-8 SQL string.
    const std::string sql(request.ticket.begin(), request.ticket.end());

    try {
        // Borrow a read connection — blocks if the pool is exhausted.
        auto handle = read_pool_->borrow();

        // Create the record batch source.
        // NOTE: Do NOT use ARROW_ASSIGN_OR_RAISE here.
        // ARROW_ASSIGN_OR_RAISE expands to an early `return status` on failure,
        // which would bypass this try/catch block entirely.  Arrow errors (SQL
        // execution failures, schema import errors) would then skip the
        // stat_errors_ increment and the structured error logging below.
        auto source_result = DuckDbRecordBatchSource::Make(std::move(handle), sql);
        if (!source_result.ok()) {
            stat_errors_.fetch_add(1);
            return source_result.status();
        }

        // Wrap the source in a Flight stream.  gRPC will call ReadNext()
        // on a worker thread as it serialises batches to the client.
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(
            source_result.ValueOrDie());

        // Count only after the stream is successfully set up.
        stat_queries_read_.fetch_add(1);
        return arrow::Status::OK();

    } catch (const std::exception& ex) {
        // Catches ConnectionPool timeout and any other std::exception.
        stat_errors_.fetch_add(1);
        return arrow::Status::Internal(ex.what());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DoAction — execute / ping / stats
// ─────────────────────────────────────────────────────────────────────────────

arrow::Status DuckFlightServer::DoAction(
    const arrow::flight::ServerCallContext& /*ctx*/,
    const arrow::flight::Action& action,
    std::unique_ptr<arrow::flight::ResultStream>* result)
{
    if (action.type == "execute") {
        // action.body can be null if the client sends an empty body buffer.
        // Guard before dereferencing to avoid a null-pointer crash.
        if (!action.body) {
            stat_errors_.fetch_add(1);
            return arrow::Status::Invalid(
                "execute action requires a non-empty SQL body");
        }
        const std::string sql(
            reinterpret_cast<const char*>(action.body->data()),
            static_cast<size_t>(action.body->size()));
        if (sql.empty()) {
            stat_errors_.fetch_add(1);
            return arrow::Status::Invalid("execute action requires non-empty SQL");
        }
        return handle_execute(sql, result);
    }
    if (action.type == "ping")  return handle_ping(result);
    if (action.type == "stats") return handle_stats(result);

    return arrow::Status::NotImplemented(
        "Unknown action '", action.type,
        "'. Supported: execute, ping, stats.");
}

// ── Action handlers ───────────────────────────────────────────────────────────

arrow::Status DuckFlightServer::handle_execute(
    const std::string& sql,
    std::unique_ptr<arrow::flight::ResultStream>* result)
{
    stat_queries_write_.fetch_add(1);

    // Submit to the write serializer and block until the batch commits.
    const WriteResult wr = writer_->submit(sql);
    if (!wr.ok) {
        stat_errors_.fetch_add(1);
        // Return the DuckDB error message as a gRPC INTERNAL status.
        // The .NET client surfaces this as a DasException.
        return arrow::Status::ExecutionError(wr.error);
    }

    // Success: empty result stream signals "no data, operation succeeded".
    *result = make_result("");
    return arrow::Status::OK();
}

arrow::Status DuckFlightServer::handle_ping(
    std::unique_ptr<arrow::flight::ResultStream>* result)
{
    *result = make_result("pong");
    return arrow::Status::OK();
}

arrow::Status DuckFlightServer::handle_stats(
    std::unique_ptr<arrow::flight::ResultStream>* result)
{
    const ServerStats s = stats();

    // Format as a compact JSON object.  No external dependency needed.
    std::ostringstream js;
    js << "{"
       << "\"queries_read\":"     << s.queries_read     << ","
       << "\"queries_write\":"    << s.queries_write    << ","
       << "\"errors\":"           << s.errors           << ","
       << "\"reader_pool_size\":" << s.reader_pool_size << ","
       << "\"port\":"             << s.port
       << "}";

    *result = make_result(js.str());
    return arrow::Status::OK();
}

// ─────────────────────────────────────────────────────────────────────────────
// ListActions
// ─────────────────────────────────────────────────────────────────────────────

arrow::Status DuckFlightServer::ListActions(
    const arrow::flight::ServerCallContext& /*ctx*/,
    std::vector<arrow::flight::ActionType>* actions)
{
    actions->clear();
    actions->push_back({
        "execute",
        "Execute DML or DDL SQL (INSERT, UPDATE, DELETE, CREATE, DROP, …). "
        "Action body = UTF-8 SQL. Returns an empty result stream on success. "
        "Returns gRPC INTERNAL status on DuckDB error."
    });
    actions->push_back({
        "ping",
        "Liveness check. Returns a single result whose body is the ASCII string 'pong'."
    });
    actions->push_back({
        "stats",
        "Live server metrics. Returns a single result whose body is a UTF-8 JSON object "
        "with fields: queries_read, queries_write, errors, reader_pool_size, port."
    });
    return arrow::Status::OK();
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

std::unique_ptr<arrow::flight::ResultStream>
DuckFlightServer::make_result(const std::string& body) {
    std::vector<arrow::flight::Result> items;
    if (!body.empty()) {
        arrow::flight::Result r;
        r.body = arrow::Buffer::FromString(body);
        items.push_back(std::move(r));
    }
    return std::make_unique<arrow::flight::SimpleResultStream>(std::move(items));
}

} // namespace das
