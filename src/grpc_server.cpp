/**
 * @file grpc_server.cpp
 * @brief Implementation of DuckGrpcServer.
 *
 * Uses the custom DuckDbService protocol (proto/duckdb_service.proto).
 * Query results are serialized as Arrow IPC streams inside QueryResponse.ipc_data.
 */

#include "grpc_server.hpp"
#include <duckdb.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>
#include <iostream>
#include <sstream>
#include <thread>

// DuckDB Arrow C API (declared here to keep headers clean)
extern "C" {
duckdb_state duckdb_query_arrow(duckdb_connection, const char*, duckdb_arrow*);
duckdb_state duckdb_query_arrow_schema(duckdb_arrow, duckdb_arrow_schema*);
duckdb_state duckdb_query_arrow_array(duckdb_arrow, duckdb_arrow_array*);
const char*  duckdb_query_arrow_error(duckdb_arrow);
void         duckdb_destroy_arrow(duckdb_arrow*);
void         duckdb_destroy_arrow_schema(duckdb_arrow_schema*);
void         duckdb_destroy_arrow_array(duckdb_arrow_array*);
}

namespace das {

// ─────────────────────────────────────────────────────────────────────────────
// Construction / destruction
// ─────────────────────────────────────────────────────────────────────────────

DuckGrpcServer::DuckGrpcServer(const ServerConfig& cfg)
    : cfg_(cfg)
    , db_(nullptr)
    , writer_conn_(nullptr)
{
    stat_queries_read_.store(0);
    stat_queries_write_.store(0);
    stat_errors_.store(0);

    if (cfg_.reader_pool_size == 0) {
        unsigned hw = std::thread::hardware_concurrency();
        cfg_.reader_pool_size = (hw > 0 ? hw : 4) * 2;
    }

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
            throw std::runtime_error("Cannot open database: " + cfg_.db_path);

        read_pool_ = std::unique_ptr<ConnectionPool>(
            new ConnectionPool(db_, cfg_.reader_pool_size));

        if (duckdb_connect(db_, &writer_conn_) == DuckDBError)
            throw std::runtime_error("Cannot create writer connection");

        writer_ = std::unique_ptr<IWriteSerializer>(
            new WriteSerializer(writer_conn_, cfg_.write_batch_ms, cfg_.write_batch_max));
    } catch (...) {
        cleanup();
        throw;
    }

    std::cout << "[das] DuckGrpcServer initialised\n"
              << "      db       = " << cfg_.db_path << "\n"
              << "      readers  = " << cfg_.reader_pool_size << "\n"
              << "      batch_ms = " << cfg_.write_batch_ms << "\n";
}

DuckGrpcServer::~DuckGrpcServer() {
    writer_.reset();
    read_pool_.reset();
    if (writer_conn_) duckdb_disconnect(&writer_conn_);
    if (db_)          duckdb_close(&db_);
}

ServerStats DuckGrpcServer::stats() const {
    return {
        stat_queries_read_.load(),
        stat_queries_write_.load(),
        stat_errors_.load(),
        cfg_.reader_pool_size,
        cfg_.port
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: serialize Arrow schema + batches from a DuckDB query into IPC bytes
// ─────────────────────────────────────────────────────────────────────────────

static std::string serialize_query_to_ipc(duckdb_connection conn, const std::string& sql) {
    // Step 1: Execute the query
    duckdb_arrow cursor = nullptr;
    if (duckdb_query_arrow(conn, sql.c_str(), &cursor) == DuckDBError) {
        std::string err = duckdb_query_arrow_error(cursor);
        if (cursor) duckdb_destroy_arrow(&cursor);
        throw std::runtime_error(err.empty() ? "DuckDB: unknown query error" : err);
    }

    // Step 2: Import schema
    duckdb_arrow_schema raw_schema = nullptr;
    if (duckdb_query_arrow_schema(cursor, &raw_schema) == DuckDBError) {
        if (raw_schema) duckdb_destroy_arrow_schema(&raw_schema);
        duckdb_destroy_arrow(&cursor);
        throw std::runtime_error("DuckDB: failed to retrieve Arrow schema");
    }

    auto schema_result = arrow::ImportSchema(reinterpret_cast<ArrowSchema*>(raw_schema));
    if (!schema_result.ok()) {
        duckdb_destroy_arrow(&cursor);
        throw std::runtime_error("ImportSchema: " + schema_result.status().ToString());
    }
    auto schema = schema_result.ValueOrDie();

    // Step 3: Write IPC stream to buffer
    auto buffer_stream = arrow::io::BufferOutputStream::Create(1024 * 1024).ValueOrDie();
    auto ipc_writer_result = arrow::ipc::MakeStreamWriter(buffer_stream, schema);
    if (!ipc_writer_result.ok()) {
        duckdb_destroy_arrow(&cursor);
        throw std::runtime_error("MakeStreamWriter: " + ipc_writer_result.status().ToString());
    }
    auto ipc_writer = ipc_writer_result.ValueOrDie();

    // Step 4: Write batches
    while (true) {
        duckdb_arrow_array raw_array = nullptr;
        if (duckdb_query_arrow_array(cursor, &raw_array) == DuckDBError) {
            ipc_writer->Close();
            duckdb_destroy_arrow(&cursor);
            throw std::runtime_error("duckdb_query_arrow_array failed");
        }

        auto* arr = reinterpret_cast<ArrowArray*>(raw_array);
        bool is_eos = (!arr || arr->length == 0);
        if (is_eos) {
            if (raw_array) duckdb_destroy_arrow_array(&raw_array);
            break;
        }

        auto batch_result = arrow::ImportRecordBatch(arr, schema);
        if (!batch_result.ok()) {
            ipc_writer->Close();
            duckdb_destroy_arrow(&cursor);
            throw std::runtime_error("ImportRecordBatch: " + batch_result.status().ToString());
        }

        auto status = ipc_writer->WriteRecordBatch(*batch_result.ValueOrDie());
        if (!status.ok()) {
            ipc_writer->Close();
            duckdb_destroy_arrow(&cursor);
            throw std::runtime_error("WriteRecordBatch: " + status.ToString());
        }
    }

    ipc_writer->Close();
    duckdb_destroy_arrow(&cursor);

    auto buffer = buffer_stream->Finish().ValueOrDie();
    return buffer->ToString();
}

// ─────────────────────────────────────────────────────────────────────────────
// Query — stream Arrow IPC results
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::Query(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::QueryRequest* request,
    grpc::ServerWriter<duckdb::v1::QueryResponse>* writer)
{
    const std::string& sql = request->sql();

    try {
        auto handle = read_pool_->borrow();

        std::string ipc_data = serialize_query_to_ipc(handle.get(), sql);

        duckdb::v1::QueryResponse response;
        response.set_ipc_data(ipc_data);
        writer->Write(response);

        stat_queries_read_.fetch_add(1);
        return grpc::Status::OK;
    } catch (const std::exception& ex) {
        stat_errors_.fetch_add(1);
        return grpc::Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Execute — DML/DDL
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::Execute(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::ExecuteRequest* request,
    duckdb::v1::ExecuteResponse* response)
{
    const std::string& sql = request->sql();

    if (sql.empty()) {
        stat_errors_.fetch_add(1);
        response->set_success(false);
        response->set_error("SQL statement is required");
        return grpc::Status::OK;
    }

    const WriteResult wr = writer_->submit(sql);
    if (!wr.ok) {
        stat_errors_.fetch_add(1);
        response->set_success(false);
        response->set_error(wr.error);
        return grpc::Status::OK;
    }

    stat_queries_write_.fetch_add(1);
    response->set_success(true);
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// Ping
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::Ping(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::PingRequest* /*request*/,
    duckdb::v1::PingResponse* response)
{
    response->set_message("pong");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// GetStats
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::GetStats(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::StatsRequest* /*request*/,
    duckdb::v1::StatsResponse* response)
{
    const ServerStats s = stats();
    response->set_queries_read(s.queries_read);
    response->set_queries_write(s.queries_write);
    response->set_errors(s.errors);
    response->set_reader_pool_size(static_cast<int>(s.reader_pool_size));
    response->set_port(s.port);
    return grpc::Status::OK;
}

} // namespace das
