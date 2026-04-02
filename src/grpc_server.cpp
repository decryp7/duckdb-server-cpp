/**
 * @file grpc_server.cpp
 * @brief Implementation of DuckGrpcServer.
 *
 * Uses DuckDB C API directly for queries. No Arrow dependency.
 * Results are serialized as protobuf rows (ColumnInfo + Row + Value).
 */

#include "grpc_server.hpp"
#include <duckdb.h>
#include <iostream>
#include <sstream>
#include <thread>

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
// Query — stream protobuf rows
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::Query(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::QueryRequest* request,
    grpc::ServerWriter<duckdb::v1::QueryResponse>* writer)
{
    const std::string& sql = request->sql();

    try {
        auto handle = read_pool_->borrow();

        // Execute query using DuckDB C API
        duckdb_result result;
        if (duckdb_query(handle.get(), sql.c_str(), &result) == DuckDBError) {
            std::string err = duckdb_result_error(&result);
            duckdb_destroy_result(&result);
            stat_errors_.fetch_add(1);
            return grpc::Status(grpc::StatusCode::INTERNAL, err);
        }

        idx_t col_count = duckdb_column_count(&result);
        idx_t row_count = duckdb_row_count(&result);

        // First response: column schema
        duckdb::v1::QueryResponse schema_response;
        for (idx_t c = 0; c < col_count; ++c) {
            auto* col_info = schema_response.add_columns();
            col_info->set_name(duckdb_column_name(&result, c));

            duckdb_type type = duckdb_column_type(&result, c);
            switch (type) {
                case DUCKDB_TYPE_BOOLEAN:   col_info->set_type("BOOLEAN"); break;
                case DUCKDB_TYPE_TINYINT:   col_info->set_type("TINYINT"); break;
                case DUCKDB_TYPE_SMALLINT:  col_info->set_type("SMALLINT"); break;
                case DUCKDB_TYPE_INTEGER:   col_info->set_type("INTEGER"); break;
                case DUCKDB_TYPE_BIGINT:    col_info->set_type("BIGINT"); break;
                case DUCKDB_TYPE_FLOAT:     col_info->set_type("FLOAT"); break;
                case DUCKDB_TYPE_DOUBLE:    col_info->set_type("DOUBLE"); break;
                case DUCKDB_TYPE_VARCHAR:   col_info->set_type("VARCHAR"); break;
                case DUCKDB_TYPE_TIMESTAMP: col_info->set_type("TIMESTAMP"); break;
                case DUCKDB_TYPE_DATE:      col_info->set_type("DATE"); break;
                case DUCKDB_TYPE_TIME:      col_info->set_type("TIME"); break;
                case DUCKDB_TYPE_BLOB:      col_info->set_type("BLOB"); break;
                case DUCKDB_TYPE_DECIMAL:   col_info->set_type("DECIMAL"); break;
                default:                    col_info->set_type("UNKNOWN"); break;
            }
        }
        writer->Write(schema_response);

        // Stream rows in batches of 8192
        const idx_t batch_size = 8192;
        duckdb::v1::QueryResponse batch;
        idx_t rows_in_batch = 0;

        for (idx_t r = 0; r < row_count; ++r) {
            auto* row = batch.add_rows();
            for (idx_t c = 0; c < col_count; ++c) {
                auto* val = row->add_values();
                if (duckdb_value_is_null(&result, c, r)) {
                    val->set_is_null(true);
                } else {
                    char* str = duckdb_value_varchar(&result, c, r);
                    if (str) {
                        val->set_text(str);
                        duckdb_free(str);
                    } else {
                        val->set_is_null(true);
                    }
                }
            }
            rows_in_batch++;

            if (rows_in_batch >= batch_size) {
                writer->Write(batch);
                batch.Clear();
                rows_in_batch = 0;
            }
        }

        // Flush remaining
        if (rows_in_batch > 0) {
            writer->Write(batch);
        }

        duckdb_destroy_result(&result);
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
// Ping / GetStats
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status DuckGrpcServer::Ping(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::PingRequest* /*request*/,
    duckdb::v1::PingResponse* response)
{
    response->set_message("pong");
    return grpc::Status::OK;
}

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
