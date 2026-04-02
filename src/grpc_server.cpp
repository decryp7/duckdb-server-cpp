/**
 * @file grpc_server.cpp
 * @brief Implementation of DuckGrpcServer — heavily optimized.
 *
 * Uses columnar encoding: values packed into typed arrays per column.
 * Uses DuckDB C API directly (duckdb_value_int32, duckdb_value_double, etc.)
 * to avoid string conversion overhead for numeric types.
 */

#include "grpc_server.hpp"
#include <duckdb.h>
#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>

namespace das {

// ─── Construction / destruction (unchanged) ──────────────────────────────────

DuckGrpcServer::DuckGrpcServer(const ServerConfig& cfg)
    : cfg_(cfg), db_(nullptr), writer_conn_(nullptr)
{
    stat_queries_read_.store(0);
    stat_queries_write_.store(0);
    stat_errors_.store(0);

    if (cfg_.reader_pool_size == 0) {
        unsigned hw = std::thread::hardware_concurrency();
        cfg_.reader_pool_size = (hw > 0 ? hw : 4) * 2;
    }

    auto cleanup = [this]() noexcept {
        writer_.reset(); read_pool_.reset();
        if (writer_conn_) { duckdb_disconnect(&writer_conn_); writer_conn_ = nullptr; }
        if (db_) { duckdb_close(&db_); db_ = nullptr; }
    };

    try {
        const bool is_memory = cfg_.db_path.empty() || cfg_.db_path == ":memory:";
        const char* path = is_memory ? nullptr : cfg_.db_path.c_str();
        if (duckdb_open(path, &db_) == DuckDBError)
            throw std::runtime_error("Cannot open database: " + cfg_.db_path);
        read_pool_ = std::unique_ptr<ConnectionPool>(new ConnectionPool(db_, cfg_.reader_pool_size));
        if (duckdb_connect(db_, &writer_conn_) == DuckDBError)
            throw std::runtime_error("Cannot create writer connection");
        writer_ = std::unique_ptr<IWriteSerializer>(
            new WriteSerializer(writer_conn_, cfg_.write_batch_ms, cfg_.write_batch_max));
    } catch (...) { cleanup(); throw; }

    std::cout << "[das] DuckGrpcServer initialised (columnar mode)\n"
              << "      db       = " << cfg_.db_path << "\n"
              << "      readers  = " << cfg_.reader_pool_size << "\n";
}

DuckGrpcServer::~DuckGrpcServer() {
    writer_.reset(); read_pool_.reset();
    if (writer_conn_) duckdb_disconnect(&writer_conn_);
    if (db_) duckdb_close(&db_);
}

ServerStats DuckGrpcServer::stats() const {
    return { stat_queries_read_.load(), stat_queries_write_.load(),
             stat_errors_.load(), cfg_.reader_pool_size, cfg_.port };
}

// ─── Helper: map DuckDB type to proto ColumnType ─────────────────────────────

static duckdb::v1::ColumnType map_column_type(duckdb_type t) {
    switch (t) {
        case DUCKDB_TYPE_BOOLEAN:   return duckdb::v1::TYPE_BOOLEAN;
        case DUCKDB_TYPE_TINYINT:   return duckdb::v1::TYPE_INT8;
        case DUCKDB_TYPE_SMALLINT:  return duckdb::v1::TYPE_INT16;
        case DUCKDB_TYPE_INTEGER:   return duckdb::v1::TYPE_INT32;
        case DUCKDB_TYPE_BIGINT:    return duckdb::v1::TYPE_INT64;
        case DUCKDB_TYPE_UTINYINT:  return duckdb::v1::TYPE_UINT8;
        case DUCKDB_TYPE_USMALLINT: return duckdb::v1::TYPE_UINT16;
        case DUCKDB_TYPE_UINTEGER:  return duckdb::v1::TYPE_UINT32;
        case DUCKDB_TYPE_UBIGINT:   return duckdb::v1::TYPE_UINT64;
        case DUCKDB_TYPE_FLOAT:     return duckdb::v1::TYPE_FLOAT;
        case DUCKDB_TYPE_DOUBLE:    return duckdb::v1::TYPE_DOUBLE;
        case DUCKDB_TYPE_VARCHAR:   return duckdb::v1::TYPE_STRING;
        case DUCKDB_TYPE_BLOB:      return duckdb::v1::TYPE_BLOB;
        case DUCKDB_TYPE_TIMESTAMP: return duckdb::v1::TYPE_TIMESTAMP;
        case DUCKDB_TYPE_DATE:      return duckdb::v1::TYPE_DATE;
        case DUCKDB_TYPE_TIME:      return duckdb::v1::TYPE_TIME;
        case DUCKDB_TYPE_DECIMAL:   return duckdb::v1::TYPE_DECIMAL;
        default:                    return duckdb::v1::TYPE_STRING;
    }
}

// ─── Helper: add a value to ColumnData using typed DuckDB accessors ──────────
// Avoids duckdb_value_varchar (which allocates a string) for numeric types.

static void add_value_to_column(
    duckdb_result* result, idx_t col, idx_t row,
    duckdb::v1::ColumnType type, duckdb::v1::ColumnData* cd)
{
    if (duckdb_value_is_null(result, col, row)) {
        cd->add_null_indices(static_cast<int>(row));
        // Add placeholder to keep indices aligned
        switch (type) {
            case duckdb::v1::TYPE_BOOLEAN:  cd->add_bool_values(false); break;
            case duckdb::v1::TYPE_INT8:
            case duckdb::v1::TYPE_INT16:
            case duckdb::v1::TYPE_INT32:
            case duckdb::v1::TYPE_UINT8:
            case duckdb::v1::TYPE_UINT16:   cd->add_int32_values(0); break;
            case duckdb::v1::TYPE_INT64:
            case duckdb::v1::TYPE_UINT32:
            case duckdb::v1::TYPE_UINT64:   cd->add_int64_values(0); break;
            case duckdb::v1::TYPE_FLOAT:    cd->add_float_values(0); break;
            case duckdb::v1::TYPE_DOUBLE:
            case duckdb::v1::TYPE_DECIMAL:  cd->add_double_values(0); break;
            default:                        cd->add_string_values(""); break;
        }
        return;
    }

    switch (type) {
        case duckdb::v1::TYPE_BOOLEAN:
            cd->add_bool_values(duckdb_value_boolean(result, col, row));
            break;
        case duckdb::v1::TYPE_INT8:
            cd->add_int32_values(duckdb_value_int8(result, col, row));
            break;
        case duckdb::v1::TYPE_INT16:
            cd->add_int32_values(duckdb_value_int16(result, col, row));
            break;
        case duckdb::v1::TYPE_INT32:
        case duckdb::v1::TYPE_UINT8:
        case duckdb::v1::TYPE_UINT16:
            cd->add_int32_values(duckdb_value_int32(result, col, row));
            break;
        case duckdb::v1::TYPE_INT64:
        case duckdb::v1::TYPE_UINT32:
        case duckdb::v1::TYPE_UINT64:
            cd->add_int64_values(duckdb_value_int64(result, col, row));
            break;
        case duckdb::v1::TYPE_FLOAT:
            cd->add_float_values(duckdb_value_float(result, col, row));
            break;
        case duckdb::v1::TYPE_DOUBLE:
        case duckdb::v1::TYPE_DECIMAL:
            cd->add_double_values(duckdb_value_double(result, col, row));
            break;
        default: {
            // String/timestamp/date/time — use varchar (allocates, but no choice)
            char* str = duckdb_value_varchar(result, col, row);
            if (str) { cd->add_string_values(str); duckdb_free(str); }
            else { cd->add_string_values(""); }
            break;
        }
    }
}

// ─── Query: columnar streaming ───────────────────────────────────────────────

grpc::Status DuckGrpcServer::Query(
    grpc::ServerContext* /*context*/,
    const duckdb::v1::QueryRequest* request,
    grpc::ServerWriter<duckdb::v1::QueryResponse>* writer)
{
    try {
        auto handle = read_pool_->borrow();

        duckdb_result result;
        if (duckdb_query(handle.get(), request->sql().c_str(), &result) == DuckDBError) {
            std::string err = duckdb_result_error(&result);
            duckdb_destroy_result(&result);
            stat_errors_.fetch_add(1);
            return grpc::Status(grpc::StatusCode::INTERNAL, err);
        }

        idx_t col_count = duckdb_column_count(&result);
        idx_t row_count = duckdb_row_count(&result);

        // Cache column types
        std::vector<duckdb::v1::ColumnType> col_types(col_count);
        std::vector<duckdb_type> duck_types(col_count);

        for (idx_t c = 0; c < col_count; ++c) {
            duck_types[c] = duckdb_column_type(&result, c);
            col_types[c] = map_column_type(duck_types[c]);
        }

        const idx_t batch_size = 8192;

        for (idx_t batch_start = 0; batch_start <= row_count; batch_start += batch_size) {
            idx_t batch_end = batch_start + batch_size;
            if (batch_end > row_count) batch_end = row_count;
            idx_t batch_rows = batch_end - batch_start;

            duckdb::v1::QueryResponse response;
            response.set_row_count(static_cast<int>(batch_rows));

            // First batch includes column metadata
            if (batch_start == 0) {
                for (idx_t c = 0; c < col_count; ++c) {
                    auto* meta = response.add_columns();
                    meta->set_name(duckdb_column_name(&result, c));
                    meta->set_type(col_types[c]);
                }
            }

            // Build columnar data
            for (idx_t c = 0; c < col_count; ++c) {
                auto* cd = response.add_data();
                for (idx_t r = batch_start; r < batch_end; ++r) {
                    add_value_to_column(&result, c, r, col_types[c], cd);
                }
            }

            if (batch_rows > 0 || batch_start == 0)
                writer->Write(response);

            if (batch_rows == 0) break;
        }

        duckdb_destroy_result(&result);
        stat_queries_read_.fetch_add(1);
        return grpc::Status::OK;

    } catch (const std::exception& ex) {
        stat_errors_.fetch_add(1);
        return grpc::Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

// ─── Execute / Ping / GetStats (unchanged) ───────────────────────────────────

grpc::Status DuckGrpcServer::Execute(
    grpc::ServerContext*, const duckdb::v1::ExecuteRequest* request,
    duckdb::v1::ExecuteResponse* response)
{
    if (request->sql().empty()) {
        stat_errors_.fetch_add(1);
        response->set_success(false);
        response->set_error("SQL required");
        return grpc::Status::OK;
    }
    const WriteResult wr = writer_->submit(request->sql());
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

grpc::Status DuckGrpcServer::Ping(
    grpc::ServerContext*, const duckdb::v1::PingRequest*,
    duckdb::v1::PingResponse* response)
{
    response->set_message("pong");
    return grpc::Status::OK;
}

grpc::Status DuckGrpcServer::GetStats(
    grpc::ServerContext*, const duckdb::v1::StatsRequest*,
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
