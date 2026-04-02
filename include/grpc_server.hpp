#pragma once
#include "connection_pool.hpp"
#include "write_serializer.hpp"

#include <grpcpp/grpcpp.h>
#include "duckdb_service.grpc.pb.h"

#include <duckdb.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace das {

struct ServerConfig {
    std::string db_path = ":memory:";
    std::string host = "0.0.0.0";
    int port = 17777;
    size_t reader_pool_size = 0;
    int write_batch_ms = 5;
    size_t write_batch_max = 512;
    int shards = 1;
    std::string tls_cert_path;
    std::string tls_key_path;
};

struct ServerStats {
    long long queries_read;
    long long queries_write;
    long long errors;
    size_t    reader_pool_size;
    int       port;
};

/// One shard = one independent DuckDB instance + pool + writer.
struct Shard {
    duckdb_database db;
    std::unique_ptr<ConnectionPool> pool;
    duckdb_connection writer_conn;
    std::unique_ptr<IWriteSerializer> writer;

    ~Shard() {
        writer.reset();
        pool.reset();
        if (writer_conn) duckdb_disconnect(&writer_conn);
        if (db) duckdb_close(&db);
    }
};

class DuckGrpcServer final : public duckdb::v1::DuckDbService::Service {
public:
    explicit DuckGrpcServer(const ServerConfig& cfg);
    ~DuckGrpcServer();

    DuckGrpcServer(const DuckGrpcServer&) = delete;
    DuckGrpcServer& operator=(const DuckGrpcServer&) = delete;

    grpc::Status Query(grpc::ServerContext*, const duckdb::v1::QueryRequest*,
        grpc::ServerWriter<duckdb::v1::QueryResponse>*) override;
    grpc::Status Execute(grpc::ServerContext*, const duckdb::v1::ExecuteRequest*,
        duckdb::v1::ExecuteResponse*) override;
    grpc::Status Ping(grpc::ServerContext*, const duckdb::v1::PingRequest*,
        duckdb::v1::PingResponse*) override;
    grpc::Status GetStats(grpc::ServerContext*, const duckdb::v1::StatsRequest*,
        duckdb::v1::StatsResponse*) override;
    grpc::Status BulkInsert(grpc::ServerContext*, const duckdb::v1::BulkInsertRequest*,
        duckdb::v1::BulkInsertResponse*) override;

    ServerStats stats() const;

private:
    /// Get next shard for reading (round-robin).
    Shard& next_for_read();

    /// Write to ALL shards (fan-out).
    WriteResult write_to_all(const std::string& sql);

    ServerConfig cfg_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<size_t> next_read_shard_;

    std::atomic<long long> stat_queries_read_;
    std::atomic<long long> stat_queries_write_;
    std::atomic<long long> stat_errors_;
};

} // namespace das
