#pragma once
/**
 * @file grpc_server.hpp
 * @brief gRPC server wrapping a DuckDB database.
 *
 * Uses the custom DuckDbService protocol (proto/duckdb_service.proto).
 * No Arrow dependency — query results are serialized as protobuf rows.
 */

#include "connection_pool.hpp"
#include "write_serializer.hpp"

#include <grpcpp/grpcpp.h>
#include "duckdb_service.grpc.pb.h"

#include <duckdb.h>
#include <atomic>
#include <memory>
#include <string>

namespace das {

struct ServerConfig {
    std::string db_path = ":memory:";
    std::string host = "0.0.0.0";
    int port = 17777;
    size_t reader_pool_size = 0;
    int write_batch_ms = 5;
    size_t write_batch_max = 512;
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

class DuckGrpcServer final : public duckdb::v1::DuckDbService::Service {
public:
    explicit DuckGrpcServer(const ServerConfig& cfg);
    ~DuckGrpcServer();

    DuckGrpcServer(const DuckGrpcServer&) = delete;
    DuckGrpcServer& operator=(const DuckGrpcServer&) = delete;

    grpc::Status Query(
        grpc::ServerContext* context,
        const duckdb::v1::QueryRequest* request,
        grpc::ServerWriter<duckdb::v1::QueryResponse>* writer) override;

    grpc::Status Execute(
        grpc::ServerContext* context,
        const duckdb::v1::ExecuteRequest* request,
        duckdb::v1::ExecuteResponse* response) override;

    grpc::Status Ping(
        grpc::ServerContext* context,
        const duckdb::v1::PingRequest* request,
        duckdb::v1::PingResponse* response) override;

    grpc::Status GetStats(
        grpc::ServerContext* context,
        const duckdb::v1::StatsRequest* request,
        duckdb::v1::StatsResponse* response) override;

    ServerStats stats() const;

private:
    ServerConfig cfg_;
    duckdb_database db_;
    duckdb_connection writer_conn_;
    std::unique_ptr<ConnectionPool> read_pool_;
    std::unique_ptr<IWriteSerializer> writer_;

    std::atomic<long long> stat_queries_read_;
    std::atomic<long long> stat_queries_write_;
    std::atomic<long long> stat_errors_;
};

} // namespace das
