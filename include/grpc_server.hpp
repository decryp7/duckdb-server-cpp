#pragma once
#include "connection_pool.hpp"
#include "write_serializer.hpp"

#include <grpcpp/grpcpp.h>
#include "duckdb_service.grpc.pb.h"

#include <duckdb.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
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

/// Thread-safe query result cache with TTL expiration.
class QueryCache {
public:
    QueryCache(size_t max_entries = 10000, int ttl_seconds = 60)
        : max_entries_(max_entries), ttl_seconds_(ttl_seconds) {}

    bool try_get(const std::string& sql, std::vector<duckdb::v1::QueryResponse>& out) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = entries_.find(sql);
        if (it == entries_.end()) { ++misses_; return false; }
        auto age = std::chrono::steady_clock::now() - it->second.created;
        if (age > std::chrono::seconds(ttl_seconds_)) {
            entries_.erase(it); ++misses_; return false;
        }
        out = it->second.responses;
        ++hits_;
        return true;
    }

    void put(const std::string& sql, const std::vector<duckdb::v1::QueryResponse>& responses) {
        std::lock_guard<std::mutex> lock(mu_);
        if (entries_.size() >= max_entries_) evict_expired();
        if (entries_.size() < max_entries_)
            entries_[sql] = {responses, std::chrono::steady_clock::now()};
    }

    void invalidate() { std::lock_guard<std::mutex> lock(mu_); entries_.clear(); }

private:
    struct Entry {
        std::vector<duckdb::v1::QueryResponse> responses;
        std::chrono::steady_clock::time_point created;
    };
    void evict_expired() {
        auto now = std::chrono::steady_clock::now();
        for (auto it = entries_.begin(); it != entries_.end(); )
            if ((now - it->second.created) > std::chrono::seconds(ttl_seconds_))
                it = entries_.erase(it);
            else ++it;
    }
    std::unordered_map<std::string, Entry> entries_;
    std::mutex mu_;
    size_t max_entries_;
    int ttl_seconds_;
    long long hits_ = 0, misses_ = 0;
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
    QueryCache cache_;
    std::atomic<size_t> next_read_shard_;

    std::atomic<long long> stat_queries_read_;
    std::atomic<long long> stat_queries_write_;
    std::atomic<long long> stat_errors_;
};

} // namespace das
