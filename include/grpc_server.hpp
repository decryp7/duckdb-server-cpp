#pragma once
/**
 * @file grpc_server.hpp
 * @brief Core header for the DuckDB gRPC Server.
 *
 * Defines the main service class (DuckGrpcServer), sharding infrastructure,
 * query result caching, and server configuration / statistics types.
 *
 * ## Architecture overview
 *
 * The server exposes DuckDB over gRPC with five RPCs:
 *   - Query   — streaming read (server-side streaming of columnar chunks)
 *   - Execute — fire-and-forget write (DDL / DML)
 *   - BulkInsert — columnar bulk insert from protobuf arrays
 *   - Ping    — health check
 *   - GetStats — runtime metrics
 *
 * ## Sharding
 *
 * The database can be split across N independent DuckDB instances ("shards").
 * Reads are distributed round-robin; writes fan out to every shard so all
 * replicas stay consistent.  Each shard has its own ConnectionPool for reads
 * and a dedicated WriteSerializer for serialised, batched writes.
 *
 * ## Thread safety
 *
 * DuckGrpcServer is designed to be called concurrently from gRPC worker
 * threads.  Internal synchronisation is provided by:
 *   - std::atomic counters for statistics and round-robin index
 *   - QueryCache's internal mutex
 *   - ConnectionPool's internal mutex + condition variable
 *   - WriteSerializer's internal mutex + condition variable
 */

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

/**
 * @brief Configuration parameters for DuckGrpcServer.
 *
 * Populated from command-line arguments in main_grpc.cpp and passed
 * to the DuckGrpcServer constructor.  All fields have sensible defaults.
 */
struct ServerConfig {
    /// Path to the DuckDB database file.  ":memory:" (default) creates an
    /// in-memory database.  When sharding is enabled (shards > 1) and a
    /// file path is given, each shard gets a suffix (e.g. "db_0.duckdb").
    std::string db_path = ":memory:";

    /// Network interface to bind on.  "0.0.0.0" listens on all interfaces.
    std::string host = "0.0.0.0";

    /// TCP port for the gRPC listener.
    int port = 19100;

    /// Total number of read connections across all shards.  0 means
    /// auto-detect: 2 * hardware_concurrency (or 8 if detection fails).
    /// Divided evenly among shards at construction time.
    size_t reader_pool_size = 0;

    /// Maximum milliseconds the WriteSerializer waits to collect additional
    /// statements before flushing a batch.  Lower = less latency, higher =
    /// more throughput from transaction amortisation.
    int write_batch_ms = 5;

    /// Maximum number of DML statements per write batch.  The batch flushes
    /// immediately once this limit is reached, regardless of the time window.
    size_t write_batch_max = 512;

    /// Number of independent DuckDB instances.  1 (default) = no sharding.
    /// Each shard gets its own database file, connection pool, and writer.
    int shards = 1;

    /// Path to PEM-encoded TLS certificate.  If non-empty, TLS is enabled
    /// and tls_key_path must also be set.
    std::string tls_cert_path;

    /// Path to PEM-encoded TLS private key.  Must be set together with
    /// tls_cert_path; if both are empty, the server uses plaintext HTTP/2.
    std::string tls_key_path;

    /// Per-shard memory limit, e.g. "4GB", "512MB".
    /// Empty = auto-calculate per shard (80% / shard_count).
    std::string memory_limit;

    /// Path for DuckDB spill-to-disk temp directory.  Empty = DuckDB default.
    std::string temp_directory;
};

/**
 * @brief Snapshot of server runtime statistics.
 *
 * Returned by DuckGrpcServer::stats() and exposed via the GetStats RPC.
 * All counters are monotonically increasing since server start.
 */
struct ServerStats {
    long long queries_read;       ///< Total successful Query RPCs served.
    long long queries_write;      ///< Total successful Execute / BulkInsert RPCs.
    long long errors;             ///< Total RPCs that returned an error.
    size_t    reader_pool_size;   ///< Aggregate reader connections across all shards.
    int       port;               ///< Port the server is listening on.
};

/**
 * @brief One shard — an independent DuckDB instance with its own pool and writer.
 *
 * Each shard encapsulates:
 *   - A DuckDB database handle (db)
 *   - A ConnectionPool of read-only connections (pool)
 *   - A dedicated write connection (writer_conn) used exclusively by the
 *     WriteSerializer
 *   - A WriteSerializer that batches and serialises write operations
 *
 * ## Destruction order
 *
 * The destructor tears down components in reverse dependency order:
 *   1. writer  — stops the background writer thread (may flush pending writes)
 *   2. pool    — waits for all borrowed connections to be returned, then closes them
 *   3. writer_conn — disconnects the write connection
 *   4. db      — closes the DuckDB database (flushes WAL, releases file locks)
 *
 * This ordering is critical: the writer and pool hold live connections that
 * reference the database, so they must be destroyed before the database is closed.
 */
struct Shard {
    duckdb_database db;                       ///< DuckDB database handle for this shard.
    std::unique_ptr<ConnectionPool> pool;     ///< Fixed-size pool of read connections.
    duckdb_connection writer_conn;            ///< Dedicated connection for write operations.
    std::unique_ptr<IWriteSerializer> writer; ///< Batching write serializer (owns a background thread).
    duckdb_connection bulk_conn;              ///< Dedicated connection for Appender-based bulk inserts.
    std::mutex bulk_mu;                       ///< Mutex protecting bulk_conn (Appender is not thread-safe).

    ~Shard() {
        writer.reset();                       /* Stop writer thread first. */
        pool.reset();                         /* Close all pooled read connections. */
        if (writer_conn) duckdb_disconnect(&writer_conn);
        if (bulk_conn) duckdb_disconnect(&bulk_conn);
        if (db) duckdb_close(&db);
    }

    // Non-copyable (raw C handles would be double-freed on copy)
    Shard(const Shard&) = delete;
    Shard& operator=(const Shard&) = delete;
};

/**
 * @brief Thread-safe LRU-style query result cache with TTL expiration.
 *
 * Caches the full serialised protobuf response for SELECT queries so
 * repeated identical queries skip DuckDB execution entirely.  The cache
 * is invalidated wholesale on any write (Execute / BulkInsert) to ensure
 * read-after-write consistency.
 *
 * ## Eviction policy
 *
 * - **TTL**: entries older than `ttl_seconds_` are considered stale and
 *   removed on access (lazy) or during eviction sweeps.
 * - **Capacity**: when the cache is full, `evict_expired()` removes all
 *   stale entries.  If the cache is still full after eviction, new entries
 *   are silently dropped (no LRU ordering is maintained).
 *
 * ## Thread safety
 *
 * All public methods acquire `mu_` (a std::mutex).  The cache is safe to
 * call from multiple gRPC handler threads concurrently.
 */
class QueryCache {
public:
    /**
     * @brief Construct a query cache.
     * @param max_entries  Maximum number of cached query results.
     * @param ttl_seconds  Time-to-live for each entry in seconds.
     */
    QueryCache(size_t max_entries = 10000, int ttl_seconds = 60)
        : max_entries_(max_entries), ttl_seconds_(ttl_seconds) {}

    /**
     * @brief Look up a cached result for the given SQL string.
     *
     * If a valid (non-expired) entry exists, copies the cached response
     * vector into `out` and returns true.  Otherwise returns false.
     * Expired entries are eagerly removed on access.
     *
     * @param sql  The SQL query string used as the cache key.
     * @param out  Receives the cached responses on a hit.
     * @return     True on cache hit, false on miss or expiration.
     */
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

    /**
     * @brief Insert or update a cache entry.
     *
     * If the cache is at capacity, runs an eviction sweep first.  If still
     * at capacity after eviction (all entries are fresh), the new entry is
     * silently dropped to avoid unbounded growth.
     *
     * @param sql        The SQL query string (cache key).
     * @param responses  The protobuf responses to cache.
     */
    void put(const std::string& sql, const std::vector<duckdb::v1::QueryResponse>& responses) {
        std::lock_guard<std::mutex> lock(mu_);
        if (entries_.size() >= max_entries_) evict_expired();
        if (entries_.size() < max_entries_)
            entries_[sql] = {responses, std::chrono::steady_clock::now()};
    }

    /**
     * @brief Remove all cached entries.
     *
     * Called after every write operation to ensure read-after-write
     * consistency.  This is a blunt but correct invalidation strategy;
     * fine-grained table-level tracking could be added if needed.
     */
    void invalidate() { std::lock_guard<std::mutex> lock(mu_); entries_.clear(); }

private:
    /** @brief A single cached query result with its creation timestamp. */
    struct Entry {
        std::vector<duckdb::v1::QueryResponse> responses; ///< Serialised response chunks.
        std::chrono::steady_clock::time_point created;    ///< When this entry was cached.
    };

    /**
     * @brief Sweep all entries and remove those that have exceeded TTL.
     *
     * Called when the cache is at capacity before inserting a new entry.
     * Iterates the entire map; O(n) but n is bounded by max_entries_.
     * Must be called with mu_ held.
     */
    void evict_expired() {
        auto now = std::chrono::steady_clock::now();
        for (auto it = entries_.begin(); it != entries_.end(); )
            if ((now - it->second.created) > std::chrono::seconds(ttl_seconds_))
                it = entries_.erase(it);
            else ++it;
    }

    std::unordered_map<std::string, Entry> entries_; ///< SQL -> cached result map.
    std::mutex mu_;               ///< Protects entries_, hits_, misses_.
    size_t max_entries_;          ///< Maximum number of entries before eviction.
    int ttl_seconds_;             ///< Time-to-live per entry in seconds.
    long long hits_ = 0;         ///< Cache hit counter (diagnostic, not exposed yet).
    long long misses_ = 0;       ///< Cache miss counter (diagnostic, not exposed yet).
};

/**
 * @brief Main gRPC service implementing the DuckDB protocol.
 *
 * Inherits from the generated DuckDbService::Service base class and overrides
 * all five RPCs.  Owns the shard vector, query cache, and atomic counters.
 *
 * ## Concurrency model
 *
 * gRPC's synchronous server calls each RPC handler on a thread-pool thread.
 * Multiple Query/Execute/BulkInsert calls may run in parallel.  Thread safety
 * is guaranteed by:
 *   - Atomic counters for statistics (stat_queries_read_, etc.)
 *   - Atomic round-robin index (next_read_shard_) for read distribution
 *   - QueryCache internal mutex
 *   - ConnectionPool internal mutex per shard
 *   - WriteSerializer internal mutex per shard
 *
 * ## Non-copyable
 *
 * The server owns threads, mutexes, and database handles, so copying is deleted.
 */
class DuckGrpcServer final : public duckdb::v1::DuckDbService::Service {
public:
    /**
     * @brief Construct the server: open shards, create pools, start writers.
     *
     * If reader_pool_size is 0 in cfg, auto-detects based on hardware
     * concurrency.  Divides readers evenly among shards.
     *
     * @param cfg  Server configuration (database path, port, pool sizes, etc.).
     * @throws std::runtime_error if any shard fails to open.
     */
    explicit DuckGrpcServer(const ServerConfig& cfg);

    /**
     * @brief Destroy the server: tears down all shards in reverse order.
     *
     * Shard destruction stops writer threads and closes all DuckDB connections
     * and database handles.
     */
    ~DuckGrpcServer();

    /* Non-copyable, non-movable (owns threads, mutexes, database handles). */
    DuckGrpcServer(const DuckGrpcServer&) = delete;
    DuckGrpcServer& operator=(const DuckGrpcServer&) = delete;

    /**
     * @brief Handle a streaming read query.
     *
     * Checks the query cache first; on a miss, borrows a read connection
     * from a round-robin shard, executes the SQL, and streams back columnar
     * protobuf chunks.  Results are cached for future identical queries.
     *
     * Uses DuckDB's chunk API for direct pointer access and memcpy fast
     * paths on non-null numeric columns.  Checks IsCancelled() per chunk
     * to support early client disconnection.
     */
    grpc::Status Query(grpc::ServerContext*, const duckdb::v1::QueryRequest*,
        grpc::ServerWriter<duckdb::v1::QueryResponse>*) override;

    /**
     * @brief Execute a write statement (DDL or DML).
     *
     * Routes the SQL through write_to_all() which fans out to every shard's
     * WriteSerializer.  On success, invalidates the query cache to maintain
     * read-after-write consistency.
     */
    grpc::Status Execute(grpc::ServerContext*, const duckdb::v1::ExecuteRequest*,
        duckdb::v1::ExecuteResponse*) override;

    /**
     * @brief Health check RPC.  Returns "pong".
     */
    grpc::Status Ping(grpc::ServerContext*, const duckdb::v1::PingRequest*,
        duckdb::v1::PingResponse*) override;

    /**
     * @brief Return server runtime statistics (read/write counts, errors, pool size).
     */
    grpc::Status GetStats(grpc::ServerContext*, const duckdb::v1::StatsRequest*,
        duckdb::v1::StatsResponse*) override;

    /**
     * @brief Columnar bulk insert from protobuf arrays using the Appender API.
     *
     * Uses DuckDB's Appender API to bypass SQL parsing entirely (10-100x
     * faster than INSERT SQL).  Each shard has a dedicated bulk_conn with
     * mutex for thread safety.  Invalidates the query cache.
     */
    grpc::Status BulkInsert(grpc::ServerContext*, const duckdb::v1::BulkInsertRequest*,
        duckdb::v1::BulkInsertResponse*) override;

    /**
     * @brief Arrow IPC streaming query (not implemented in C++ server).
     *
     * Returns UNIMPLEMENTED — Arrow IPC serialization requires the Arrow C++
     * library which is not linked.  Use the Rust server for Arrow IPC support.
     */
    grpc::Status QueryArrow(grpc::ServerContext*, const duckdb::v1::QueryRequest*,
        grpc::ServerWriter<duckdb::v1::ArrowResponse>*) override;

    /**
     * @brief Collect a snapshot of server statistics.
     * @return ServerStats with current counter values and pool size.
     */
    ServerStats stats() const;

private:
    /**
     * @brief Select the next shard for a read operation (round-robin).
     *
     * Uses an atomic fetch_add with relaxed ordering for maximum throughput.
     * The modulo operation handles wrap-around of the counter.
     *
     * @return Reference to the selected Shard.
     */
    Shard& next_for_read();

    /**
     * @brief Fan out a write to all shards, ensuring consistency.
     *
     * For single-shard configurations, directly submits to the one shard.
     * For multi-shard, iterates all shards serially (could be parallelised).
     * Returns the first error encountered, or success if all shards succeed.
     *
     * @param sql  The DML or DDL SQL to execute on every shard.
     * @return     WriteResult indicating success or the first error.
     */
    WriteResult write_to_all(const std::string& sql);

    /**
     * @brief Execute SQL directly on bulk_conn for all shards, bypassing WriteSerializer.
     *
     * Used for INSERT statements which can safely run concurrently with other
     * appends. Avoids the WriteSerializer batch queue for lower latency.
     *
     * @param sql  The INSERT SQL to execute on every shard's bulk_conn.
     * @return     WriteResult indicating success or the first error encountered.
     */
    WriteResult bulk_execute_all(const std::string& sql);

    ServerConfig cfg_;                             ///< Copy of the server configuration.
    std::vector<std::unique_ptr<Shard>> shards_;   ///< All shards (1 unless --shards > 1).
    QueryCache cache_;                             ///< Thread-safe query result cache.
    std::atomic<size_t> next_read_shard_;           ///< Round-robin counter for read distribution.

    std::atomic<long long> stat_queries_read_;      ///< Monotonic counter of successful reads.
    std::atomic<long long> stat_queries_write_;     ///< Monotonic counter of successful writes.
    std::atomic<long long> stat_errors_;            ///< Monotonic counter of error responses.
};

} // namespace das
