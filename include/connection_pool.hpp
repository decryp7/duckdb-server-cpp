#pragma once
/**
 * @file connection_pool.hpp
 * @brief Thread-safe fixed-size pool of DuckDB connections.
 */

#include <duckdb.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>

namespace das {

class ConnectionPool {
public:
    ConnectionPool(duckdb_database db,
                   size_t pool_size,
                   int borrow_timeout_ms = 10000)
        : owns_db_(false)
        , db_(db)
        , pool_size_(pool_size)
        , borrow_timeout_ms_(borrow_timeout_ms)
    {
        init_connections();
    }

    ~ConnectionPool() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return idle_.size() == pool_size_; });
        while (!idle_.empty()) {
            duckdb_disconnect(&idle_.front());
            idle_.pop();
        }
        if (owns_db_) duckdb_close(&db_);
    }

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    class Handle {
    public:
        Handle(ConnectionPool& pool, duckdb_connection conn)
            : pool_(pool), conn_(conn) {}

        ~Handle() { if (conn_) pool_.release(conn_); }

        Handle(Handle&& other) noexcept
            : pool_(other.pool_), conn_(other.conn_)
        { other.conn_ = nullptr; }

        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        duckdb_connection get() const { return conn_; }

    private:
        ConnectionPool& pool_;
        duckdb_connection conn_;
    };

    Handle borrow() {
        std::unique_lock<std::mutex> lock(mu_);

        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(borrow_timeout_ms_);

        const bool available = cv_.wait_until(lock, deadline,
            [this] { return !idle_.empty(); });

        if (!available)
            throw std::runtime_error(
                "ConnectionPool: timed out waiting for an idle connection "
                "(pool_size=" + std::to_string(pool_size_) + "). "
                "Consider increasing --readers.");

        duckdb_connection conn = idle_.front();
        idle_.pop();
        return Handle(*this, conn);
    }

    size_t size() const { return pool_size_; }

private:
    void init_connections() {
        try {
            for (size_t i = 0; i < pool_size_; ++i) {
                duckdb_connection conn;
                if (duckdb_connect(db_, &conn) == DuckDBError)
                    throw std::runtime_error(
                        "ConnectionPool: failed to create connection "
                        + std::to_string(i + 1) + "/" + std::to_string(pool_size_));
                apply_connection_pragmas(conn);
                idle_.push(conn);
            }
        } catch (...) {
            while (!idle_.empty()) {
                duckdb_disconnect(&idle_.front());
                idle_.pop();
            }
            pool_size_ = 0;
            throw;
        }
    }

    /// Apply per-connection performance settings.
    /// threads=1 eliminates internal thread contention (pool provides parallelism).
    /// preserve_insertion_order=false speeds up scans without ORDER BY.
    /// enable_object_cache caches metadata for faster lookups.
    static void apply_connection_pragmas(duckdb_connection conn) {
        duckdb_query(conn, "SET threads=1", nullptr);
        duckdb_query(conn, "SET preserve_insertion_order=false", nullptr);
        duckdb_query(conn, "PRAGMA enable_object_cache", nullptr);
        duckdb_query(conn, "SET checkpoint_threshold='256MB'", nullptr);
    }

    void release(duckdb_connection conn) {
        if (!conn) return;
        {
            std::lock_guard<std::mutex> lock(mu_);
            idle_.push(conn);
        }
        cv_.notify_one();
    }

    bool owns_db_;
    duckdb_database db_;
    size_t pool_size_;
    int borrow_timeout_ms_;
    std::queue<duckdb_connection> idle_;
    std::mutex mu_;
    std::condition_variable cv_;
};

} // namespace das
