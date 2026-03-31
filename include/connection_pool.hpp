#pragma once
/**
 * @file connection_pool.hpp
 * @brief Thread-safe fixed-size pool of DuckDB connections.
 *
 * DuckDB supports many concurrent readers on separate connections but only one
 * writer at a time (MVCC design).  This pool manages the reader connections:
 *
 * @code
 *   // In the server constructor:
 *   ConnectionPool pool(db_handle, /*size=*/ 16);
 *
 *   // In a request handler (any thread):
 *   auto handle = pool.borrow();         // blocks if all connections busy
 *   duckdb_connection conn = handle.get();
 *   // ... execute query ...
 *   // handle destroyed here → connection returned to pool automatically
 * @endcode
 *
 * @note Implements IConnectionPool for testability, but callers typically
 *       interact with the concrete type directly via the Handle RAII wrapper.
 */

#include "interfaces.hpp"
#include <duckdb.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>

namespace das {

/**
 * @brief Concrete thread-safe pool of `duckdb_connection` handles.
 *
 * All connections share a single `duckdb_database`.  The pool either:
 *   - Accepts an externally-opened database handle (does **not** take ownership), or
 *   - Opens and owns a database given a file path.
 *
 * Thread safety: `borrow()` and the internal `release()` are both guarded by
 * a mutex + condition variable.  Multiple threads may call `borrow()`
 * simultaneously; each blocks independently if no connection is idle.
 */
class ConnectionPool {
public:
    // ── Construction ──────────────────────────────────────────────────────────

    /**
     * @brief Construct from a pre-opened database handle.
     *
     * The caller retains ownership of `db` and must ensure it outlives this pool.
     *
     * @param db                 Open DuckDB database handle (not owned by pool).
     * @param pool_size          Number of connections to create.
     * @param borrow_timeout_ms  Max milliseconds to wait for an idle connection
     *                           before throwing `std::runtime_error`.
     */
    ConnectionPool(duckdb_database db,
                   size_t pool_size,
                   int borrow_timeout_ms = 10'000)
        : owns_db_(false)
        , db_(db)
        , pool_size_(pool_size)
        , borrow_timeout_ms_(borrow_timeout_ms)
    {
        init_connections();
    }

    /**
     * @brief Construct by opening a database from a file path.
     *
     * The pool opens the database and owns the resulting handle.
     *
     * @param db_path            Path to a DuckDB file, or `":memory:"`.
     * @param pool_size          Number of connections to create.
     * @param borrow_timeout_ms  Max milliseconds to wait for an idle connection.
     * @throws std::runtime_error if the database cannot be opened.
     */
    ConnectionPool(const std::string& db_path,
                   size_t pool_size,
                   int borrow_timeout_ms = 10'000)
        : owns_db_(true)
        , pool_size_(pool_size)
        , borrow_timeout_ms_(borrow_timeout_ms)
    {
        const bool is_memory = db_path.empty() || db_path == ":memory:";
        const char* path = is_memory ? nullptr : db_path.c_str();

        if (duckdb_open(path, &db_) == DuckDBError)
            throw std::runtime_error("ConnectionPool: failed to open database: " + db_path);

        try {
            init_connections();
        } catch (...) {
            // init_connections cleans up its own connections on failure,
            // but we must close the database we opened.
            duckdb_close(&db_);
            throw;
        }
    }

    /** @brief Destroys all idle connections and, if owned, closes the database.
     *  Waits for all borrowed connections to be returned before destroying. */
    ~ConnectionPool() {
        std::unique_lock<std::mutex> lock(mu_);
        // Wait until all borrowed connections have been returned.
        // Without this, Handle destructors would write to a destroyed queue.
        cv_.wait(lock, [this] { return idle_.size() == pool_size_; });
        while (!idle_.empty()) {
            duckdb_disconnect(&idle_.front());
            idle_.pop();
        }
        if (owns_db_) duckdb_close(&db_);
    }

    // Non-copyable, non-movable (owns mutex + cv + connections)
    ConnectionPool(const ConnectionPool&)            = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // ── RAII Handle ───────────────────────────────────────────────────────────

    /**
     * @brief RAII wrapper that holds a borrowed connection.
     *
     * Automatically returns the connection to the pool when destroyed,
     * even if an exception is thrown.  Non-copyable; movable once.
     *
     * @code
     *   {
     *     auto h = pool.borrow();
     *     execute_query(h.get(), "SELECT ...");
     *   } // ← connection returned here
     * @endcode
     */
    class Handle {
    public:
        /** @brief Construct from pool reference and raw connection pointer. */
        Handle(ConnectionPool& pool, duckdb_connection conn)
            : pool_(pool), conn_(conn) {}

        /** @brief Returns the connection to the pool. */
        ~Handle() { if (conn_) pool_.release(conn_); }

        /** @brief Move constructor: transfers ownership, nullifies source. */
        Handle(Handle&& other) noexcept
            : pool_(other.pool_), conn_(other.conn_)
        { other.conn_ = nullptr; }

        Handle(const Handle&)            = delete;
        Handle& operator=(const Handle&) = delete;

        /** @brief The raw DuckDB connection. Valid for the Handle's lifetime. */
        duckdb_connection get() const { return conn_; }

    private:
        ConnectionPool&   pool_;
        duckdb_connection conn_;
    };

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * @brief Borrow an idle connection, blocking until one is available.
     *
     * If no connection becomes idle within `borrow_timeout_ms`, throws
     * `std::runtime_error` with a diagnostic message suggesting pool size
     * adjustment.
     *
     * @return Handle that returns the connection on destruction.
     * @throws std::runtime_error on timeout.
     */
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

    /** @brief Total number of connections (idle + borrowed). */
    size_t size() const { return pool_size_; }

private:
    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * @brief Create `pool_size_` connections and enqueue them.
     * @throws std::runtime_error if any connection fails.
     */
    void init_connections() {
        try {
            for (size_t i = 0; i < pool_size_; ++i) {
                duckdb_connection conn;
                if (duckdb_connect(db_, &conn) == DuckDBError)
                    throw std::runtime_error(
                        "ConnectionPool: failed to create connection "
                        + std::to_string(i + 1) + "/" + std::to_string(pool_size_));
                idle_.push(conn);
            }
        } catch (...) {
            // Clean up connections created so far.
            while (!idle_.empty()) {
                duckdb_disconnect(&idle_.front());
                idle_.pop();
            }
            pool_size_ = 0; // prevent destructor wait
            throw;
        }
    }

    /**
     * @brief Return a connection to the idle queue and notify one waiter.
     * Called exclusively by Handle's destructor.
     */
    void release(duckdb_connection conn) {
        if (!conn) return;
        {
            std::lock_guard<std::mutex> lock(mu_);
            idle_.push(conn);
        }
        cv_.notify_one();
    }

    // ── State ─────────────────────────────────────────────────────────────────
    bool                          owns_db_;           ///< Whether we opened (and must close) db_.
    duckdb_database               db_       = nullptr;///< The shared database handle.
    size_t                        pool_size_;         ///< Fixed total connection count.
    int                           borrow_timeout_ms_; ///< Timeout for borrow().
    std::queue<duckdb_connection> idle_;              ///< Currently idle connections.
    std::mutex                    mu_;                ///< Guards idle_ queue.
    std::condition_variable       cv_;                ///< Wakes blocked borrow() callers.
};

} // namespace das
