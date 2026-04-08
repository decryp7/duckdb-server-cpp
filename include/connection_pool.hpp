#pragma once
/**
 * @file connection_pool.hpp
 * @brief Thread-safe fixed-size pool of DuckDB read connections.
 *
 * ## Purpose
 *
 * DuckDB supports many concurrent readers but each `duckdb_connection` is
 * **not** thread-safe — it must be used by at most one thread at a time.
 * This pool pre-creates N connections at construction and lends them out
 * via the RAII Handle class, ensuring each connection is used by exactly
 * one thread.
 *
 * ## Concurrency model
 *
 * The pool is protected by a single mutex + condition variable.
 *   - `borrow()` blocks (with timeout) until a connection becomes available.
 *   - When the Handle is destroyed it returns the connection to the pool and
 *     wakes one waiting borrower.
 *
 * ## Thread safety
 *
 * All public methods (borrow, size) are thread-safe.  The Handle class is
 * move-only; its destructor returns the connection to the pool under the
 * lock.  The pool must outlive all outstanding Handles.
 *
 * ## Destruction
 *
 * The destructor waits until all borrowed connections have been returned
 * (idle_.size() == pool_size_), then closes every connection.  This means
 * destroying the pool while Handles are outstanding will block indefinitely
 * — callers must ensure all Handles are dropped before the pool is destroyed.
 */

#include <duckdb.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>

namespace das {

/**
 * @brief Fixed-size, thread-safe pool of DuckDB connections.
 *
 * Non-copyable, non-movable.  Connections are created eagerly at construction
 * and kept alive for the pool's lifetime (no lazy creation, no shrinking).
 *
 * @see Handle — RAII wrapper that auto-returns a borrowed connection.
 */
class ConnectionPool {
public:
    /**
     * @brief Create a pool of `pool_size` connections against `db`.
     *
     * All connections are created immediately.  If any connection fails,
     * all previously created connections are closed and the constructor
     * throws.
     *
     * @param db               An open DuckDB database handle.  The caller
     *                         retains ownership; the database must outlive
     *                         the pool.
     * @param pool_size        Number of connections to pre-create.
     * @param borrow_timeout_ms  Maximum milliseconds borrow() will block
     *                         waiting for an idle connection before throwing.
     *                         Default: 10 000 ms (10 seconds).
     * @throws std::runtime_error if any connection cannot be created.
     */
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

    /**
     * @brief Destroy the pool: wait for all Handles, then close connections.
     *
     * Blocks until idle_.size() == pool_size_ (all borrowed connections
     * have been returned), then disconnects every connection.  If the pool
     * owns the database (owns_db_ is true), closes it too.
     */
    ~ConnectionPool() {
        std::unique_lock<std::mutex> lock(mu_);
        /* Wait until every outstanding Handle has been destroyed, returning
         * its connection to the idle queue. */
        cv_.wait(lock, [this] { return idle_.size() == pool_size_; });
        while (!idle_.empty()) {
            duckdb_disconnect(&idle_.front());
            idle_.pop();
        }
        if (owns_db_) duckdb_close(&db_);
    }

    /* Non-copyable: the pool owns mutex state and connection handles. */
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    /**
     * @brief RAII wrapper around a borrowed DuckDB connection.
     *
     * On destruction (or move-from), returns the connection to the pool.
     * Move-only; cannot be copied.
     *
     * Usage:
     * @code
     *   auto handle = pool.borrow();
     *   duckdb_query(handle.get(), "SELECT 1", nullptr);
     *   // handle goes out of scope -> connection returned to pool
     * @endcode
     */
    class Handle {
    public:
        /**
         * @brief Construct a Handle wrapping a borrowed connection.
         * @param pool  The pool to return the connection to on destruction.
         * @param conn  The borrowed DuckDB connection.
         */
        Handle(ConnectionPool& pool, duckdb_connection conn)
            : pool_(pool), conn_(conn) {}

        /**
         * @brief Return the borrowed connection to the pool.
         *
         * Only returns if conn_ is non-null (i.e., this Handle has not
         * been moved-from).
         */
        ~Handle() { if (conn_) pool_.release(conn_); }

        /**
         * @brief Move constructor: transfers ownership of the connection.
         *
         * The moved-from Handle's conn_ is set to nullptr so its
         * destructor becomes a no-op.
         */
        Handle(Handle&& other) noexcept
            : pool_(other.pool_), conn_(other.conn_)
        { other.conn_ = nullptr; }

        /* Non-copyable: only one Handle should own a given connection. */
        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        /**
         * @brief Access the underlying DuckDB connection handle.
         * @return The raw duckdb_connection (valid until Handle is destroyed).
         */
        duckdb_connection get() const { return conn_; }

    private:
        ConnectionPool& pool_;     ///< Back-reference to the owning pool for return.
        duckdb_connection conn_;   ///< The borrowed connection (nullptr if moved-from).
    };

    /**
     * @brief Borrow a connection from the pool (blocking with timeout).
     *
     * Waits up to borrow_timeout_ms_ for an idle connection.  If no
     * connection becomes available within the timeout, throws
     * std::runtime_error with a diagnostic message suggesting the user
     * increase the pool size via --readers.
     *
     * @return An RAII Handle that automatically returns the connection.
     * @throws std::runtime_error on timeout.
     */
    Handle borrow() {
        std::unique_lock<std::mutex> lock(mu_);

        /* Compute the absolute deadline for the wait. */
        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(borrow_timeout_ms_);

        /* Block until a connection is available or deadline is reached. */
        const bool available = cv_.wait_until(lock, deadline,
            [this] { return !idle_.empty(); });

        if (!available)
            throw std::runtime_error(
                "ConnectionPool: timed out waiting for an idle connection "
                "(pool_size=" + std::to_string(pool_size_) + "). "
                "Consider increasing --readers.");

        /* Pop one connection from the idle queue. */
        duckdb_connection conn = idle_.front();
        idle_.pop();
        return Handle(*this, conn);
    }

    /**
     * @brief Return the fixed pool size (number of connections created).
     * @return The pool_size passed at construction.
     */
    size_t size() const { return pool_size_; }

private:
    /**
     * @brief Create all connections and push them onto the idle queue.
     *
     * If any connection fails to create, all previously created connections
     * are disconnected and the exception is re-thrown.  This ensures no
     * leaked connections on partial initialisation failure.
     */
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
            /* Clean up any connections that were successfully created. */
            while (!idle_.empty()) {
                duckdb_disconnect(&idle_.front());
                idle_.pop();
            }
            pool_size_ = 0;
            throw;
        }
    }

    /**
     * @brief Apply per-connection performance pragmas.
     *
     * These settings optimise each connection for use in a pooled,
     * read-heavy workload:
     *
     *   - **threads=1**: Each connection uses a single thread internally.
     *     Parallelism is achieved at the pool level (many connections),
     *     so internal thread spawning would waste resources and cause
     *     contention.
     *
     *   - **preserve_insertion_order=false**: Allows DuckDB to return rows
     *     in the most efficient order during scans.  Only matters for
     *     queries without ORDER BY — those that do have ORDER BY will
     *     sort regardless.
     *
     *   - **enable_object_cache**: Caches catalog metadata (table schemas,
     *     column stats) in memory for faster repeated lookups.
     *
     *   - **checkpoint_threshold='256MB'**: Delays WAL checkpointing until
     *     256 MB of WAL data has accumulated, reducing I/O from frequent
     *     checkpoints during bursty writes.
     *
     * @param conn  A freshly created DuckDB connection.
     */
    static void apply_connection_pragmas(duckdb_connection conn) {
        duckdb_query(conn, "SET threads=1", nullptr);
        duckdb_query(conn, "SET preserve_insertion_order=false", nullptr);
        duckdb_query(conn, "PRAGMA enable_object_cache", nullptr);
        duckdb_query(conn, "SET checkpoint_threshold='256MB'", nullptr);
        duckdb_query(conn, "SET allocator_flush_threshold='128MB'", nullptr);
    }

    /**
     * @brief Return a borrowed connection to the idle queue.
     *
     * Called by Handle's destructor.  Pushes the connection back, then
     * wakes one thread waiting in borrow().
     *
     * @param conn  The connection to return.  Must not be nullptr
     *              (checked by the caller).
     */
    void release(duckdb_connection conn) {
        if (!conn) return;
        {
            std::lock_guard<std::mutex> lock(mu_);
            idle_.push(conn);
        }
        cv_.notify_one();  /* Wake one waiter in borrow(). */
    }

    bool owns_db_;                      ///< If true, destructor closes db_ (not used in current design).
    duckdb_database db_;                ///< The DuckDB database handle (not owned; must outlive pool).
    size_t pool_size_;                  ///< Number of connections in the pool (set to 0 on init failure).
    int borrow_timeout_ms_;             ///< Timeout for borrow() in milliseconds.
    std::queue<duckdb_connection> idle_; ///< FIFO queue of idle (available) connections.
    std::mutex mu_;                     ///< Protects idle_ and is used by cv_.
    std::condition_variable cv_;        ///< Notified when a connection is returned to idle_.
};

} // namespace das
