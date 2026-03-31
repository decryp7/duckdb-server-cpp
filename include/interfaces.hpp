#pragma once
/**
 * @file interfaces.hpp
 * @brief Pure-virtual (abstract) interfaces for every major subsystem.
 *
 * Separating interfaces from implementations serves two purposes:
 *   1. **Testability** — unit tests can inject mock implementations without
 *      starting a real DuckDB database or Arrow Flight server.
 *   2. **Extensibility** — alternative implementations (e.g. a sharded pool,
 *      a read-only replica pool) can satisfy the same interface without
 *      changing call sites.
 *
 * Dependency hierarchy (no cycles):
 *
 *   IRecordBatchSource
 *       ^
 *       |  (used by FlightServer)
 *   IConnectionPool   IWriteSerializer
 *       ^                   ^
 *       |                   |
 *   ConnectionPool    WriteSerializer
 */

#include <arrow/api.h>
#include <future>
#include <memory>
#include <string>

namespace das {

// ─── Forward declarations ─────────────────────────────────────────────────────
struct WriteResult;

// ─────────────────────────────────────────────────────────────────────────────
/// @brief Outcome of a write (DML / DDL) operation.
///
/// On success, `ok` is true and `error` is empty.
/// On failure, `ok` is false and `error` contains a human-readable message.
// ─────────────────────────────────────────────────────────────────────────────
struct WriteResult {
    bool        ok    = true;  ///< True if the statement executed without error.
    std::string error;         ///< Non-empty error message on failure.
};

// ─────────────────────────────────────────────────────────────────────────────
/// @brief A source of Arrow record batches produced from a SQL query.
///
/// Implementations open a query cursor and expose it as a standard
/// `arrow::RecordBatchReader`, which Arrow Flight can stream directly
/// to the client without an intermediate buffer.
///
/// The source owns whatever resources are needed to keep the cursor alive
/// (e.g. a borrowed database connection) and releases them on destruction.
// ─────────────────────────────────────────────────────────────────────────────
class IRecordBatchSource {
public:
    virtual ~IRecordBatchSource() = default;

    /**
     * @brief The Arrow schema of the query result.
     *
     * Available immediately after construction — before any batches are read.
     * @return Shared pointer to the schema; never null.
     */
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    /**
     * @brief Read the next record batch from the query cursor.
     *
     * Follows the `arrow::RecordBatchReader::ReadNext` contract:
     *   - Sets `*out` to the next batch when one is available.
     *   - Sets `*out` to `nullptr` to signal end-of-stream.
     *   - Returns a non-OK Status on error (DuckDB failure, network issue, etc.).
     *
     * @param[out] out Pointer to fill; set to nullptr at end-of-stream.
     * @return `arrow::Status::OK()` on success or end-of-stream.
     */
    virtual arrow::Status read_next(
        std::shared_ptr<arrow::RecordBatch>* out) = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
/// @brief A thread-safe pool of database connections.
///
/// Callers borrow a connection via `borrow()`, use it, and return it by
/// destroying the returned Handle (RAII).  The pool blocks (up to a timeout)
/// if all connections are in use.
///
/// Concrete implementations decide whether to create new connections, validate
/// existing ones, or maintain a fixed-size pool.
// ─────────────────────────────────────────────────────────────────────────────
class IConnectionPool {
public:
    virtual ~IConnectionPool() = default;

    /**
     * @brief Acquire an exclusive connection from the pool.
     *
     * Blocks until a connection is available or the implementation-defined
     * timeout expires.  The returned opaque handle releases the connection
     * back to the pool when destroyed.
     *
     * @return An opaque token whose type is defined by the concrete pool.
     *         The token must be kept alive for as long as the connection is needed.
     * @throws std::runtime_error on timeout or pool failure.
     */
    virtual void* borrow_raw() = 0;

    /** @brief Number of connections in the pool (idle + in-use). */
    virtual size_t size() const = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
/// @brief Serialises concurrent write operations into batched transactions.
///
/// DuckDB supports exactly one writer at a time.  Rather than reject concurrent
/// writes, an implementation of this interface should queue them and execute each
/// batch in a single `BEGIN … COMMIT` transaction, maximising throughput while
/// preserving correctness.
///
/// DDL statements (CREATE, DROP, ALTER, …) must be executed outside any explicit
/// transaction and therefore cannot be batched with DML.  Implementations are
/// responsible for detecting and routing DDL appropriately.
// ─────────────────────────────────────────────────────────────────────────────
class IWriteSerializer {
public:
    virtual ~IWriteSerializer() = default;

    /**
     * @brief Submit a write statement and block until it is committed.
     *
     * The call returns only after the statement (and the transaction batch it
     * belongs to) has been committed to the database.
     *
     * @param sql  UTF-8 DML or DDL SQL statement.
     * @return WriteResult indicating success or the error message.
     */
    virtual WriteResult submit(const std::string& sql) = 0;

    /**
     * @brief Submit a write statement and return a future for the result.
     *
     * The future resolves once the statement's transaction batch commits.
     * Useful when the caller wants to overlap query preparation with write
     * execution.
     *
     * @param sql  UTF-8 DML or DDL SQL statement.
     * @return `std::future<WriteResult>` that resolves on commit.
     */
    virtual std::future<WriteResult> submit_async(const std::string& sql) = 0;
};

} // namespace das
