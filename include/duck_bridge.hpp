#pragma once
/**
 * @file duck_bridge.hpp
 * @brief Bridge between DuckDB's Arrow C Data Interface and Apache Arrow C++.
 *
 * ## Arrow version independence
 *
 * This file uses only APIs stable since Apache Arrow C++ 3.0.0 (2021):
 * | API                               | Stable since |
 * |-----------------------------------|--------------|
 * | `arrow::ImportSchema()`           | 1.0 (C Data Interface) |
 * | `arrow::ImportRecordBatch()`      | 1.0 |
 * | `arrow::ipc::MakeStreamWriter()`  | 1.0 |
 * | `arrow::io::BufferOutputStream`   | 1.0 |
 * | `arrow::ipc::IpcWriteOptions`     | 3.0 |
 * | `ARROW_ASSIGN_OR_RAISE`           | 1.0 |
 * | `ARROW_RETURN_NOT_OK`             | 0.17 |
 *
 * The server (C++) and client (.NET) Arrow versions are **independent**: they
 * share only the Arrow IPC wire format, which has been frozen since Arrow 0.8
 * (2018).  You may run Arrow C++ 6.0 on the server and Apache.Arrow 14.0 on
 * the .NET client without compatibility issues.
 *
 * ## Data path (zero-copy)
 *
 * @code
 *   DuckDB internal columnar buffer
 *     → duckdb_query_arrow_array()     [DuckDB C API — no copy]
 *     → arrow::ImportRecordBatch()     [Arrow C Data Interface — no copy]
 *     → arrow::flight::RecordBatchStream  [gRPC serialises on demand]
 * @endcode
 *
 * No intermediate copy occurs before the IPC serialisation step in gRPC.
 */

#include "interfaces.hpp"
#include "connection_pool.hpp"
#include <arrow/api.h>
#include <duckdb.h>

namespace das {

/**
 * @brief Adapts a live DuckDB Arrow query cursor to `IRecordBatchSource`.
 *
 * Wraps a `ConnectionPool::Handle` (keeping the connection borrowed) and a
 * `duckdb_arrow` cursor.  Each call to `read_next()` fetches the next batch
 * from DuckDB via the Arrow C Data Interface and imports it into Apache Arrow
 * C++ without copying the underlying column buffers.
 *
 * ### Lifecycle
 * 1. Construct via the `Make()` factory — executes the SQL and imports the schema.
 * 2. Pass to `arrow::flight::RecordBatchStream` (which wraps it as a
 *    `RecordBatchReader`).
 * 3. Arrow Flight calls `read_next()` as it serialises batches to the gRPC stream.
 * 4. Destruction releases the Arrow cursor and returns the borrowed connection
 *    to the pool — the connection is held for the **full duration** of the stream.
 *
 * ### Why hold the connection for the full stream?
 * DuckDB cursors are attached to the connection that created them.  Releasing
 * the connection while the cursor is open would corrupt the query state.
 */
class DuckDbRecordBatchSource : public IRecordBatchSource,
                                public arrow::RecordBatchReader {
public:
    // Non-copyable, non-movable (owns C handles)
    DuckDbRecordBatchSource(const DuckDbRecordBatchSource&)            = delete;
    DuckDbRecordBatchSource& operator=(const DuckDbRecordBatchSource&) = delete;

    /**
     * @brief Factory: execute SQL and open the Arrow cursor.
     *
     * Preferred over direct construction — handles the two-step Arrow C Data
     * Interface import (schema first, then batches) and propagates errors cleanly.
     *
     * @param handle  A borrowed connection from `ConnectionPool::borrow()`.
     *                Ownership is transferred to the returned source.
     * @param sql     UTF-8 SELECT / WITH / EXPLAIN / SHOW statement.
     * @return A shared pointer to the source, or an error Status.
     */
    static arrow::Result<std::shared_ptr<DuckDbRecordBatchSource>>
    Make(ConnectionPool::Handle handle, const std::string& sql);

    ~DuckDbRecordBatchSource() override;

    // ── IRecordBatchSource / RecordBatchReader ────────────────────────────────

    /**
     * @brief Arrow schema of the query result.
     * Populated during `Make()` before any batches are fetched.
     */
    std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

    /**
     * @brief Read the next record batch from DuckDB.
     *
     * - Sets `*out` to the next batch on success.
     * - Sets `*out` to `nullptr` at end-of-stream (DuckDB signals EOS by
     *   returning an `ArrowArray` with `length == 0`).
     * - Returns a non-OK Status on DuckDB or Arrow error.
     *
     * Satisfies both `IRecordBatchSource::read_next()` and the
     * `arrow::RecordBatchReader::ReadNext()` contract, so this object can be
     * passed directly to `arrow::flight::RecordBatchStream`.
     */
    arrow::Status read_next(std::shared_ptr<arrow::RecordBatch>* out) override;

    /** @brief Alias for Arrow C++ RecordBatchReader compatibility. */
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override
    { return read_next(out); }

private:
    explicit DuckDbRecordBatchSource(ConnectionPool::Handle handle);

    /// Imported from `duckdb_query_arrow_schema()`. Immutable after construction.
    std::shared_ptr<arrow::Schema> schema_;

    /// Borrowed from the connection pool. Released when this object is destroyed.
    ConnectionPool::Handle handle_;

    /// Opaque DuckDB Arrow result handle (cursor). Freed in destructor.
    duckdb_arrow cursor_;

    /// Set to true after DuckDB returns an empty-length batch (EOS).
    bool done_;
};

} // namespace das
