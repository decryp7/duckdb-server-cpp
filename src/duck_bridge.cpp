/**
 * @file duck_bridge.cpp
 * @brief Implementation of DuckDbRecordBatchSource.
 *
 * DuckDB exposes query results via the Arrow C Data Interface (ArrowSchema +
 * ArrowArray structs).  Apache Arrow C++ imports these into its own object model
 * via `arrow::ImportSchema()` and `arrow::ImportRecordBatch()`, which transfer
 * ownership of the C structs — Arrow will call `release()` on them when done.
 *
 * This means the data buffers themselves are **never copied**: DuckDB allocates
 * them, Arrow C++ wraps them in its shared_ptr ref-counted model, and gRPC
 * serialises directly from those buffers when writing to the network.
 */

#include "duck_bridge.hpp"
#include <arrow/c/bridge.h>     // ImportSchema, ImportRecordBatch
#include <stdexcept>

// ── DuckDB Arrow C API ────────────────────────────────────────────────────────
// Declared explicitly here rather than including all of <duckdb.h> in the
// header, keeping the public interface clean.
extern "C" {
duckdb_state duckdb_query_arrow(duckdb_connection, const char*, duckdb_arrow*);
duckdb_state duckdb_query_arrow_schema(duckdb_arrow, duckdb_arrow_schema*);
duckdb_state duckdb_query_arrow_array(duckdb_arrow, duckdb_arrow_array*);
const char*  duckdb_query_arrow_error(duckdb_arrow);
void         duckdb_destroy_arrow(duckdb_arrow*);
void         duckdb_destroy_arrow_schema(duckdb_arrow_schema*);
void         duckdb_destroy_arrow_array(duckdb_arrow_array*);
} // extern "C"

namespace das {

// ── Private constructor ───────────────────────────────────────────────────────

DuckDbRecordBatchSource::DuckDbRecordBatchSource(ConnectionPool::Handle handle)
    : handle_(std::move(handle))
    , cursor_(nullptr)
    , done_(false)
{}

// ── Destructor ────────────────────────────────────────────────────────────────

DuckDbRecordBatchSource::~DuckDbRecordBatchSource() {
    // Free the DuckDB Arrow cursor before the Handle destructor
    // returns the connection to the pool.
    if (cursor_) duckdb_destroy_arrow(&cursor_);
}

// ── Factory ───────────────────────────────────────────────────────────────────

arrow::Result<std::shared_ptr<DuckDbRecordBatchSource>>
DuckDbRecordBatchSource::Make(ConnectionPool::Handle handle,
                              const std::string& sql)
{
    // Allocate via raw new so the private constructor is accessible;
    // immediately wrap in shared_ptr for exception safety.
    auto self = std::shared_ptr<DuckDbRecordBatchSource>(
        new DuckDbRecordBatchSource(std::move(handle)));

    // Step 1: Execute the query.  DuckDB returns an opaque cursor handle.
    if (duckdb_query_arrow(self->handle_.get(), sql.c_str(),
                           &self->cursor_) == DuckDBError)
    {
        const char* msg = duckdb_query_arrow_error(self->cursor_);
        return arrow::Status::ExecutionError(
            msg ? msg : "DuckDB: unknown query error");
    }

    // Step 2: Import the Arrow schema via the C Data Interface.
    // `duckdb_query_arrow_schema` fills a raw ArrowSchema struct which
    // `arrow::ImportSchema` takes ownership of (calls schema->release()).
    duckdb_arrow_schema raw_schema = nullptr;
    if (duckdb_query_arrow_schema(self->cursor_, &raw_schema) == DuckDBError) {
        // DuckDB may have partially filled raw_schema even on failure.
        // Destroy it to avoid a potential leak.
        if (raw_schema) duckdb_destroy_arrow_schema(&raw_schema);
        return arrow::Status::IOError("DuckDB: failed to retrieve Arrow schema");
    }

    // ImportSchema takes ownership of the ArrowSchema (calls schema->release).
    // Do NOT call duckdb_destroy_arrow_schema after this point.
    ARROW_ASSIGN_OR_RAISE(
        self->schema_,
        arrow::ImportSchema(reinterpret_cast<ArrowSchema*>(raw_schema)));

    return self;
}

// ── read_next ─────────────────────────────────────────────────────────────────

arrow::Status
DuckDbRecordBatchSource::read_next(
    std::shared_ptr<arrow::RecordBatch>* out)
{
    *out = nullptr;

    // Already signalled EOS in a previous call — return immediately.
    if (done_) return arrow::Status::OK();

    // Fetch the next Arrow batch from DuckDB.
    duckdb_arrow_array raw_array = nullptr;
    if (duckdb_query_arrow_array(cursor_, &raw_array) == DuckDBError)
        return arrow::Status::IOError(
            "DuckDB: duckdb_query_arrow_array failed: ",
            duckdb_query_arrow_error(cursor_));

    auto* arr = reinterpret_cast<ArrowArray*>(raw_array);

    // DuckDB signals end-of-stream by returning a zero-length array.
    // Release via duckdb_destroy_arrow_array only — do NOT also call arr->release()
    // manually, because duckdb_destroy_arrow_array will call it internally, and
    // calling release() twice is undefined behaviour (double-free of internal buffers).
    const bool is_eos = (!arr || arr->length == 0);
    if (is_eos) {
        if (raw_array) duckdb_destroy_arrow_array(&raw_array);
        done_ = true;
        return arrow::Status::OK(); // *out stays nullptr → caller sees EOS
    }

    // Import the batch into Arrow C++.  `ImportRecordBatch` takes ownership
    // of `arr` and will call `arr->release()` when the RecordBatch is freed.
    ARROW_ASSIGN_OR_RAISE(*out, arrow::ImportRecordBatch(arr, schema_));
    return arrow::Status::OK();
}

} // namespace das
