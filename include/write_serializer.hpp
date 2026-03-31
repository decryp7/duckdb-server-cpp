#pragma once
/**
 * @file write_serializer.hpp
 * @brief Batches concurrent write requests into single DuckDB transactions.
 *
 * ## The DuckDB single-writer constraint
 *
 * DuckDB's MVCC design permits many concurrent readers but only **one writer
 * at a time**.  Simply executing each write on a dedicated thread would cause
 * all concurrent writers to queue serially, with the overhead of one
 * `BEGIN … COMMIT` round-trip per statement.
 *
 * ## Transaction batching
 *
 * WriteSerializer solves this by accumulating incoming write requests for a
 * configurable window (`batch_window_ms`) or until a maximum count
 * (`batch_max`) is reached, then executing the entire accumulated batch inside
 * a **single transaction**:
 *
 * @code
 *   Thread A: INSERT ... ─┐
 *   Thread B: INSERT ... ──┤ ── collected over 5 ms ──► BEGIN; A; B; C; D; COMMIT
 *   Thread C: UPDATE ... ──┤
 *   Thread D: DELETE ... ─┘
 * @endcode
 *
 * Each caller blocks on a `std::future<WriteResult>` and wakes once its
 * statement's batch has committed.
 *
 * ## DDL isolation
 *
 * DDL statements (CREATE, DROP, ALTER, TRUNCATE, ATTACH, …) cannot be mixed
 * with DML inside an explicit transaction in DuckDB.  WriteSerializer detects
 * DDL by its opening keyword and executes it individually outside any batch.
 *
 * ## Error recovery
 *
 * If any DML statement in a batch fails, the whole transaction rolls back and
 * each statement is re-executed individually.  This ensures every caller
 * receives an accurate success/failure result even in partial-failure scenarios.
 *
 * ## Implements IWriteSerializer
 *
 * Callers should hold an `IWriteSerializer*` pointer (or `unique_ptr`) so they
 * can be tested with a mock that does not touch a real database.
 */

#include "interfaces.hpp"
#include "insert_batcher.hpp"
#include <duckdb.h>
#include <atomic>
#include <cctype>          // std::isspace, std::isalpha, std::toupper
#include <chrono>
#include <condition_variable>
#include <cstring>         // std::strcmp
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace das {

/**
 * @brief Concrete implementation of IWriteSerializer.
 *
 * Owns a single `duckdb_connection` dedicated to writes.  A background thread
 * drains the request queue in batches.  All public methods are thread-safe.
 */
class WriteSerializer : public IWriteSerializer {
public:
    /**
     * @brief Construct and start the writer background thread.
     *
     * @param conn            A dedicated DuckDB write connection.
     *                        The caller retains ownership; the connection must
     *                        outlive this object.
     * @param batch_window_ms Maximum milliseconds to wait before flushing a
     *                        partial batch.  Smaller → lower latency, more
     *                        transactions.  Larger → higher throughput.
     * @param batch_max       Maximum number of DML statements per transaction.
     *                        Once reached the batch flushes immediately regardless
     *                        of the time window.
     */
    explicit WriteSerializer(duckdb_connection conn,
                             int    batch_window_ms = 5,
                             size_t batch_max       = 512)
        : conn_(conn)
        , batch_window_ms_(batch_window_ms)
        , batch_max_(batch_max)
        , stop_(false)                         // Must be before writer_thread_ in the
                                               // initialiser list so the atomic is set to
                                               // false BEFORE the writer thread starts.
                                               // (Members are initialised in declaration
                                               // order; stop_ is declared before writer_thread_.)
        , writer_thread_([this] { drain_loop(); })
    {}

    /**
     * @brief Stop the writer thread and wait for it to finish.
     *
     * Any enqueued requests that have not yet started executing will be
     * abandoned with their promises broken (callers will get `std::future_error`
     * if they block after this point).
     */
    ~WriteSerializer() override {
        { std::lock_guard<std::mutex> lock(mu_); stop_.store(true); }
        cv_.notify_all();
        if (writer_thread_.joinable()) writer_thread_.join();
    }

    // Non-copyable, non-movable (owns mutex + cv + thread)
    WriteSerializer(const WriteSerializer&)            = delete;
    WriteSerializer& operator=(const WriteSerializer&) = delete;

    // ── IWriteSerializer ──────────────────────────────────────────────────────

    /**
     * @brief Enqueue a write and block until its transaction commits.
     *
     * Suitable for synchronous request handlers.  Returns as soon as the batch
     * containing this statement has been committed (or rolled back).
     *
     * @param sql  UTF-8 DML or DDL statement.
     * @return WriteResult indicating success or the DuckDB error message.
     */
    WriteResult submit(const std::string& sql) override {
        return submit_async(sql).get();
    }

    /**
     * @brief Enqueue a write and return a future for the result.
     *
     * Use when you want to overlap request handling with write execution.
     *
     * @param sql  UTF-8 DML or DDL statement.
     * @return Future that resolves once the batch commits.
     */
    std::future<WriteResult> submit_async(const std::string& sql) override {
        auto req = std::make_shared<Request>(sql);
        std::future<WriteResult> future = req->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mu_);
            queue_.push(std::move(req));
        }
        cv_.notify_one();
        return future;
    }

private:
    // ── Internal types ────────────────────────────────────────────────────────

    /** @brief A single pending write request waiting in the queue. */
    struct Request {
        std::string               sql;
        std::promise<WriteResult> promise;
        explicit Request(const std::string& s) : sql(s) {}
    };
    using RequestPtr = std::shared_ptr<Request>;

    // ── Writer thread ─────────────────────────────────────────────────────────

    /**
     * @brief Background loop: collects batches and executes them.
     *
     * Runs on `writer_thread_` until `stop_` is set.
     * Each iteration:
     *   1. Waits for at least one request (or stop signal).
     *   2. Drains up to `batch_max_` requests within `batch_window_ms_`.
     *   3. Splits the batch into DDL (individual) and DML (transactional) runs.
     *   4. Executes each run and resolves all promises.
     */
    void drain_loop() {
        while (true) {
            std::vector<RequestPtr> batch;
            collect_batch(batch);
            if (batch.empty()) break; // stop_ was set and queue is drained
            execute_batch(batch);
        }
    }

    /**
     * @brief Collect up to `batch_max_` requests within the batch window.
     *
     * Blocks until at least one request arrives or `stop_` is set.
     * Then waits up to `batch_window_ms_` for additional requests to arrive
     * before returning.
     *
     * @param[out] batch  Populated with the requests to execute.
     */
    void collect_batch(std::vector<RequestPtr>& batch) {
        std::unique_lock<std::mutex> lock(mu_);

        // Block until work arrives or we are told to stop.
        cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
        if (stop_ && queue_.empty()) return;

        // Drain immediately available requests.
        drain_queue_locked(batch);

        // Wait briefly for more requests to arrive (amortises transaction cost).
        if (batch.size() < batch_max_) {
            const auto deadline = std::chrono::steady_clock::now()
                                + std::chrono::milliseconds(batch_window_ms_);
            cv_.wait_until(lock, deadline,
                [this] { return !queue_.empty() || stop_; });
            drain_queue_locked(batch);
        }
    }

    /**
     * @brief Move up to `batch_max_` requests from `queue_` into `batch`.
     * Must be called with `mu_` held.
     */
    void drain_queue_locked(std::vector<RequestPtr>& batch) {
        while (!queue_.empty() && batch.size() < batch_max_) {
            batch.push_back(std::move(queue_.front()));
            queue_.pop();
        }
    }

    // ── Batch execution ───────────────────────────────────────────────────────

    /**
     * @brief Execute a collected batch, routing DDL and DML appropriately.
     *
     * Walks the batch and identifies contiguous "runs" of the same type
     * (DDL or DML).  Each DDL statement is executed on its own; each DML
     * run is wrapped in a single `BEGIN … COMMIT`.
     */
    void execute_batch(std::vector<RequestPtr>& batch) {
        size_t i = 0;
        while (i < batch.size()) {
            if (is_ddl(batch[i]->sql)) {
                execute_single(*batch[i]);
                ++i;
            } else {
                // Find the end of this contiguous DML run.
                size_t j = i + 1;
                while (j < batch.size() && !is_ddl(batch[j]->sql)) ++j;
                execute_dml_run(batch, i, j);
                i = j;
            }
        }
    }

    /**
     * @brief Execute a contiguous slice of DML requests in one transaction.
     *
     * On failure, rolls back and falls back to individual execution so each
     * caller gets an accurate result.
     *
     * @param batch  Full batch vector.
     * @param from   Index of the first DML request in this run (inclusive).
     * @param to     Index past the last DML request (exclusive).
     */
    void execute_dml_run(std::vector<RequestPtr>& batch,
                         size_t from, size_t to)
    {
        // Step 1: Merge compatible INSERTs into multi-row statements.
        // This reduces DuckDB parse/plan overhead from N to 1 per group.
        std::vector<std::string> sql_list;
        sql_list.reserve(to - from);
        for (size_t k = from; k < to; ++k)
            sql_list.push_back(batch[k]->sql);

        std::vector<BatchGroup> groups = merge_inserts(sql_list);

        // Step 2: Begin transaction.
        if (!exec_sql("BEGIN")) {
            for (size_t k = from; k < to; ++k) execute_single(*batch[k]);
            return;
        }

        // Step 3: Execute each merged group. Collect results BEFORE
        // setting any promise (must wait for COMMIT to succeed).
        std::vector<WriteResult> group_results;
        group_results.reserve(groups.size());

        bool any_failed = false;
        for (size_t g = 0; g < groups.size(); ++g) {
            WriteResult wr = exec_one(groups[g].sql);
            if (!wr.ok) {
                any_failed = true;
                break;
            }
            group_results.push_back(std::move(wr));
        }

        if (any_failed) {
            exec_sql("ROLLBACK");
            for (size_t k = from; k < to; ++k)
                execute_single(*batch[k]);
            return;
        }

        // Step 4: Commit.
        if (!exec_sql("COMMIT")) {
            for (size_t k = from; k < to; ++k)
                execute_single(*batch[k]);
            return;
        }

        // Step 5: COMMIT succeeded — resolve promises.
        // Map group results back to individual requests.
        for (size_t g = 0; g < groups.size(); ++g) {
            for (size_t idx : groups[g].request_indices)
                batch[from + idx]->promise.set_value(group_results[g]);
        }
    }

    /**
     * @brief Execute a single statement with an implicit (auto-commit) transaction.
     * Resolves the request's promise with the outcome.
     */
    void execute_single(Request& req) {
        req.promise.set_value(exec_one(req.sql));
    }

    /**
     * @brief Execute one SQL statement and return a WriteResult.
     * Does not touch any transaction; caller is responsible for wrapping.
     */
    WriteResult exec_one(const std::string& sql) {
        WriteResult result;
        duckdb_result raw{};
        if (duckdb_query(conn_, sql.c_str(), &raw) == DuckDBError) {
            result.ok    = false;
            result.error = duckdb_result_error(&raw);
        }
        duckdb_destroy_result(&raw);
        return result;
    }

    /**
     * @brief Execute a control statement (BEGIN / COMMIT / ROLLBACK).
     * @return True on success, false on failure (failure is generally ignorable).
     */
    bool exec_sql(const char* control_sql) {
        duckdb_result raw{};
        const bool ok = duckdb_query(conn_, control_sql, &raw) != DuckDBError;
        duckdb_destroy_result(&raw);
        return ok;
    }

    // ── DDL detection ─────────────────────────────────────────────────────────

    /**
     * @brief Return true if `sql` is a DDL statement.
     *
     * DDL statements must be executed outside explicit transactions in DuckDB.
     * Detection is based on the first keyword (case-insensitive), after
     * stripping leading whitespace and single-line comments.
     *
     * Recognised DDL prefixes: CREATE, DROP, ALTER, TRUNCATE, ATTACH, DETACH,
     * VACUUM, PRAGMA, COPY, EXPORT, IMPORT, LOAD.
     *
     * @note False negatives (DML mis-identified as DDL) are safe — the statement
     *       will execute in its own auto-commit transaction rather than in a batch.
     *       False positives (DDL identified as DML) would cause a DuckDB error.
     */
    static bool is_ddl(const std::string& sql) {
        // Skip leading whitespace, single-line comments (--), and block comments (/* */).
        // Loop handles multiple consecutive comments.
        size_t i = 0;
        while (i < sql.size()) {
            if (std::isspace(static_cast<unsigned char>(sql[i]))) {
                ++i;
            } else if (i + 1 < sql.size() && sql[i] == '-' && sql[i+1] == '-') {
                // Single-line comment: skip to end of line.
                while (i < sql.size() && sql[i] != '\n') ++i;
                if (i < sql.size()) ++i;
            } else if (i + 1 < sql.size() && sql[i] == '/' && sql[i+1] == '*') {
                // Block comment: skip to closing */.
                i += 2;
                bool closed = false;
                while (i + 1 < sql.size()) {
                    if (sql[i] == '*' && sql[i+1] == '/') { i += 2; closed = true; break; }
                    ++i;
                }
                if (!closed) i = sql.size(); // unterminated — skip to end
            } else {
                break; // Not whitespace or comment — stop.
            }
        }

        // Extract the first keyword (up to 8 characters, upper-cased).
        char kw[9] = {};
        size_t klen = 0;
        while (klen < 8 && i < sql.size()
               && std::isalpha(static_cast<unsigned char>(sql[i])))
        {
            kw[klen++] = static_cast<char>(
                std::toupper(static_cast<unsigned char>(sql[i++])));
        }

        // Check against the known DDL keyword set.
        static const char* const DDL_KEYWORDS[] = {
            "CREATE", "DROP", "ALTER", "TRUNCATE",  // schema modification
            "ATTACH", "DETACH", "VACUUM", "PRAGMA",  // database administration
            "COPY", "EXPORT", "IMPORT", "LOAD",      // data loading / export
            nullptr
        };
        for (const char* const* kp = DDL_KEYWORDS; *kp; ++kp)
            if (std::strcmp(kw, *kp) == 0) return true;

        return false;
    }

    // ── State ─────────────────────────────────────────────────────────────────

    duckdb_connection           conn_;             ///< Dedicated write connection (not owned).
    int                         batch_window_ms_;  ///< Max ms to wait before flushing.
    size_t                      batch_max_;        ///< Max statements per batch.
    std::atomic<bool>           stop_;             ///< Set to true in destructor (atomic: read by writer thread).
    std::queue<RequestPtr>      queue_;            ///< Pending write requests.
    std::mutex                  mu_;               ///< Guards queue_ (stop_ is self-guarding via atomic).
    std::condition_variable     cv_;               ///< Wakes drain_loop().
    std::thread                 writer_thread_;    ///< Dedicated writer thread.
};

} // namespace das
