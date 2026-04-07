using System;
using System.Collections.Generic;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckDbServer
{
    /// <summary>
    /// Serializes concurrent write requests onto a single dedicated DuckDB connection
    /// and batches them into transactions for throughput.
    ///
    /// <para><b>Thread Safety:</b> The <see cref="Submit"/> method is thread-safe and may
    /// be called from any number of concurrent threads. Internally, all SQL execution
    /// happens on a single background thread (<c>DAS-Writer</c>) that owns the writer
    /// connection. This satisfies DuckDB's single-writer constraint without requiring
    /// callers to coordinate.</para>
    ///
    /// <para><b>How it works:</b></para>
    /// <list type="number">
    ///   <item><description>
    ///     Callers submit SQL via <see cref="Submit"/>. Each call enqueues a
    ///     <see cref="WriteRequest"/> and blocks on a <see cref="ManualResetEventSlim"/>
    ///     until the background thread signals completion.
    ///   </description></item>
    ///   <item><description>
    ///     The background thread (<see cref="DrainLoop"/>) collects requests over a short
    ///     time window (<see cref="batchWindowMs"/>) to form a batch.
    ///   </description></item>
    ///   <item><description>
    ///     Before executing, consecutive INSERTs to the same table are merged into
    ///     multi-row INSERTs via <see cref="InsertBatcher"/> (much faster -- DuckDB
    ///     parses and plans once instead of N times).
    ///   </description></item>
    ///   <item><description>
    ///     Merged DML statements are wrapped in a single <c>BEGIN...COMMIT</c> transaction.
    ///     DDL statements (CREATE, DROP, etc.) run individually outside transactions because
    ///     DuckDB does not support DDL inside explicit transactions.
    ///   </description></item>
    ///   <item><description>
    ///     If any DML statement in the batch fails, the entire transaction is rolled back
    ///     and each statement is retried individually so that only the failing statement
    ///     reports an error.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Gotchas:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     The <see cref="ManualResetEventSlim.Dispose"/> call in <see cref="Submit"/> is
    ///     conditional: it only disposes the signal if the wait was signalled. If the wait
    ///     timed out, the background thread may still call <c>Set()</c> on the signal later.
    ///     Disposing prematurely would crash the background thread with
    ///     <see cref="ObjectDisposedException"/>, permanently killing all future writes.
    ///   </description></item>
    ///   <item><description>
    ///     If the writer thread crashes, the <see cref="writerThreadDead"/> flag is set and
    ///     all subsequent <see cref="Submit"/> calls return failure immediately.
    ///   </description></item>
    /// </list>
    /// </summary>
    public sealed class WriteSerializer : IWriteSerializer
    {
        /// <summary>
        /// Dedicated DuckDB connection for writes. Only accessed by the background
        /// writer thread (<see cref="DrainLoop"/>), so no synchronization is needed
        /// for this field after construction.
        /// </summary>
        private readonly DuckDBConnection writerConnection;

        /// <summary>
        /// Duration in milliseconds to wait for additional requests after the first
        /// request arrives, before executing the batch. Longer windows accumulate more
        /// requests per transaction (higher throughput) but increase latency.
        /// Typical values: 1-10 ms. Default: 5 ms.
        /// </summary>
        private readonly int batchWindowMs;

        /// <summary>
        /// Maximum number of DML statements per batch/transaction. Prevents unbounded
        /// memory growth and limits transaction size. Default: 512.
        /// </summary>
        private readonly int batchMax;

        /// <summary>
        /// FIFO queue of pending write requests. Protected by <see cref="lockObj"/>.
        /// Producers (Submit) enqueue; the consumer (DrainLoop) dequeues.
        /// </summary>
        private readonly Queue<WriteRequest> queue = new Queue<WriteRequest>();

        /// <summary>
        /// Lock object for the queue and Monitor wait/pulse signalling between
        /// <see cref="Submit"/> (producer) and <see cref="DrainLoop"/> (consumer).
        /// </summary>
        private readonly object lockObj = new object();

        /// <summary>The dedicated background writer thread running <see cref="DrainLoop"/>.</summary>
        private readonly Thread writerThread;

        /// <summary>
        /// Shutdown flag. Set to true during <see cref="Dispose"/> to signal the
        /// writer thread to exit after draining remaining requests.
        /// </summary>
        private volatile bool stop;

        /// <summary>
        /// Set to true if the writer thread crashes with an unhandled exception.
        /// Checked by <see cref="Submit"/> to return immediate failure instead of
        /// enqueueing requests that will never be processed.
        /// </summary>
        private volatile bool writerThreadDead;

        /// <summary>
        /// Creates a new WriteSerializer with a dedicated writer connection and
        /// background thread.
        /// </summary>
        /// <param name="dbManager">
        /// Database manager for creating the writer connection. The connection is opened
        /// immediately and kept alive for the lifetime of this instance.
        /// </param>
        /// <param name="batchWindowMs">
        /// Batch accumulation window in milliseconds. After the first request arrives,
        /// the writer waits this long for additional requests before executing the batch.
        /// Set to 0 for no batching (each request runs individually). Default: 5 ms.
        /// </param>
        /// <param name="batchMax">
        /// Maximum requests per batch. Limits transaction size and memory usage.
        /// Default: 512.
        /// </param>
        public WriteSerializer(DatabaseManager dbManager, int batchWindowMs = 5, int batchMax = 512)
        {
            this.batchWindowMs = batchWindowMs;
            this.batchMax = batchMax;

            // Create a dedicated connection for writes. This connection is used
            // exclusively by the writer thread and is never returned to any pool.
            writerConnection = dbManager.CreateConnection();

            // Apply per-connection PRAGMAs to the writer connection.
            // Without this, the writer connection uses DuckDB defaults (threads=nCPU)
            // which causes thread over-subscription when running multiple shards.
            ApplyWriterPragmas(writerConnection);

            writerThread = new Thread(DrainLoop)
            {
                Name = "DAS-Writer",
                IsBackground = true // Allows process exit without explicit shutdown.
            };
            writerThread.Start();
        }

        /// <summary>
        /// Submits a SQL write statement and blocks the calling thread until the
        /// background writer thread has executed it (within a batched transaction).
        ///
        /// <para><b>Flow:</b></para>
        /// <list type="number">
        ///   <item><description>Check for shutdown/crash conditions and return failure immediately if detected.</description></item>
        ///   <item><description>Create a <see cref="WriteRequest"/> with a fresh <see cref="ManualResetEventSlim"/>.</description></item>
        ///   <item><description>Enqueue the request under lock and pulse the writer thread.</description></item>
        ///   <item><description>Block on the <see cref="ManualResetEventSlim"/> for up to 30 seconds.</description></item>
        ///   <item><description>Return the result set by the writer thread.</description></item>
        /// </list>
        ///
        /// <para><b>DoneSignal Disposal Gotcha:</b> The <see cref="ManualResetEventSlim"/>
        /// is only disposed if the wait was signalled (completed normally). If the wait
        /// timed out, the writer thread may still hold a reference to the signal and call
        /// <c>Set()</c> on it later. Disposing the signal prematurely would cause the
        /// writer thread to throw <see cref="ObjectDisposedException"/>, killing it
        /// permanently and breaking all future writes. The un-disposed signal will be
        /// collected by the GC when both the caller and writer thread release their
        /// references.</para>
        /// </summary>
        /// <param name="sql">The DML or DDL statement to execute.</param>
        /// <returns>
        /// A <see cref="WriteResult"/> indicating success or failure. On timeout, returns
        /// failure with an appropriate message.
        /// </returns>
        public WriteResult Submit(string sql)
        {
            // Fast-fail if the serializer is shutting down or the writer thread has crashed.
            if (stop)
                return WriteResult.Failure("WriteSerializer has been disposed");

            if (writerThreadDead)
                return WriteResult.Failure("Writer thread has terminated unexpectedly");

            var request = new WriteRequest(sql);

            // Enqueue under lock and signal the writer thread.
            // Monitor.Pulse wakes the writer thread if it is blocked in Monitor.Wait.
            lock (lockObj)
            {
                queue.Enqueue(request);
                Monitor.Pulse(lockObj);
            }

            // Wait with a timeout so we don't hang forever if the writer thread dies.
            // 30 seconds is generous; most writes complete in < 100 ms.
            bool signalled = request.DoneSignal.Wait(TimeSpan.FromSeconds(30));

            // CRITICAL: Only dispose DoneSignal if the wait was signalled.
            // If the timeout fired, the writer thread may still call Set() later.
            // Disposing here would crash the writer thread with ObjectDisposedException,
            // permanently killing all future writes. The leaked ManualResetEventSlim
            // will be GC'd when both references are released.
            if (signalled)
                request.DoneSignal.Dispose();

            if (!signalled)
            {
                // Distinguish between "writer thread died" and "write is just slow".
                if (writerThreadDead)
                    return WriteResult.Failure("Writer thread terminated while processing request");
                return WriteResult.Failure("Write timed out after 30 seconds");
            }
            return request.Result;
        }

        /// <summary>
        /// Shuts down the writer thread and disposes the writer connection.
        ///
        /// <para><b>Shutdown sequence:</b></para>
        /// <list type="number">
        ///   <item><description>Set the <see cref="stop"/> flag to signal the writer thread to exit.</description></item>
        ///   <item><description>Pulse all waiting threads so the writer thread wakes from <c>Monitor.Wait</c>.</description></item>
        ///   <item><description>Wait up to 30 seconds for the writer thread to finish processing its current batch and exit.</description></item>
        ///   <item><description>Dispose the writer connection (safe because the thread has stopped).</description></item>
        ///   <item><description>Signal any remaining callers stuck in <see cref="Submit"/> with failure results.</description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> The writer connection must NOT be disposed before the
        /// writer thread has stopped, or the thread will crash with an
        /// <see cref="ObjectDisposedException"/> mid-transaction.</para>
        /// </summary>
        public void Dispose()
        {
            stop = true;
            lock (lockObj) { Monitor.PulseAll(lockObj); }

            // Wait for the writer thread to finish, but don't dispose the connection
            // until the thread has actually stopped.
            if (writerThread.IsAlive)
                writerThread.Join(TimeSpan.FromSeconds(30));

            writerConnection.Dispose();

            // Signal any remaining callers stuck in Submit() so they don't hang forever.
            lock (lockObj)
            {
                while (queue.Count > 0)
                {
                    var orphan = queue.Dequeue();
                    orphan.Result = WriteResult.Failure("WriteSerializer disposed");
                    orphan.DoneSignal.Set();
                }
            }
        }

        // ── Background writer thread ─────────────────────────────────────────

        /// <summary>
        /// Main loop of the background writer thread. Runs continuously, collecting
        /// batches of write requests and executing them until shutdown is requested
        /// (via <see cref="stop"/> flag) and the queue is empty.
        ///
        /// <para><b>Error handling:</b> If an unhandled exception escapes (which should
        /// not happen in normal operation because <see cref="TryExecute"/> catches
        /// exceptions), the thread sets <see cref="writerThreadDead"/> and signals all
        /// pending callers with failure results so they don't hang forever.</para>
        /// </summary>
        private void DrainLoop()
        {
            try
            {
                while (true)
                {
                    // CollectBatch blocks until at least one request is available
                    // or shutdown is requested with an empty queue.
                    var batch = CollectBatch();
                    if (batch.Count == 0) break; // Shutdown: queue is empty and stop is set.
                    ExecuteBatch(batch);
                }
            }
            catch (Exception ex)
            {
                // Mark the thread as dead so Submit() can report it immediately
                // instead of enqueueing requests that will never be processed.
                writerThreadDead = true;
                Console.Error.WriteLine("[das] Writer thread crashed: " + ex.Message);

                // Signal all pending requests so they don't hang forever in Submit().
                lock (lockObj)
                {
                    while (queue.Count > 0)
                    {
                        var orphan = queue.Dequeue();
                        orphan.Result = WriteResult.Failure("Writer thread crashed: " + ex.Message);
                        orphan.DoneSignal.Set();
                    }
                }
            }
        }

        /// <summary>
        /// Collects a batch of write requests using a two-phase collection strategy.
        ///
        /// <para><b>Phase 1 (immediate drain):</b> Under lock, wait for at least one
        /// request to arrive (or shutdown). Once one or more requests are available,
        /// dequeue them all into the batch.</para>
        ///
        /// <para><b>Phase 2 (batching window):</b> If the batch is not yet full,
        /// wait an additional <see cref="batchWindowMs"/> milliseconds for more requests
        /// to arrive. This is the key batching mechanism: during the window, concurrent
        /// callers' requests accumulate in the queue and are merged into a single
        /// transaction, amortizing DuckDB's per-transaction overhead.</para>
        ///
        /// <para><b>Return value:</b> Returns an empty list only when <see cref="stop"/>
        /// is set and the queue is empty, signalling the caller to exit the drain loop.</para>
        /// </summary>
        /// <returns>
        /// A list of write requests to execute, or an empty list on shutdown.
        /// </returns>
        private List<WriteRequest> CollectBatch()
        {
            var batch = new List<WriteRequest>();

            lock (lockObj)
            {
                // Phase 1: wait for at least one request or shutdown signal.
                while (queue.Count == 0 && !stop)
                    Monitor.Wait(lockObj);

                // If shutdown was requested and queue is empty, return empty batch
                // to signal the drain loop to exit.
                if (stop && queue.Count == 0)
                    return batch;

                // Drain all currently queued requests.
                MoveQueueToBatch(batch);

                // Phase 2: if the batch is not full, wait a short window for more
                // requests to arrive. This trades a small latency increase for
                // significantly higher throughput under concurrent write load.
                if (batch.Count < batchMax)
                {
                    Monitor.Wait(lockObj, batchWindowMs);
                    // Drain any requests that arrived during the window.
                    MoveQueueToBatch(batch);
                }
            }

            return batch;
        }

        /// <summary>
        /// Transfers requests from the shared queue into the batch list, up to
        /// <see cref="batchMax"/> total. Must be called under <see cref="lockObj"/>.
        /// </summary>
        /// <param name="batch">The batch list to append requests to.</param>
        private void MoveQueueToBatch(List<WriteRequest> batch)
        {
            while (queue.Count > 0 && batch.Count < batchMax)
                batch.Add(queue.Dequeue());
        }

        // ── Batch execution ──────────────────────────────────────────────────

        /// <summary>
        /// Executes a batch of write requests, splitting them into DDL and DML runs.
        ///
        /// <para><b>DDL/DML Split:</b> DuckDB does not support DDL (CREATE, DROP, ALTER,
        /// etc.) inside explicit transactions. This method scans the batch linearly,
        /// running DDL statements individually and grouping consecutive DML statements
        /// into a single transaction via <see cref="RunDmlTransaction"/>.</para>
        ///
        /// <para><b>Example batch:</b></para>
        /// <code>
        /// INSERT INTO t VALUES (1)   -- DML run start
        /// INSERT INTO t VALUES (2)   -- DML run continues
        /// CREATE TABLE t2 (...)      -- DDL: run individually, breaks DML run
        /// INSERT INTO t2 VALUES (1)  -- New DML run
        /// </code>
        /// </summary>
        /// <param name="batch">The batch of write requests to execute.</param>
        private void ExecuteBatch(List<WriteRequest> batch)
        {
            int i = 0;
            while (i < batch.Count)
            {
                if (DdlDetector.IsDdl(batch[i].Sql))
                {
                    // DDL: execute individually (no transaction wrapping).
                    RunSingleStatement(batch[i]);
                    i++;
                }
                else
                {
                    // DML: find the end of the consecutive DML run and execute as one transaction.
                    int end = FindEndOfDmlRun(batch, i);
                    RunDmlTransaction(batch, i, end);
                    i = end;
                }
            }
        }

        /// <summary>
        /// Finds the end index (exclusive) of a consecutive run of DML statements
        /// starting at <paramref name="start"/>. The run ends at the first DDL statement
        /// or the end of the batch.
        /// </summary>
        /// <param name="batch">The batch of write requests.</param>
        /// <param name="start">Start index of the DML run.</param>
        /// <returns>Exclusive end index of the DML run.</returns>
        private static int FindEndOfDmlRun(List<WriteRequest> batch, int start)
        {
            int end = start + 1;
            while (end < batch.Count && !DdlDetector.IsDdl(batch[end].Sql))
                end++;
            return end;
        }

        /// <summary>
        /// Executes a run of DML statements as a single transaction, with INSERT merging
        /// and automatic fallback to individual execution on failure.
        ///
        /// <para><b>Algorithm:</b></para>
        /// <list type="number">
        ///   <item><description>
        ///     <b>Merge:</b> Extract SQL strings from the batch range and pass them to
        ///     <see cref="InsertBatcher.MergeInserts"/>. This merges consecutive single-row
        ///     INSERTs to the same table into multi-row INSERTs. Non-INSERT DML statements
        ///     and INSERTs that cannot be parsed are left unchanged.
        ///   </description></item>
        ///   <item><description>
        ///     <b>BEGIN:</b> Start an explicit transaction. If BEGIN fails (rare), fall back
        ///     to individual execution.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Execute groups:</b> Execute each merged group. If any fails, stop immediately.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Rollback on failure:</b> If any group failed, ROLLBACK the entire transaction
        ///     and retry each original statement individually. This isolates the failing
        ///     statement so that only it reports an error; other statements in the batch succeed.
        ///   </description></item>
        ///   <item><description>
        ///     <b>COMMIT:</b> If all groups succeeded, commit the transaction. If COMMIT fails
        ///     (e.g., constraint violation detected at commit time), fall back to individual
        ///     execution.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Signal callers:</b> For each merged group, set the result and signal the
        ///     <see cref="ManualResetEventSlim"/> of every original request that was part of
        ///     that group. This unblocks the callers waiting in <see cref="Submit"/>.
        ///   </description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> The <c>RequestIndices</c> in each <see cref="InsertBatcher.BatchGroup"/>
        /// are relative to the DML run (starting at 0), not the full batch. They are offset
        /// by <paramref name="from"/> when accessing the batch array.</para>
        /// </summary>
        /// <param name="batch">The full batch of write requests.</param>
        /// <param name="from">Inclusive start index of the DML run.</param>
        /// <param name="to">Exclusive end index of the DML run.</param>
        private void RunDmlTransaction(List<WriteRequest> batch, int from, int to)
        {
            // Step 1: Merge compatible INSERTs into multi-row statements.
            var sqlList = new List<string>(to - from);
            for (int i = from; i < to; i++)
                sqlList.Add(batch[i].Sql);

            var groups = InsertBatcher.MergeInserts(sqlList);

            // Step 2: Begin transaction. If this fails, DuckDB is in a bad state
            // (rare); fall back to individual execution without transactions.
            if (!TryExecuteControl("BEGIN"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 3: Execute each merged group within the transaction.
            var groupResults = new List<WriteResult>();
            bool allSucceeded = true;

            for (int g = 0; g < groups.Count; g++)
            {
                var result = TryExecute(groups[g].Sql);
                if (!result.Ok)
                {
                    allSucceeded = false;
                    break; // Stop on first failure; the transaction will be rolled back.
                }
                groupResults.Add(result);
            }

            // Step 4: If any group failed, roll back and retry each statement individually.
            // This ensures only the failing statement reports an error; others succeed.
            if (!allSucceeded)
            {
                TryExecuteControl("ROLLBACK");
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 5: Commit the transaction. If COMMIT fails (rare), fall back to
            // individual execution.
            if (!TryExecuteControl("COMMIT"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 6: Signal all callers that their writes have committed.
            // Each group may contain multiple original requests (merged INSERTs).
            for (int g = 0; g < groups.Count; g++)
            {
                var group = groups[g];
                foreach (int idx in group.RequestIndices)
                {
                    // idx is relative to the DML run start (0-based), offset by 'from'
                    // to get the absolute batch index.
                    batch[from + idx].Result = groupResults[g];
                    batch[from + idx].DoneSignal.Set(); // Unblocks the caller in Submit().
                }
            }
        }

        /// <summary>
        /// Fallback: executes each request individually without transaction wrapping.
        /// Used when batch transaction fails (BEGIN/COMMIT error or DML error within
        /// the transaction). Each statement runs in DuckDB's implicit auto-commit mode.
        /// </summary>
        /// <param name="batch">The full batch of write requests.</param>
        /// <param name="from">Inclusive start index.</param>
        /// <param name="to">Exclusive end index.</param>
        private void RunEachIndividually(List<WriteRequest> batch, int from, int to)
        {
            for (int i = from; i < to; i++)
                RunSingleStatement(batch[i]);
        }

        /// <summary>
        /// Executes a single statement, sets the result, and signals the caller.
        /// </summary>
        /// <param name="request">The write request to execute.</param>
        private void RunSingleStatement(WriteRequest request)
        {
            request.Result = TryExecute(request.Sql);
            request.DoneSignal.Set(); // Unblocks the caller in Submit().
        }

        // ── SQL execution helpers ────────────────────────────────────────────

        /// <summary>
        /// Executes a SQL statement on the writer connection and returns the result.
        /// Catches all exceptions and wraps them as <see cref="WriteResult.Failure"/>.
        /// </summary>
        /// <param name="sql">The SQL statement to execute.</param>
        /// <returns>
        /// <see cref="WriteResult.Success()"/> on success, or
        /// <see cref="WriteResult.Failure(string)"/> with the exception message on error.
        /// </returns>
        private WriteResult TryExecute(string sql)
        {
            try
            {
                using (var cmd = writerConnection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                return WriteResult.Success();
            }
            catch (Exception ex)
            {
                return WriteResult.Failure(ex.Message);
            }
        }

        /// <summary>
        /// Executes a transaction control statement (BEGIN, COMMIT, ROLLBACK) and returns
        /// whether it succeeded. This is a convenience wrapper around <see cref="TryExecute"/>
        /// that discards the error message (control statements either work or they don't).
        /// </summary>
        /// <param name="sql">Transaction control SQL (e.g., "BEGIN", "COMMIT", "ROLLBACK").</param>
        /// <returns><c>true</c> if the statement succeeded, <c>false</c> otherwise.</returns>
        private bool TryExecuteControl(string sql)
        {
            return TryExecute(sql).Ok;
        }

        // ── Request type ─────────────────────────────────────────────────────

        /// <summary>
        /// Represents a single write request in the queue. Contains the SQL string,
        /// a completion signal, and a result slot.
        ///
        /// <para><b>Lifecycle:</b></para>
        /// <list type="number">
        ///   <item><description>Created by <see cref="Submit"/> with a fresh <see cref="ManualResetEventSlim"/>.</description></item>
        ///   <item><description>Enqueued into the shared queue.</description></item>
        ///   <item><description>Dequeued by the writer thread, which sets <see cref="Result"/> and calls <see cref="ManualResetEventSlim.Set"/> on <see cref="DoneSignal"/>.</description></item>
        ///   <item><description><see cref="DoneSignal"/> is disposed by the caller in <see cref="Submit"/> (only if the wait completed normally).</description></item>
        /// </list>
        /// </summary>
        /// <summary>
        /// Apply performance PRAGMAs to the writer connection.
        /// Matches what ConnectionPool.ApplyConnectionPragmas does for reader connections.
        /// Critical: without threads=1, the writer spawns nCPU internal threads per shard,
        /// causing severe contention when running 8 shards.
        /// </summary>
        private static void ApplyWriterPragmas(DuckDB.NET.Data.DuckDBConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand()) { cmd.CommandText = "SET threads=1"; cmd.ExecuteNonQuery(); }
                using (var cmd = conn.CreateCommand()) { cmd.CommandText = "SET preserve_insertion_order=false"; cmd.ExecuteNonQuery(); }
                using (var cmd = conn.CreateCommand()) { cmd.CommandText = "PRAGMA enable_object_cache"; cmd.ExecuteNonQuery(); }
            }
            catch { /* best-effort — PRAGMAs are optimization hints */ }
        }

        private sealed class WriteRequest
        {
            /// <summary>The SQL statement to execute.</summary>
            public readonly string Sql;

            /// <summary>
            /// Completion signal. The writer thread calls <c>Set()</c> after executing
            /// the SQL. The caller blocks on <c>Wait()</c> in <see cref="Submit"/>.
            /// Initialized as non-signalled (false).
            /// </summary>
            public readonly ManualResetEventSlim DoneSignal = new ManualResetEventSlim(false);

            /// <summary>
            /// The execution result, set by the writer thread before signalling
            /// <see cref="DoneSignal"/>. Read by the caller after the signal fires.
            /// </summary>
            public WriteResult Result;

            public WriteRequest(string sql) { Sql = sql; }
        }
    }
}
