using System;
using System.Collections.Generic;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Batches concurrent write requests into single DuckDB transactions.
    ///
    /// How it works:
    ///   1. Callers submit SQL via Submit(). Each call blocks until done.
    ///   2. A background thread collects requests over a short time window.
    ///   3. DML statements in the window are wrapped in one BEGIN...COMMIT.
    ///   4. DDL statements (CREATE, DROP, etc.) run individually.
    ///   5. If any DML fails, the batch rolls back and each is retried alone.
    /// </summary>
    public sealed class WriteSerializer : IWriteSerializer
    {
        private readonly DuckDBConnection writerConnection;
        private readonly int batchWindowMs;
        private readonly int batchMax;
        private readonly Queue<WriteRequest> queue = new Queue<WriteRequest>();
        private readonly object lockObj = new object();
        private readonly Thread writerThread;
        private volatile bool stop;

        public WriteSerializer(string connectionString, int batchWindowMs = 5, int batchMax = 512)
        {
            this.batchWindowMs = batchWindowMs;
            this.batchMax = batchMax;

            writerConnection = new DuckDBConnection(connectionString);
            writerConnection.Open();

            writerThread = new Thread(DrainLoop)
            {
                Name = "DAS-Writer",
                IsBackground = true
            };
            writerThread.Start();
        }

        /// <summary>
        /// Submit a SQL statement and wait for it to complete.
        /// </summary>
        public WriteResult Submit(string sql)
        {
            var request = new WriteRequest(sql);

            lock (lockObj)
            {
                queue.Enqueue(request);
                Monitor.Pulse(lockObj);
            }

            // Wait until the writer thread finishes this request.
            request.DoneSignal.WaitOne();
            return request.Result;
        }

        public void Dispose()
        {
            stop = true;
            lock (lockObj) { Monitor.PulseAll(lockObj); }
            writerThread.Join(TimeSpan.FromSeconds(10));
            writerConnection.Dispose();
        }

        // ── Background writer thread ─────────────────────────────────────────

        private void DrainLoop()
        {
            while (true)
            {
                var batch = CollectBatch();
                if (batch.Count == 0) break; // stop signal, queue empty
                ExecuteBatch(batch);
            }
        }

        /// <summary>
        /// Wait for requests to arrive, then collect a batch.
        /// Returns an empty list when it's time to shut down.
        /// </summary>
        private List<WriteRequest> CollectBatch()
        {
            var batch = new List<WriteRequest>();

            lock (lockObj)
            {
                // Wait for work or stop signal.
                while (queue.Count == 0 && !stop)
                    Monitor.Wait(lockObj);

                if (stop && queue.Count == 0)
                    return batch; // empty = shut down

                // Take everything currently in the queue.
                MoveQueueToBatch(batch);

                // Wait a short time for more requests to arrive.
                if (batch.Count < batchMax)
                {
                    Monitor.Wait(lockObj, batchWindowMs);
                    MoveQueueToBatch(batch);
                }
            }

            return batch;
        }

        private void MoveQueueToBatch(List<WriteRequest> batch)
        {
            while (queue.Count > 0 && batch.Count < batchMax)
                batch.Add(queue.Dequeue());
        }

        // ── Batch execution ──────────────────────────────────────────────────

        /// <summary>
        /// Execute a batch of requests. DDL runs individually.
        /// Consecutive DML runs are grouped into transactions.
        /// </summary>
        private void ExecuteBatch(List<WriteRequest> batch)
        {
            int i = 0;
            while (i < batch.Count)
            {
                if (DdlDetector.IsDdl(batch[i].Sql))
                {
                    // DDL cannot be inside a transaction. Run it alone.
                    RunSingleStatement(batch[i]);
                    i++;
                }
                else
                {
                    // Find how many consecutive DML statements we have.
                    int end = FindEndOfDmlRun(batch, i);
                    RunDmlTransaction(batch, i, end);
                    i = end;
                }
            }
        }

        /// <summary>
        /// Find the index where the DML run ends (first DDL or end of list).
        /// </summary>
        private static int FindEndOfDmlRun(List<WriteRequest> batch, int start)
        {
            int end = start + 1;
            while (end < batch.Count && !DdlDetector.IsDdl(batch[end].Sql))
                end++;
            return end;
        }

        /// <summary>
        /// Run a group of DML statements in one BEGIN...COMMIT transaction.
        /// If anything fails, roll back and retry each statement individually.
        /// </summary>
        private void RunDmlTransaction(List<WriteRequest> batch, int from, int to)
        {
            // Step 1: Try to begin a transaction.
            if (!TryExecuteControl("BEGIN"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 2: Run each DML statement. Collect results but don't
            // signal callers yet -- we need COMMIT to succeed first.
            var results = new List<WriteResult>();
            bool allSucceeded = true;

            for (int i = from; i < to; i++)
            {
                var result = TryExecute(batch[i].Sql);
                if (!result.Ok)
                {
                    allSucceeded = false;
                    break;
                }
                results.Add(result);
            }

            // Step 3: If any statement failed, roll back and retry each alone.
            if (!allSucceeded)
            {
                TryExecuteControl("ROLLBACK");
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 4: Try to commit. If COMMIT fails, retry each alone.
            if (!TryExecuteControl("COMMIT"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 5: COMMIT succeeded! Tell all callers their write is done.
            for (int i = from; i < to; i++)
            {
                batch[i].Result = results[i - from];
                batch[i].DoneSignal.Set();
            }
        }

        /// <summary>
        /// Run each request individually (outside a transaction).
        /// Used as a fallback when the batch transaction fails.
        /// </summary>
        private void RunEachIndividually(List<WriteRequest> batch, int from, int to)
        {
            for (int i = from; i < to; i++)
                RunSingleStatement(batch[i]);
        }

        /// <summary>
        /// Run one SQL statement and signal the caller when done.
        /// </summary>
        private void RunSingleStatement(WriteRequest request)
        {
            request.Result = TryExecute(request.Sql);
            request.DoneSignal.Set();
        }

        // ── SQL execution helpers ────────────────────────────────────────────

        /// <summary>
        /// Execute a SQL statement and return the result.
        /// </summary>
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
        /// Execute a control statement (BEGIN, COMMIT, ROLLBACK).
        /// Returns true if it worked.
        /// </summary>
        private bool TryExecuteControl(string sql)
        {
            return TryExecute(sql).Ok;
        }

        // ── Request type ─────────────────────────────────────────────────────

        private sealed class WriteRequest
        {
            public readonly string Sql;
            public readonly ManualResetEventSlim DoneSignal = new ManualResetEventSlim(false);
            public WriteResult Result;

            public WriteRequest(string sql) { Sql = sql; }
        }
    }
}
