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
    ///   3. Before executing, consecutive INSERTs to the same table are
    ///      merged into multi-row INSERTs (much faster — parse once, not N times).
    ///   4. Merged DML statements are wrapped in one BEGIN...COMMIT.
    ///   5. DDL statements (CREATE, DROP, etc.) run individually.
    ///   6. If any DML fails, the batch rolls back and each is retried alone.
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
                if (batch.Count == 0) break;
                ExecuteBatch(batch);
            }
        }

        private List<WriteRequest> CollectBatch()
        {
            var batch = new List<WriteRequest>();

            lock (lockObj)
            {
                while (queue.Count == 0 && !stop)
                    Monitor.Wait(lockObj);

                if (stop && queue.Count == 0)
                    return batch;

                MoveQueueToBatch(batch);

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
        /// Within each DML run, compatible INSERTs are merged into multi-row statements.
        /// </summary>
        private void ExecuteBatch(List<WriteRequest> batch)
        {
            int i = 0;
            while (i < batch.Count)
            {
                if (DdlDetector.IsDdl(batch[i].Sql))
                {
                    RunSingleStatement(batch[i]);
                    i++;
                }
                else
                {
                    int end = FindEndOfDmlRun(batch, i);
                    RunDmlTransaction(batch, i, end);
                    i = end;
                }
            }
        }

        private static int FindEndOfDmlRun(List<WriteRequest> batch, int start)
        {
            int end = start + 1;
            while (end < batch.Count && !DdlDetector.IsDdl(batch[end].Sql))
                end++;
            return end;
        }

        /// <summary>
        /// Run a group of DML statements in one BEGIN...COMMIT transaction.
        /// Before executing, merge compatible INSERTs into multi-row statements.
        /// </summary>
        private void RunDmlTransaction(List<WriteRequest> batch, int from, int to)
        {
            // Step 1: Merge compatible INSERTs into multi-row statements.
            var sqlList = new List<string>(to - from);
            for (int i = from; i < to; i++)
                sqlList.Add(batch[i].Sql);

            var groups = InsertBatcher.MergeInserts(sqlList);

            // Step 2: Begin transaction.
            if (!TryExecuteControl("BEGIN"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 3: Execute each group. Collect results per group.
            var groupResults = new List<WriteResult>();
            bool allSucceeded = true;

            for (int g = 0; g < groups.Count; g++)
            {
                var result = TryExecute(groups[g].Sql);
                if (!result.Ok)
                {
                    allSucceeded = false;
                    break;
                }
                groupResults.Add(result);
            }

            // Step 4: If any failed, roll back and retry each individually.
            if (!allSucceeded)
            {
                TryExecuteControl("ROLLBACK");
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 5: Commit.
            if (!TryExecuteControl("COMMIT"))
            {
                RunEachIndividually(batch, from, to);
                return;
            }

            // Step 6: Signal all callers. Map group results back to individual requests.
            for (int g = 0; g < groups.Count; g++)
            {
                var group = groups[g];
                foreach (int idx in group.RequestIndices)
                {
                    batch[from + idx].Result = groupResults[g];
                    batch[from + idx].DoneSignal.Set();
                }
            }
        }

        private void RunEachIndividually(List<WriteRequest> batch, int from, int to)
        {
            for (int i = from; i < to; i++)
                RunSingleStatement(batch[i]);
        }

        private void RunSingleStatement(WriteRequest request)
        {
            request.Result = TryExecute(request.Sql);
            request.DoneSignal.Set();
        }

        // ── SQL execution helpers ────────────────────────────────────────────

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
