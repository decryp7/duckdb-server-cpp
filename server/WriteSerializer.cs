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
        private volatile bool writerThreadDead;

        public WriteSerializer(DatabaseManager dbManager, int batchWindowMs = 5, int batchMax = 512)
        {
            this.batchWindowMs = batchWindowMs;
            this.batchMax = batchMax;

            writerConnection = dbManager.CreateConnection();

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
            if (stop)
                return WriteResult.Failure("WriteSerializer has been disposed");

            if (writerThreadDead)
                return WriteResult.Failure("Writer thread has terminated unexpectedly");

            var request = new WriteRequest(sql);

            lock (lockObj)
            {
                queue.Enqueue(request);
                Monitor.Pulse(lockObj);
            }

            // Wait with a timeout so we don't hang forever if the writer thread dies.
            bool signalled = request.DoneSignal.Wait(TimeSpan.FromSeconds(30));

            // Only dispose DoneSignal if the wait was signalled.
            // If timeout fired, the writer thread may still call Set() later.
            // Disposing here would crash the writer thread with ObjectDisposedException,
            // permanently killing all future writes.
            if (signalled)
                request.DoneSignal.Dispose();

            if (!signalled)
            {
                if (writerThreadDead)
                    return WriteResult.Failure("Writer thread terminated while processing request");
                return WriteResult.Failure("Write timed out after 30 seconds");
            }
            return request.Result;
        }

        public void Dispose()
        {
            stop = true;
            lock (lockObj) { Monitor.PulseAll(lockObj); }

            // Wait for the writer thread, but don't dispose the connection
            // until the thread has actually stopped.
            if (writerThread.IsAlive)
                writerThread.Join(TimeSpan.FromSeconds(30));

            writerConnection.Dispose();

            // Signal any remaining callers stuck in Submit().
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

        private void DrainLoop()
        {
            try
            {
                while (true)
                {
                    var batch = CollectBatch();
                    if (batch.Count == 0) break;
                    ExecuteBatch(batch);
                }
            }
            catch (Exception ex)
            {
                // Log the error. Mark the thread as dead so Submit() can report it.
                writerThreadDead = true;
                Console.Error.WriteLine("[das] Writer thread crashed: " + ex.Message);

                // Signal all pending requests so they don't hang forever.
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

            // Step 3: Execute each merged group.
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

            // Step 4: If any failed, roll back and retry individually.
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

            // Step 6: Signal all callers.
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
