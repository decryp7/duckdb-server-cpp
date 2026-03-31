using System;
using System.Collections.Generic;
using System.Threading;
using DuckDB.NET.Data;

namespace DuckArrowServer
{
    /// <summary>
    /// Result of a write (DML/DDL) operation.
    /// </summary>
    public struct WriteResult
    {
        public bool Ok;
        public string Error;

        public static WriteResult Success()
        {
            return new WriteResult { Ok = true, Error = null };
        }

        public static WriteResult Failure(string error)
        {
            return new WriteResult { Ok = false, Error = error };
        }
    }

    /// <summary>
    /// Serialises concurrent write operations into batched transactions.
    /// Mirrors the C++ WriteSerializer class.
    ///
    /// DuckDB supports exactly one writer at a time. Rather than reject concurrent
    /// writes, this class queues them and executes each batch in a single
    /// BEGIN...COMMIT transaction, maximising throughput while preserving correctness.
    ///
    /// DDL statements (CREATE, DROP, ALTER, ...) are executed outside any explicit
    /// transaction and therefore cannot be batched with DML.
    /// </summary>
    public sealed class WriteSerializer : IDisposable
    {
        private readonly DuckDBConnection _conn;
        private readonly int _batchWindowMs;
        private readonly int _batchMax;
        private readonly Queue<Request> _queue = new Queue<Request>();
        private readonly object _lock = new object();
        private readonly Thread _writerThread;
        private volatile bool _stop;

        /// <summary>
        /// Construct and start the writer background thread.
        /// </summary>
        /// <param name="connectionString">DuckDB connection string for the dedicated write connection.</param>
        /// <param name="batchWindowMs">Max milliseconds to wait before flushing a partial batch.</param>
        /// <param name="batchMax">Maximum number of DML statements per transaction.</param>
        public WriteSerializer(string connectionString, int batchWindowMs = 5, int batchMax = 512)
        {
            _batchWindowMs = batchWindowMs;
            _batchMax = batchMax;
            _conn = new DuckDBConnection(connectionString);
            _conn.Open();

            _writerThread = new Thread(DrainLoop)
            {
                Name = "DAS-Writer",
                IsBackground = true
            };
            _writerThread.Start();
        }

        /// <summary>
        /// Submit a write statement and block until its transaction commits.
        /// </summary>
        /// <param name="sql">UTF-8 DML or DDL statement.</param>
        /// <returns>WriteResult indicating success or the DuckDB error message.</returns>
        public WriteResult Submit(string sql)
        {
            var req = new Request(sql);

            lock (_lock)
            {
                _queue.Enqueue(req);
                Monitor.Pulse(_lock);
            }

            // Block until the writer thread resolves this request.
            req.Done.WaitOne();
            return req.Result;
        }

        /// <summary>
        /// Stop the writer thread and wait for it to finish.
        /// </summary>
        public void Dispose()
        {
            _stop = true;
            lock (_lock) { Monitor.PulseAll(_lock); }
            _writerThread.Join(TimeSpan.FromSeconds(10));
            _conn.Dispose();
        }

        // ── Internal types ───────────────────────────────────────────────────

        private sealed class Request
        {
            public readonly string Sql;
            public readonly ManualResetEventSlim Done = new ManualResetEventSlim(false);
            public WriteResult Result;

            public Request(string sql) { Sql = sql; }
        }

        // ── Writer thread ────────────────────────────────────────────────────

        private void DrainLoop()
        {
            while (true)
            {
                var batch = new List<Request>();
                CollectBatch(batch);
                if (batch.Count == 0) break; // _stop was set and queue is drained
                ExecuteBatch(batch);
            }
        }

        private void CollectBatch(List<Request> batch)
        {
            lock (_lock)
            {
                // Block until work arrives or we are told to stop.
                while (_queue.Count == 0 && !_stop)
                    Monitor.Wait(_lock);

                if (_stop && _queue.Count == 0) return;

                // Drain immediately available requests.
                DrainQueueLocked(batch);

                // Wait briefly for more requests to arrive.
                if (batch.Count < _batchMax)
                {
                    Monitor.Wait(_lock, _batchWindowMs);
                    DrainQueueLocked(batch);
                }
            }
        }

        private void DrainQueueLocked(List<Request> batch)
        {
            while (_queue.Count > 0 && batch.Count < _batchMax)
            {
                batch.Add(_queue.Dequeue());
            }
        }

        // ── Batch execution ──────────────────────────────────────────────────

        private void ExecuteBatch(List<Request> batch)
        {
            int i = 0;
            while (i < batch.Count)
            {
                if (IsDdl(batch[i].Sql))
                {
                    ExecuteSingle(batch[i]);
                    i++;
                }
                else
                {
                    // Find the end of this contiguous DML run.
                    int j = i + 1;
                    while (j < batch.Count && !IsDdl(batch[j].Sql)) j++;
                    ExecuteDmlRun(batch, i, j);
                    i = j;
                }
            }
        }

        private void ExecuteDmlRun(List<Request> batch, int from, int to)
        {
            if (!ExecControl("BEGIN"))
            {
                for (int k = from; k < to; k++) ExecuteSingle(batch[k]);
                return;
            }

            // Execute each statement inside the open transaction.
            // Collect results BEFORE setting any promise — promises must be
            // set only after COMMIT succeeds.
            var results = new List<WriteResult>(to - from);
            bool anyFailed = false;

            for (int k = from; k < to; k++)
            {
                var wr = ExecOne(batch[k].Sql);
                if (!wr.Ok)
                {
                    anyFailed = true;
                    break;
                }
                results.Add(wr);
            }

            if (anyFailed)
            {
                ExecControl("ROLLBACK");
                for (int k = from; k < to; k++) ExecuteSingle(batch[k]);
                return;
            }

            if (!ExecControl("COMMIT"))
            {
                for (int k = from; k < to; k++) ExecuteSingle(batch[k]);
                return;
            }

            // COMMIT succeeded — resolve all promises.
            for (int k = from; k < to; k++)
            {
                batch[k].Result = results[k - from];
                batch[k].Done.Set();
            }
        }

        private void ExecuteSingle(Request req)
        {
            req.Result = ExecOne(req.Sql);
            req.Done.Set();
        }

        private WriteResult ExecOne(string sql)
        {
            try
            {
                using (var cmd = _conn.CreateCommand())
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

        private bool ExecControl(string sql)
        {
            try
            {
                using (var cmd = _conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        // ── DDL detection ────────────────────────────────────────────────────

        private static readonly string[] DdlKeywords = {
            "CREATE", "DROP", "ALTER", "TRUNCATE",
            "ATTACH", "DETACH", "VACUUM", "PRAGMA",
            "COPY", "EXPORT", "IMPORT", "LOAD"
        };

        private static bool IsDdl(string sql)
        {
            int i = 0;
            // Skip leading whitespace.
            while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;

            // Skip single-line comments (-- ...).
            if (i + 1 < sql.Length && sql[i] == '-' && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n') i++;
                i++;
                while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;
            }

            // Extract the first keyword (up to 8 characters, upper-cased).
            int start = i;
            int klen = 0;
            while (klen < 8 && i < sql.Length && char.IsLetter(sql[i]))
            {
                klen++;
                i++;
            }

            if (klen == 0) return false;
            string keyword = sql.Substring(start, klen).ToUpperInvariant();

            for (int k = 0; k < DdlKeywords.Length; k++)
            {
                if (keyword == DdlKeywords[k]) return true;
            }
            return false;
        }
    }
}
