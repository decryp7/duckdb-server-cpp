using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// Arrow Flight server backed by a DuckDB database.
    ///
    /// This is the main server class. It handles three types of requests:
    ///   - DoGet:   Run a SELECT query and stream Arrow batches to the client.
    ///   - DoAction: Run a command (execute SQL, ping, or get stats).
    ///   - ListActions: Tell the client what actions are available.
    ///
    /// Uses a connection pool for read queries and a write serializer
    /// for batched write operations.
    /// </summary>
    public sealed class DuckFlightServer : FlightServer, IDisposable
    {
        private readonly ServerConfig config;
        private readonly IConnectionPool readPool;
        private readonly IWriteSerializer writer;

        private long queriesRead;
        private long queriesWrite;
        private long errors;

        /// <summary>How many rows to put in each Arrow batch when streaming.</summary>
        private const int BatchSize = 1024;

        public DuckFlightServer(ServerConfig config, IConnectionPool readPool, IWriteSerializer writer)
        {
            this.config = config;
            this.readPool = readPool;
            this.writer = writer;
        }

        /// <summary>Get a snapshot of live server metrics.</summary>
        public ServerStats GetStats()
        {
            return new ServerStats
            {
                QueriesRead = Interlocked.Read(ref queriesRead),
                QueriesWrite = Interlocked.Read(ref queriesWrite),
                Errors = Interlocked.Read(ref errors),
                ReaderPoolSize = readPool.Size,
                Port = config.Port
            };
        }

        // ── DoGet: Read queries ──────────────────────────────────────────────

        /// <summary>
        /// Handle a DoGet request: execute a SELECT query and stream results.
        ///
        /// Steps:
        ///   1. Decode the SQL from the ticket.
        ///   2. Borrow a read connection from the pool.
        ///   3. Execute the query.
        ///   4. Build an Arrow schema from the result columns.
        ///   5. Stream record batches (1024 rows each) to the client.
        /// </summary>
        public override async Task DoGet(
            FlightTicket ticket,
            FlightServerRecordBatchStreamWriter responseStream,
            ServerCallContext context)
        {
            string sql = Encoding.UTF8.GetString(ticket.Ticket.ToArray());

            try
            {
                using (var handle = readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var schema = RecordBatchBuilder.BuildSchema(reader);

                        // Stream batches until there are no more rows.
                        RecordBatch batch;
                        while ((batch = RecordBatchBuilder.ReadNextBatch(reader, schema, BatchSize)) != null)
                        {
                            await responseStream.WriteAsync(batch).ConfigureAwait(false);
                        }
                    }
                }

                Interlocked.Increment(ref queriesRead);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        // ── DoAction: Execute / Ping / Stats ─────────────────────────────────

        /// <summary>
        /// Handle a DoAction request. Routes to the correct handler
        /// based on the action type.
        /// </summary>
        public override async Task DoAction(
            FlightAction request,
            IAsyncStreamWriter<FlightResult> responseStream,
            ServerCallContext context)
        {
            switch (request.Type)
            {
                case "execute":
                    HandleExecute(request);
                    return;

                case "ping":
                    await HandlePing(responseStream).ConfigureAwait(false);
                    return;

                case "stats":
                    await HandleStats(responseStream).ConfigureAwait(false);
                    return;

                default:
                    throw new RpcException(new Status(StatusCode.Unimplemented,
                        "Unknown action '" + request.Type + "'. Supported: execute, ping, stats."));
            }
        }

        /// <summary>
        /// Execute a DML or DDL statement via the write serializer.
        /// </summary>
        private void HandleExecute(FlightAction request)
        {
            // Validate the request body.
            if (request.Body == null || request.Body.Length == 0)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.InvalidArgument,
                    "execute action requires a non-empty SQL body"));
            }

            string sql = Encoding.UTF8.GetString(request.Body.ToArray());
            Interlocked.Increment(ref queriesWrite);

            // Submit to the write serializer and wait for commit.
            var result = writer.Submit(sql);
            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, result.Error));
            }
        }

        /// <summary>
        /// Respond with "pong" for liveness checks.
        /// </summary>
        private static async Task HandlePing(IAsyncStreamWriter<FlightResult> responseStream)
        {
            await responseStream.WriteAsync(
                new FlightResult(Encoding.UTF8.GetBytes("pong")))
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Respond with a JSON object of server metrics.
        /// </summary>
        private async Task HandleStats(IAsyncStreamWriter<FlightResult> responseStream)
        {
            var s = GetStats();
            string json = string.Format(
                "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2}," +
                "\"reader_pool_size\":{3},\"port\":{4}}}",
                s.QueriesRead, s.QueriesWrite, s.Errors, s.ReaderPoolSize, s.Port);

            await responseStream.WriteAsync(
                new FlightResult(Encoding.UTF8.GetBytes(json)))
                .ConfigureAwait(false);
        }

        // ── ListActions ──────────────────────────────────────────────────────

        /// <summary>
        /// Tell clients what actions this server supports.
        /// </summary>
        public override Task<List<FlightActionType>> ListActions(ServerCallContext context)
        {
            var actions = new List<FlightActionType>
            {
                new FlightActionType("execute",
                    "Execute DML or DDL SQL. Body = UTF-8 SQL. Returns empty stream on success."),
                new FlightActionType("ping",
                    "Liveness check. Returns 'pong'."),
                new FlightActionType("stats",
                    "Server metrics as JSON.")
            };
            return Task.FromResult(actions);
        }

        // ── Dispose ──────────────────────────────────────────────────────────

        public void Dispose()
        {
            writer.Dispose();
            readPool.Dispose();
        }
    }
}
