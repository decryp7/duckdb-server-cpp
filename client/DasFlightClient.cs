using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDbProto;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Thread-safe gRPC client for the DuckDB server.
    /// Implements <see cref="IDasFlightClient"/>.
    ///
    /// Uses the custom DuckDbService protocol (proto/duckdb_service.proto).
    /// Compatible with any server implementing the same proto (C# or C++).
    ///
    /// One instance per application — HTTP/2 handles all concurrency.
    /// </summary>
    public sealed class DasFlightClient : IDasFlightClient
    {
        private readonly Channel channel;
        private readonly CallInvoker invoker;
        private int disposed;

        // ── Construction ─────────────────────────────────────────────────────

        public DasFlightClient(string host = "localhost", int port = 17777)
            : this(new Channel(host + ":" + port, ChannelCredentials.Insecure))
        { }

        public DasFlightClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials))
        { }

        private DasFlightClient(Channel channel)
        {
            this.channel = channel;
            this.invoker = channel.CreateCallInvoker();
        }

        // ── Query ────────────────────────────────────────────────────────────

        public IFlightQueryResult Query(string sql, CancellationToken ct = default)
        {
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();

            var batches = new List<RecordBatch>();
            Schema schema = null;

            try
            {
                var request = new QueryRequest { Sql = sql };
                using (var call = invoker.AsyncServerStreamingCall(
                    DuckDbService.QueryMethod, null, new CallOptions(cancellationToken: ct), request))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    {
                        byte[] ipcData = call.ResponseStream.Current.IpcData;
                        if (ipcData == null || ipcData.Length == 0) continue;

                        // Each chunk is a complete Arrow IPC stream.
                        using (var ms = new MemoryStream(ipcData))
                        using (var ipcReader = new ArrowStreamReader(ms))
                        {
                            RecordBatch batch;
                            while ((batch = ipcReader.ReadNextRecordBatch()) != null)
                            {
                                if (schema == null)
                                    schema = ipcReader.Schema;
                                if (batch.Length > 0)
                                    batches.Add(batch);
                                else
                                    batch.Dispose();
                            }
                            if (schema == null)
                                schema = ipcReader.Schema;
                        }
                    }
                }

                if (schema == null)
                    schema = new Schema(new List<Field>(), null);

                return new FlightQueryResult(schema, batches);
            }
            catch (RpcException ex)
            {
                foreach (var b in batches) b?.Dispose();
                throw new DasException("Query failed: " + ex.Status.Detail, ex);
            }
            catch (Exception)
            {
                foreach (var b in batches) b?.Dispose();
                throw;
            }
        }

        // ── Execute ──────────────────────────────────────────────────────────

        public void Execute(string sql, CancellationToken ct = default)
        {
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();

            try
            {
                var request = new ExecuteRequest { Sql = sql };
                var response = await invoker.AsyncUnaryCall(
                    DuckDbService.ExecuteMethod, null, new CallOptions(cancellationToken: ct), request)
                    .ConfigureAwait(false);

                if (!response.Success)
                    throw new DasException("Execute failed: " + response.Error);
            }
            catch (RpcException ex)
            {
                throw new DasException("Execute failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Ping ─────────────────────────────────────────────────────────────

        public void Ping()
        {
            EnsureNotDisposed();

            try
            {
                var response = Task.Run(() => invoker.AsyncUnaryCall(
                    DuckDbService.PingMethod, null, new CallOptions(), new PingRequest()))
                    .GetAwaiter().GetResult();

                if (response.Message != "pong")
                    throw new DasException("Ping: unexpected response '" + response.Message + "'");
            }
            catch (RpcException ex)
            {
                throw new DasException("Ping failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Stats ────────────────────────────────────────────────────────────

        public string GetStats()
        {
            EnsureNotDisposed();

            try
            {
                var response = Task.Run(() => invoker.AsyncUnaryCall(
                    DuckDbService.StatsMethod, null, new CallOptions(), new StatsRequest()))
                    .GetAwaiter().GetResult();

                return response.ToJson();
            }
            catch (RpcException ex)
            {
                throw new DasException("GetStats failed: " + ex.Status.Detail, ex);
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(DasFlightClient));
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            try { channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort */ }
        }
    }
}
