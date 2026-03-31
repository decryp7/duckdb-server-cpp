using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Thread-safe Arrow Flight client for the DuckDB Flight Server.
    /// Implements <see cref="IDasFlightClient"/>.
    ///
    /// <para><b>One instance per application</b></para>
    /// <para>
    /// Arrow Flight runs over gRPC / HTTP/2, which multiplexes many concurrent
    /// RPCs over a single TCP connection.  Create ONE DasFlightClient per target
    /// server and share it across all threads.  A connection pool is not needed
    /// and would be counterproductive (more channels means more TCP connections,
    /// not more throughput).
    /// </para>
    ///
    /// <para><b>TLS</b></para>
    /// <para>
    /// Use the <see cref="DasFlightClient(string,int,ChannelCredentials)"/>
    /// constructor with <c>new SslCredentials(caPem)</c> to enable TLS.
    /// </para>
    ///
    /// <para><b>Disposal</b></para>
    /// <para>
    /// Call <see cref="Dispose"/> when the application shuts down.  The method
    /// waits up to 5 seconds for in-flight RPCs to complete before closing the
    /// underlying gRPC channel.
    /// </para>
    /// </summary>
    public sealed class DasFlightClient : IDasFlightClient
    {
        private readonly Channel      _channel;
        private readonly FlightClient _flight;
        private int                   _disposed; // 0 = alive, 1 = disposed; use Interlocked

        // ── Construction ──────────────────────────────────────────────────────

        /// <summary>
        /// Connect to a plaintext (non-TLS) Flight server.
        /// </summary>
        /// <param name="host">DNS name or IP address of the server.</param>
        /// <param name="port">gRPC listen port (default 17777).</param>
        public DasFlightClient(string host = "localhost", int port = 17777)
            : this(new Channel(
                host + ":" + port,
                ChannelCredentials.Insecure))
        { }

        /// <summary>
        /// Connect with a custom channel credential (e.g. TLS).
        /// </summary>
        /// <param name="host">DNS name or IP address of the server.</param>
        /// <param name="port">gRPC listen port.</param>
        /// <param name="credentials">
        /// gRPC channel credentials.  Use <c>new SslCredentials(caPem)</c>
        /// for TLS with a custom CA certificate.
        /// </param>
        public DasFlightClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials))
        { }

        private DasFlightClient(Channel channel)
        {
            _channel = channel;
            _flight  = new FlightClient(_channel.CreateCallInvoker());
        }

        // ── IDasFlightClient — read queries ───────────────────────────────────

        /// <inheritdoc/>
        public IFlightQueryResult Query(
            string sql, CancellationToken ct = default)
        {
            // Task.Run prevents SynchronizationContext deadlock on UI threads.
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task<IFlightQueryResult> QueryAsync(
            string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            var ticket = new FlightTicket(Encoding.UTF8.GetBytes(sql));

            var batches = new List<RecordBatch>();
            try
            {
                var stream = _flight.GetStream(ticket);
                Schema schema = await stream.Schema.ConfigureAwait(false);

                RecordBatch batch;
                while ((batch = await stream
                    .ReadNextRecordBatchAsync(ct)
                    .ConfigureAwait(false)) != null)
                {
                    batches.Add(batch);
                }

                return new FlightQueryResult(schema, batches);
            }
            catch (RpcException ex)
            {
                // Dispose any batches already collected before re-throwing,
                // otherwise the Arrow column buffers leak.
                foreach (var b in batches) b?.Dispose();
                throw new DasException(
                    "Flight DoGet failed: " + ex.Status.Detail, ex);
            }
            catch (Exception)
            {
                // Covers OperationCanceledException (ct cancelled mid-stream)
                // and any other unexpected exception.  Dispose collected batches
                // to prevent Arrow column buffer leaks, then re-throw as-is
                // so the caller receives the original exception type
                // (e.g. OperationCanceledException preserves cancellation semantics).
                foreach (var b in batches) b?.Dispose();
                throw;
            }
        }

        // ── IDasFlightClient — write / DDL ────────────────────────────────────

        /// <inheritdoc/>
        public void Execute(string sql, CancellationToken ct = default)
        {
            // Task.Run prevents SynchronizationContext deadlock on UI threads.
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            await DoActionAndDrain("execute", sql, ct).ConfigureAwait(false);
        }

        // ── IDasFlightClient — liveness / metrics ─────────────────────────────

        /// <inheritdoc/>
        public void Ping()
        {
            EnsureNotDisposed();
            // Use Task.Run to execute on a thread-pool thread, avoiding deadlock
            // when called from a WPF/WinForms UI thread that has a
            // SynchronizationContext. Without Task.Run, GetAwaiter().GetResult()
            // would block the UI thread while the async continuation tries to
            // resume on the same UI thread — a classic deadlock.
            string body = Task.Run(() => DoActionFirstResult("ping"))
                              .GetAwaiter().GetResult();
            if (body != "pong")
                throw new DasException(
                    "Ping: unexpected response body '" + body + "' (expected 'pong')");
        }

        /// <inheritdoc/>
        public string GetStats()
        {
            EnsureNotDisposed();
            // Task.Run prevents deadlock when called from a UI thread with
            // a SynchronizationContext (WPF, WinForms).
            return Task.Run(() => DoActionFirstResult("stats"))
                        .GetAwaiter().GetResult();
        }

        // ── Internal helpers ──────────────────────────────────────────────────

        /// <summary>
        /// Send a DoAction RPC with the given type and body, draining the
        /// result stream (and propagating any gRPC error as DasException).
        /// Used for execute, ping, and any action whose result we do not need.
        /// </summary>
        private async Task DoActionAndDrain(
            string actionType,
            string body,
            CancellationToken ct)
        {
            var action = new FlightAction(
                actionType,
                body.Length > 0 ? Encoding.UTF8.GetBytes(body) : Array.Empty<byte>());
            try
            {
                var call = _flight.DoAction(action);
                while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                { /* drain — result is intentionally ignored */ }
            }
            catch (RpcException ex)
            {
                throw new DasException(
                    "Flight DoAction(" + actionType + ") failed: " + ex.Status.Detail,
                    ex);
            }
        }

        /// <summary>
        /// Send a DoAction RPC and return the body of the first result item.
        /// Returns an empty string if the stream has no items.
        /// Used for stats.
        /// </summary>
        private async Task<string> DoActionFirstResult(string actionType)
        {
            var action = new FlightAction(actionType, Array.Empty<byte>());
            try
            {
                var call = _flight.DoAction(action);
                // Collect the first non-null body but continue draining the stream
                // to completion.  Returning early from the loop while the server still
                // has results queued leaves the gRPC call in an incomplete state,
                // which can cause resource leaks and server-side warnings.
                string firstBody = string.Empty;
                while (await call.ResponseStream
                    .MoveNext(CancellationToken.None)
                    .ConfigureAwait(false))
                {
                    var body = call.ResponseStream.Current.Body;
                    if (body != null && firstBody.Length == 0)
                        firstBody = Encoding.UTF8.GetString(body.ToArray());
                    // Continue iterating to drain the stream fully.
                }
                return firstBody;
            }
            catch (RpcException ex)
            {
                throw new DasException(
                    "Flight DoAction(" + actionType + ") failed: " + ex.Status.Detail,
                    ex);
            }
        }

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref _disposed) != 0)
                throw new ObjectDisposedException(nameof(DasFlightClient));
        }

        // ── IDisposable ───────────────────────────────────────────────────────

        /// <summary>
        /// Gracefully shut down the gRPC channel.
        /// Waits up to 5 seconds for in-flight RPCs to complete.
        /// </summary>
        public void Dispose()
        {
            // Interlocked.CompareExchange atomically sets _disposed to 1 only if it was 0.
            // Returns the original value; if it was already 1, another thread got here first.
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
            try { _channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort — ignore errors during shutdown */ }
        }
    }
}
