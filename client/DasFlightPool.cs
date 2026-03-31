using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Optional fixed-size pool of <see cref="DasFlightClient"/> instances.
    /// Implements <see cref="IDasFlightPool"/>.
    ///
    /// <para><b>You rarely need this class.</b>
    /// A single <see cref="DasFlightClient"/> handles hundreds of concurrent
    /// callers via HTTP/2 multiplexing.  Use <see cref="DasFlightPool"/> only
    /// when you need:
    /// <list type="bullet">
    ///   <item>Hard per-channel concurrency limits</item>
    ///   <item>Independent TLS sessions per caller</item>
    ///   <item>Load distribution across multiple server addresses</item>
    /// </list>
    /// </para>
    ///
    /// <para><b>Usage</b></para>
    /// <code>
    /// using (var pool = new DasFlightPool("server", 17777, size: 4))
    /// using (var lease = await pool.BorrowAsync())
    /// using (var result = await lease.Client.QueryAsync("SELECT 1"))
    /// {
    ///     Console.WriteLine(result.RowCount);
    /// } // lease disposed here — client returned to pool
    /// </code>
    /// </summary>
    public sealed class DasFlightPool : IDasFlightPool
    {
        private readonly string             _host;
        private readonly int                _port;
        private readonly ChannelCredentials _credentials;
        private readonly ConcurrentBag<DasFlightClient> _idle;
        private readonly SemaphoreSlim      _sem;
        private int                         _disposed; // 0 = alive, 1 = disposed; use Interlocked

        // ── Construction ──────────────────────────────────────────────────────

        /// <summary>
        /// Create a pool of plaintext connections to <paramref name="host"/>:<paramref name="port"/>.
        /// </summary>
        /// <param name="host">Server DNS name or IP.</param>
        /// <param name="port">gRPC port.</param>
        /// <param name="size">Number of client instances to maintain.</param>
        public DasFlightPool(string host = "localhost", int port = 17777, int size = 4)
            : this(host, port, size, ChannelCredentials.Insecure)
        { }

        /// <summary>
        /// Create a pool with a custom channel credential (e.g. TLS).
        /// </summary>
        public DasFlightPool(
            string host, int port, int size,
            ChannelCredentials credentials)
        {
            if (size < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(size),
                    size,
                    "DasFlightPool size must be at least 1.");

            _host        = host;
            _port        = port;
            _credentials = credentials;
            _idle        = new ConcurrentBag<DasFlightClient>();
            _sem         = new SemaphoreSlim(size, size);

            // Pre-create all clients so borrowing is instant.
            // Wrapped in try/catch: if any DasFlightClient constructor
            // throws (e.g. Grpc.Core rejects the credentials), we
            // dispose all clients already added to _idle before rethrowing,
            // because the DasFlightPool destructor will not run for a
            // partially-constructed object.
            try
            {
                for (int i = 0; i < size; i++)
                    _idle.Add(new DasFlightClient(host, port, credentials));
            }
            catch
            {
                foreach (var c in _idle) c.Dispose();
                _sem.Dispose();
                throw;
            }
        }

        // ── IDasFlightPool ────────────────────────────────────────────────────

        /// <inheritdoc/>
        public IPoolLease Borrow(CancellationToken ct = default)
        {
            EnsureNotDisposed();
            _sem.Wait(ct);
            return TakeLease();
        }

        /// <inheritdoc/>
        public async Task<IPoolLease> BorrowAsync(CancellationToken ct = default)
        {
            EnsureNotDisposed();
            await _sem.WaitAsync(ct).ConfigureAwait(false);
            return TakeLease();
        }

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref _disposed) != 0)
                throw new ObjectDisposedException(nameof(DasFlightPool));
        }

        // ── Internal helpers ──────────────────────────────────────────────────

        private IPoolLease TakeLease()
        {
            if (_idle.TryTake(out var client))
                return new Lease(this, client);

            // Should not happen because the semaphore guards the count.
            _sem.Release();
            throw new InvalidOperationException("DasFlightPool: internal inconsistency.");
        }

        private void Return(DasFlightClient client)
        {
            if (Thread.VolatileRead(ref _disposed) != 0) {
                // Pool is disposed — dispose the client directly instead.
                client.Dispose();
                return;
            }
            _idle.Add(client);
            try
            {
                _sem.Release();
            }
            catch (ObjectDisposedException)
            {
                // The pool was disposed between the _disposed check above and
                // _sem.Release(). The client was already added back to _idle
                // where Dispose() will find and clean it up. Nothing to do here.
            }
        }

        // ── Lease ─────────────────────────────────────────────────────────────

        /// <summary>
        /// RAII lease that returns the client to the pool when disposed.
        /// </summary>
        private sealed class Lease : IPoolLease
        {
            private readonly DasFlightPool   _pool;
            private readonly DasFlightClient _client;
            private int  _disposed; // 0=alive, 1=disposed; Interlocked-guarded

            internal Lease(DasFlightPool pool, DasFlightClient client)
            {
                _pool   = pool;
                _client = client;
            }

            /// <inheritdoc/>
            public IDasFlightClient Client => _client;

            /// <summary>Returns the client to the pool.</summary>
            public void Dispose()
            {
                // Use Interlocked to prevent double-return if two threads
                // somehow call Dispose() on the same lease simultaneously.
                if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
                _pool.Return(_client);
            }
        }

        // ── IDisposable ───────────────────────────────────────────────────────

        /// <summary>Dispose all idle client instances and the semaphore.</summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
            foreach (var c in _idle) c.Dispose();
            _sem.Dispose();
        }
    }
}
