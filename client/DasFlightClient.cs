using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Core;

namespace DuckArrowClient
{
    /// <summary>
    /// Thread-safe Arrow Flight client for the DuckDB Flight Server.
    /// Implements <see cref="IDasFlightClient"/>.
    ///
    /// Uses raw gRPC with manual proto encoding because Apache.Arrow.Flight 14
    /// FlightClient requires Grpc.Net.Client (needs .NET 5+), which is
    /// incompatible with .NET Framework 4.6.2 / Grpc.Core.
    ///
    /// IMPORTANT: Compatible with the C# DuckArrowServer only. The IPC
    /// serialization embeds full Arrow IPC streams inside FlightData.data_header,
    /// which differs from the standard Flight wire format. For cross-language
    /// interop, use the C++ server with a standard Flight client library.
    ///
    /// One instance per application — HTTP/2 handles all concurrency.
    /// </summary>
    public sealed class DasFlightClient : IDasFlightClient
    {
        private readonly Channel _channel;
        private readonly CallInvoker _invoker;
        private int _disposed;

        // ── gRPC method definitions (raw byte[] marshalling) ─────────────────

        private static readonly Marshaller<byte[]> RawMarshaller = new Marshaller<byte[]>(
            bytes => bytes,
            bytes => bytes);

        private static readonly Method<byte[], byte[]> DoGetMethod = new Method<byte[], byte[]>(
            MethodType.ServerStreaming,
            "arrow.flight.protocol.FlightService",
            "DoGet",
            RawMarshaller, RawMarshaller);

        private static readonly Method<byte[], byte[]> DoActionMethod = new Method<byte[], byte[]>(
            MethodType.ServerStreaming,
            "arrow.flight.protocol.FlightService",
            "DoAction",
            RawMarshaller, RawMarshaller);

        // ── Construction ─────────────────────────────────────────────────────

        public DasFlightClient(string host = "localhost", int port = 17777)
            : this(new Channel(host + ":" + port, ChannelCredentials.Insecure))
        { }

        public DasFlightClient(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host + ":" + port, credentials))
        { }

        private DasFlightClient(Channel channel)
        {
            _channel = channel;
            _invoker = channel.CreateCallInvoker();
        }

        // ── IDasFlightClient — read queries ──────────────────────────────────

        public IFlightQueryResult Query(string sql, CancellationToken ct = default)
        {
            return Task.Run(() => QueryAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task<IFlightQueryResult> QueryAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();

            // Build the Ticket proto message: field 1 = bytes.
            byte[] ticketProto = EncodeTicket(Encoding.UTF8.GetBytes(sql));

            var batches = new List<RecordBatch>();
            Schema schema = null;

            try
            {
                using (var call = _invoker.AsyncServerStreamingCall(DoGetMethod, null, new CallOptions(cancellationToken: ct), ticketProto))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    {
                        byte[] flightDataBytes = call.ResponseStream.Current;

                        // Decode FlightData proto: extract IPC data.
                        byte[] ipcData = DecodeFlightDataHeader(flightDataBytes);
                        if (ipcData == null || ipcData.Length == 0) continue;

                        // Parse the IPC message to get schema or record batch.
                        using (var ms = new System.IO.MemoryStream(ipcData))
                        using (var ipcReader = new Apache.Arrow.Ipc.ArrowStreamReader(ms))
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
                throw new DasException("Flight DoGet failed: " + ex.Status.Detail, ex);
            }
            catch (Exception)
            {
                foreach (var b in batches) b?.Dispose();
                throw;
            }
        }

        // ── IDasFlightClient — write / DDL ───────────────────────────────────

        public void Execute(string sql, CancellationToken ct = default)
        {
            Task.Run(() => ExecuteAsync(sql, ct)).GetAwaiter().GetResult();
        }

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            EnsureNotDisposed();
            await DoActionAndDrain("execute", Encoding.UTF8.GetBytes(sql), ct).ConfigureAwait(false);
        }

        // ── IDasFlightClient — liveness / metrics ────────────────────────────

        public void Ping()
        {
            EnsureNotDisposed();
            string body = Task.Run(() => DoActionFirstResult("ping", new byte[0]))
                              .GetAwaiter().GetResult();
            if (body != "pong")
                throw new DasException("Ping: unexpected response '" + body + "' (expected 'pong')");
        }

        public string GetStats()
        {
            EnsureNotDisposed();
            return Task.Run(() => DoActionFirstResult("stats", new byte[0]))
                        .GetAwaiter().GetResult();
        }

        // ── Internal helpers ─────────────────────────────────────────────────

        private async Task DoActionAndDrain(string actionType, byte[] body, CancellationToken ct)
        {
            byte[] actionProto = EncodeAction(actionType, body);
            try
            {
                using (var call = _invoker.AsyncServerStreamingCall(DoActionMethod, null, new CallOptions(cancellationToken: ct), actionProto))
                {
                    while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    { /* drain */ }
                }
            }
            catch (RpcException ex)
            {
                throw new DasException("Flight DoAction(" + actionType + ") failed: " + ex.Status.Detail, ex);
            }
        }

        private async Task<string> DoActionFirstResult(string actionType, byte[] body)
        {
            byte[] actionProto = EncodeAction(actionType, body);
            try
            {
                using (var call = _invoker.AsyncServerStreamingCall(DoActionMethod, null, new CallOptions(), actionProto))
                {
                    string firstBody = string.Empty;
                    while (await call.ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false))
                    {
                        if (firstBody.Length == 0)
                        {
                            byte[] resultBytes = DecodeResultBody(call.ResponseStream.Current);
                            if (resultBytes != null)
                                firstBody = Encoding.UTF8.GetString(resultBytes);
                        }
                    }
                    return firstBody;
                }
            }
            catch (RpcException ex)
            {
                throw new DasException("Flight DoAction(" + actionType + ") failed: " + ex.Status.Detail, ex);
            }
        }

        private void EnsureNotDisposed()
        {
            if (Thread.VolatileRead(ref _disposed) != 0)
                throw new ObjectDisposedException(nameof(DasFlightClient));
        }

        // ── Proto encoding/decoding ──────────────────────────────────────────

        /// <summary>Encode Ticket proto: field 1 = bytes.</summary>
        private static byte[] EncodeTicket(byte[] ticket)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                cos.WriteBytes(ByteString.CopyFrom(ticket));
                cos.Flush();
                return ms.ToArray();
            }
        }

        /// <summary>Encode Action proto: field 1 = string type, field 2 = bytes body.</summary>
        private static byte[] EncodeAction(string type, byte[] body)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                cos.WriteString(type);
                if (body != null && body.Length > 0)
                {
                    cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
                    cos.WriteBytes(ByteString.CopyFrom(body));
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        /// <summary>Decode FlightData proto: extract data_header (field 2).</summary>
        private static byte[] DecodeFlightDataHeader(byte[] data)
        {
            var input = new CodedInputStream(data);
            byte[] header = null;
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 18) // field 2, wire type 2
                    header = input.ReadBytes().ToByteArray();
                else
                    input.SkipLastField();
            }
            return header;
        }

        /// <summary>Decode Result proto: extract body (field 1).</summary>
        private static byte[] DecodeResultBody(byte[] data)
        {
            var input = new CodedInputStream(data);
            byte[] body = null;
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) // field 1, wire type 2
                    body = input.ReadBytes().ToByteArray();
                else
                    input.SkipLastField();
            }
            return body;
        }

        // ── IDisposable ──────────────────────────────────────────────────────

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
            try { _channel.ShutdownAsync().Wait(TimeSpan.FromSeconds(5)); }
            catch { /* best-effort */ }
        }
    }
}
