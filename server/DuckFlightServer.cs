using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// Arrow Flight server backed by a DuckDB database.
    /// Mirrors the C++ DuckFlightServer class.
    ///
    /// Overrides DoGet, DoAction, and ListActions. All other gRPC plumbing
    /// (thread pool, TLS handshake, HTTP/2 framing) is handled by the
    /// Arrow Flight / gRPC runtime.
    /// </summary>
    public sealed class DuckFlightServer : FlightServer, IDisposable
    {
        private readonly ServerConfig _cfg;
        private readonly ConnectionPool _readPool;
        private readonly WriteSerializer _writer;

        private long _statQueriesRead;
        private long _statQueriesWrite;
        private long _statErrors;

        public DuckFlightServer(ServerConfig cfg)
        {
            _cfg = cfg;

            // Auto-size the reader pool to 2x the number of logical CPUs.
            if (_cfg.ReaderPoolSize <= 0)
                _cfg.ReaderPoolSize = Math.Max(Environment.ProcessorCount * 2, 4);

            string connStr = BuildConnectionString(cfg.DbPath);

            _readPool = new ConnectionPool(connStr, _cfg.ReaderPoolSize);
            _writer = new WriteSerializer(connStr, _cfg.WriteBatchMs, _cfg.WriteBatchMax);

            Console.WriteLine("[das] DuckFlightServer initialised");
            Console.WriteLine("      db       = " + _cfg.DbPath);
            Console.WriteLine("      readers  = " + _cfg.ReaderPoolSize);
            Console.WriteLine("      batch_ms = " + _cfg.WriteBatchMs);
        }

        public ServerStats Stats()
        {
            return new ServerStats
            {
                QueriesRead = Interlocked.Read(ref _statQueriesRead),
                QueriesWrite = Interlocked.Read(ref _statQueriesWrite),
                Errors = Interlocked.Read(ref _statErrors),
                ReaderPoolSize = _cfg.ReaderPoolSize,
                Port = _cfg.Port
            };
        }

        // ── DoGet — read queries ─────────────────────────────────────────────

        public override async Task DoGet(
            FlightTicket ticket,
            FlightServerRecordBatchStreamWriter responseStream,
            ServerCallContext context)
        {
            string sql = Encoding.UTF8.GetString(ticket.Ticket.ToArray());

            try
            {
                using (var handle = _readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        // Build Arrow schema from the DuckDB result columns.
                        var fields = new List<Field>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var clrType = reader.GetFieldType(i);
                            var arrowType = ClrToArrowType(clrType);
                            fields.Add(new Field(reader.GetName(i), arrowType, nullable: true));
                        }
                        var schema = new Schema(fields, null);

                        // Stream record batches — read up to 1024 rows per batch.
                        const int batchSize = 1024;

                        while (true)
                        {
                            var arrays = BuildBatchArrays(reader, schema, batchSize, out int rowCount);
                            if (rowCount == 0) break;

                            var batch = new RecordBatch(schema, arrays, rowCount);
                            await responseStream.WriteAsync(batch).ConfigureAwait(false);
                        }
                    }
                }

                Interlocked.Increment(ref _statQueriesRead);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _statErrors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        // ── DoAction — execute / ping / stats ────────────────────────────────

        public override async Task DoAction(
            FlightAction request,
            IAsyncStreamWriter<FlightResult> responseStream,
            ServerCallContext context)
        {
            string actionType = request.Type;

            if (actionType == "execute")
            {
                if (request.Body == null || request.Body.Length == 0)
                {
                    Interlocked.Increment(ref _statErrors);
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        "execute action requires a non-empty SQL body"));
                }

                string sql = Encoding.UTF8.GetString(request.Body.ToArray());
                if (string.IsNullOrEmpty(sql))
                {
                    Interlocked.Increment(ref _statErrors);
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        "execute action requires non-empty SQL"));
                }

                Interlocked.Increment(ref _statQueriesWrite);
                var wr = _writer.Submit(sql);
                if (!wr.Ok)
                {
                    Interlocked.Increment(ref _statErrors);
                    throw new RpcException(new Status(StatusCode.Internal, wr.Error));
                }

                // Success: empty result stream.
                return;
            }

            if (actionType == "ping")
            {
                await responseStream.WriteAsync(
                    new FlightResult(Encoding.UTF8.GetBytes("pong")))
                    .ConfigureAwait(false);
                return;
            }

            if (actionType == "stats")
            {
                var s = Stats();
                string json = string.Format(
                    "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                    s.QueriesRead, s.QueriesWrite, s.Errors, s.ReaderPoolSize, s.Port);

                await responseStream.WriteAsync(
                    new FlightResult(Encoding.UTF8.GetBytes(json)))
                    .ConfigureAwait(false);
                return;
            }

            throw new RpcException(new Status(StatusCode.Unimplemented,
                "Unknown action '" + actionType + "'. Supported: execute, ping, stats."));
        }

        // ── ListActions ──────────────────────────────────────────────────────

        public override Task<List<FlightActionType>> ListActions(ServerCallContext context)
        {
            var actions = new List<FlightActionType>
            {
                new FlightActionType("execute",
                    "Execute DML or DDL SQL. Action body = UTF-8 SQL. Returns empty stream on success."),
                new FlightActionType("ping",
                    "Liveness check. Returns a single result whose body is 'pong'."),
                new FlightActionType("stats",
                    "Live server metrics. Returns JSON with queries_read, queries_write, errors, reader_pool_size, port.")
            };
            return Task.FromResult(actions);
        }

        // ── Arrow type mapping ───────────────────────────────────────────────

        private static IArrowType ClrToArrowType(Type clrType)
        {
            if (clrType == typeof(bool))           return BooleanType.Default;
            if (clrType == typeof(sbyte))          return Int8Type.Default;
            if (clrType == typeof(short))          return Int16Type.Default;
            if (clrType == typeof(int))            return Int32Type.Default;
            if (clrType == typeof(long))           return Int64Type.Default;
            if (clrType == typeof(byte))           return UInt8Type.Default;
            if (clrType == typeof(ushort))         return UInt16Type.Default;
            if (clrType == typeof(uint))           return UInt32Type.Default;
            if (clrType == typeof(ulong))          return UInt64Type.Default;
            if (clrType == typeof(float))          return FloatType.Default;
            if (clrType == typeof(double))         return DoubleType.Default;
            if (clrType == typeof(decimal))        return DoubleType.Default; // approximate
            if (clrType == typeof(string))         return StringType.Default;
            if (clrType == typeof(byte[]))         return BinaryType.Default;
            if (clrType == typeof(DateTime))       return TimestampType.Default;
            if (clrType == typeof(DateTimeOffset)) return TimestampType.Default;
            if (clrType == typeof(TimeSpan))       return Int64Type.Default; // ticks
            if (clrType == typeof(Guid))           return StringType.Default;
            return StringType.Default; // fallback: toString
        }

        // ── Batch building ───────────────────────────────────────────────────

        private static IArrowArray[] BuildBatchArrays(
            System.Data.IDataReader reader, Schema schema, int maxRows, out int rowCount)
        {
            int colCount = schema.FieldsList.Count;
            rowCount = 0;

            // Accumulate rows into lists, then build Arrow arrays.
            var columns = new List<object>[colCount];
            for (int c = 0; c < colCount; c++)
                columns[c] = new List<object>(maxRows);

            while (rowCount < maxRows && reader.Read())
            {
                for (int c = 0; c < colCount; c++)
                {
                    object val = reader.IsDBNull(c) ? null : reader.GetValue(c);
                    columns[c].Add(val);
                }
                rowCount++;
            }

            if (rowCount == 0) return null;

            var arrays = new IArrowArray[colCount];
            for (int c = 0; c < colCount; c++)
                arrays[c] = BuildArrowArray(schema.GetFieldByIndex(c).DataType, columns[c]);

            return arrays;
        }

        private static IArrowArray BuildArrowArray(IArrowType type, List<object> values)
        {
            switch (type.TypeId)
            {
                case ArrowTypeId.Boolean:
                {
                    var builder = new BooleanArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToBoolean(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Int8:
                {
                    var builder = new Int8Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToSByte(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Int16:
                {
                    var builder = new Int16Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToInt16(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToInt32(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToInt64(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.UInt8:
                {
                    var builder = new UInt8Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToByte(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.UInt16:
                {
                    var builder = new UInt16Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToUInt16(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.UInt32:
                {
                    var builder = new UInt32Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToUInt32(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.UInt64:
                {
                    var builder = new UInt64Array.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToUInt64(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToSingle(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(Convert.ToDouble(v));
                    }
                    return builder.Build();
                }
                case ArrowTypeId.String:
                {
                    var builder = new StringArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(v.ToString());
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Binary:
                {
                    var builder = new BinaryArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append((byte[])v);
                    }
                    return builder.Build();
                }
                case ArrowTypeId.Timestamp:
                {
                    var builder = new TimestampArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else if (v is DateTimeOffset dto) builder.Append(dto);
                        else builder.Append(new DateTimeOffset(Convert.ToDateTime(v), TimeSpan.Zero));
                    }
                    return builder.Build();
                }
                default:
                {
                    // Fallback: convert everything to string.
                    var builder = new StringArray.Builder();
                    foreach (var v in values)
                    {
                        if (v == null) builder.AppendNull();
                        else builder.Append(v.ToString());
                    }
                    return builder.Build();
                }
            }
        }

        private static string BuildConnectionString(string dbPath)
        {
            if (string.IsNullOrEmpty(dbPath) || dbPath == ":memory:")
                return "Data Source=:memory:";
            return "Data Source=" + dbPath;
        }

        public void Dispose()
        {
            _writer.Dispose();
            _readPool.Dispose();
        }
    }
}
