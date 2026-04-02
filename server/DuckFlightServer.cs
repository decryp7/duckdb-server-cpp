using System;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckArrowServer
{
    /// <summary>
    /// DuckDB gRPC server — extreme performance edition.
    ///
    /// Key optimizations:
    ///   - Typed values (int64/double/bool) skip ToString/Parse entirely
    ///   - GetValues() bulk reads all columns in one call
    ///   - Column type mapping cached per query (not per row)
    ///   - Shared null TypedValue instance (no allocation per null cell)
    /// </summary>
    public sealed class DuckFlightServer : DuckDbService.DuckDbServiceBase, IDisposable
    {
        private readonly ServerConfig config;
        private readonly IConnectionPool readPool;
        private readonly IWriteSerializer writer;

        private long queriesRead;
        private long queriesWrite;
        private long errors;

        private readonly int batchSize;

        // Shared null value — reused across all rows to avoid allocation.
        private static readonly TypedValue NullValue = new TypedValue { IsNull = true };

        public DuckFlightServer(ServerConfig config, IConnectionPool readPool, IWriteSerializer writer)
        {
            this.config = config;
            this.readPool = readPool;
            this.writer = writer;
            this.batchSize = config.BatchSize > 0 ? config.BatchSize : 8192;
        }

        public ServerServiceDefinition BuildGrpcService()
        {
            return DuckDbService.BindService(this);
        }

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

        // ── Query: stream typed rows ─────────────────────────────────────────

        public override async Task Query(
            QueryRequest request,
            IServerStreamWriter<QueryResponse> responseStream,
            ServerCallContext context)
        {
            string sql = request.Sql ?? "";

            try
            {
                using (var handle = readPool.Borrow())
                using (var cmd = handle.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var reader = cmd.ExecuteReader())
                    {
                        int colCount = reader.FieldCount;

                        // Map CLR types to proto ColumnType (cached for all rows).
                        var colTypes = new ColumnType[colCount];
                        var clrTypes = new Type[colCount];
                        var schemaResponse = new QueryResponse();

                        for (int c = 0; c < colCount; c++)
                        {
                            clrTypes[c] = reader.GetFieldType(c);
                            colTypes[c] = MapColumnType(clrTypes[c]);
                            schemaResponse.Columns.Add(new ColumnInfo
                            {
                                Name = reader.GetName(c),
                                Type = colTypes[c]
                            });
                        }
                        await responseStream.WriteAsync(schemaResponse).ConfigureAwait(false);

                        // Stream rows with typed values — no ToString overhead.
                        var batch = new QueryResponse();
                        int rowsInBatch = 0;
                        var values = new object[colCount];

                        while (reader.Read())
                        {
                            reader.GetValues(values);

                            var row = new Row();
                            for (int c = 0; c < colCount; c++)
                            {
                                row.Values.Add(ConvertToTypedValue(values[c], colTypes[c]));
                            }
                            batch.Rows.Add(row);
                            rowsInBatch++;

                            if (rowsInBatch >= batchSize)
                            {
                                await responseStream.WriteAsync(batch).ConfigureAwait(false);
                                batch = new QueryResponse();
                                rowsInBatch = 0;
                            }
                        }

                        if (rowsInBatch > 0)
                        {
                            await responseStream.WriteAsync(batch).ConfigureAwait(false);
                        }
                    }
                }

                Interlocked.Increment(ref queriesRead);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        // ── Type mapping ─────────────────────────────────────────────────────

        /// <summary>
        /// Convert a CLR value to a TypedValue without ToString().
        /// int/long/double/bool are encoded as native protobuf types (binary).
        /// Only strings and unknown types use ToString() as fallback.
        /// </summary>
        private static TypedValue ConvertToTypedValue(object value, ColumnType colType)
        {
            if (value == null || value is DBNull)
                return NullValue;

            switch (colType)
            {
                case ColumnType.TypeBoolean:
                    return new TypedValue { BoolValue = Convert.ToBoolean(value) };

                case ColumnType.TypeInt32:
                case ColumnType.TypeInt8:
                case ColumnType.TypeInt16:
                case ColumnType.TypeUint8:
                case ColumnType.TypeUint16:
                    return new TypedValue { Int32Value = Convert.ToInt32(value) };

                case ColumnType.TypeInt64:
                case ColumnType.TypeUint32:
                case ColumnType.TypeUint64:
                    return new TypedValue { Int64Value = Convert.ToInt64(value) };

                case ColumnType.TypeFloat:
                case ColumnType.TypeDouble:
                case ColumnType.TypeDecimal:
                    return new TypedValue { DoubleValue = Convert.ToDouble(value) };

                case ColumnType.TypeBlob:
                    if (value is byte[] bytes)
                        return new TypedValue { BlobValue = Google.Protobuf.ByteString.CopyFrom(bytes) };
                    return new TypedValue { StringValue = value.ToString() };

                default:
                    // String, timestamp, date, time, unknown — use ToString as fallback.
                    return new TypedValue { StringValue = value.ToString() };
            }
        }

        /// <summary>
        /// Map CLR type to proto ColumnType. Called once per column per query.
        /// </summary>
        private static ColumnType MapColumnType(Type clrType)
        {
            if (clrType == typeof(bool))           return ColumnType.TypeBoolean;
            if (clrType == typeof(sbyte))          return ColumnType.TypeInt8;
            if (clrType == typeof(short))          return ColumnType.TypeInt16;
            if (clrType == typeof(int))            return ColumnType.TypeInt32;
            if (clrType == typeof(long))           return ColumnType.TypeInt64;
            if (clrType == typeof(byte))           return ColumnType.TypeUint8;
            if (clrType == typeof(ushort))         return ColumnType.TypeUint16;
            if (clrType == typeof(uint))           return ColumnType.TypeUint32;
            if (clrType == typeof(ulong))          return ColumnType.TypeUint64;
            if (clrType == typeof(float))          return ColumnType.TypeFloat;
            if (clrType == typeof(double))         return ColumnType.TypeDouble;
            if (clrType == typeof(decimal))        return ColumnType.TypeDecimal;
            if (clrType == typeof(string))         return ColumnType.TypeString;
            if (clrType == typeof(byte[]))         return ColumnType.TypeBlob;
            if (clrType == typeof(DateTime))       return ColumnType.TypeTimestamp;
            if (clrType == typeof(DateTimeOffset)) return ColumnType.TypeTimestamp;
            if (clrType == typeof(TimeSpan))       return ColumnType.TypeTime;
            return ColumnType.TypeString; // fallback
        }

        // ── Execute / Ping / GetStats ────────────────────────────────────────

        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;
            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = "SQL statement is required" });
            }

            var result = writer.Submit(sql);
            if (!result.Ok)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = result.Error });
            }

            Interlocked.Increment(ref queriesWrite);
            return Task.FromResult(new ExecuteResponse { Success = true });
        }

        public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new PingResponse { Message = "pong" });
        }

        public override Task<StatsResponse> GetStats(StatsRequest request, ServerCallContext context)
        {
            var s = GetStats();
            return Task.FromResult(new StatsResponse
            {
                QueriesRead = s.QueriesRead,
                QueriesWrite = s.QueriesWrite,
                Errors = s.Errors,
                ReaderPoolSize = s.ReaderPoolSize,
                Port = s.Port
            });
        }

        private int disposed;

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) return;
            writer.Dispose();
            readPool.Dispose();
        }
    }
}
