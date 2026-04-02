using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDbProto;
using Grpc.Core;

namespace DuckDbServer
{
    /// <summary>
    /// DuckDB gRPC server — columnar encoding for extreme performance.
    ///
    /// Values are packed into typed arrays per column (not per-cell objects).
    /// 1000 rows × 10 columns = 10 ColumnData objects instead of 11,000 TypedValue objects.
    /// Packed repeated fields use minimal protobuf overhead.
    /// </summary>
    public sealed class DuckDbServer : DuckDbService.DuckDbServiceBase, IDisposable
    {
        private readonly ServerConfig config;
        private readonly IConnectionPool readPool;
        private readonly IWriteSerializer writer;

        private long queriesRead;
        private long queriesWrite;
        private long errors;

        private readonly int batchSize;

        public DuckDbServer(ServerConfig config, IConnectionPool readPool, IWriteSerializer writer)
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

        // ── Query: columnar streaming ────────────────────────────────────────

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

                        // Cache column types
                        var colTypes = new ColumnType[colCount];
                        var clrTypes = new Type[colCount];
                        var meta = new List<ColumnMeta>(colCount);

                        for (int c = 0; c < colCount; c++)
                        {
                            clrTypes[c] = reader.GetFieldType(c);
                            colTypes[c] = MapColumnType(clrTypes[c]);
                            meta.Add(new ColumnMeta
                            {
                                Name = reader.GetName(c),
                                Type = colTypes[c]
                            });
                        }

                        // Pre-allocate column builders
                        var builders = new ColumnBuilder[colCount];
                        for (int c = 0; c < colCount; c++)
                            builders[c] = new ColumnBuilder(colTypes[c], batchSize);

                        var values = new object[colCount];
                        int rowsInBatch = 0;
                        bool firstBatch = true;

                        while (reader.Read())
                        {
                            // Check client cancellation to avoid wasted work
                            if (context.CancellationToken.IsCancellationRequested)
                                return;

                            reader.GetValues(values);

                            for (int c = 0; c < colCount; c++)
                                builders[c].Add(values[c], rowsInBatch);

                            rowsInBatch++;

                            if (rowsInBatch >= batchSize)
                            {
                                await SendBatch(responseStream, builders, colCount, rowsInBatch,
                                    firstBatch ? meta : null).ConfigureAwait(false);
                                firstBatch = false;
                                rowsInBatch = 0;
                                for (int c = 0; c < colCount; c++)
                                    builders[c].Reset();
                            }
                        }

                        if (rowsInBatch > 0 || firstBatch)
                        {
                            await SendBatch(responseStream, builders, colCount, rowsInBatch,
                                firstBatch ? meta : null).ConfigureAwait(false);
                        }
                    }
                }

                Interlocked.Increment(ref queriesRead);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }

        private static async Task SendBatch(
            IServerStreamWriter<QueryResponse> stream,
            ColumnBuilder[] builders, int colCount, int rowCount,
            List<ColumnMeta> meta)
        {
            var response = new QueryResponse { RowCount = rowCount };

            if (meta != null)
                response.Columns.AddRange(meta);

            for (int c = 0; c < colCount; c++)
                response.Data.Add(builders[c].Build());

            await stream.WriteAsync(response).ConfigureAwait(false);
        }

        // ── Column builder: accumulates values into packed arrays ─────────────

        private sealed class ColumnBuilder
        {
            private readonly ColumnType type;
            private List<bool> bools;
            private List<int> int32s;
            private List<long> int64s;
            private List<float> floats;
            private List<double> doubles;
            private List<string> strings;
            private List<int> nullIndices;

            public ColumnBuilder(ColumnType type, int capacity)
            {
                this.type = type;
                AllocateStorage(capacity);
            }

            public void Add(object value, int rowIndex)
            {
                if (value == null || value is DBNull)
                {
                    if (nullIndices == null)
                        nullIndices = new List<int>();
                    nullIndices.Add(rowIndex);
                    AddDefault();
                    return;
                }

                switch (type)
                {
                    case ColumnType.TypeBoolean:
                        bools.Add(Convert.ToBoolean(value));
                        break;
                    case ColumnType.TypeInt32:
                    case ColumnType.TypeInt8:
                    case ColumnType.TypeInt16:
                    case ColumnType.TypeUint8:
                    case ColumnType.TypeUint16:
                        int32s.Add(Convert.ToInt32(value));
                        break;
                    case ColumnType.TypeInt64:
                    case ColumnType.TypeUint32:
                    case ColumnType.TypeUint64:
                        int64s.Add(Convert.ToInt64(value));
                        break;
                    case ColumnType.TypeFloat:
                        floats.Add(Convert.ToSingle(value));
                        break;
                    case ColumnType.TypeDouble:
                    case ColumnType.TypeDecimal:
                        doubles.Add(Convert.ToDouble(value));
                        break;
                    default:
                        strings.Add(value.ToString());
                        break;
                }
            }

            private void AddDefault()
            {
                switch (type)
                {
                    case ColumnType.TypeBoolean: bools.Add(false); break;
                    case ColumnType.TypeInt32:
                    case ColumnType.TypeInt8:
                    case ColumnType.TypeInt16:
                    case ColumnType.TypeUint8:
                    case ColumnType.TypeUint16: int32s.Add(0); break;
                    case ColumnType.TypeInt64:
                    case ColumnType.TypeUint32:
                    case ColumnType.TypeUint64: int64s.Add(0); break;
                    case ColumnType.TypeFloat: floats.Add(0); break;
                    case ColumnType.TypeDouble:
                    case ColumnType.TypeDecimal: doubles.Add(0); break;
                    default: strings.Add(""); break;
                }
            }

            public ColumnData Build()
            {
                var cd = new ColumnData();
                switch (type)
                {
                    case ColumnType.TypeBoolean: cd.BoolValues.AddRange(bools); break;
                    case ColumnType.TypeInt32:
                    case ColumnType.TypeInt8:
                    case ColumnType.TypeInt16:
                    case ColumnType.TypeUint8:
                    case ColumnType.TypeUint16: cd.Int32Values.AddRange(int32s); break;
                    case ColumnType.TypeInt64:
                    case ColumnType.TypeUint32:
                    case ColumnType.TypeUint64: cd.Int64Values.AddRange(int64s); break;
                    case ColumnType.TypeFloat: cd.FloatValues.AddRange(floats); break;
                    case ColumnType.TypeDouble:
                    case ColumnType.TypeDecimal: cd.DoubleValues.AddRange(doubles); break;
                    default: cd.StringValues.AddRange(strings); break;
                }
                if (nullIndices != null)
                    cd.NullIndices.AddRange(nullIndices);
                return cd;
            }

            public void Reset()
            {
                switch (type)
                {
                    case ColumnType.TypeBoolean: bools.Clear(); break;
                    case ColumnType.TypeInt32:
                    case ColumnType.TypeInt8:
                    case ColumnType.TypeInt16:
                    case ColumnType.TypeUint8:
                    case ColumnType.TypeUint16: int32s.Clear(); break;
                    case ColumnType.TypeInt64:
                    case ColumnType.TypeUint32:
                    case ColumnType.TypeUint64: int64s.Clear(); break;
                    case ColumnType.TypeFloat: floats.Clear(); break;
                    case ColumnType.TypeDouble:
                    case ColumnType.TypeDecimal: doubles.Clear(); break;
                    default: strings.Clear(); break;
                }
                if (nullIndices != null) nullIndices.Clear();
            }

            private void AllocateStorage(int capacity)
            {
                switch (type)
                {
                    case ColumnType.TypeBoolean: bools = new List<bool>(capacity); break;
                    case ColumnType.TypeInt32:
                    case ColumnType.TypeInt8:
                    case ColumnType.TypeInt16:
                    case ColumnType.TypeUint8:
                    case ColumnType.TypeUint16: int32s = new List<int>(capacity); break;
                    case ColumnType.TypeInt64:
                    case ColumnType.TypeUint32:
                    case ColumnType.TypeUint64: int64s = new List<long>(capacity); break;
                    case ColumnType.TypeFloat: floats = new List<float>(capacity); break;
                    case ColumnType.TypeDouble:
                    case ColumnType.TypeDecimal: doubles = new List<double>(capacity); break;
                    default: strings = new List<string>(capacity); break;
                }
            }
        }

        private static ColumnType MapColumnType(Type clrType)
        {
            if (clrType == typeof(bool))    return ColumnType.TypeBoolean;
            if (clrType == typeof(sbyte))   return ColumnType.TypeInt8;
            if (clrType == typeof(short))   return ColumnType.TypeInt16;
            if (clrType == typeof(int))     return ColumnType.TypeInt32;
            if (clrType == typeof(long))    return ColumnType.TypeInt64;
            if (clrType == typeof(byte))    return ColumnType.TypeUint8;
            if (clrType == typeof(ushort))  return ColumnType.TypeUint16;
            if (clrType == typeof(uint))    return ColumnType.TypeUint32;
            if (clrType == typeof(ulong))   return ColumnType.TypeUint64;
            if (clrType == typeof(float))   return ColumnType.TypeFloat;
            if (clrType == typeof(double))  return ColumnType.TypeDouble;
            if (clrType == typeof(decimal)) return ColumnType.TypeDecimal;
            if (clrType == typeof(byte[]))  return ColumnType.TypeBlob;
            return ColumnType.TypeString;
        }

        // ── Execute / Ping / GetStats ────────────────────────────────────────

        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            string sql = request.Sql;
            if (string.IsNullOrEmpty(sql))
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new ExecuteResponse { Success = false, Error = "SQL required" });
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
                QueriesRead = s.QueriesRead, QueriesWrite = s.QueriesWrite,
                Errors = s.Errors, ReaderPoolSize = s.ReaderPoolSize, Port = s.Port
            });
        }

        // ── BulkInsert: Appender API (100x faster than INSERT SQL) ────────

        public override Task<BulkInsertResponse> BulkInsert(
            BulkInsertRequest request, ServerCallContext context)
        {
            string table = request.Table;
            int rowCount = request.RowCount;

            if (string.IsNullOrEmpty(table) || rowCount == 0 || request.Columns.Count == 0)
                return Task.FromResult(new BulkInsertResponse
                {
                    Success = false,
                    Error = "table, columns, and row_count are required"
                });

            try
            {
                int colCount = request.Columns.Count;

                // Build INSERT using parameterized values (DuckDB.NET doesn't expose Appender)
                // This is still much faster than individual INSERTs because we batch into
                // one multi-row INSERT statement.
                var sb = new System.Text.StringBuilder(rowCount * colCount * 10);

                for (int r = 0; r < rowCount; r++)
                {
                    if (r == 0)
                        sb.AppendFormat("INSERT INTO {0} VALUES ", table);
                    else
                        sb.Append(", ");

                    sb.Append("(");
                    for (int c = 0; c < colCount; c++)
                    {
                        if (c > 0) sb.Append(", ");

                        var cd = request.Data[c];
                        bool isNull = false;
                        for (int n = 0; n < cd.NullIndices.Count; n++)
                            if (cd.NullIndices[n] == r) { isNull = true; break; }

                        if (isNull)
                        {
                            sb.Append("NULL");
                            continue;
                        }

                        switch (request.Columns[c].Type)
                        {
                            case ColumnType.TypeBoolean:
                                sb.Append(r < cd.BoolValues.Count && cd.BoolValues[r] ? "true" : "false");
                                break;
                            case ColumnType.TypeInt32:
                            case ColumnType.TypeInt8:
                            case ColumnType.TypeInt16:
                            case ColumnType.TypeUint8:
                            case ColumnType.TypeUint16:
                                sb.Append(r < cd.Int32Values.Count ? cd.Int32Values[r] : 0);
                                break;
                            case ColumnType.TypeInt64:
                            case ColumnType.TypeUint32:
                            case ColumnType.TypeUint64:
                                sb.Append(r < cd.Int64Values.Count ? cd.Int64Values[r] : 0);
                                break;
                            case ColumnType.TypeFloat:
                                sb.Append((r < cd.FloatValues.Count ? cd.FloatValues[r] : 0f)
                                    .ToString(System.Globalization.CultureInfo.InvariantCulture));
                                break;
                            case ColumnType.TypeDouble:
                            case ColumnType.TypeDecimal:
                                sb.Append((r < cd.DoubleValues.Count ? cd.DoubleValues[r] : 0d)
                                    .ToString(System.Globalization.CultureInfo.InvariantCulture));
                                break;
                            default:
                                sb.Append("'");
                                string val = r < cd.StringValues.Count ? cd.StringValues[r] : "";
                                sb.Append(val.Replace("'", "''"));
                                sb.Append("'");
                                break;
                        }
                    }
                    sb.Append(")");
                }

                var result = writer.Submit(sb.ToString());
                if (!result.Ok)
                {
                    Interlocked.Increment(ref errors);
                    return Task.FromResult(new BulkInsertResponse
                    {
                        Success = false,
                        Error = result.Error
                    });
                }

                Interlocked.Increment(ref queriesWrite);
                return Task.FromResult(new BulkInsertResponse
                {
                    Success = true,
                    RowsInserted = rowCount
                });
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                return Task.FromResult(new BulkInsertResponse
                {
                    Success = false,
                    Error = ex.Message
                });
            }
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
