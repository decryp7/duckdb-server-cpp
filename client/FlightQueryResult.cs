using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DuckArrowClient
{
    /// <summary>
    /// Concrete implementation of <see cref="IFlightQueryResult"/>.
    /// Holds the Arrow schema and all record batches returned by a DoGet call.
    /// Provides ToRows() for row-dict access and ToDataTable() for ADO.NET binding.
    /// Value boxing delegates to <see cref="ArrowValueConverter.Box"/>.
    /// </summary>
    public sealed class FlightQueryResult : IFlightQueryResult
    {
        private readonly List<RecordBatch> _batches;
        private int _disposed; // 0 = alive, 1 = disposed; use Interlocked

        /// <inheritdoc/>
        public Schema Schema { get; }

        /// <inheritdoc/>
        public int RowCount { get; }

        /// <summary>Internal constructor used by DasFlightClient and unit tests.</summary>
        internal FlightQueryResult(Schema schema, List<RecordBatch> batches)
        {
            Schema   = schema;
            _batches = batches;
            int total = 0;
            foreach (var b in batches) total += b.Length;
            RowCount = total;
        }

        /// <inheritdoc/>
        public IReadOnlyList<RecordBatch> Batches => _batches.AsReadOnly();

        /// <inheritdoc/>
        public List<Dictionary<string, object>> ToRows()
        {
            int colCount = Schema.FieldsList.Count;
            var rows = new List<Dictionary<string, object>>(RowCount);
            foreach (var batch in _batches)
            {
                for (int row = 0; row < batch.Length; row++)
                {
                    var dict = new Dictionary<string, object>(colCount);
                    for (int col = 0; col < colCount; col++)
                        dict[Schema.GetFieldByIndex(col).Name] =
                            ArrowValueConverter.Box(batch.Column(col), row);
                    rows.Add(dict);
                }
            }
            return rows;
        }

        /// <inheritdoc/>
        public DataTable ToDataTable(string tableName = "Result")
        {
            var dt = new DataTable(tableName);
            int colCount = Schema.FieldsList.Count;
            foreach (var field in Schema.FieldsList)
                dt.Columns.Add(field.Name, ArrowTypeMapper.ToClrType(field.DataType));
            foreach (var batch in _batches)
            {
                for (int row = 0; row < batch.Length; row++)
                {
                    var dr = dt.NewRow();
                    for (int col = 0; col < colCount; col++)
                    {
                        object v = ArrowValueConverter.Box(batch.Column(col), row);
                        dr[col] = v ?? (object)DBNull.Value;
                    }
                    dt.Rows.Add(dr);
                }
            }
            return dt;
        }

        /// <summary>Disposes all underlying Arrow record batches.</summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
            foreach (var b in _batches) b?.Dispose();
        }
    }

    /// <summary>
    /// Maps Arrow type IDs to CLR types for ADO.NET DataTable column creation.
    /// Shared between FlightQueryResult and DataTableExtensions.
    /// </summary>
    internal static class ArrowTypeMapper
    {
        /// <summary>Return the most appropriate CLR type for an Arrow type.</summary>
        public static Type ToClrType(IArrowType t)
        {
            switch (t.TypeId)
            {
                case ArrowTypeId.Boolean:   return typeof(bool);
                case ArrowTypeId.Int8:      return typeof(sbyte);
                case ArrowTypeId.Int16:     return typeof(short);
                case ArrowTypeId.Int32:     return typeof(int);
                case ArrowTypeId.Int64:     return typeof(long);
                case ArrowTypeId.UInt8:     return typeof(byte);
                case ArrowTypeId.UInt16:    return typeof(ushort);
                case ArrowTypeId.UInt32:    return typeof(uint);
                case ArrowTypeId.UInt64:    return typeof(ulong);
                case ArrowTypeId.Float:     return typeof(float);
                case ArrowTypeId.Double:    return typeof(double);
                case ArrowTypeId.String:    return typeof(string);
                case ArrowTypeId.Binary:    return typeof(byte[]);
                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64:
                case ArrowTypeId.Timestamp: return typeof(DateTimeOffset);
                default:                    return typeof(object);
            }
        }
    }
}
