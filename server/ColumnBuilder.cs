using System;
using System.Collections.Generic;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Accumulates column values into packed protobuf arrays.
    /// One builder per column per batch. Call Add() per row, Build() to get ColumnData.
    ///
    /// Why packed arrays are fast:
    ///   1000 rows × 10 columns = 10 ColumnData objects (not 10,000 per-cell objects)
    ///   Numeric arrays use contiguous memory (cache-friendly)
    ///   Protobuf packed encoding has minimal framing overhead
    /// </summary>
    public sealed class ColumnBuilder
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

        /// <summary>Add a value at the given row index. Null/DBNull → null entry.</summary>
        public void Add(object value, int rowIndex)
        {
            if (value == null || value is DBNull)
            {
                if (nullIndices == null) nullIndices = new List<int>();
                nullIndices.Add(rowIndex);
                AddDefault();
                return;
            }

            switch (type)
            {
                case ColumnType.TypeBoolean:
                    bools.Add(Convert.ToBoolean(value)); break;
                case ColumnType.TypeInt32:
                case ColumnType.TypeInt8:
                case ColumnType.TypeInt16:
                case ColumnType.TypeUint8:
                case ColumnType.TypeUint16:
                    int32s.Add(Convert.ToInt32(value)); break;
                case ColumnType.TypeInt64:
                case ColumnType.TypeUint32:
                case ColumnType.TypeUint64:
                    int64s.Add(Convert.ToInt64(value)); break;
                case ColumnType.TypeFloat:
                    floats.Add(Convert.ToSingle(value)); break;
                case ColumnType.TypeDouble:
                case ColumnType.TypeDecimal:
                    doubles.Add(Convert.ToDouble(value)); break;
                default:
                    strings.Add(value.ToString()); break;
            }
        }

        /// <summary>Build the protobuf ColumnData from accumulated values.</summary>
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
            if (nullIndices != null) cd.NullIndices.AddRange(nullIndices);
            return cd;
        }

        /// <summary>Clear values for reuse in the next batch (keeps allocated memory).</summary>
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

    /// <summary>
    /// Maps CLR types to protobuf ColumnType enum values.
    /// Called once per column per query (not per row).
    /// </summary>
    public static class TypeMapper
    {
        public static ColumnType FromClrType(Type clrType)
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
    }
}
