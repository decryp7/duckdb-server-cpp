using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DuckArrowServer
{
    /// <summary>
    /// Converts between CLR types and Apache Arrow types.
    /// Also builds Arrow arrays from lists of CLR values.
    ///
    /// This is a helper class used by DuckFlightServer to convert
    /// DuckDB query results into Arrow record batches for streaming.
    /// </summary>
    public static class ArrowTypeConverter
    {
        /// <summary>
        /// Convert a CLR type (e.g. typeof(int)) to the matching Arrow type.
        /// Used when building the Arrow schema from DuckDB column types.
        /// </summary>
        public static IArrowType FromClrType(Type clrType)
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
            if (clrType == typeof(decimal))        return DoubleType.Default;
            if (clrType == typeof(string))         return StringType.Default;
            if (clrType == typeof(byte[]))         return BinaryType.Default;
            if (clrType == typeof(DateTime))       return TimestampType.Default;
            if (clrType == typeof(DateTimeOffset)) return TimestampType.Default;
            if (clrType == typeof(TimeSpan))       return Int64Type.Default;
            if (clrType == typeof(Guid))           return StringType.Default;
            return StringType.Default; // fallback: everything becomes a string
        }

        /// <summary>
        /// Build an Arrow array from a list of CLR values.
        /// The array type matches the given Arrow type.
        /// Null values in the list become Arrow nulls.
        /// </summary>
        public static IArrowArray BuildArray(IArrowType type, List<object> values)
        {
            switch (type.TypeId)
            {
                case ArrowTypeId.Boolean:   return BuildBooleanArray(values);
                case ArrowTypeId.Int8:      return BuildInt8Array(values);
                case ArrowTypeId.Int16:     return BuildInt16Array(values);
                case ArrowTypeId.Int32:     return BuildInt32Array(values);
                case ArrowTypeId.Int64:     return BuildInt64Array(values);
                case ArrowTypeId.UInt8:     return BuildUInt8Array(values);
                case ArrowTypeId.UInt16:    return BuildUInt16Array(values);
                case ArrowTypeId.UInt32:    return BuildUInt32Array(values);
                case ArrowTypeId.UInt64:    return BuildUInt64Array(values);
                case ArrowTypeId.Float:     return BuildFloatArray(values);
                case ArrowTypeId.Double:    return BuildDoubleArray(values);
                case ArrowTypeId.Binary:    return BuildBinaryArray(values);
                case ArrowTypeId.Timestamp: return BuildTimestampArray(values);
                default:                    return BuildStringArray(values);
            }
        }

        // ── Array builders (one per Arrow type) ──────────────────────────────
        // Each method creates an Arrow array from a list of boxed CLR values.
        // Pattern: loop through values, append null or converted value.

        private static IArrowArray BuildBooleanArray(List<object> values)
        {
            var b = new BooleanArray.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToBoolean(v));
            return b.Build();
        }

        private static IArrowArray BuildInt8Array(List<object> values)
        {
            var b = new Int8Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToSByte(v));
            return b.Build();
        }

        private static IArrowArray BuildInt16Array(List<object> values)
        {
            var b = new Int16Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToInt16(v));
            return b.Build();
        }

        private static IArrowArray BuildInt32Array(List<object> values)
        {
            var b = new Int32Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToInt32(v));
            return b.Build();
        }

        private static IArrowArray BuildInt64Array(List<object> values)
        {
            var b = new Int64Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToInt64(v));
            return b.Build();
        }

        private static IArrowArray BuildUInt8Array(List<object> values)
        {
            var b = new UInt8Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToByte(v));
            return b.Build();
        }

        private static IArrowArray BuildUInt16Array(List<object> values)
        {
            var b = new UInt16Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToUInt16(v));
            return b.Build();
        }

        private static IArrowArray BuildUInt32Array(List<object> values)
        {
            var b = new UInt32Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToUInt32(v));
            return b.Build();
        }

        private static IArrowArray BuildUInt64Array(List<object> values)
        {
            var b = new UInt64Array.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToUInt64(v));
            return b.Build();
        }

        private static IArrowArray BuildFloatArray(List<object> values)
        {
            var b = new FloatArray.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToSingle(v));
            return b.Build();
        }

        private static IArrowArray BuildDoubleArray(List<object> values)
        {
            var b = new DoubleArray.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(Convert.ToDouble(v));
            return b.Build();
        }

        private static IArrowArray BuildStringArray(List<object> values)
        {
            var b = new StringArray.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(v.ToString());
            return b.Build();
        }

        private static IArrowArray BuildBinaryArray(List<object> values)
        {
            var b = new BinaryArray.Builder();
            foreach (var v in values)
                if (v == null) b.AppendNull(); else b.Append(new ReadOnlySpan<byte>((byte[])v));
            return b.Build();
        }

        private static IArrowArray BuildTimestampArray(List<object> values)
        {
            var b = new TimestampArray.Builder();
            foreach (var v in values)
            {
                if (v == null)
                    b.AppendNull();
                else if (v is DateTimeOffset dto)
                    b.Append(dto);
                else
                    b.Append(new DateTimeOffset(Convert.ToDateTime(v), TimeSpan.Zero));
            }
            return b.Build();
        }
    }
}
