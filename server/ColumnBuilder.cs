using System;
using System.Collections.Generic;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Accumulates column values from a DuckDB result set into packed protobuf arrays.
    /// One builder is created per column per batch. Values are added row-by-row via
    /// <see cref="Add"/>, then the final protobuf <see cref="ColumnData"/> is produced
    /// via <see cref="Build"/>.
    ///
    /// <para><b>Thread Safety:</b> NOT thread-safe. Each builder is used by a single
    /// query handler on a single thread. No synchronization is needed.</para>
    ///
    /// <para><b>Why packed arrays instead of per-cell objects?</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     1000 rows x 10 columns = 10 <see cref="ColumnData"/> objects (not 10,000
    ///     per-cell wrapper objects). This dramatically reduces GC pressure and object
    ///     header overhead.
    ///   </description></item>
    ///   <item><description>
    ///     Numeric arrays (int[], long[], double[]) use contiguous memory, which is
    ///     CPU-cache-friendly for both serialization and deserialization.
    ///   </description></item>
    ///   <item><description>
    ///     Protobuf packed encoding for numeric repeated fields has minimal framing
    ///     overhead (one tag + length prefix for the entire array, not per element).
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Type Mapping:</b></para>
    /// <para>DuckDB's integer types (INT8, INT16, UINT8, UINT16) are widened to INT32
    /// for protobuf transport because protobuf does not have 8-bit or 16-bit integer
    /// types. Similarly, UINT32 and UINT64 are stored as INT64. DECIMAL is stored as
    /// DOUBLE (lossy for very large or very precise decimals). All other types (DATE,
    /// TIME, UUID, etc.) fall through to STRING.</para>
    ///
    /// <para><b>NULL handling:</b> NULL values are tracked via a separate
    /// <c>nullIndices</c> list that records the row index of each NULL. A default
    /// placeholder value (0, false, or "") is added to the typed array at that position
    /// to maintain array alignment (every array has exactly <c>rowCount</c> elements).
    /// The client checks <c>NullIndices</c> to distinguish NULLs from real values.</para>
    /// </summary>
    public sealed class ColumnBuilder
    {
        /// <summary>The protobuf column type. Determines which internal list is used.</summary>
        private readonly ColumnType type;

        // Only one of these lists is allocated, based on the column type.
        // The others remain null to save memory.
        private List<bool> bools;
        private List<int> int32s;
        private List<long> int64s;
        private List<float> floats;
        private List<double> doubles;
        private List<string> strings;

        /// <summary>
        /// Row indices where the value is NULL. Lazily allocated (many columns have
        /// no NULLs, so this saves a List allocation per column in the common case).
        /// </summary>
        private List<int> nullIndices;

        /// <summary>
        /// Creates a new column builder for the specified type, pre-allocating storage
        /// for <paramref name="capacity"/> rows to avoid resizing during accumulation.
        /// </summary>
        /// <param name="type">The protobuf column type (determines which internal list to use).</param>
        /// <param name="capacity">Expected number of rows per batch (used for List pre-allocation).</param>
        public ColumnBuilder(ColumnType type, int capacity)
        {
            this.type = type;
            AllocateStorage(capacity);
        }

        /// <summary>
        /// Adds a value at the given row index. If the value is <c>null</c> or
        /// <see cref="DBNull"/>, records the row index in <see cref="nullIndices"/>
        /// and appends a type-appropriate default placeholder to maintain array alignment.
        /// </summary>
        /// <param name="value">
        /// The value from <c>DataReader.GetValues()</c>. May be any CLR type that
        /// <see cref="Convert"/> can handle, <c>null</c>, or <see cref="DBNull.Value"/>.
        /// </param>
        /// <param name="rowIndex">
        /// The zero-based row index within the current batch. Used for NULL tracking.
        /// </param>
        /// <remarks>
        /// The <see cref="Convert"/> methods (ToInt32, ToDouble, etc.) handle implicit
        /// widening (e.g., byte to int, float to double). If the CLR type does not match
        /// the expected protobuf type, <see cref="Convert"/> throws <see cref="InvalidCastException"/>
        /// or <see cref="FormatException"/>. This is intentional: type mismatches indicate
        /// a bug in the <see cref="TypeMapper"/>.
        /// </remarks>
        public void Add(object value, int rowIndex)
        {
            if (value == null || value is DBNull)
            {
                // Track this row as NULL and add a placeholder to keep arrays aligned.
                if (nullIndices == null) nullIndices = new List<int>();
                nullIndices.Add(rowIndex);
                AddDefault();
                return;
            }

            // Dispatch to the appropriate typed list based on the column type.
            switch (type)
            {
                case ColumnType.TypeBoolean:
                    bools.Add(Convert.ToBoolean(value)); break;
                case ColumnType.TypeInt32:
                case ColumnType.TypeInt8:
                case ColumnType.TypeInt16:
                case ColumnType.TypeUint8:
                case ColumnType.TypeUint16:
                    // All small integer types are widened to int32 for protobuf transport.
                    int32s.Add(Convert.ToInt32(value)); break;
                case ColumnType.TypeInt64:
                case ColumnType.TypeUint32:
                case ColumnType.TypeUint64:
                    // All large integer types use int64 for protobuf transport.
                    int64s.Add(Convert.ToInt64(value)); break;
                case ColumnType.TypeBlob:
                    // BLOB values stored as base64 string since ColumnBuilder uses string list.
                    // The proto has blob_values (bytes) but we encode as string for simplicity.
                    strings.Add(value is byte[] bytes ? System.Convert.ToBase64String(bytes) : value.ToString());
                    break;
                case ColumnType.TypeFloat:
                    floats.Add(Convert.ToSingle(value)); break;
                case ColumnType.TypeDouble:
                case ColumnType.TypeDecimal:
                    // DECIMAL is stored as DOUBLE (lossy for >15 significant digits).
                    doubles.Add(Convert.ToDouble(value)); break;
                default:
                    // All other types (STRING, DATE, TIME, UUID, BLOB, etc.) use ToString.
                    strings.Add(value.ToString()); break;
            }
        }

        /// <summary>
        /// Builds the protobuf <see cref="ColumnData"/> message from the accumulated values.
        /// Copies values from the internal list into the protobuf repeated field.
        /// Also appends <see cref="nullIndices"/> if any NULLs were recorded.
        /// </summary>
        /// <returns>
        /// A new <see cref="ColumnData"/> containing the packed array for this column.
        /// The internal lists are NOT cleared by this method; call <see cref="Reset"/>
        /// separately if reusing the builder.
        /// </returns>
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

        /// <summary>
        /// Clears accumulated values for reuse in the next batch. Uses <c>List.Clear()</c>,
        /// which sets the count to zero but keeps the internal array allocation. This avoids
        /// re-allocating memory for each batch, reducing GC pressure during streaming of
        /// large result sets.
        /// </summary>
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

        /// <summary>
        /// Appends a type-appropriate default value (0, false, or "") as a placeholder
        /// for a NULL row. This keeps the typed array aligned with the row count so that
        /// array index N always corresponds to row N.
        /// </summary>
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

        /// <summary>
        /// Allocates the appropriate typed list based on the column type. Only one list
        /// is allocated per builder instance; the others remain null to save memory.
        /// </summary>
        /// <param name="capacity">Pre-allocation capacity (expected rows per batch).</param>
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
    /// Maps CLR types (from <see cref="System.Data.IDataReader.GetFieldType"/>) to
    /// protobuf <see cref="ColumnType"/> enum values. Called once per column per query
    /// (not per row), so performance is not critical.
    ///
    /// <para><b>Thread Safety:</b> All methods are static and pure (no state), so this
    /// class is inherently thread-safe.</para>
    ///
    /// <para><b>Type Mapping Table:</b></para>
    /// <list type="table">
    ///   <listheader><term>CLR Type</term><description>ColumnType</description></listheader>
    ///   <item><term>bool</term><description>TypeBoolean</description></item>
    ///   <item><term>sbyte</term><description>TypeInt8</description></item>
    ///   <item><term>short</term><description>TypeInt16</description></item>
    ///   <item><term>int</term><description>TypeInt32</description></item>
    ///   <item><term>long</term><description>TypeInt64</description></item>
    ///   <item><term>byte</term><description>TypeUint8</description></item>
    ///   <item><term>ushort</term><description>TypeUint16</description></item>
    ///   <item><term>uint</term><description>TypeUint32</description></item>
    ///   <item><term>ulong</term><description>TypeUint64</description></item>
    ///   <item><term>float</term><description>TypeFloat</description></item>
    ///   <item><term>double</term><description>TypeDouble</description></item>
    ///   <item><term>decimal</term><description>TypeDecimal (stored as double)</description></item>
    ///   <item><term>byte[]</term><description>TypeBlob</description></item>
    ///   <item><term>everything else</term><description>TypeString (via ToString)</description></item>
    /// </list>
    /// </summary>
    public static class TypeMapper
    {
        /// <summary>
        /// Converts a CLR <see cref="Type"/> to the corresponding protobuf
        /// <see cref="ColumnType"/>. Falls back to <see cref="ColumnType.TypeString"/>
        /// for any unrecognized type (DateTime, Guid, TimeSpan, etc.).
        /// </summary>
        /// <param name="clrType">The CLR type from the data reader's field metadata.</param>
        /// <returns>The matching <see cref="ColumnType"/> enum value.</returns>
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
            // Fallback: everything else (DateTime, Guid, TimeSpan, etc.) is serialized
            // as a string via value.ToString() in ColumnBuilder.Add.
            return ColumnType.TypeString;
        }
    }
}
