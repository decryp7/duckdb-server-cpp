using System;
using Apache.Arrow;

namespace DuckArrowClient
{
    /// <summary>
    /// Converts individual Arrow array cells to plain CLR objects.
    ///
    /// <para>
    /// Centralises the boxing logic that was previously duplicated in
    /// <see cref="FlightQueryResult"/> and <see cref="ArrowStreamReader"/>.
    /// All code that needs to convert Arrow values to <c>object</c> should
    /// call <see cref="Box"/> rather than reimplementing the switch.
    /// </para>
    ///
    /// <para><b>Apache Arrow 14 nullable GetValue contract</b></para>
    /// <para>
    /// In Apache Arrow 14, all typed array <c>GetValue(int)</c> methods return
    /// <c>T?</c> (nullable value types), not <c>T</c>.  For example:
    /// <code>
    ///   Int32Array.GetValue(row) → int?
    ///   DoubleArray.GetValue(row) → double?
    ///   BooleanArray.GetValue(row) → bool?
    /// </code>
    /// Directly boxing a <c>bool?</c> into <c>object</c> and storing it in an
    /// ADO.NET column declared as <c>typeof(bool)</c> causes a runtime
    /// <see cref="InvalidCastException"/>.  This class always unwraps the
    /// nullable before boxing.
    /// </para>
    ///
    /// <para><b>BinaryArray.GetBytes — ReadOnlySpan requirement</b></para>
    /// <para>
    /// <c>BinaryArray.GetBytes(int)</c> returns <c>ReadOnlySpan&lt;byte&gt;</c>
    /// in Arrow 12+.  On .NET Framework 4.6.2, <c>ReadOnlySpan&lt;T&gt;.ToArray()</c>
    /// is provided by the <c>System.Memory</c> package, which is a transitive
    /// dependency of <c>Apache.Arrow</c> and does not need to be listed explicitly.
    /// </para>
    /// </summary>
    internal static class ArrowValueConverter
    {
        /// <summary>
        /// Return the value at <paramref name="row"/> as a boxed CLR object,
        /// or <c>null</c> if the cell is null.
        /// </summary>
        /// <param name="array">The typed Arrow array containing the cell.</param>
        /// <param name="row">Zero-based row index within the array.</param>
        /// <returns>
        /// A boxed CLR value matching the array's element type, or <c>null</c>
        /// for Arrow null cells.
        /// </returns>
        public static object Box(IArrowArray array, int row)
        {
            // Null cells are always returned as null, regardless of type.
            if (array.IsNull(row)) return null;

            switch (array)
            {
                // ── Numeric types ─────────────────────────────────────────────
                // Each case unwraps the nullable T? before boxing to avoid
                // InvalidCastException when the value is stored in a typed DataTable column.
                case BooleanArray   a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case Int8Array      a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case Int16Array     a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case Int32Array     a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case Int64Array     a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case UInt8Array     a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case UInt16Array    a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case UInt32Array    a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case UInt64Array    a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case FloatArray     a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }
                case DoubleArray    a: { var v = a.GetValue(row); return v.HasValue ? (object)v.Value : null; }

                // ── Text ──────────────────────────────────────────────────────
                case StringArray    a: return a.GetString(row);

                // ── Binary ───────────────────────────────────────────────────
                // GetBytes returns ReadOnlySpan<byte>; .ToArray() copies into a byte[].
                // System.Memory (transitive dep of Apache.Arrow) provides this on net462.
                case BinaryArray    a: return a.GetBytes(row).ToArray();

                // ── Date / time ───────────────────────────────────────────────
                // Date32/Date64: GetDateTime returns DateTime?.
                // Convert to DateTimeOffset (UTC) to match ArrowTypeMapper.ToClrType,
                // which maps both to typeof(DateTimeOffset). Storing a raw DateTime
                // into a DataTable column typed as DateTimeOffset throws InvalidCastException.
                case Date32Array    a: { var v = a.GetDateTime(row);  return v.HasValue ? (object)new DateTimeOffset(v.Value, TimeSpan.Zero) : null; }
                case Date64Array    a: { var v = a.GetDateTime(row);  return v.HasValue ? (object)new DateTimeOffset(v.Value, TimeSpan.Zero) : null; }
                case TimestampArray a: { var v = a.GetTimestamp(row); return v.HasValue ? (object)v.Value : null; }

                // ── Fallback ──────────────────────────────────────────────────
                // Unknown or complex types (List, Map, Struct, …) are rendered
                // as a diagnostic string rather than throwing.
                default:
                    return string.Format("[{0}@{1}]", array.GetType().Name, row);
            }
        }
    }
}
