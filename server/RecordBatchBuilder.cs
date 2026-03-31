using System;
using System.Collections.Generic;
using System.Data;
using Apache.Arrow;

namespace DuckArrowServer
{
    /// <summary>
    /// Reads rows from a DataReader and builds Arrow RecordBatch objects.
    ///
    /// Used by DuckFlightServer to convert DuckDB query results into
    /// Arrow batches for streaming to Flight clients.
    /// </summary>
    public static class RecordBatchBuilder
    {
        /// <summary>
        /// Build an Arrow schema from a DataReader's column metadata.
        /// </summary>
        public static Schema BuildSchema(IDataReader reader)
        {
            var fields = new List<Field>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var clrType = reader.GetFieldType(i);
                var arrowType = ArrowTypeConverter.FromClrType(clrType);
                fields.Add(new Field(reader.GetName(i), arrowType, nullable: true));
            }
            return new Schema(fields, null);
        }

        /// <summary>
        /// Read up to maxRows from the reader and build a RecordBatch.
        /// Returns null when there are no more rows.
        /// </summary>
        public static RecordBatch ReadNextBatch(IDataReader reader, Schema schema, int maxRows)
        {
            int columnCount = schema.FieldsList.Count;

            // Read rows into column lists.
            var columns = CreateColumnLists(columnCount, maxRows);
            int rowCount = ReadRows(reader, columns, columnCount, maxRows);

            if (rowCount == 0)
                return null; // no more data

            // Convert column lists into Arrow arrays.
            var arrays = BuildArrowArrays(schema, columns, columnCount);
            return new RecordBatch(schema, arrays, rowCount);
        }

        // ── Private helpers ──────────────────────────────────────────────────

        /// <summary>
        /// Create empty lists to hold values for each column.
        /// </summary>
        private static List<object>[] CreateColumnLists(int columnCount, int capacity)
        {
            var columns = new List<object>[columnCount];
            for (int c = 0; c < columnCount; c++)
                columns[c] = new List<object>(capacity);
            return columns;
        }

        /// <summary>
        /// Read up to maxRows from the reader into the column lists.
        /// Returns how many rows were read.
        /// </summary>
        private static int ReadRows(
            IDataReader reader, List<object>[] columns, int columnCount, int maxRows)
        {
            int rowCount = 0;
            while (rowCount < maxRows && reader.Read())
            {
                for (int c = 0; c < columnCount; c++)
                {
                    object value = reader.IsDBNull(c) ? null : reader.GetValue(c);
                    columns[c].Add(value);
                }
                rowCount++;
            }
            return rowCount;
        }

        /// <summary>
        /// Convert each column list into an Arrow array.
        /// </summary>
        private static IArrowArray[] BuildArrowArrays(
            Schema schema, List<object>[] columns, int columnCount)
        {
            var arrays = new IArrowArray[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var arrowType = schema.GetFieldByIndex(c).DataType;
                arrays[c] = ArrowTypeConverter.BuildArray(arrowType, columns[c]);
            }
            return arrays;
        }
    }
}
