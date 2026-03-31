using System;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DuckArrowClient
{
    /// <summary>
    /// Extension methods for <see cref="ArrowStreamReader"/> to convert raw
    /// Arrow IPC streams into ADO.NET DataTable objects.
    ///
    /// <para>
    /// <b>Prefer <see cref="FlightQueryResult.ToDataTable"/> for new code.</b>
    /// This class is retained as a compatibility shim for code that receives
    /// raw Arrow IPC bytes from non-Flight sources (e.g. files, message queues).
    /// </para>
    ///
    /// <para>
    /// All type mapping and value boxing delegates to
    /// <see cref="ArrowTypeMapper"/> and <see cref="ArrowValueConverter"/>,
    /// which are shared with <see cref="FlightQueryResult"/>.
    /// </para>
    /// </summary>
    public static class DataTableExtensions
    {
        /// <summary>
        /// Read all batches from <paramref name="reader"/> and return a single
        /// consolidated <see cref="DataTable"/>.
        /// </summary>
        /// <param name="reader">Source of Arrow record batches.</param>
        /// <param name="tableName">Name assigned to the returned table.</param>
        public static DataTable ToDataTable(
            this ArrowStreamReader reader,
            string tableName = "Result")
        {
            var dt = new DataTable(tableName);
            int colCount = reader.Schema.FieldsList.Count;

            // Create typed columns from the Arrow schema.
            foreach (var field in reader.Schema.FieldsList)
                dt.Columns.Add(field.Name, ArrowTypeMapper.ToClrType(field.DataType));

            // Fill rows from each batch.
            foreach (var batch in reader.ReadAll())
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
    }
}
