using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DuckArrowClient
{
    /// <summary>
    /// Reads Arrow record batches from raw Arrow IPC stream bytes.
    ///
    /// <para>
    /// <b>Prefer <see cref="FlightQueryResult"/> for new code.</b>
    /// This class is retained for interoperability scenarios where Arrow IPC
    /// bytes arrive outside of a Flight RPC call (e.g. from a file, a message
    /// queue, or a non-Flight HTTP endpoint).
    /// </para>
    ///
    /// <para><b>Usage</b></para>
    /// <code>
    /// byte[] ipcBytes = File.ReadAllBytes("result.arrow");
    /// using (var reader = new ArrowStreamReader(ipcBytes))
    /// {
    ///     Console.WriteLine(reader.Schema);
    ///     foreach (var batch in reader.ReadAll())
    ///         Console.WriteLine("Rows: " + batch.Length);
    ///
    ///     // Or as DataTable (requires DataTableExtensions):
    ///     DataTable dt = reader.ToDataTable();
    /// }
    /// </code>
    /// </summary>
    public sealed class ArrowStreamReader : IDisposable
    {
        private readonly MemoryStream _ms;
        private readonly Apache.Arrow.Ipc.ArrowStreamReader _inner;
        private bool _disposed;

        /// <summary>Arrow schema of the IPC stream.</summary>
        public Schema Schema { get; }

        /// <summary>
        /// Construct from raw Arrow IPC bytes.
        /// The bytes are read from memory; no network call is made.
        /// </summary>
        /// <param name="ipcBytes">
        /// A complete Arrow IPC stream: Schema + RecordBatch(es) + EOS marker.
        /// </param>
        public ArrowStreamReader(byte[] ipcBytes)
        {
            _ms    = new MemoryStream(ipcBytes, writable: false);
            _inner = new Apache.Arrow.Ipc.ArrowStreamReader(_ms);
            Schema = _inner.Schema;
        }

        /// <summary>
        /// Read all record batches eagerly into a list.
        ///
        /// <para>
        /// For very large results (millions of rows) prefer iterating the
        /// underlying reader directly via <c>ReadNextRecordBatch()</c> to avoid
        /// allocating a large list at once.
        /// </para>
        /// </summary>
        /// <returns>All batches in order; may be empty for zero-row results.</returns>
        public List<RecordBatch> ReadAll()
        {
            var batches = new List<RecordBatch>();
            RecordBatch batch;
            while ((batch = _inner.ReadNextRecordBatch()) != null)
                batches.Add(batch);
            return batches;
        }

        /// <summary>
        /// Materialise all rows as a list of column-name-to-value dictionaries.
        /// Null cells are represented as <c>null</c>.
        /// Delegates boxing to <see cref="ArrowValueConverter.Box"/>.
        /// </summary>
        public List<Dictionary<string, object>> ToRows()
        {
            int colCount = Schema.FieldsList.Count;
            var rows = new List<Dictionary<string, object>>();
            foreach (var batch in ReadAll())
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

        /// <summary>Dispose the underlying MemoryStream and Arrow reader.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _inner?.Dispose();
            _ms?.Dispose();
        }
    }
}
