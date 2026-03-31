using System;

namespace DuckArrowServer
{
    /// <summary>
    /// Serialises concurrent write operations into batched transactions.
    /// DuckDB supports one writer at a time. This interface queues writes
    /// and executes them in batched transactions for throughput.
    /// </summary>
    public interface IWriteSerializer : IDisposable
    {
        /// <summary>
        /// Submit a write statement and block until its transaction commits.
        /// </summary>
        /// <param name="sql">UTF-8 DML or DDL statement.</param>
        /// <returns>WriteResult indicating success or the error message.</returns>
        WriteResult Submit(string sql);
    }
}
