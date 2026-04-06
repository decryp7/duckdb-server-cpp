using System;

namespace DuckDbServer
{
    /// <summary>
    /// Serializes concurrent write operations into batched transactions for throughput.
    ///
    /// <para><b>Why this interface exists:</b> DuckDB supports only one concurrent writer
    /// at a time. Without serialization, concurrent <c>Execute</c> calls would fail with
    /// lock contention errors. This interface provides a single entry point
    /// (<see cref="Submit"/>) that queues writes and executes them on a dedicated writer
    /// thread, batching multiple writes into a single transaction for dramatically better
    /// throughput (amortized BEGIN/COMMIT overhead).</para>
    ///
    /// <para><b>Thread Safety:</b> Implementations MUST be thread-safe. <see cref="Submit"/>
    /// may be called concurrently from any number of gRPC handler threads. Internally,
    /// all SQL execution MUST happen on a single thread to satisfy DuckDB's single-writer
    /// constraint.</para>
    ///
    /// <para><b>Contract:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="Submit"/> MUST block the calling thread until the write has been
    ///     executed (committed or rolled back). This provides synchronous semantics to
    ///     callers even though execution is deferred to a background thread.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="Submit"/> MUST return a <see cref="WriteResult"/> indicating success
    ///     or failure. It MUST NOT throw exceptions for SQL errors (these are reported
    ///     via <c>WriteResult.Ok = false</c>).
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="IDisposable.Dispose"/> MUST stop the writer thread and signal any
    ///     pending callers with failure results so they do not hang indefinitely.
    ///   </description></item>
    ///   <item><description>
    ///     After disposal, <see cref="Submit"/> MUST return failure immediately without
    ///     enqueueing the request.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Usage pattern:</b></para>
    /// <code>
    /// var result = writer.Submit("INSERT INTO t VALUES (1, 'hello')");
    /// if (!result.Ok) Console.Error.WriteLine("Write failed: " + result.Error);
    /// </code>
    /// </summary>
    public interface IWriteSerializer : IDisposable
    {
        /// <summary>
        /// Submits a write (DML or DDL) statement for execution and blocks until the
        /// statement has been committed (or failed).
        ///
        /// <para>The statement may be batched with other concurrent writes into a single
        /// transaction for throughput. DDL statements are always executed individually
        /// (outside transactions) because DuckDB does not support DDL inside explicit
        /// transactions.</para>
        /// </summary>
        /// <param name="sql">
        /// A UTF-8 DML or DDL SQL statement. Examples:
        /// <list type="bullet">
        ///   <item><description>DML: <c>INSERT INTO t VALUES (1, 'hello')</c></description></item>
        ///   <item><description>DML: <c>UPDATE t SET name = 'world' WHERE id = 1</c></description></item>
        ///   <item><description>DDL: <c>CREATE TABLE t (id INT, name TEXT)</c></description></item>
        /// </list>
        /// </param>
        /// <returns>
        /// A <see cref="WriteResult"/> with <c>Ok = true</c> on success, or
        /// <c>Ok = false</c> and an error message on failure (SQL error, timeout,
        /// or disposal).
        /// </returns>
        WriteResult Submit(string sql);
    }
}
