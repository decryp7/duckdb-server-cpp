using System.Collections.Generic;
using System.Text;

namespace DuckDbServer
{
    /// <summary>
    /// Merges multiple single-row INSERT statements targeting the same table
    /// into one multi-row INSERT statement for dramatically better performance.
    ///
    /// <para><b>Thread Safety:</b> All methods are static and stateless, so this class
    /// is inherently thread-safe.</para>
    ///
    /// <para><b>Why this is faster:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Parse overhead:</b> DuckDB parses and plans the query ONCE for a multi-row
    ///     INSERT instead of N times for N single-row INSERTs. For simple tables, parsing
    ///     takes ~50% of the total INSERT time.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Transaction overhead:</b> A single merged INSERT executes as one operation
    ///     within the transaction, reducing per-statement overhead (command creation,
    ///     result handling, etc.).
    ///   </description></item>
    ///   <item><description>
    ///     <b>Batch processing:</b> DuckDB's INSERT engine can batch-process rows more
    ///     efficiently (vectorized execution) when they arrive in a single statement.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Example:</b></para>
    /// <code>
    /// Input:
    ///   INSERT INTO t VALUES (1, 'a')
    ///   INSERT INTO t VALUES (2, 'b')
    ///   INSERT INTO t VALUES (3, 'c')
    ///
    /// Output:
    ///   INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')
    /// </code>
    ///
    /// <para><b>Merging rules:</b> Only consecutive statements that target the same table
    /// AND have the same prefix (e.g., same column list) can be merged. Statements that
    /// cannot be parsed as simple single-row INSERTs are left unchanged.</para>
    /// </summary>
    public static class InsertBatcher
    {
        /// <summary>
        /// A group of write requests that can be executed together as a single SQL statement.
        ///
        /// <para>If merged, <see cref="Sql"/> contains the combined multi-row INSERT and
        /// <see cref="RequestIndices"/> lists all original request indices that were merged.
        /// The writer thread uses these indices to signal all original callers with the
        /// same result.</para>
        ///
        /// <para>If not merged, <see cref="Sql"/> is the original SQL string and
        /// <see cref="RequestIndices"/> has exactly one element.</para>
        /// </summary>
        public sealed class BatchGroup
        {
            /// <summary>
            /// The SQL to execute. Either the original statement (unmerged) or a
            /// combined multi-row INSERT (merged).
            /// </summary>
            public string Sql;

            /// <summary>
            /// Indices into the original request list that this group covers.
            /// For merged groups, all listed requests share the same execution result.
            /// </summary>
            public List<int> RequestIndices;

            /// <summary>True if this group was merged from multiple INSERTs. False if it is a single original statement.</summary>
            public bool IsMerged;
        }

        /// <summary>
        /// Takes a list of SQL statements and merges compatible consecutive single-row
        /// INSERTs into multi-row INSERTs.
        ///
        /// <para><b>Algorithm:</b></para>
        /// <list type="number">
        ///   <item><description>
        ///     Parse all statements via <see cref="InsertParser.TryParse"/>. This extracts
        ///     the table name, prefix (everything before VALUES), and values tuple for each
        ///     statement, or marks it as non-mergeable.
        ///   </description></item>
        ///   <item><description>
        ///     Scan linearly. Non-INSERT statements emit a single-element group. Consecutive
        ///     simple INSERTs with the same <see cref="InsertParser.ParsedInsert.TableKey"/>
        ///     and <see cref="InsertParser.ParsedInsert.Prefix"/> form a "run".
        ///   </description></item>
        ///   <item><description>
        ///     Runs of length 1 emit a single-element group (no merging needed). Runs of
        ///     length > 1 are merged via <see cref="MergeRun"/>.
        ///   </description></item>
        /// </list>
        ///
        /// <para><b>Gotcha:</b> The prefix includes the column list (if any). This means
        /// <c>INSERT INTO t (a,b) VALUES (1,2)</c> and <c>INSERT INTO t VALUES (1,2)</c>
        /// will NOT be merged even though they target the same table, because their
        /// prefixes differ. This is correct: merging them would produce invalid SQL.</para>
        /// </summary>
        /// <param name="statements">List of SQL statements to process.</param>
        /// <returns>
        /// A list of <see cref="BatchGroup"/>s. The total of all <c>RequestIndices</c>
        /// across all groups covers every index in <paramref name="statements"/> exactly once.
        /// </returns>
        public static List<BatchGroup> MergeInserts(List<string> statements)
        {
            var groups = new List<BatchGroup>();

            // Parse all statements upfront. O(N) where N = statement count.
            var parsed = new InsertParser.ParsedInsert[statements.Count];
            for (int i = 0; i < statements.Count; i++)
                parsed[i] = InsertParser.TryParse(statements[i]);

            int pos = 0;
            while (pos < statements.Count)
            {
                // If this statement is not a simple INSERT, emit it as a standalone group.
                if (!parsed[pos].IsSimpleInsert)
                {
                    groups.Add(MakeSingleGroup(statements[pos], pos));
                    pos++;
                    continue;
                }

                // Find a run of consecutive simple INSERTs to the same table with the
                // same prefix (column list). These can be merged into one multi-row INSERT.
                string prefix = parsed[pos].Prefix;
                string tableKey = parsed[pos].TableKey;
                int runStart = pos;

                while (pos < statements.Count
                    && parsed[pos].IsSimpleInsert
                    && parsed[pos].TableKey == tableKey
                    && parsed[pos].Prefix == prefix)
                {
                    pos++;
                }

                int runLength = pos - runStart;

                if (runLength == 1)
                {
                    // Only one INSERT in this run -- no merging benefit.
                    groups.Add(MakeSingleGroup(statements[runStart], runStart));
                }
                else
                {
                    // Merge multiple INSERTs into one multi-row INSERT.
                    groups.Add(MergeRun(parsed, runStart, pos, prefix));
                }
            }

            return groups;
        }

        /// <summary>
        /// Creates a single-element batch group for a non-mergeable statement.
        /// </summary>
        /// <param name="sql">The original SQL statement.</param>
        /// <param name="index">The index of this statement in the original list.</param>
        /// <returns>A <see cref="BatchGroup"/> with one request index and IsMerged = false.</returns>
        private static BatchGroup MakeSingleGroup(string sql, int index)
        {
            return new BatchGroup
            {
                Sql = sql,
                RequestIndices = new List<int> { index },
                IsMerged = false
            };
        }

        /// <summary>
        /// Merges a consecutive run of parsed INSERTs into a single multi-row INSERT
        /// statement by concatenating their VALUES tuples.
        ///
        /// <para><b>Output format:</b></para>
        /// <code>INSERT INTO table VALUES (1,'a'), (2,'b'), (3,'c')</code>
        ///
        /// <para>The prefix (e.g., <c>"INSERT INTO table"</c> or
        /// <c>"INSERT INTO table (col1, col2)"</c>) is shared by all statements in
        /// the run and is written once. Each statement's VALUES tuple is appended
        /// with comma separators.</para>
        /// </summary>
        /// <param name="parsed">Array of all parsed statements (only [from..to) are used).</param>
        /// <param name="from">Inclusive start index of the run.</param>
        /// <param name="to">Exclusive end index of the run.</param>
        /// <param name="prefix">The shared prefix (e.g., "INSERT INTO t" or "INSERT INTO t (a,b)").</param>
        /// <returns>A merged <see cref="BatchGroup"/> covering all statements in the run.</returns>
        private static BatchGroup MergeRun(
            InsertParser.ParsedInsert[] parsed, int from, int to, string prefix)
        {
            // Estimate StringBuilder capacity: prefix + ~30 chars per VALUES tuple.
            var sb = new StringBuilder(prefix.Length + (to - from) * 30);
            sb.Append(prefix);
            sb.Append(" VALUES ");

            var indices = new List<int>(to - from);
            for (int i = from; i < to; i++)
            {
                if (i > from) sb.Append(", ");
                // Append the values tuple, e.g., "(1, 'hello', 3.14)"
                sb.Append(parsed[i].ValuesTuple);
                indices.Add(i);
            }

            return new BatchGroup
            {
                Sql = sb.ToString(),
                RequestIndices = indices,
                IsMerged = true
            };
        }
    }
}
