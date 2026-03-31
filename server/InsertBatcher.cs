using System.Collections.Generic;
using System.Text;

namespace DuckArrowServer
{
    /// <summary>
    /// Merges multiple single-row INSERT statements targeting the same table
    /// into one multi-row INSERT statement.
    ///
    /// Why this is faster:
    ///   - DuckDB parses and plans the query ONCE instead of N times.
    ///   - Network/IPC overhead is reduced to a single round-trip.
    ///   - The INSERT engine can batch-process rows more efficiently.
    ///
    /// Example:
    ///   Input:
    ///     INSERT INTO t VALUES (1, 'a')
    ///     INSERT INTO t VALUES (2, 'b')
    ///     INSERT INTO t VALUES (3, 'c')
    ///
    ///   Output:
    ///     INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')
    ///
    /// Statements that can't be parsed as simple INSERTs are left unchanged.
    /// </summary>
    public static class InsertBatcher
    {
        /// <summary>
        /// A group of write requests that can be executed together.
        /// If merged, the MergedSql contains the combined multi-row INSERT.
        /// If not merged, OriginalRequests has exactly one item.
        /// </summary>
        public sealed class BatchGroup
        {
            /// <summary>The merged SQL if multiple INSERTs were combined, or the original SQL.</summary>
            public string Sql;

            /// <summary>Indices into the original request list that this group covers.</summary>
            public List<int> RequestIndices;

            /// <summary>True if this group was merged from multiple INSERTs.</summary>
            public bool IsMerged;
        }

        /// <summary>
        /// Take a list of SQL statements and merge compatible INSERTs.
        /// Returns a list of BatchGroups, each containing one or more requests.
        /// </summary>
        public static List<BatchGroup> MergeInserts(List<string> statements)
        {
            var groups = new List<BatchGroup>();

            // Parse all statements.
            var parsed = new InsertParser.ParsedInsert[statements.Count];
            for (int i = 0; i < statements.Count; i++)
                parsed[i] = InsertParser.TryParse(statements[i]);

            int pos = 0;
            while (pos < statements.Count)
            {
                // If this statement isn't a simple INSERT, emit it alone.
                if (!parsed[pos].IsSimpleInsert)
                {
                    groups.Add(MakeSingleGroup(statements[pos], pos));
                    pos++;
                    continue;
                }

                // Find consecutive simple INSERTs to the same table with the same prefix.
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
                    // Only one INSERT — no merging needed.
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

        private static BatchGroup MakeSingleGroup(string sql, int index)
        {
            return new BatchGroup
            {
                Sql = sql,
                RequestIndices = new List<int> { index },
                IsMerged = false
            };
        }

        private static BatchGroup MergeRun(
            InsertParser.ParsedInsert[] parsed, int from, int to, string prefix)
        {
            // Build: INSERT INTO table VALUES (1,'a'), (2,'b'), (3,'c')
            var sb = new StringBuilder(prefix.Length + (to - from) * 30);
            sb.Append(prefix);
            sb.Append(" VALUES ");

            var indices = new List<int>(to - from);
            for (int i = from; i < to; i++)
            {
                if (i > from) sb.Append(", ");
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
