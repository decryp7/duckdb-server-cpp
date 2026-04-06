using System;

namespace DuckDbServer
{
    /// <summary>
    /// Lightweight SQL parser that extracts the table name, prefix, and VALUES clause
    /// from simple INSERT statements, enabling the <see cref="InsertBatcher"/> to merge
    /// multiple single-row INSERTs into one multi-row INSERT.
    ///
    /// <para><b>Thread Safety:</b> All methods are static and stateless, so this class
    /// is inherently thread-safe.</para>
    ///
    /// <para><b>Supported patterns:</b></para>
    /// <list type="bullet">
    ///   <item><description><c>INSERT INTO tableName VALUES (...)</c></description></item>
    ///   <item><description><c>INSERT INTO tableName (col1, col2) VALUES (...)</c></description></item>
    ///   <item><description><c>INSERT INTO "quoted-table" VALUES (...)</c></description></item>
    /// </list>
    ///
    /// <para><b>Rejected patterns</b> (returns <c>IsSimpleInsert = false</c>):</para>
    /// <list type="bullet">
    ///   <item><description>Multi-row INSERTs: <c>INSERT INTO t VALUES (1), (2), (3)</c></description></item>
    ///   <item><description>INSERT ... SELECT: <c>INSERT INTO t SELECT ...</c></description></item>
    ///   <item><description>Any SQL that does not exactly match the INSERT INTO ... VALUES pattern.</description></item>
    /// </list>
    ///
    /// <para><b>Design:</b> This parser is intentionally simple and hand-written (no regex)
    /// for performance. It processes each character once (O(N)) and does not allocate
    /// intermediate objects beyond the result struct. It handles quoted identifiers and
    /// string literals correctly, but does not handle all SQL edge cases (e.g., comments
    /// within INSERT statements). This is acceptable because the parser is conservative:
    /// unrecognized patterns are left unchanged (not merged).</para>
    /// </summary>
    public static class InsertParser
    {
        /// <summary>
        /// Result of parsing an INSERT statement. A value type (struct) to avoid
        /// heap allocation when processing many statements.
        /// </summary>
        public struct ParsedInsert
        {
            /// <summary>
            /// True if the SQL was successfully parsed as a simple single-row INSERT
            /// that can be merged with other INSERTs to the same table.
            /// </summary>
            public bool IsSimpleInsert;

            /// <summary>
            /// Everything before the VALUES keyword (trimmed), including any column list.
            /// Used as the shared prefix when merging multiple INSERTs.
            /// Examples:
            /// <list type="bullet">
            ///   <item><description><c>"INSERT INTO my_table"</c></description></item>
            ///   <item><description><c>"INSERT INTO my_table (col1, col2)"</c></description></item>
            /// </list>
            /// </summary>
            public string Prefix;

            /// <summary>
            /// The VALUES clause including parentheses, e.g., <c>"(1, 'hello', 3.14)"</c>.
            /// Used as the individual tuple when building the merged multi-row VALUES clause.
            /// </summary>
            public string ValuesTuple;

            /// <summary>
            /// The table name lowercased, used as a grouping key for merge eligibility.
            /// Two INSERTs can only be merged if they have the same <see cref="TableKey"/>
            /// AND the same <see cref="Prefix"/>.
            /// </summary>
            public string TableKey;
        }

        /// <summary>
        /// Attempts to parse a SQL string as a simple <c>INSERT INTO ... VALUES (...)</c>.
        /// Returns a <see cref="ParsedInsert"/> with <c>IsSimpleInsert = false</c> if the
        /// SQL does not match the expected pattern.
        ///
        /// <para><b>Algorithm:</b></para>
        /// <list type="number">
        ///   <item><description>Skip leading whitespace.</description></item>
        ///   <item><description>Match the keyword <c>INSERT</c> (case-insensitive).</description></item>
        ///   <item><description>Match the keyword <c>INTO</c> (case-insensitive).</description></item>
        ///   <item><description>Extract the table name. Supports double-quoted identifiers
        ///     with escaped quotes (<c>""</c> within quoted names).</description></item>
        ///   <item><description>If a <c>(</c> follows the table name, skip the column list
        ///     by tracking parenthesis depth.</description></item>
        ///   <item><description>Match the keyword <c>VALUES</c> (case-insensitive).</description></item>
        ///   <item><description>Extract the remaining text as the values tuple. Strip
        ///     trailing semicolons.</description></item>
        ///   <item><description>Verify the values tuple is a single tuple (not multi-row)
        ///     via <see cref="ContainsMultipleTuples"/>.</description></item>
        /// </list>
        /// </summary>
        /// <param name="sql">The SQL string to parse. May be null.</param>
        /// <returns>
        /// A <see cref="ParsedInsert"/> struct. Check <c>IsSimpleInsert</c> to determine
        /// if parsing succeeded.
        /// </returns>
        public static ParsedInsert TryParse(string sql)
        {
            var result = new ParsedInsert { IsSimpleInsert = false };

            // Minimum length: "INSERT INTO t VALUES (x)" = 24 chars. Reject obvious non-matches.
            if (sql == null || sql.Length < 20)
                return result;

            int pos = 0;

            // Skip leading whitespace.
            pos = SkipSpaces(sql, pos);

            // Match "INSERT" (case-insensitive, with word boundary check).
            if (!MatchKeyword(sql, pos, "INSERT", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // Match "INTO" (case-insensitive, with word boundary check).
            if (!MatchKeyword(sql, pos, "INTO", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // Read the table name. Supports two formats:
            // 1. Quoted: "my-table" or "my ""quoted"" table"
            // 2. Unquoted: my_table (terminated by whitespace or '(')
            int tableStart = pos;
            if (pos < sql.Length && sql[pos] == '"')
            {
                // Quoted identifier: skip to the closing double-quote.
                // Handles escaped double-quotes ("") within the identifier.
                pos++;
                while (pos < sql.Length)
                {
                    if (sql[pos] == '"')
                    {
                        if (pos + 1 < sql.Length && sql[pos + 1] == '"')
                            pos += 2; // Escaped double-quote ("") -- skip both chars.
                        else
                            { pos++; break; } // Closing quote found.
                    }
                    else
                        pos++;
                }
            }
            else
            {
                // Unquoted identifier: read until whitespace or '(' (column list start).
                while (pos < sql.Length && !char.IsWhiteSpace(sql[pos]) && sql[pos] != '(')
                    pos++;
            }

            // No table name found (e.g., "INSERT INTO " with nothing after).
            if (pos == tableStart)
                return result;

            string tableName = sql.Substring(tableStart, pos - tableStart);
            pos = SkipSpaces(sql, pos);

            // Check for optional column list: (col1, col2, ...)
            // If present, skip it by tracking parenthesis depth. The column list is
            // included in the prefix so that INSERTs with different column lists are
            // not merged (which would produce invalid SQL).
            int prefixEndPos = pos;
            if (pos < sql.Length && sql[pos] == '(')
            {
                // Skip the entire column list by tracking parenthesis nesting depth.
                int depth = 0;
                while (pos < sql.Length)
                {
                    if (sql[pos] == '(') depth++;
                    else if (sql[pos] == ')') { depth--; if (depth == 0) { pos++; break; } }
                    pos++;
                }
                pos = SkipSpaces(sql, pos);
            }

            // Save position before the VALUES keyword for prefix extraction.
            int beforeValues = pos;

            // Match "VALUES" (case-insensitive, with word boundary check).
            if (!MatchKeyword(sql, pos, "VALUES", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // The prefix is everything before the VALUES keyword, trimmed.
            // e.g., "INSERT INTO my_table (col1, col2)"
            string prefix = sql.Substring(0, beforeValues).TrimEnd();

            // The remaining text should be the values tuple: (...)
            string valuesPart = sql.Substring(pos).TrimEnd();
            // Strip trailing semicolons (common in SQL files).
            if (valuesPart.EndsWith(";"))
                valuesPart = valuesPart.Substring(0, valuesPart.Length - 1).TrimEnd();

            // Must start with '(' and end with ')' to be a valid VALUES tuple.
            if (valuesPart.Length < 2 || valuesPart[0] != '(' || valuesPart[valuesPart.Length - 1] != ')')
                return result;

            // Verify this is a single-tuple INSERT, not already a multi-row INSERT.
            // Multi-row INSERTs like "VALUES (1), (2), (3)" must not be merged further
            // because the merging logic appends additional tuples after the VALUES keyword.
            if (ContainsMultipleTuples(valuesPart))
                return result;

            result.IsSimpleInsert = true;
            result.Prefix = prefix;
            result.ValuesTuple = valuesPart;
            result.TableKey = tableName.ToLowerInvariant();
            return result;
        }

        /// <summary>
        /// Checks if the VALUES part contains multiple tuples, e.g., <c>(1), (2), (3)</c>.
        ///
        /// <para><b>Algorithm:</b> Tracks parenthesis depth. When the depth returns to 0
        /// (closing the first tuple) and there is a comma followed by another open
        /// parenthesis, it is a multi-tuple VALUES clause.</para>
        ///
        /// <para><b>String literal handling:</b> Skips over single-quoted string literals
        /// (handling escaped quotes <c>''</c>) so that commas and parentheses inside strings
        /// do not cause false positives. For example, <c>('hello, (world)')</c> is correctly
        /// recognized as a single tuple.</para>
        /// </summary>
        /// <param name="values">The VALUES part of the INSERT statement (starts with '(').</param>
        /// <returns><c>true</c> if multiple tuples are detected; <c>false</c> for a single tuple.</returns>
        private static bool ContainsMultipleTuples(string values)
        {
            int depth = 0;
            for (int i = 0; i < values.Length; i++)
            {
                char c = values[i];
                if (c == '\'')
                {
                    // Skip string literal contents. Handles escaped quotes ('').
                    i++;
                    while (i < values.Length)
                    {
                        if (values[i] == '\'' && i + 1 < values.Length && values[i + 1] == '\'')
                            i += 2; // Escaped quote ('') -- skip both chars.
                        else if (values[i] == '\'')
                            break; // End of string literal.
                        else
                            i++;
                    }
                }
                else if (c == '(') depth++;
                else if (c == ')')
                {
                    depth--;
                    if (depth == 0 && i < values.Length - 1)
                    {
                        // Depth returned to 0 before end of string: check if another
                        // tuple follows (indicated by a comma after optional whitespace).
                        int next = SkipSpaces(values, i + 1);
                        if (next < values.Length && values[next] == ',')
                            return true; // Multi-tuple detected.
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Advances past whitespace characters starting at <paramref name="pos"/>.
        /// </summary>
        /// <param name="s">The string to scan.</param>
        /// <param name="pos">Starting position.</param>
        /// <returns>The position of the first non-whitespace character, or <c>s.Length</c> if none.</returns>
        private static int SkipSpaces(string s, int pos)
        {
            while (pos < s.Length && char.IsWhiteSpace(s[pos]))
                pos++;
            return pos;
        }

        /// <summary>
        /// Matches a SQL keyword at the given position (case-insensitive) with a word
        /// boundary check to prevent matching partial words (e.g., "INSERT" should not
        /// match "INSERTING").
        /// </summary>
        /// <param name="s">The SQL string to match against.</param>
        /// <param name="pos">The position to start matching.</param>
        /// <param name="keyword">The keyword to match (must be uppercase).</param>
        /// <param name="newPos">On success, the position after the keyword. On failure, unchanged.</param>
        /// <returns><c>true</c> if the keyword was matched at the given position.</returns>
        /// <remarks>
        /// Word boundary is defined as: the character after the keyword must be whitespace,
        /// <c>'('</c>, or end-of-string. This allows matching <c>INSERT(</c> as a keyword
        /// followed by <c>(</c>, which is needed for <c>VALUES(1,2)</c> without a space
        /// before the parenthesis.
        /// </remarks>
        private static bool MatchKeyword(string s, int pos, string keyword, out int newPos)
        {
            newPos = pos;
            if (pos + keyword.Length > s.Length)
                return false;

            // Case-insensitive comparison, character by character.
            for (int i = 0; i < keyword.Length; i++)
            {
                if (char.ToUpperInvariant(s[pos + i]) != keyword[i])
                    return false;
            }

            // Word boundary check: must be followed by whitespace, '(', or end of string.
            int after = pos + keyword.Length;
            if (after < s.Length && !char.IsWhiteSpace(s[after]) && s[after] != '(')
                return false;

            newPos = after;
            return true;
        }
    }
}
