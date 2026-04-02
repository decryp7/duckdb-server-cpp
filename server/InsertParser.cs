using System;

namespace DuckDbServer
{
    /// <summary>
    /// Parses simple INSERT statements to extract the table name and values clause.
    ///
    /// Handles the pattern:
    ///   INSERT INTO tableName VALUES (...)
    ///   INSERT INTO tableName (col1, col2) VALUES (...)
    ///
    /// This allows the WriteSerializer to merge multiple single-row INSERTs
    /// into one multi-row INSERT, which is much faster because DuckDB only
    /// parses and plans the query once instead of N times.
    /// </summary>
    public static class InsertParser
    {
        /// <summary>
        /// Result of parsing an INSERT statement.
        /// </summary>
        public struct ParsedInsert
        {
            /// <summary>True if the SQL was a simple INSERT we can batch.</summary>
            public bool IsSimpleInsert;

            /// <summary>
            /// Everything before the VALUES keyword, e.g.:
            ///   "INSERT INTO my_table"
            ///   "INSERT INTO my_table (col1, col2)"
            /// </summary>
            public string Prefix;

            /// <summary>
            /// The values clause including parentheses, e.g.:
            ///   "(1, 'hello', 3.14)"
            /// </summary>
            public string ValuesTuple;

            /// <summary>The table name (lowercase, for grouping).</summary>
            public string TableKey;
        }

        /// <summary>
        /// Try to parse a SQL string as a simple INSERT INTO ... VALUES (...).
        /// Returns a ParsedInsert with IsSimpleInsert = false if it can't be parsed.
        /// </summary>
        public static ParsedInsert TryParse(string sql)
        {
            var result = new ParsedInsert { IsSimpleInsert = false };

            // We need at least: INSERT INTO t VALUES (x)
            if (sql == null || sql.Length < 20)
                return result;

            int pos = 0;

            // Skip leading whitespace.
            pos = SkipSpaces(sql, pos);

            // Match "INSERT" (case-insensitive).
            if (!MatchKeyword(sql, pos, "INSERT", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // Match "INTO" (case-insensitive).
            if (!MatchKeyword(sql, pos, "INTO", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // Read the table name (may be quoted with double quotes).
            int tableStart = pos;
            if (pos < sql.Length && sql[pos] == '"')
            {
                // Quoted identifier: skip to closing quote.
                pos++;
                while (pos < sql.Length)
                {
                    if (sql[pos] == '"')
                    {
                        if (pos + 1 < sql.Length && sql[pos + 1] == '"')
                            pos += 2; // escaped double-quote
                        else
                            { pos++; break; }
                    }
                    else
                        pos++;
                }
            }
            else
            {
                while (pos < sql.Length && !char.IsWhiteSpace(sql[pos]) && sql[pos] != '(')
                    pos++;
            }

            if (pos == tableStart)
                return result;

            string tableName = sql.Substring(tableStart, pos - tableStart);
            pos = SkipSpaces(sql, pos);

            // Remember where the prefix ends (before VALUES).
            // There might be a column list: (col1, col2, ...)
            int prefixEndPos = pos;
            if (pos < sql.Length && sql[pos] == '(')
            {
                // Skip the column list.
                int depth = 0;
                while (pos < sql.Length)
                {
                    if (sql[pos] == '(') depth++;
                    else if (sql[pos] == ')') { depth--; if (depth == 0) { pos++; break; } }
                    pos++;
                }
                pos = SkipSpaces(sql, pos);
            }

            // Save position before VALUES keyword for prefix extraction.
            int beforeValues = pos;

            // Match "VALUES" (case-insensitive).
            if (!MatchKeyword(sql, pos, "VALUES", out pos))
                return result;

            pos = SkipSpaces(sql, pos);

            // The prefix is everything before the VALUES keyword.
            string prefix = sql.Substring(0, beforeValues).TrimEnd();

            // The rest should be the values tuple: (...)
            string valuesPart = sql.Substring(pos).TrimEnd();
            if (valuesPart.EndsWith(";"))
                valuesPart = valuesPart.Substring(0, valuesPart.Length - 1).TrimEnd();

            // Must start with ( and end with ).
            if (valuesPart.Length < 2 || valuesPart[0] != '(' || valuesPart[valuesPart.Length - 1] != ')')
                return result;

            // Check there's only one tuple (no multi-row INSERT already).
            // Count top-level commas between balanced parens.
            // If there's a ), ( pattern at the top level, it's already multi-row.
            if (ContainsMultipleTuples(valuesPart))
                return result;

            result.IsSimpleInsert = true;
            result.Prefix = prefix;
            result.ValuesTuple = valuesPart;
            result.TableKey = tableName.ToLowerInvariant();
            return result;
        }

        /// <summary>
        /// Check if the values part contains multiple tuples like (1), (2), (3).
        /// </summary>
        private static bool ContainsMultipleTuples(string values)
        {
            int depth = 0;
            for (int i = 0; i < values.Length; i++)
            {
                char c = values[i];
                if (c == '\'')
                {
                    // Skip string literal.
                    i++;
                    while (i < values.Length)
                    {
                        if (values[i] == '\'' && i + 1 < values.Length && values[i + 1] == '\'')
                            i += 2; // escaped quote
                        else if (values[i] == '\'')
                            break;
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
                        // There's more content after the closing paren — multi-tuple.
                        int next = SkipSpaces(values, i + 1);
                        if (next < values.Length && values[next] == ',')
                            return true;
                    }
                }
            }
            return false;
        }

        private static int SkipSpaces(string s, int pos)
        {
            while (pos < s.Length && char.IsWhiteSpace(s[pos]))
                pos++;
            return pos;
        }

        private static bool MatchKeyword(string s, int pos, string keyword, out int newPos)
        {
            newPos = pos;
            if (pos + keyword.Length > s.Length)
                return false;

            for (int i = 0; i < keyword.Length; i++)
            {
                if (char.ToUpperInvariant(s[pos + i]) != keyword[i])
                    return false;
            }

            // Must be followed by whitespace or end of string (word boundary).
            int after = pos + keyword.Length;
            if (after < s.Length && !char.IsWhiteSpace(s[after]) && s[after] != '(')
                return false;

            newPos = after;
            return true;
        }
    }
}
