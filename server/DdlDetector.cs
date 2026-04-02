using System;

namespace DuckDbServer
{
    /// <summary>
    /// Detects whether a SQL statement is DDL (CREATE, DROP, ALTER, etc.)
    /// or DML (INSERT, UPDATE, DELETE, etc.).
    ///
    /// Why this matters:
    ///   DuckDB does not allow DDL statements inside explicit transactions.
    ///   DDL must run on its own, while DML can be batched together.
    /// </summary>
    public static class DdlDetector
    {
        /// <summary>
        /// Keywords that start a DDL statement.
        /// </summary>
        private static readonly string[] DdlKeywords =
        {
            "CREATE", "DROP", "ALTER", "TRUNCATE",
            "ATTACH", "DETACH", "VACUUM", "PRAGMA",
            "COPY", "EXPORT", "IMPORT", "LOAD"
        };

        /// <summary>
        /// Returns true if the SQL statement is DDL.
        /// Looks at the first keyword after skipping whitespace and comments.
        /// </summary>
        public static bool IsDdl(string sql)
        {
            string firstKeyword = ExtractFirstKeyword(sql);
            if (firstKeyword == null) return false;

            for (int i = 0; i < DdlKeywords.Length; i++)
            {
                if (firstKeyword == DdlKeywords[i])
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Extract the first SQL keyword (up to 8 chars, uppercased).
        /// Skips leading whitespace and single-line comments (-- ...).
        /// </summary>
        private static string ExtractFirstKeyword(string sql)
        {
            int pos = 0;

            // Skip leading whitespace and comments (multiple lines, block comments).
            pos = SkipWhitespaceAndComments(sql, pos);

            // Read up to 8 letters as the keyword.
            int start = pos;
            int length = 0;
            while (length < 8 && pos < sql.Length && char.IsLetter(sql[pos]))
            {
                length++;
                pos++;
            }

            if (length == 0) return null;
            return sql.Substring(start, length).ToUpperInvariant();
        }

        private static int SkipWhitespace(string sql, int pos)
        {
            while (pos < sql.Length && char.IsWhiteSpace(sql[pos]))
                pos++;
            return pos;
        }

        /// <summary>
        /// Skip all leading whitespace, single-line comments (--), and block comments.
        /// Handles multiple consecutive comments.
        /// </summary>
        private static int SkipWhitespaceAndComments(string sql, int pos)
        {
            while (pos < sql.Length)
            {
                // Skip whitespace.
                if (char.IsWhiteSpace(sql[pos]))
                {
                    pos++;
                    continue;
                }

                // Skip single-line comment: -- ...
                if (pos + 1 < sql.Length && sql[pos] == '-' && sql[pos + 1] == '-')
                {
                    pos = SkipToEndOfLine(sql, pos);
                    continue;
                }

                // Skip block comment: /* ... */
                if (pos + 1 < sql.Length && sql[pos] == '/' && sql[pos + 1] == '*')
                {
                    pos += 2;
                    bool closed = false;
                    while (pos + 1 < sql.Length)
                    {
                        if (sql[pos] == '*' && sql[pos + 1] == '/')
                        {
                            pos += 2;
                            closed = true;
                            break;
                        }
                        pos++;
                    }
                    // Unterminated block comment — skip to end of string.
                    if (!closed) pos = sql.Length;
                    continue;
                }

                // Not whitespace or comment — stop.
                break;
            }
            return pos;
        }

        private static int SkipToEndOfLine(string sql, int pos)
        {
            while (pos < sql.Length && sql[pos] != '\n')
                pos++;
            if (pos < sql.Length) pos++;
            return pos;
        }
    }
}
