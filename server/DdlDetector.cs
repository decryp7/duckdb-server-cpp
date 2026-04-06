using System;

namespace DuckDbServer
{
    /// <summary>
    /// Classifies SQL statements as DDL (Data Definition Language) or DML
    /// (Data Manipulation Language) by inspecting the first keyword.
    ///
    /// <para><b>Thread Safety:</b> All methods are static and stateless, so this class
    /// is inherently thread-safe.</para>
    ///
    /// <para><b>Why this matters:</b> DuckDB does not allow DDL statements (CREATE, DROP,
    /// ALTER, etc.) inside explicit transactions. The <see cref="WriteSerializer"/> uses
    /// this detector to split batches at DDL boundaries: DML statements are grouped into
    /// a single BEGIN...COMMIT transaction, while DDL statements are executed individually
    /// outside any transaction.</para>
    ///
    /// <para><b>Algorithm:</b> Extracts the first keyword from the SQL string (up to 8
    /// characters, case-insensitive) after skipping any leading whitespace, single-line
    /// comments (<c>-- ...</c>), and block comments (<c>/* ... */</c>). The keyword is
    /// compared against a known list of DDL keywords. This is a conservative classifier:
    /// unrecognized keywords default to DML, which means they will be batched into
    /// transactions (safe, since DuckDB will reject invalid DDL inside a transaction and
    /// the batch will fall back to individual execution).</para>
    ///
    /// <para><b>Limitations:</b></para>
    /// <list type="bullet">
    ///   <item><description>Does not handle nested block comments (<c>/* /* */ */</c>).</description></item>
    ///   <item><description>Only reads up to 8 characters for the keyword (sufficient for all
    ///     known DDL keywords; the longest is TRUNCATE at 8 characters).</description></item>
    ///   <item><description>Does not detect DDL embedded in CTEs or subqueries (e.g.,
    ///     <c>WITH ... AS (...) CREATE ...</c>), but such patterns are extremely rare.</description></item>
    /// </list>
    /// </summary>
    public static class DdlDetector
    {
        /// <summary>
        /// Keywords that identify a DDL statement. All entries are uppercase (matching
        /// is done after <see cref="string.ToUpperInvariant"/>).
        ///
        /// <para><b>Keyword justifications:</b></para>
        /// <list type="bullet">
        ///   <item><description><c>CREATE</c>: Creates tables, views, indexes, sequences, etc.</description></item>
        ///   <item><description><c>DROP</c>: Drops tables, views, indexes, etc.</description></item>
        ///   <item><description><c>ALTER</c>: Modifies table schema (add/drop/rename columns).</description></item>
        ///   <item><description><c>TRUNCATE</c>: Removes all rows from a table (DDL semantics in DuckDB).</description></item>
        ///   <item><description><c>ATTACH</c>/<c>DETACH</c>: Attaches/detaches external databases.</description></item>
        ///   <item><description><c>VACUUM</c>: Reclaims storage space (modifies file structure).</description></item>
        ///   <item><description><c>PRAGMA</c>: DuckDB configuration commands (often modify database state).</description></item>
        ///   <item><description><c>COPY</c>: Bulk data import/export (modifies table data but has DDL-like semantics).</description></item>
        ///   <item><description><c>EXPORT</c>/<c>IMPORT</c>: Database-level export/import operations.</description></item>
        ///   <item><description><c>LOAD</c>: Loads DuckDB extensions.</description></item>
        /// </list>
        /// </summary>
        private static readonly string[] DdlKeywords =
        {
            "CREATE", "DROP", "ALTER", "TRUNCATE",
            "ATTACH", "DETACH", "VACUUM", "PRAGMA",
            "COPY", "EXPORT", "IMPORT", "LOAD"
        };

        /// <summary>
        /// Returns <c>true</c> if the SQL statement is DDL (should be executed outside
        /// a transaction), or <c>false</c> if it is DML (can be batched in a transaction).
        /// </summary>
        /// <param name="sql">The SQL statement to classify.</param>
        /// <returns><c>true</c> for DDL statements, <c>false</c> for DML or unrecognized.</returns>
        public static bool IsDdl(string sql)
        {
            string firstKeyword = ExtractFirstKeyword(sql);
            if (firstKeyword == null) return false;

            // Linear scan of the keyword list. With 12 keywords, this is faster than
            // a HashSet (avoids allocation and hashing overhead for short strings).
            for (int i = 0; i < DdlKeywords.Length; i++)
            {
                if (firstKeyword == DdlKeywords[i])
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Extracts the first SQL keyword from the string (up to 8 characters, uppercased).
        /// Skips leading whitespace, single-line comments (<c>-- ...</c>), and block
        /// comments (<c>/* ... */</c>).
        /// </summary>
        /// <param name="sql">The SQL string to extract the keyword from.</param>
        /// <returns>
        /// The first keyword uppercased (up to 8 chars), or <c>null</c> if the string
        /// is empty, all whitespace, or all comments.
        /// </returns>
        private static string ExtractFirstKeyword(string sql)
        {
            int pos = 0;

            // Skip leading whitespace and comments. This loop handles multiple
            // consecutive comments (e.g., "-- comment\n/* block */\nCREATE ...").
            pos = SkipWhitespaceAndComments(sql, pos);

            // Read up to 8 alphabetic characters as the keyword.
            // 8 is sufficient because the longest DDL keyword is TRUNCATE (8 chars).
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

        /// <summary>
        /// Skips whitespace characters starting at the given position.
        /// </summary>
        /// <param name="sql">The SQL string.</param>
        /// <param name="pos">Starting position.</param>
        /// <returns>Position of the first non-whitespace character.</returns>
        private static int SkipWhitespace(string sql, int pos)
        {
            while (pos < sql.Length && char.IsWhiteSpace(sql[pos]))
                pos++;
            return pos;
        }

        /// <summary>
        /// Skips all leading whitespace, single-line comments (<c>--</c>), and block
        /// comments (<c>/* ... */</c>). Handles multiple consecutive comments by looping
        /// until a non-whitespace, non-comment character is found or the string is exhausted.
        ///
        /// <para><b>Comment types handled:</b></para>
        /// <list type="bullet">
        ///   <item><description>
        ///     <b>Single-line (<c>--</c>):</b> Skips from <c>--</c> to the end of the line
        ///     (newline character). Common in SQL scripts and generated SQL.
        ///   </description></item>
        ///   <item><description>
        ///     <b>Block (<c>/* ... */</c>):</b> Skips from <c>/*</c> to the matching <c>*/</c>.
        ///     Does NOT handle nested block comments. An unterminated block comment causes
        ///     the position to advance to the end of the string, effectively treating the
        ///     entire remaining string as a comment.
        ///   </description></item>
        /// </list>
        /// </summary>
        /// <param name="sql">The SQL string.</param>
        /// <param name="pos">Starting position.</param>
        /// <returns>Position of the first non-whitespace, non-comment character.</returns>
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

                // Skip single-line comment: -- ... (to end of line)
                if (pos + 1 < sql.Length && sql[pos] == '-' && sql[pos + 1] == '-')
                {
                    pos = SkipToEndOfLine(sql, pos);
                    continue;
                }

                // Skip block comment: /* ... */
                if (pos + 1 < sql.Length && sql[pos] == '/' && sql[pos + 1] == '*')
                {
                    pos += 2; // Skip past the opening "/*"
                    bool closed = false;
                    while (pos + 1 < sql.Length)
                    {
                        if (sql[pos] == '*' && sql[pos + 1] == '/')
                        {
                            pos += 2; // Skip past the closing "*/"
                            closed = true;
                            break;
                        }
                        pos++;
                    }
                    // Unterminated block comment -- treat rest of string as comment.
                    if (!closed) pos = sql.Length;
                    continue;
                }

                // Not whitespace or comment -- stop. This is where the first keyword starts.
                break;
            }
            return pos;
        }

        /// <summary>
        /// Advances the position past the end of the current line (past the newline character).
        /// Used to skip single-line comments (<c>-- ...</c>).
        /// </summary>
        /// <param name="sql">The SQL string.</param>
        /// <param name="pos">Starting position (should be at the <c>--</c>).</param>
        /// <returns>Position after the newline, or <c>sql.Length</c> if no newline found.</returns>
        private static int SkipToEndOfLine(string sql, int pos)
        {
            while (pos < sql.Length && sql[pos] != '\n')
                pos++;
            if (pos < sql.Length) pos++; // Skip past the newline itself.
            return pos;
        }
    }
}
