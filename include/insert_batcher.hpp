#pragma once
/**
 * @file insert_batcher.hpp
 * @brief Merges multiple single-row INSERT statements into multi-row INSERTs.
 *
 * Why this is faster:
 *   - DuckDB parses and plans the query ONCE instead of N times.
 *   - The INSERT engine can batch-process rows more efficiently.
 *
 * Example:
 *   Input:
 *     INSERT INTO t VALUES (1, 'a')
 *     INSERT INTO t VALUES (2, 'b')
 *     INSERT INTO t VALUES (3, 'c')
 *
 *   Output:
 *     INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')
 */

#include <string>
#include <vector>
#include <cctype>
#include <cstring>

namespace das {

// ── Parsed INSERT result ─────────────────────────────────────────────────────

struct ParsedInsert {
    bool is_simple;         // true if we can merge this INSERT
    std::string prefix;     // "INSERT INTO table" or "INSERT INTO table (cols)"
    std::string values_tuple; // "(1, 'a', 3.14)"
    std::string table_key;  // lowercase table name for grouping
};

// ── INSERT parser ────────────────────────────────────────────────────────────

inline bool match_keyword(const std::string& s, size_t pos,
                          const char* keyword, size_t& new_pos) {
    size_t klen = std::strlen(keyword);
    if (pos + klen > s.size()) return false;

    for (size_t i = 0; i < klen; ++i) {
        char c = s[pos + i];
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 32); // toupper
        if (c != keyword[i]) return false;
    }

    size_t after = pos + klen;
    if (after < s.size() && !std::isspace(static_cast<unsigned char>(s[after]))
        && s[after] != '(')
        return false;

    new_pos = after;
    return true;
}

inline size_t skip_spaces(const std::string& s, size_t pos) {
    while (pos < s.size() && std::isspace(static_cast<unsigned char>(s[pos])))
        ++pos;
    return pos;
}

inline ParsedInsert try_parse_insert(const std::string& sql) {
    ParsedInsert result;
    result.is_simple = false;

    if (sql.size() < 20) return result;

    size_t pos = skip_spaces(sql, 0);

    // Match INSERT
    if (!match_keyword(sql, pos, "INSERT", pos)) return result;
    pos = skip_spaces(sql, pos);

    // Match INTO
    if (!match_keyword(sql, pos, "INTO", pos)) return result;
    pos = skip_spaces(sql, pos);

    // Read table name (may be quoted with double quotes)
    size_t table_start = pos;
    if (pos < sql.size() && sql[pos] == '"') {
        // Quoted identifier: skip to closing quote.
        ++pos;
        while (pos < sql.size()) {
            if (sql[pos] == '"') {
                if (pos + 1 < sql.size() && sql[pos + 1] == '"')
                    pos += 2; // escaped double-quote
                else
                    { ++pos; break; }
            } else {
                ++pos;
            }
        }
    } else {
        while (pos < sql.size() && !std::isspace(static_cast<unsigned char>(sql[pos]))
               && sql[pos] != '(')
            ++pos;
    }
    if (pos == table_start) return result;

    std::string table_name = sql.substr(table_start, pos - table_start);
    pos = skip_spaces(sql, pos);

    // Skip optional column list
    if (pos < sql.size() && sql[pos] == '(') {
        int depth = 0;
        while (pos < sql.size()) {
            if (sql[pos] == '(') ++depth;
            else if (sql[pos] == ')') { --depth; if (depth == 0) { ++pos; break; } }
            ++pos;
        }
        pos = skip_spaces(sql, pos);
    }

    // Match VALUES
    size_t values_pos = pos;
    if (!match_keyword(sql, pos, "VALUES", pos)) return result;
    pos = skip_spaces(sql, pos);

    // Prefix is everything before VALUES
    std::string prefix = sql.substr(0, values_pos);
    // Trim trailing whitespace
    while (!prefix.empty() && std::isspace(static_cast<unsigned char>(prefix.back())))
        prefix.pop_back();

    // Values tuple is the rest
    std::string values_part = sql.substr(pos);
    // Trim trailing whitespace and semicolons
    while (!values_part.empty() &&
           (std::isspace(static_cast<unsigned char>(values_part.back()))
            || values_part.back() == ';'))
        values_part.pop_back();

    // Must start with ( and end with )
    if (values_part.size() < 2 || values_part.front() != '('
        || values_part.back() != ')')
        return result;

    // Check it's a single tuple (not already multi-row)
    int depth = 0;
    for (size_t i = 0; i < values_part.size(); ++i) {
        char c = values_part[i];
        if (c == '\'') {
            ++i;
            while (i < values_part.size()) {
                if (values_part[i] == '\'' && i + 1 < values_part.size()
                    && values_part[i + 1] == '\'')
                    i += 2;
                else if (values_part[i] == '\'') break;
                else ++i;
            }
        } else if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
            if (depth == 0 && i < values_part.size() - 1) {
                size_t next = skip_spaces(values_part, i + 1);
                if (next < values_part.size() && values_part[next] == ',')
                    return result; // multi-tuple, can't merge
            }
        }
    }

    // Build table key (lowercase)
    result.table_key = table_name;
    for (size_t i = 0; i < result.table_key.size(); ++i) {
        char& c = result.table_key[i];
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
    }

    result.is_simple = true;
    result.prefix = prefix;
    result.values_tuple = values_part;
    return result;
}

// ── Batch group ──────────────────────────────────────────────────────────────

struct BatchGroup {
    std::string sql;
    std::vector<size_t> request_indices; // indices into the original batch
    bool is_merged;
};

// ── Merge function ───────────────────────────────────────────────────────────

inline std::vector<BatchGroup> merge_inserts(
    const std::vector<std::string>& statements)
{
    std::vector<BatchGroup> groups;
    std::vector<ParsedInsert> parsed(statements.size());

    for (size_t i = 0; i < statements.size(); ++i)
        parsed[i] = try_parse_insert(statements[i]);

    size_t pos = 0;
    while (pos < statements.size()) {
        if (!parsed[pos].is_simple) {
            // Not a simple INSERT — emit as-is.
            BatchGroup g;
            g.sql = statements[pos];
            g.request_indices.push_back(pos);
            g.is_merged = false;
            groups.push_back(std::move(g));
            ++pos;
            continue;
        }

        // Find consecutive simple INSERTs to the same table with the same prefix.
        const std::string& prefix = parsed[pos].prefix;
        const std::string& table_key = parsed[pos].table_key;
        size_t run_start = pos;

        while (pos < statements.size()
               && parsed[pos].is_simple
               && parsed[pos].table_key == table_key
               && parsed[pos].prefix == prefix)
            ++pos;

        size_t run_length = pos - run_start;

        if (run_length == 1) {
            BatchGroup g;
            g.sql = statements[run_start];
            g.request_indices.push_back(run_start);
            g.is_merged = false;
            groups.push_back(std::move(g));
        } else {
            // Merge into multi-row INSERT.
            std::string merged = prefix + " VALUES ";
            BatchGroup g;
            for (size_t i = run_start; i < pos; ++i) {
                if (i > run_start) merged += ", ";
                merged += parsed[i].values_tuple;
                g.request_indices.push_back(i);
            }
            g.sql = std::move(merged);
            g.is_merged = true;
            groups.push_back(std::move(g));
        }
    }

    return groups;
}

} // namespace das
