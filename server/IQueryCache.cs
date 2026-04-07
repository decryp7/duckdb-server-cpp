using System.Collections.Generic;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Cache for serialized query results, keyed by SQL string. Implementations may be
    /// in-memory LRU (<see cref="QueryCache"/>), distributed (e.g., Redis), or no-op
    /// (for testing or when caching is not desired).
    ///
    /// <para><b>Thread Safety:</b> Implementations MUST be thread-safe. All methods may
    /// be called concurrently from multiple gRPC handler threads. The hot path is
    /// <see cref="TryGet"/>, which is called on every query; it MUST be lock-free or
    /// use minimal synchronization for good performance under high concurrency.</para>
    ///
    /// <para><b>Contract:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="TryGet"/> MUST return <c>false</c> for expired or evicted entries.
    ///     It MUST NOT return stale data.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="Put"/> MAY silently drop entries if the cache is full (write-skip
    ///     eviction). It MUST NOT throw exceptions.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="Invalidate"/> MUST remove all entries immediately. It is called
    ///     after every write operation to prevent stale reads.
    ///   </description></item>
    ///   <item><description>
    ///     The cached <see cref="QueryResponse"/> objects are shared references. Callers
    ///     MUST NOT modify them after caching.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Usage pattern:</b></para>
    /// <code>
    /// List&lt;QueryResponse&gt; cached;
    /// if (cache.TryGet(sql, out cached))
    /// {
    ///     // Cache hit -- replay cached responses (near-zero latency).
    ///     foreach (var resp in cached) stream.WriteAsync(resp);
    ///     return;
    /// }
    /// // Cache miss -- execute query on DuckDB, then cache the result.
    /// var responses = ExecuteQuery(sql);
    /// cache.Put(sql, responses);
    /// </code>
    /// </summary>
    public interface IQueryCache
    {
        /// <summary>
        /// Attempts to retrieve cached query responses for the given SQL string.
        /// </summary>
        /// <param name="sql">The SQL query string (case-sensitive key).</param>
        /// <param name="responses">
        /// On cache hit, set to the list of cached <see cref="QueryResponse"/> messages.
        /// On cache miss, set to <c>null</c>.
        /// </param>
        /// <returns><c>true</c> on cache hit; <c>false</c> on miss or expiration.</returns>
        bool TryGet(string sql, out List<QueryResponse> responses);

        /// <summary>
        /// Stores query responses in the cache. May silently drop the entry if the cache
        /// is at capacity.
        /// </summary>
        /// <param name="sql">The SQL query string (case-sensitive key).</param>
        /// <param name="responses">
        /// The list of <see cref="QueryResponse"/> messages to cache. The caller MUST NOT
        /// modify these objects after calling Put.
        /// </param>
        void Put(string sql, List<QueryResponse> responses);

        /// <summary>
        /// Invalidates (removes) all cached entries. Called after any write operation to
        /// ensure subsequent reads reflect the latest data.
        /// </summary>
        void Invalidate();

        /// <summary>
        /// Invalidates cache entries whose SQL key contains the specified table name
        /// (case-insensitive substring match). If tableName is null or empty, falls back
        /// to full invalidation via <see cref="Invalidate"/>.
        /// </summary>
        /// <param name="tableName">The table name to match. Null/empty triggers full invalidation.</param>
        void InvalidateTable(string tableName);

        /// <summary>Total number of cache hits since creation. Used for monitoring.</summary>
        long Hits { get; }

        /// <summary>Total number of cache misses since creation. Used for monitoring.</summary>
        long Misses { get; }
    }
}
