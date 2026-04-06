using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Thread-safe in-memory query result cache with TTL-based expiration.
    ///
    /// <para><b>Thread Safety:</b> This class is fully thread-safe.
    /// <see cref="ConcurrentDictionary{TKey,TValue}"/> provides lock-free reads (the
    /// common path) and fine-grained locking for writes. Hit/miss counters use
    /// <see cref="Interlocked"/> for atomic updates.</para>
    ///
    /// <para><b>Design:</b></para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>ConcurrentDictionary:</b> Lock-free reads via per-bucket spinlocks. This is
    ///     critical because cache lookups happen on every query in the hot path.
    ///   </description></item>
    ///   <item><description>
    ///     <b>TTL-based expiration:</b> Each entry records its creation time. On read, if
    ///     the entry is older than <see cref="ttlSeconds"/>, it is removed and treated as
    ///     a miss. This provides passive eviction without background timers.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Max entries cap:</b> Prevents unbounded memory growth. When the cache is full,
    ///     expired entries are evicted first. If still full, new entries are dropped (write-skip).
    ///     This is simpler than true LRU but avoids the overhead of maintaining a linked list.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Write-through invalidation:</b> Any write operation clears the entire cache
    ///     via <see cref="Invalidate"/>. This is a broad but safe strategy that avoids the
    ///     complexity of tracking which queries are affected by which writes.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Usage:</b></para>
    /// <code>
    /// var cache = new QueryCache(maxEntries: 10000, ttlSeconds: 60);
    /// if (cache.TryGet(sql, out responses)) return responses; // HIT -- near-zero latency
    /// // ... execute query on DuckDB ...
    /// cache.Put(sql, responses); // store for future hits
    /// </code>
    ///
    /// <para><b>Memory:</b> Cached <see cref="QueryResponse"/> objects contain protobuf
    /// packed arrays. A cache with 10,000 entries where each result is ~1 KB uses ~10 MB.
    /// Large result sets (e.g., 1M rows) can consume significantly more memory per entry.</para>
    /// </summary>
    public sealed class QueryCache : IQueryCache
    {
        /// <summary>
        /// The backing store. Keys are SQL strings (case-sensitive); values are
        /// <see cref="CacheEntry"/> with the response list and creation timestamp.
        /// </summary>
        private readonly ConcurrentDictionary<string, CacheEntry> entries;

        /// <summary>Maximum number of entries. Prevents unbounded memory growth.</summary>
        private readonly int maxEntries;

        /// <summary>
        /// Time-to-live in seconds. Entries older than this are considered expired
        /// and are removed on access (<see cref="TryGet"/>) or during eviction
        /// (<see cref="EvictExpired"/>).
        /// </summary>
        private readonly int ttlSeconds;

        /// <summary>Total cache hits (atomic counter).</summary>
        private long hits;

        /// <summary>Total cache misses (atomic counter).</summary>
        private long misses;

        /// <summary>
        /// Creates a new query cache with the specified capacity and TTL.
        /// </summary>
        /// <param name="maxEntries">
        /// Maximum number of cached query results. When full, expired entries are
        /// evicted first; if still full, new entries are dropped. Recommended: 1,000-50,000
        /// depending on average result set size and available memory.
        /// </param>
        /// <param name="ttlSeconds">
        /// Time-to-live for cached entries in seconds. After this duration, entries are
        /// considered stale and removed on next access. Lower values ensure fresher data;
        /// higher values improve hit rate. Recommended: 30-300 seconds.
        /// </param>
        public QueryCache(int maxEntries = 10000, int ttlSeconds = 60)
        {
            this.maxEntries = maxEntries;
            this.ttlSeconds = ttlSeconds;
            entries = new ConcurrentDictionary<string, CacheEntry>();
        }

        /// <summary>
        /// Attempts to retrieve cached query responses for the given SQL string.
        ///
        /// <para><b>TTL Check:</b> If the entry exists but has expired (older than
        /// <see cref="ttlSeconds"/>), it is removed from the cache and the method
        /// returns <c>false</c> (cache miss). This provides passive eviction without
        /// background cleanup threads.</para>
        ///
        /// <para><b>Race condition note:</b> Between <c>TryGetValue</c> and
        /// <c>TryRemove</c>, another thread may have already removed or updated the
        /// entry. This is harmless: the expired entry is either already gone or will
        /// be replaced. The miss counter may be slightly inflated, but this is
        /// acceptable for monitoring purposes.</para>
        /// </summary>
        /// <param name="sql">The SQL string to look up (case-sensitive).</param>
        /// <param name="responses">
        /// On cache hit, set to the list of <see cref="QueryResponse"/> objects.
        /// On cache miss, set to <c>null</c>.
        /// </param>
        /// <returns><c>true</c> on cache hit, <c>false</c> on miss or expiration.</returns>
        public bool TryGet(string sql, out List<QueryResponse> responses)
        {
            responses = null;

            CacheEntry entry;
            if (!entries.TryGetValue(sql, out entry))
            {
                Interlocked.Increment(ref misses);
                return false;
            }

            // Check TTL: reject entries older than ttlSeconds.
            if ((DateTime.UtcNow - entry.CreatedAt).TotalSeconds > ttlSeconds)
            {
                // Expired -- remove and count as a miss.
                CacheEntry removed;
                entries.TryRemove(sql, out removed);
                Interlocked.Increment(ref misses);
                return false;
            }

            responses = entry.Responses;
            Interlocked.Increment(ref hits);
            return true;
        }

        /// <summary>
        /// Stores query responses in the cache, keyed by the SQL string.
        ///
        /// <para><b>Eviction strategy:</b> If the cache is at capacity, first attempts
        /// to evict expired entries. If the cache is still full after eviction, the new
        /// entry is silently dropped (write-skip). This is simpler than true LRU eviction
        /// and avoids the overhead of maintaining access timestamps or a linked list.</para>
        ///
        /// <para><b>Gotcha:</b> The <c>entries.Count</c> property on
        /// <see cref="ConcurrentDictionary{TKey,TValue}"/> iterates all buckets and is
        /// O(N). Under high concurrency, this can be a bottleneck. In practice, the
        /// Put path is much less frequent than TryGet (only on cache misses), so this
        /// is acceptable.</para>
        /// </summary>
        /// <param name="sql">The SQL string to use as the cache key.</param>
        /// <param name="responses">
        /// The list of <see cref="QueryResponse"/> objects to cache. These are shared
        /// references -- the caller must not modify them after caching.
        /// </param>
        public void Put(string sql, List<QueryResponse> responses)
        {
            // Don't cache if at capacity. Try evicting expired entries first.
            if (entries.Count >= maxEntries)
                EvictExpired();

            if (entries.Count >= maxEntries)
                return; // Still full after eviction -- skip this entry.

            entries[sql] = new CacheEntry
            {
                Responses = responses,
                CreatedAt = DateTime.UtcNow,
            };
        }

        /// <summary>
        /// Invalidates all cache entries. Called after any write operation to prevent
        /// stale reads.
        ///
        /// <para><b>Why clear everything?</b> Determining which cached queries are
        /// affected by an arbitrary DML/DDL statement requires SQL parsing and dependency
        /// tracking, which is complex and error-prone. Clearing the entire cache is simple,
        /// correct, and fast (<c>ConcurrentDictionary.Clear</c> is O(N) but completes
        /// quickly in practice because it only nulls bucket heads).</para>
        /// </summary>
        public void Invalidate()
        {
            entries.Clear();
        }

        /// <summary>
        /// Invalidates cache entries whose SQL key contains the specified table name
        /// (case-insensitive substring match).
        ///
        /// <para><b>Algorithm:</b> Scans all cache keys, converts to lowercase, and
        /// checks if the key contains the table name. This is O(N) where N is the number
        /// of cache entries. Used for targeted invalidation when the modified table is
        /// known.</para>
        ///
        /// <para><b>Gotcha:</b> Substring matching can produce false positives. For example,
        /// invalidating table "users" also removes cached queries containing "users_archive"
        /// or column aliases like "num_users". This is a conservative (safe) strategy:
        /// over-invalidation causes cache misses but never stale reads.</para>
        /// </summary>
        /// <param name="tableName">
        /// The table name to match (case-insensitive). If null or empty, falls back to
        /// full cache invalidation via <see cref="Invalidate"/>.
        /// </param>
        public void InvalidateTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                Invalidate();
                return;
            }

            string lower = tableName.ToLowerInvariant();
            var keysToRemove = new List<string>();

            // Collect keys to remove (cannot modify dictionary during enumeration).
            foreach (var kvp in entries)
            {
                if (kvp.Key.ToLowerInvariant().Contains(lower))
                    keysToRemove.Add(kvp.Key);
            }

            CacheEntry removed;
            foreach (var key in keysToRemove)
                entries.TryRemove(key, out removed);
        }

        /// <summary>Total number of cache hits. Read atomically via <see cref="Interlocked.Read"/>.</summary>
        public long Hits { get { return Interlocked.Read(ref hits); } }

        /// <summary>Total number of cache misses. Read atomically via <see cref="Interlocked.Read"/>.</summary>
        public long Misses { get { return Interlocked.Read(ref misses); } }

        /// <summary>Current number of entries in the cache (O(N) on ConcurrentDictionary).</summary>
        public int Count { get { return entries.Count; } }

        /// <summary>
        /// Removes all expired entries from the cache. Called by <see cref="Put"/> when
        /// the cache is at capacity, as a lightweight eviction pass.
        ///
        /// <para><b>Concurrency:</b> Safe to call concurrently. Each TryRemove is atomic.
        /// Between the scan and the removal, entries may have been updated by other threads;
        /// TryRemove simply does nothing if the key is already gone.</para>
        /// </summary>
        private void EvictExpired()
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            foreach (var kvp in entries)
            {
                if ((now - kvp.Value.CreatedAt).TotalSeconds > ttlSeconds)
                    keysToRemove.Add(kvp.Key);
            }

            CacheEntry removed;
            foreach (var key in keysToRemove)
                entries.TryRemove(key, out removed);
        }

        /// <summary>
        /// Internal cache entry storing the query responses and creation timestamp.
        /// The creation timestamp is used for TTL-based expiration.
        /// </summary>
        private sealed class CacheEntry
        {
            /// <summary>The cached protobuf response messages for this query.</summary>
            public List<QueryResponse> Responses;

            /// <summary>UTC timestamp when this entry was created. Used for TTL expiration.</summary>
            public DateTime CreatedAt;
        }
    }
}
