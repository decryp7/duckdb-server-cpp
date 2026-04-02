using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Thread-safe LRU query result cache.
    ///
    /// Caches protobuf QueryResponse objects keyed by SQL string.
    /// Cache hits bypass DuckDB entirely — near-zero latency.
    ///
    /// Design:
    ///   - ConcurrentDictionary for lock-free reads (common path)
    ///   - TTL-based expiration (stale data auto-evicts)
    ///   - Max entries cap (prevents unbounded memory growth)
    ///   - Write-through: writes invalidate affected cache entries
    ///
    /// Usage:
    ///   var cache = new QueryCache(maxEntries: 10000, ttlSeconds: 60);
    ///   if (cache.TryGet(sql, out responses)) return responses; // HIT
    ///   // ... execute query ...
    ///   cache.Put(sql, responses);
    /// </summary>
    public sealed class QueryCache : IQueryCache
    {
        private readonly ConcurrentDictionary<string, CacheEntry> entries;
        private readonly int maxEntries;
        private readonly int ttlSeconds;
        private long hits;
        private long misses;

        public QueryCache(int maxEntries = 10000, int ttlSeconds = 60)
        {
            this.maxEntries = maxEntries;
            this.ttlSeconds = ttlSeconds;
            entries = new ConcurrentDictionary<string, CacheEntry>();
        }

        /// <summary>
        /// Try to get cached query responses. Returns true on cache hit.
        /// </summary>
        public bool TryGet(string sql, out List<QueryResponse> responses)
        {
            responses = null;

            CacheEntry entry;
            if (!entries.TryGetValue(sql, out entry))
            {
                Interlocked.Increment(ref misses);
                return false;
            }

            // Check TTL
            if ((DateTime.UtcNow - entry.CreatedAt).TotalSeconds > ttlSeconds)
            {
                // Expired — remove and miss
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
        /// Cache query responses for a SQL string.
        /// </summary>
        public void Put(string sql, List<QueryResponse> responses)
        {
            // Don't cache if at capacity (simple eviction: skip new entries)
            if (entries.Count >= maxEntries)
                EvictExpired();

            if (entries.Count >= maxEntries)
                return; // still full after eviction, skip

            entries[sql] = new CacheEntry
            {
                Responses = responses,
                CreatedAt = DateTime.UtcNow,
            };
        }

        /// <summary>
        /// Invalidate all cache entries (called after writes).
        /// Simple but effective — any write clears the entire cache.
        /// </summary>
        public void Invalidate()
        {
            entries.Clear();
        }

        /// <summary>
        /// Invalidate entries matching a table name.
        /// More targeted than full invalidation.
        /// </summary>
        public void InvalidateTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                Invalidate();
                return;
            }

            string lower = tableName.ToLowerInvariant();
            var keysToRemove = new List<string>();

            foreach (var kvp in entries)
            {
                if (kvp.Key.ToLowerInvariant().Contains(lower))
                    keysToRemove.Add(kvp.Key);
            }

            CacheEntry removed;
            foreach (var key in keysToRemove)
                entries.TryRemove(key, out removed);
        }

        public long Hits { get { return Interlocked.Read(ref hits); } }
        public long Misses { get { return Interlocked.Read(ref misses); } }
        public int Count { get { return entries.Count; } }

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

        private sealed class CacheEntry
        {
            public List<QueryResponse> Responses;
            public DateTime CreatedAt;
        }
    }
}
