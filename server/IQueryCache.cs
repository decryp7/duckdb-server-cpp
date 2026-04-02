using System.Collections.Generic;
using DuckDbProto;

namespace DuckDbServer
{
    /// <summary>
    /// Cache for query results. Implementations can be in-memory LRU,
    /// distributed (Redis), or no-op for testing.
    /// </summary>
    public interface IQueryCache
    {
        bool TryGet(string sql, out List<QueryResponse> responses);
        void Put(string sql, List<QueryResponse> responses);
        void Invalidate();
        long Hits { get; }
        long Misses { get; }
    }
}
