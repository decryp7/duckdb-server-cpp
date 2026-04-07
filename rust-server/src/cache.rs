//! # Thread-Safe Query Result Cache with TTL Expiration
//!
//! Caches serialized protobuf `QueryResponse` vectors keyed by SQL string.
//! Cache hits bypass DuckDB entirely — near-zero latency (~0.1ms).
//!
//! ## Design
//!
//! - **Mutex<HashMap>** (not RwLock): writes are frequent due to cache invalidation
//!   on every Execute/BulkInsert. Mutex is simpler and lock hold time is short.
//! - **Full invalidation on write**: any write clears entire cache. Simple and correct.
//! - **Passive TTL eviction**: expired entries checked on get(), evicted on put() when full.
//! - **Max 10K entries**: prevents unbounded memory growth.

use crate::proto::QueryResponse;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct CacheEntry {
    responses: Vec<QueryResponse>,
    created_at: Instant,
}

pub struct QueryCache {
    entries: Mutex<HashMap<String, CacheEntry>>,
    max_entries: usize,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl QueryCache {
    pub fn new(max_entries: usize, ttl_seconds: u64) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries,
            ttl: Duration::from_secs(ttl_seconds),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, sql: &str) -> Option<Vec<QueryResponse>> {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get(sql) {
            if entry.created_at.elapsed() < self.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.responses.clone());
            }
            // Expired — remove eagerly to prevent stale entry accumulation
        }
        // Remove the expired entry (if any) outside the borrow
        map.remove(sql);
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn put(&self, sql: String, responses: Vec<QueryResponse>) {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= self.max_entries {
            // Evict expired entries
            let now = Instant::now();
            map.retain(|_, v| now.duration_since(v.created_at) < self.ttl);
        }
        if map.len() < self.max_entries {
            map.insert(sql, CacheEntry {
                responses,
                created_at: Instant::now(),
            });
        }
    }

    /// Invalidate all entries (called after writes).
    pub fn invalidate(&self) {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Invalidate only cache entries whose SQL key contains the given table name
    /// (case-insensitive substring match). If table_name is empty, falls back to
    /// full invalidation.
    pub fn invalidate_table(&self, table_name: &str) {
        if table_name.is_empty() { self.invalidate(); return; }
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let lower = table_name.to_ascii_lowercase();
        map.retain(|key, _| !key.to_ascii_lowercase().contains(&lower));
    }

    pub fn hits(&self) -> u64 { self.hits.load(Ordering::Relaxed) }
    pub fn misses(&self) -> u64 { self.misses.load(Ordering::Relaxed) }
}
