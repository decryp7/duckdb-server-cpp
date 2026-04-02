//! Sharded DuckDB: read-all / write-all strategy.
//!
//! READ:  Round-robin across all shards (each has full data copy).
//! WRITE: Fan-out to ALL shards in parallel (every shard gets same write).
//!
//! N shards = N× read throughput, same write throughput.

use crate::pool::ConnectionPool;
use crate::writer::WriteSerializer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Shard {
    pub pool: Arc<ConnectionPool>,
    pub writer: Arc<WriteSerializer>,
}

pub struct ShardedDuckDb {
    shards: Vec<Shard>,
    next_read: AtomicUsize,
}

impl ShardedDuckDb {
    pub fn new(db_path: &str, shard_count: usize, readers_per_shard: usize,
               batch_ms: u64, batch_max: usize) -> Result<Self, String>
    {
        let shard_count = std::cmp::max(1, shard_count);
        let mut shards = Vec::with_capacity(shard_count);

        for i in 0..shard_count {
            let path = shard_path(db_path, i, shard_count);

            let pool = Arc::new(ConnectionPool::new(&path, readers_per_shard)?);

            // Apply DuckDB tuning on first connection
            {
                let conn = pool.borrow().map_err(|e| e)?;
                let _ = conn.execute_batch("SET threads=1");
                let _ = conn.execute_batch("PRAGMA enable_object_cache");
                let _ = conn.execute_batch("SET preserve_insertion_order=false");
                let _ = conn.execute_batch("SET checkpoint_threshold='256MB'");
            }

            let writer = {
                let conn = pool.borrow().map_err(|e| e)?;
                Arc::new(WriteSerializer::from_conn(&conn, batch_ms, batch_max)?)
            };

            shards.push(Shard { pool, writer });
        }

        println!("[das] Sharded DuckDB (read-all / write-all):");
        println!("      shards   = {}", shard_count);
        println!("      readers  = {} per shard, {} total",
                 readers_per_shard, readers_per_shard * shard_count);

        Ok(Self {
            shards,
            next_read: AtomicUsize::new(0),
        })
    }

    /// Round-robin shard for reads.
    pub fn next_for_read(&self) -> &Shard {
        let idx = self.next_read.fetch_add(1, Ordering::Relaxed);
        &self.shards[idx % self.shards.len()]
    }

    /// Fan-out write to ALL shards. Returns first error or Ok.
    pub fn write_to_all(&self, sql: &str) -> Result<(), String> {
        if self.shards.len() == 1 {
            return self.shards[0].writer.submit(sql);
        }

        // Fan-out in parallel using threads
        let results: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = self.shards.iter().map(|shard| {
                s.spawn(|| shard.writer.submit(sql))
            }).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for r in results {
            if let Err(e) = r { return Err(e); }
        }
        Ok(())
    }

    pub fn shard_count(&self) -> usize { self.shards.len() }

    pub fn total_pool_size(&self) -> usize {
        self.shards.iter().map(|s| s.pool.size()).sum()
    }
}

fn shard_path(base: &str, index: usize, count: usize) -> String {
    if count <= 1 { return base.to_string(); }
    if base.is_empty() || base == ":memory:" { return ":memory:".to_string(); }

    if let Some(dot) = base.rfind('.') {
        format!("{}_{}{}", &base[..dot], index, &base[dot..])
    } else {
        format!("{}_{}", base, index)
    }
}
