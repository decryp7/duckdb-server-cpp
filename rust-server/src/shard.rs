//! # Sharded DuckDB: Read-All / Write-All Strategy
//!
//! Manages N independent DuckDB instances for horizontal read scaling.
//!
//! ## Strategy
//!
//! - **READ**: Round-robin across all shards via atomic counter. Each shard has a
//!   full copy of the data, so any shard can serve any query. N shards = N× read throughput.
//!
//! - **WRITE**: Fan-out to ALL shards in parallel using `std::thread::scope`. Every shard
//!   executes the same write, keeping data consistent. Write throughput = single shard speed.
//!
//! ## Shard Path Generation
//!
//! For file-based databases, each shard gets a unique file: `data_0.duckdb`, `data_1.duckdb`.
//! For `:memory:` databases, each shard is an independent in-memory instance.
//!
//! ## Thread Safety
//!
//! `next_for_read()` uses `AtomicUsize` with relaxed ordering (exact distribution doesn't matter).
//! `write_to_all()` uses scoped threads for parallel fan-out with automatic join.

use crate::pool::ConnectionPool;
use crate::writer::WriteSerializer;
use duckdb::Connection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct Shard {
    pub pool: Arc<ConnectionPool>,
    pub writer: Arc<WriteSerializer>,
    /// Dedicated connection for BulkInsert, bypassing the WriteSerializer queue.
    /// Protected by Mutex since DuckDB connections are not thread-safe.
    pub bulk_conn: Mutex<Connection>,
}

pub struct ShardedDuckDb {
    shards: Vec<Shard>,
    next_read: AtomicUsize,
    /// Index of the first shard eligible for reads.
    /// 0 in normal mode, 1 in hybrid mode (skips the file-based backup shard).
    read_start_index: usize,
}

impl ShardedDuckDb {
    pub fn new(db_path: &str, shard_count: usize, readers_per_shard: usize,
               batch_ms: u64, batch_max: usize, temp_dir: &str, backup_db: &str) -> Result<Self, String>
    {
        let hybrid_mode = !backup_db.is_empty();
        let memory_shard_count = std::cmp::max(1, shard_count);
        let total_shard_count = if hybrid_mode { memory_shard_count + 1 } else { memory_shard_count };
        let read_start_index = if hybrid_mode { 1 } else { 0 };
        let readable_shards = if hybrid_mode { memory_shard_count } else { total_shard_count };
        let readers_per_shard = std::cmp::max(1, readers_per_shard);

        let mut shards = Vec::with_capacity(total_shard_count);

        for i in 0..total_shard_count {
            let path = if hybrid_mode && i == 0 {
                backup_db.to_string() // File DB for durability
            } else if hybrid_mode {
                ":memory:".to_string() // Memory DB for speed
            } else {
                shard_path(db_path, i, total_shard_count)
            };

            // File shard (index 0 in hybrid) gets minimal readers since it's write-only
            let pool_size = if hybrid_mode && i == 0 { 1 } else { readers_per_shard };
            let pool = Arc::new(ConnectionPool::new(&path, pool_size)?);

            let writer = {
                let conn = pool.borrow()?;
                // Apply database-wide settings (affect all connections on this database).
                // memory_limit prevents N shards each claiming 80% RAM.
                if total_shard_count > 1 {
                    let pct = std::cmp::max(10, 80 / total_shard_count);
                    let _ = conn.execute_batch(&format!("SET memory_limit='{}%'", pct));
                }
                if !temp_dir.is_empty() {
                    let _ = conn.execute_batch(&format!("SET temp_directory='{}'", temp_dir));
                }
                // IMPORTANT: Reset threads=1 before conn returns to pool.
                let _ = conn.execute_batch("SET threads=1");
                Arc::new(WriteSerializer::from_conn(&conn, batch_ms, batch_max)?)
            };

            // Dedicated bulk connection for BulkInsert — bypasses WriteSerializer
            // queue/batch overhead for direct SQL execution on each shard.
            let bulk_conn = {
                let conn = pool.borrow()?;
                let cloned = conn.try_clone().map_err(|e| format!("Bulk clone: {}", e))?;
                let _ = cloned.execute_batch("SET threads=1");
                let _ = cloned.execute_batch("SET preserve_insertion_order=false");
                cloned
            };

            shards.push(Shard { pool, writer, bulk_conn: Mutex::new(bulk_conn) });
        }

        let result = Self {
            shards,
            next_read: AtomicUsize::new(0),
            read_start_index,
        };

        // Hybrid mode: sync existing tables from file DB to memory shards
        if hybrid_mode {
            result.sync_file_to_memory(backup_db)?;
            println!("[das] Hybrid mode: file backup at {}", backup_db);
        }

        println!("[das] Sharded DuckDB ({}):",
                 if hybrid_mode { "hybrid: file backup + memory shards" } else { "read-all / write-all" });
        println!("      total shards = {} ({})", total_shard_count,
                 if hybrid_mode { format!("1 file + {} memory", memory_shard_count) }
                 else { format!("all {}", total_shard_count) });
        println!("      readers      = {} per shard, {} total",
                 readers_per_shard, result.total_pool_size());

        Ok(result)
    }

    /// Round-robin shard for reads (skips file shard in hybrid mode).
    pub fn next_for_read(&self) -> &Shard {
        let idx = self.next_read.fetch_add(1, Ordering::Relaxed);
        let readable = self.shards.len() - self.read_start_index;
        &self.shards[self.read_start_index + (idx % readable)]
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

    /// Execute SQL directly on each shard's dedicated bulk connection.
    /// Bypasses the WriteSerializer queue/batch entirely for direct execution.
    /// Used by BulkInsert for lower latency (no queue wait, no transaction batching).
    pub fn bulk_execute_all(&self, sql: &str) -> Result<(), String> {
        if self.shards.len() == 1 {
            let conn = self.shards[0].bulk_conn.lock().unwrap_or_else(|e| e.into_inner());
            return conn.execute_batch(sql).map_err(|e| format!("Bulk exec: {}", e));
        }

        // Fan-out in parallel using scoped threads
        let results: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = self.shards.iter().map(|shard| {
                s.spawn(|| {
                    let conn = shard.bulk_conn.lock().unwrap_or_else(|e| e.into_inner());
                    conn.execute_batch(sql).map_err(|e| format!("Bulk exec: {}", e))
                })
            }).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for r in results {
            if let Err(e) = r { return Err(e); }
        }
        Ok(())
    }

    /// Sync existing tables from the file-based backup DB (shard 0) to all memory shards.
    /// Uses DuckDB ATTACH to read from the file DB and CREATE TABLE AS SELECT to copy data.
    fn sync_file_to_memory(&self, file_path: &str) -> Result<(), String> {
        // Get list of tables from file DB (shard 0)
        let tables: Vec<String> = {
            let conn = self.shards[0].pool.borrow()?;
            let mut stmt = conn.prepare(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).map_err(|e| format!("Hybrid sync query: {}", e))?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Hybrid sync rows: {}", e))?;
            rows.filter_map(|r| r.ok()).collect()
        };

        if tables.is_empty() {
            println!("[das] Hybrid sync: no tables in file DB (fresh start)");
            return Ok(());
        }

        println!("[das] Hybrid sync: copying {} tables from file DB to {} memory shards...",
                 tables.len(), self.shards.len() - 1);

        let escaped_path = file_path.replace('\'', "''");

        for s in 1..self.shards.len() {
            let conn = self.shards[s].pool.borrow()?;

            conn.execute_batch(&format!(
                "ATTACH '{}' AS backup_src (READ_ONLY)", escaped_path
            )).map_err(|e| format!("Hybrid sync ATTACH shard {}: {}", s, e))?;

            for table in &tables {
                let quoted = format!("\"{}\"", table.replace('"', "\"\""));
                let copy_sql = format!(
                    "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM backup_src.{}",
                    quoted, quoted
                );
                if let Err(e) = conn.execute_batch(&copy_sql) {
                    eprintln!("[das] Hybrid sync: failed to copy table '{}' to shard {}: {}",
                             table, s, e);
                }
            }

            conn.execute_batch("DETACH backup_src")
                .map_err(|e| format!("Hybrid sync DETACH shard {}: {}", s, e))?;
        }

        println!("[das] Hybrid sync: complete");
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
