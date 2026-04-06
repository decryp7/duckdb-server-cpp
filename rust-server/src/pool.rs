//! # DuckDB Connection Pool using `Connection::try_clone()`
//!
//! Fixed-size, lock-free connection pool for concurrent DuckDB read queries.
//!
//! ## Why try_clone() instead of Connection::open()?
//!
//! `Connection::open()` opens a new database file and acquires an exclusive lock.
//! Calling it N times would fail with "database is locked" errors.
//! `Connection::try_clone()` calls `duckdb_connect()` on the SAME underlying
//! `duckdb_database` handle, creating N connections that share the same database.
//!
//! ## Data Structure: crossbeam ArrayQueue
//!
//! ArrayQueue is a bounded, lock-free, multi-producer/multi-consumer queue.
//! It outperforms Mutex<VecDeque> under high contention because it uses
//! atomic CAS operations instead of kernel-level locks.
//!
//! ## Borrow Strategy: spin + yield
//!
//! Fast path: pop from queue (lock-free, ~10ns).
//! Slow path: spin with `thread::yield_now()` until a connection is returned
//! or the 10-second deadline expires. yield_now() is `sched_yield()` on Linux
//! which gives up the CPU time slice but doesn't sleep.
//!
//! ## Per-Connection PRAGMAs
//!
//! Every connection gets: SET threads=1, preserve_insertion_order=false,
//! PRAGMA enable_object_cache, SET checkpoint_threshold='256MB'.
//! These are per-connection settings in DuckDB — they must be applied to EACH connection.
//!
//! See: https://github.com/duckdb/duckdb-rs/issues/378

use crossbeam::queue::ArrayQueue;
use duckdb::Connection;
use std::sync::Arc;

pub struct Handle {
    conn: Option<Connection>,
    queue: Arc<ArrayQueue<Connection>>,
}

impl std::ops::Deref for Handle {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = self.queue.push(conn);
        }
    }
}

pub struct ConnectionPool {
    queue: Arc<ArrayQueue<Connection>>,
    size: usize,
}

impl ConnectionPool {
    pub fn new(db_path: &str, size: usize) -> Result<Self, String> {
        let queue = Arc::new(ArrayQueue::new(size));

        // Open database ONCE
        let first = Connection::open(db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        // Create N-1 clones (each calls duckdb_connect internally)
        for i in 1..size {
            let clone = first.try_clone()
                .map_err(|e| format!("Failed to clone connection {}/{}: {}", i + 1, size, e))?;
            Self::apply_connection_pragmas(&clone);
            queue.push(clone).map_err(|_| "Queue full".to_string())?;
        }

        // Push the original as the last one
        Self::apply_connection_pragmas(&first);
        queue.push(first).map_err(|_| "Queue full".to_string())?;

        Ok(Self { queue, size })
    }

    /// Apply per-connection performance settings.
    /// threads=1 eliminates internal contention (pool provides parallelism).
    /// preserve_insertion_order=false speeds up scans without ORDER BY.
    /// enable_object_cache caches metadata for faster lookups.
    fn apply_connection_pragmas(conn: &Connection) {
        let _ = conn.execute_batch("SET threads=1");
        let _ = conn.execute_batch("SET preserve_insertion_order=false");
        let _ = conn.execute_batch("PRAGMA enable_object_cache");
        let _ = conn.execute_batch("SET checkpoint_threshold='256MB'");
        let _ = conn.execute_batch("SET late_materialization_max_rows=1000");
        let _ = conn.execute_batch("SET allocator_flush_threshold='128MB'");
    }

    pub fn borrow(&self) -> Result<Handle, String> {
        // Fast path
        if let Some(conn) = self.queue.pop() {
            return Ok(Handle { conn: Some(conn), queue: self.queue.clone() });
        }

        // Spin + yield
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            std::thread::yield_now();
            if let Some(conn) = self.queue.pop() {
                return Ok(Handle { conn: Some(conn), queue: self.queue.clone() });
            }
            if std::time::Instant::now() > deadline {
                return Err(format!("Pool timed out (size={}). Increase --readers.", self.size));
            }
        }
    }

    pub fn size(&self) -> usize { self.size }
}
