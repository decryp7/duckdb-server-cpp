//! Thread-safe fixed-size DuckDB connection pool.
//!
//! Opens the database ONCE, then creates multiple connections from it.
//! Uses crossbeam::ArrayQueue (lock-free bounded MPMC queue).

use crossbeam::queue::ArrayQueue;
use duckdb::{Connection, Database};
use std::sync::Arc;

/// RAII handle that returns the connection to the pool on drop.
pub struct Handle {
    conn: Option<Connection>,
    pool: Arc<ArrayQueue<Connection>>,
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
            let _ = self.pool.push(conn);
        }
    }
}

/// Lock-free connection pool using crossbeam ArrayQueue.
pub struct ConnectionPool {
    queue: Arc<ArrayQueue<Connection>>,
    db: Database,
    size: usize,
}

impl ConnectionPool {
    pub fn new(db_path: &str, size: usize) -> Result<Self, String> {
        let queue = Arc::new(ArrayQueue::new(size));

        // Open the database ONCE
        let db = Database::open(db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        // Create multiple connections from the same database handle
        for i in 0..size {
            let conn = db.connect()
                .map_err(|e| format!("Failed to create connection {}/{}: {}", i + 1, size, e))?;
            queue.push(conn)
                .map_err(|_| "Queue full during init".to_string())?;
        }

        Ok(Self { queue, db, size })
    }

    pub fn borrow(&self) -> Result<Handle, String> {
        if let Some(conn) = self.queue.pop() {
            return Ok(Handle {
                conn: Some(conn),
                pool: self.queue.clone(),
            });
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            std::thread::yield_now();
            if let Some(conn) = self.queue.pop() {
                return Ok(Handle {
                    conn: Some(conn),
                    pool: self.queue.clone(),
                });
            }
            if std::time::Instant::now() > deadline {
                return Err(format!(
                    "ConnectionPool: timed out (pool_size={}). Increase --readers.",
                    self.size
                ));
            }
        }
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
