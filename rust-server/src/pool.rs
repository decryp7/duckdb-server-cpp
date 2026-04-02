//! DuckDB connection pool using Connection::try_clone().
//!
//! Opens the database ONCE, creates N connections via try_clone().
//! Each cloned connection shares the same underlying database handle
//! (internally calls duckdb_connect on the same duckdb_database).
//! True concurrent reads — no mutex serialization.
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
            queue.push(clone).map_err(|_| "Queue full".to_string())?;
        }

        // Push the original as the last one
        queue.push(first).map_err(|_| "Queue full".to_string())?;

        Ok(Self { queue, size })
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
