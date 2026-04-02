//! Thread-safe fixed-size DuckDB connection pool.
//!
//! Uses crossbeam::ArrayQueue (lock-free bounded MPMC queue).

use crossbeam::queue::ArrayQueue;
use duckdb::Connection;
use std::sync::Arc;

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

pub struct ConnectionPool {
    queue: Arc<ArrayQueue<Connection>>,
    db_path: String,
    size: usize,
}

impl ConnectionPool {
    pub fn new(db_path: &str, size: usize) -> Result<Self, String> {
        let queue = Arc::new(ArrayQueue::new(size));

        // Open the first connection (creates the database file)
        let first = Connection::open(db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;
        queue.push(first).map_err(|_| "Queue full".to_string())?;

        // Remaining connections share the same database via the file path
        for i in 1..size {
            let conn = Connection::open(db_path)
                .map_err(|e| format!("Failed to create connection {}/{}: {}", i + 1, size, e))?;
            queue.push(conn).map_err(|_| "Queue full".to_string())?;
        }

        Ok(Self {
            queue,
            db_path: db_path.to_string(),
            size,
        })
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
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

    pub fn size(&self) -> usize {
        self.size
    }
}
