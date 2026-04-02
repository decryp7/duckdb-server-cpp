//! Thread-safe DuckDB connection pool.
//!
//! For :memory: databases, opens N independent connections (each is isolated).
//! For file databases, uses Arc<Mutex<Connection>> with a semaphore for
//! concurrency control (DuckDB file locking prevents multiple opens).

use duckdb::Connection;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

/// RAII handle that signals the pool when dropped.
pub struct Handle {
    conn: Arc<Mutex<Connection>>,
    done_signal: Arc<(Mutex<usize>, Condvar)>,
}

impl Handle {
    pub fn execute_query(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let col_count = stmt.column_count();

        let rows = stmt.query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let val: Option<String> = row.get::<_, Option<String>>(i).unwrap_or(None);
                values.push(val);
            }
            Ok(values)
        }).map_err(|e| e.to_string())?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(|e| e.to_string())?);
        }
        Ok(result)
    }

    pub fn column_names(&self, sql: &str) -> Result<Vec<String>, String> {
        let conn = self.conn.lock().unwrap();
        let stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        Ok(stmt.column_names().iter().map(|s| s.to_string()).collect())
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(sql).map_err(|e| e.to_string())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.done_signal;
        let mut count = lock.lock().unwrap();
        *count += 1;
        cvar.notify_all();
    }
}

/// Connection pool with semaphore-based concurrency control.
pub struct ConnectionPool {
    connections: Vec<Arc<Mutex<Connection>>>,
    available: Arc<(Mutex<usize>, Condvar)>,
    next: Mutex<usize>,
    size: usize,
}

impl ConnectionPool {
    pub fn new(db_path: &str, size: usize) -> Result<Self, String> {
        // Open ONE connection — DuckDB allows concurrent reads on the same connection
        let conn = Connection::open(db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        // Wrap in Arc<Mutex> and share across all "slots"
        let shared = Arc::new(Mutex::new(conn));
        let connections: Vec<_> = (0..size).map(|_| shared.clone()).collect();

        Ok(Self {
            connections,
            available: Arc::new((Mutex::new(size), Condvar::new())),
            next: Mutex::new(0),
            size,
        })
    }

    pub fn borrow(&self) -> Result<Handle, String> {
        let (lock, cvar) = &*self.available;
        let mut available = lock.lock().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while *available == 0 {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() {
                return Err(format!("ConnectionPool: timed out (size={})", self.size));
            }
            let result = cvar.wait_timeout(available, timeout).unwrap();
            available = result.0;
        }

        *available -= 1;
        let mut next = self.next.lock().unwrap();
        let idx = *next % self.connections.len();
        *next = idx + 1;

        Ok(Handle {
            conn: self.connections[idx].clone(),
            done_signal: self.available.clone(),
        })
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
