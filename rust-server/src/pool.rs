//! Thread-safe DuckDB connection pool.
//!
//! Uses a single shared Connection behind Arc<Mutex> with a semaphore.
//! DuckDB handles concurrent reads internally on the same connection.

use duckdb::Connection;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

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
                values.push(row.get::<_, Option<String>>(i).unwrap_or(None));
            }
            Ok(values)
        }).map_err(|e| e.to_string())?;
        let mut result = Vec::new();
        for row in rows { result.push(row.map_err(|e| e.to_string())?); }
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

pub struct ConnectionPool {
    conn: Arc<Mutex<Connection>>,
    available: Arc<(Mutex<usize>, Condvar)>,
    size: usize,
}

impl ConnectionPool {
    pub fn from_shared(conn: Arc<Mutex<Connection>>, size: usize) -> Self {
        Self {
            conn,
            available: Arc::new((Mutex::new(size), Condvar::new())),
            size,
        }
    }

    pub fn borrow(&self) -> Result<Handle, String> {
        let (lock, cvar) = &*self.available;
        let mut available = lock.lock().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while *available == 0 {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() {
                return Err(format!("Pool timed out (size={})", self.size));
            }
            available = cvar.wait_timeout(available, timeout).unwrap().0;
        }
        *available -= 1;
        Ok(Handle {
            conn: self.conn.clone(),
            done_signal: self.available.clone(),
        })
    }

    pub fn size(&self) -> usize { self.size }
}
