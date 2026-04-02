//! DuckDB connection pool using r2d2.
//!
//! Opens the database ONCE, creates N connections via try_clone() internally.
//! Each connection can run queries independently — true concurrent reads.

use duckdb::r2d2::DuckdbConnectionManager;
use r2d2::{Pool, PooledConnection};

pub type DuckDbPool = Pool<DuckdbConnectionManager>;
pub type DuckDbConn = PooledConnection<DuckdbConnectionManager>;

/// Create an r2d2 connection pool for DuckDB.
pub fn create_pool(db_path: &str, size: usize) -> Result<DuckDbPool, String> {
    let manager = if db_path == ":memory:" {
        DuckdbConnectionManager::memory()
            .map_err(|e| format!("Failed to create memory manager: {}", e))?
    } else {
        DuckdbConnectionManager::file(db_path)
            .map_err(|e| format!("Failed to create file manager: {}", e))?
    };

    Pool::builder()
        .max_size(size as u32)
        .build(manager)
        .map_err(|e| format!("Failed to build pool: {}", e))
}
