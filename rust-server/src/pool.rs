//! DuckDB connection pool using r2d2-duckdb.

use r2d2::Pool;
use r2d2_duckdb::DuckdbConnectionManager;

pub type DuckDbPool = Pool<DuckdbConnectionManager>;
pub type DuckDbConn = r2d2::PooledConnection<DuckdbConnectionManager>;

pub fn create_pool(db_path: &str, size: usize) -> Result<DuckDbPool, String> {
    let manager = DuckdbConnectionManager::file(db_path)
        .map_err(|e| format!("Failed to create manager: {}", e))?;

    Pool::builder()
        .max_size(size as u32)
        .build(manager)
        .map_err(|e| format!("Failed to build pool: {}", e))
}
