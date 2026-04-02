//! Write serializer: batches concurrent DML into single transactions.

use crate::proto::{ColumnData, ColumnMeta};
use duckdb::Connection;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

struct WriteRequest {
    sql: String,
    result_tx: mpsc::Sender<Result<(), String>>,
}

pub struct WriteSerializer {
    request_tx: Mutex<mpsc::Sender<WriteRequest>>,
    bulk_conn: Mutex<Connection>,
    _writer_thread: thread::JoinHandle<()>,
}

impl WriteSerializer {
    /// Create a writer. `source_conn` is cloned for the writer's own connections.
    pub fn from_conn(source_conn: &Connection, batch_ms: u64, batch_max: usize) -> Result<Self, String> {
        let write_conn = source_conn.try_clone()
            .map_err(|e| format!("Writer clone failed: {}", e))?;
        let bulk_conn = source_conn.try_clone()
            .map_err(|e| format!("Bulk clone failed: {}", e))?;

        let (tx, rx) = mpsc::channel::<WriteRequest>();

        let handle = thread::Builder::new()
            .name("duckdb-writer".into())
            .spawn(move || { drain_loop(write_conn, rx, batch_ms, batch_max); })
            .map_err(|e| format!("Writer thread failed: {}", e))?;

        Ok(Self {
            request_tx: Mutex::new(tx),
            bulk_conn: Mutex::new(bulk_conn),
            _writer_thread: handle,
        })
    }

    pub fn submit(&self, sql: &str) -> Result<(), String> {
        let (result_tx, result_rx) = mpsc::channel();
        self.request_tx.lock().unwrap()
            .send(WriteRequest { sql: sql.to_string(), result_tx })
            .map_err(|_| "Writer thread died".to_string())?;
        result_rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| "Write timed out".to_string())?
    }

    /// Optimization 6: Appender API — bypasses SQL parser entirely (10-100x faster)
    pub fn bulk_insert(&self, table: &str, _columns: &[ColumnMeta], data: &[ColumnData], row_count: usize) -> Result<usize, String> {
        let conn = self.bulk_conn.lock().unwrap();
        let col_count = data.len();
        if col_count == 0 || row_count == 0 { return Ok(0); }

        let mut appender = conn.appender(table)
            .map_err(|e| format!("Appender create: {}", e))?;

        for r in 0..row_count {
            let mut params: Vec<duckdb::types::Value> = Vec::with_capacity(col_count);
            for c in 0..col_count {
                let cd = &data[c];
                if cd.null_indices.contains(&(r as i32)) {
                    params.push(duckdb::types::Value::Null);
                } else if r < cd.int32_values.len() {
                    params.push(duckdb::types::Value::Int(cd.int32_values[r]));
                } else if r < cd.int64_values.len() {
                    params.push(duckdb::types::Value::BigInt(cd.int64_values[r]));
                } else if r < cd.double_values.len() {
                    params.push(duckdb::types::Value::Double(cd.double_values[r]));
                } else if r < cd.bool_values.len() {
                    params.push(duckdb::types::Value::Boolean(cd.bool_values[r]));
                } else if r < cd.string_values.len() {
                    params.push(duckdb::types::Value::Text(cd.string_values[r].clone()));
                } else {
                    params.push(duckdb::types::Value::Null);
                }
            }
            appender.append_row(duckdb::params_from_iter(params))
                .map_err(|e| format!("Appender row: {}", e))?;
        }
        appender.flush().map_err(|e| format!("Appender flush: {}", e))?;
        Ok(row_count)
    }
}

fn drain_loop(conn: Connection, rx: mpsc::Receiver<WriteRequest>, batch_ms: u64, batch_max: usize) {
    loop {
        let first = match rx.recv() { Ok(r) => r, Err(_) => return };
        let mut batch = vec![first];
        let deadline = std::time::Instant::now() + Duration::from_millis(batch_ms);
        while batch.len() < batch_max {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() { break; }
            match rx.recv_timeout(timeout) { Ok(req) => batch.push(req), Err(_) => break }
        }

        if conn.execute_batch("BEGIN").is_err() {
            for req in &batch { let _ = req.result_tx.send(conn.execute_batch(&req.sql).map_err(|e| e.to_string())); }
            continue;
        }
        let mut ok = true;
        for req in &batch { if conn.execute_batch(&req.sql).is_err() { ok = false; break; } }
        if !ok {
            let _ = conn.execute_batch("ROLLBACK");
            for req in &batch { let _ = req.result_tx.send(conn.execute_batch(&req.sql).map_err(|e| e.to_string())); }
            continue;
        }
        if conn.execute_batch("COMMIT").is_err() {
            for req in &batch { let _ = req.result_tx.send(conn.execute_batch(&req.sql).map_err(|e| e.to_string())); }
            continue;
        }
        for req in &batch { let _ = req.result_tx.send(Ok(())); }
    }
}
