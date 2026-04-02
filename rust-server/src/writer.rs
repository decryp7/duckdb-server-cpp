//! Write serializer: batches concurrent DML into single transactions.

use crate::proto::{ColumnData, ColumnMeta};
use duckdb::Connection;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct WriteRequest {
    sql: String,
    result_tx: mpsc::Sender<Result<(), String>>,
}

pub struct WriteSerializer {
    request_tx: Mutex<mpsc::Sender<WriteRequest>>,
    writer_conn: Arc<Mutex<Connection>>,
    _writer_thread: thread::JoinHandle<()>,
}

impl WriteSerializer {
    pub fn new(shared_conn: Arc<Mutex<Connection>>, batch_ms: u64, batch_max: usize) -> Result<Self, String> {
        let conn_for_thread = shared_conn.clone();
        let writer_conn_holder = shared_conn;

        let (tx, rx) = mpsc::channel::<WriteRequest>();

        let handle = thread::Builder::new()
            .name("duckdb-writer".into())
            .spawn(move || {
                drain_loop(conn_for_thread, rx, batch_ms, batch_max);
            })
            .map_err(|e| format!("Writer thread failed: {}", e))?;

        Ok(Self {
            request_tx: Mutex::new(tx),
            writer_conn: writer_conn_holder,
            _writer_thread: handle,
        })
    }

    pub fn submit(&self, sql: &str) -> Result<(), String> {
        let (result_tx, result_rx) = mpsc::channel();
        self.request_tx
            .lock().unwrap()
            .send(WriteRequest { sql: sql.to_string(), result_tx })
            .map_err(|_| "Writer thread died".to_string())?;

        result_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| "Write timed out".to_string())?
    }

    pub fn bulk_insert(
        &self, table: &str, _columns: &[ColumnMeta],
        data: &[ColumnData], row_count: usize,
    ) -> Result<usize, String> {
        let conn = self.writer_conn.lock().unwrap();
        // conn is now MutexGuard<Connection>
        let col_count = data.len();
        if col_count == 0 || row_count == 0 { return Ok(0); }

        let mut sql = format!("INSERT INTO {} VALUES ", table);
        for r in 0..row_count {
            if r > 0 { sql.push_str(", "); }
            sql.push('(');
            for c in 0..col_count {
                if c > 0 { sql.push_str(", "); }
                let cd = &data[c];
                let is_null = cd.null_indices.contains(&(r as i32));
                if is_null { sql.push_str("NULL"); }
                else if r < cd.int32_values.len() { sql.push_str(&cd.int32_values[r].to_string()); }
                else if r < cd.int64_values.len() { sql.push_str(&cd.int64_values[r].to_string()); }
                else if r < cd.double_values.len() { sql.push_str(&cd.double_values[r].to_string()); }
                else if r < cd.bool_values.len() { sql.push_str(if cd.bool_values[r] { "true" } else { "false" }); }
                else if r < cd.string_values.len() {
                    sql.push('\'');
                    sql.push_str(&cd.string_values[r].replace('\'', "''"));
                    sql.push('\'');
                } else { sql.push_str("NULL"); }
            }
            sql.push(')');
        }

        conn.execute_batch(&sql).map_err(|e| format!("Bulk insert failed: {}", e))?;
        Ok(row_count)
    }
}

fn drain_loop(conn: Arc<Mutex<Connection>>, rx: mpsc::Receiver<WriteRequest>, batch_ms: u64, batch_max: usize) {
    loop {
        let first = match rx.recv() { Ok(r) => r, Err(_) => return };
        let mut batch = vec![first];

        let deadline = std::time::Instant::now() + Duration::from_millis(batch_ms);
        while batch.len() < batch_max {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() { break; }
            match rx.recv_timeout(timeout) {
                Ok(req) => batch.push(req),
                Err(_) => break,
            }
        }
        execute_batch(&conn, &batch);
    }
}

fn execute_batch(conn: &Arc<Mutex<Connection>>, batch: &[WriteRequest]) {
    let c = conn.lock().unwrap();
    if c.execute_batch("BEGIN").is_err() {
        for req in batch { let _ = req.result_tx.send(c.execute_batch(&req.sql).map_err(|e| e.to_string())); }
        return;
    }
    let mut all_ok = true;
    for req in batch { if c.execute_batch(&req.sql).is_err() { all_ok = false; break; } }
    if !all_ok {
        let _ = c.execute_batch("ROLLBACK");
        for req in batch { let _ = req.result_tx.send(c.execute_batch(&req.sql).map_err(|e| e.to_string())); }
        return;
    }
    if c.execute_batch("COMMIT").is_err() {
        for req in batch { let _ = req.result_tx.send(c.execute_batch(&req.sql).map_err(|e| e.to_string())); }
        return;
    }
    for req in batch { let _ = req.result_tx.send(Ok(())); }
}
