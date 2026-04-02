//! Write serializer: batches concurrent DML into single transactions.
//!
//! DuckDB supports one writer at a time. This module queues write requests
//! and executes them in batched transactions for maximum throughput.
//! Also supports bulk insert via DuckDB's Appender API.

use crate::proto::{ColumnData, ColumnMeta, ColumnType};
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
    writer_conn: Mutex<Connection>,
    _writer_thread: thread::JoinHandle<()>,
}

impl WriteSerializer {
    pub fn new(db_path: &str, batch_ms: u64, batch_max: usize) -> Result<Self, String> {
        let conn = Connection::open(db_path)
            .map_err(|e| format!("Writer connection failed: {}", e))?;

        let writer_conn = Connection::open(db_path)
            .map_err(|e| format!("Bulk insert connection failed: {}", e))?;

        let (tx, rx) = mpsc::channel::<WriteRequest>();

        let handle = thread::Builder::new()
            .name("duckdb-writer".into())
            .spawn(move || {
                drain_loop(conn, rx, batch_ms, batch_max);
            })
            .map_err(|e| format!("Writer thread failed: {}", e))?;

        Ok(Self {
            request_tx: Mutex::new(tx),
            writer_conn: Mutex::new(writer_conn),
            _writer_thread: handle,
        })
    }

    /// Submit a SQL statement and block until committed.
    pub fn submit(&self, sql: &str) -> Result<(), String> {
        let (result_tx, result_rx) = mpsc::channel();
        let req = WriteRequest {
            sql: sql.to_string(),
            result_tx,
        };

        self.request_tx
            .lock()
            .unwrap()
            .send(req)
            .map_err(|_| "Writer thread died".to_string())?;

        result_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| "Write timed out after 30 seconds".to_string())?
    }

    /// Bulk insert using DuckDB Appender API (bypasses SQL parser).
    pub fn bulk_insert(
        &self,
        table: &str,
        _columns: &[ColumnMeta],
        data: &[ColumnData],
        row_count: usize,
    ) -> Result<usize, String> {
        let conn = self.writer_conn.lock().unwrap();
        let mut appender = conn
            .appender(table)
            .map_err(|e| format!("Appender failed for {}: {}", table, e))?;

        for r in 0..row_count {
            for cd in data.iter() {
                let is_null = cd.null_indices.contains(&(r as i32));
                if is_null {
                    appender
                        .append_null()
                        .map_err(|e| format!("Append null failed: {}", e))?;
                } else if r < cd.int32_values.len() {
                    appender
                        .append_row([cd.int32_values[r]])
                        .map_err(|e| format!("Append int32 failed: {}", e))?;
                    continue; // appended full row
                } else if r < cd.int64_values.len() {
                    appender
                        .append_row([cd.int64_values[r]])
                        .map_err(|e| format!("Append int64 failed: {}", e))?;
                    continue;
                } else if r < cd.double_values.len() {
                    appender
                        .append_row([cd.double_values[r]])
                        .map_err(|e| format!("Append double failed: {}", e))?;
                    continue;
                } else if r < cd.string_values.len() {
                    appender
                        .append_row([cd.string_values[r].as_str()])
                        .map_err(|e| format!("Append string failed: {}", e))?;
                    continue;
                }
            }
        }

        appender
            .flush()
            .map_err(|e| format!("Appender flush failed: {}", e))?;

        Ok(row_count)
    }
}

// ── Writer background thread ─────────────────────────────────────────────────

fn drain_loop(
    conn: Connection,
    rx: mpsc::Receiver<WriteRequest>,
    batch_ms: u64,
    batch_max: usize,
) {
    loop {
        // Wait for first request
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => return, // channel closed, shut down
        };

        let mut batch = vec![first];

        // Collect more for batch_ms or until batch_max
        let deadline = std::time::Instant::now() + Duration::from_millis(batch_ms);
        while batch.len() < batch_max {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() {
                break;
            }
            match rx.recv_timeout(timeout) {
                Ok(req) => batch.push(req),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        execute_batch(&conn, &batch);
    }
}

fn execute_batch(conn: &Connection, batch: &[WriteRequest]) {
    // Try batched transaction
    if conn.execute_batch("BEGIN").is_err() {
        // Fallback: execute each individually
        for req in batch {
            let result = conn.execute_batch(&req.sql);
            let _ = req.result_tx.send(result.map_err(|e| e.to_string()));
        }
        return;
    }

    let mut all_ok = true;
    for req in batch {
        if conn.execute_batch(&req.sql).is_err() {
            all_ok = false;
            break;
        }
    }

    if !all_ok {
        let _ = conn.execute_batch("ROLLBACK");
        for req in batch {
            let result = conn.execute_batch(&req.sql);
            let _ = req.result_tx.send(result.map_err(|e| e.to_string()));
        }
        return;
    }

    if conn.execute_batch("COMMIT").is_err() {
        for req in batch {
            let result = conn.execute_batch(&req.sql);
            let _ = req.result_tx.send(result.map_err(|e| e.to_string()));
        }
        return;
    }

    // All succeeded
    for req in batch {
        let _ = req.result_tx.send(Ok(()));
    }
}
