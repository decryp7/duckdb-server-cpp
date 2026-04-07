//! # Write Serializer: Batches Concurrent DML into Single Transactions
//!
//! Serializes concurrent write requests through a single background thread
//! that collects requests over a short time window and executes them in a
//! single BEGIN...COMMIT transaction.
//!
//! ## Architecture
//!
//! ```text
//! Thread A: Submit("INSERT ...") ─┐
//! Thread B: Submit("INSERT ...") ──┤── collected over batch_ms ──► BEGIN; A; B; COMMIT
//! Thread C: Submit("UPDATE ...") ─┘
//! ```
//!
//! Each caller blocks on a `mpsc::Receiver` until its write completes.
//! Timeout is 30 seconds — if the writer thread dies, callers don't hang forever.

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
    _writer_thread: thread::JoinHandle<()>,
}

impl WriteSerializer {
    /// Create a writer. `source_conn` is cloned for the writer's dedicated connection.
    pub fn from_conn(source_conn: &Connection, batch_ms: u64, batch_max: usize) -> Result<Self, String> {
        let write_conn = source_conn.try_clone()
            .map_err(|e| format!("Writer clone failed: {}", e))?;

        let (tx, rx) = mpsc::channel::<WriteRequest>();

        let handle = thread::Builder::new()
            .name("duckdb-writer".into())
            .spawn(move || { drain_loop(write_conn, rx, batch_ms, batch_max); })
            .map_err(|e| format!("Writer thread failed: {}", e))?;

        Ok(Self {
            request_tx: Mutex::new(tx),
            _writer_thread: handle,
        })
    }

    pub fn submit(&self, sql: &str) -> Result<(), String> {
        let (result_tx, result_rx) = mpsc::channel();
        // Use unwrap_or_else to recover from mutex poisoning instead of panicking
        self.request_tx.lock().unwrap_or_else(|e| e.into_inner())
            .send(WriteRequest { sql: sql.to_string(), result_tx })
            .map_err(|_| "Writer thread died".to_string())?;
        result_rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| "Write timed out".to_string())?
    }
}

/// Quote a SQL identifier with double quotes, escaping embedded double quotes.
pub fn quote_ident(name: &str) -> String {
    let mut q = String::with_capacity(name.len() + 2);
    q.push('"');
    for ch in name.chars() {
        if ch == '"' { q.push_str("\"\""); }
        else { q.push(ch); }
    }
    q.push('"');
    q
}

pub fn build_bulk_insert_sql(
    table: &str, columns: &[ColumnMeta], data: &[ColumnData], row_count: usize,
) -> Result<String, String> {
    let col_count = columns.len();
    if col_count == 0 || data.len() != col_count {
        return Err("column count mismatch".into());
    }
    let mut sql = String::with_capacity(row_count * col_count * 10);
    for r in 0..row_count {
        if r == 0 {
            sql.push_str("INSERT INTO ");
            sql.push_str(&quote_ident(table));
            sql.push_str(" VALUES (");
        } else {
            sql.push_str(", (");
        }
        for c in 0..col_count {
            if c > 0 { sql.push_str(", "); }
            let cd = &data[c];
            let is_null = cd.null_indices.iter().any(|&n| n == r as i32);
            if is_null { sql.push_str("NULL"); continue; }

            match columns[c].r#type() {
                crate::proto::ColumnType::TypeBoolean => {
                    let v = cd.bool_values.get(r).copied().unwrap_or(false);
                    sql.push_str(if v { "true" } else { "false" });
                }
                crate::proto::ColumnType::TypeInt8
                | crate::proto::ColumnType::TypeInt16
                | crate::proto::ColumnType::TypeInt32
                | crate::proto::ColumnType::TypeUint8
                | crate::proto::ColumnType::TypeUint16 => {
                    let v = cd.int32_values.get(r).copied().unwrap_or(0);
                    sql.push_str(&v.to_string());
                }
                crate::proto::ColumnType::TypeInt64
                | crate::proto::ColumnType::TypeUint32
                | crate::proto::ColumnType::TypeUint64 => {
                    let v = cd.int64_values.get(r).copied().unwrap_or(0);
                    sql.push_str(&v.to_string());
                }
                crate::proto::ColumnType::TypeFloat => {
                    let v = cd.float_values.get(r).copied().unwrap_or(0.0);
                    sql.push_str(&v.to_string());
                }
                crate::proto::ColumnType::TypeDouble
                | crate::proto::ColumnType::TypeDecimal => {
                    let v = cd.double_values.get(r).copied().unwrap_or(0.0);
                    sql.push_str(&v.to_string());
                }
                _ => {
                    sql.push('\'');
                    let s = cd.string_values.get(r).map(|s| s.as_str()).unwrap_or("");
                    sql.push_str(&s.replace('\'', "''"));
                    sql.push('\'');
                }
            }
        }
        sql.push(')');
    }
    Ok(sql)
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
