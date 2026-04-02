//! DuckDB gRPC Server — Rust edition (extreme performance)
//!
//! Uses the same proto/duckdb_service.proto as the C# and C++ servers.
//! All three servers are fully interoperable with any gRPC client.
//!
//! Performance advantages over C#/C++:
//!   - Zero GC: no garbage collection pauses
//!   - Zero-cost async: tokio runtime with work-stealing scheduler
//!   - Native protobuf: prost generates zero-copy Rust types
//!   - DuckDB bundled: statically linked, no DLL dependency

mod pool;
mod writer;

use clap::Parser;
use pool::ConnectionPool;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use writer::WriteSerializer;

// Generated from proto/duckdb_service.proto by tonic-build
pub mod proto {
    tonic::include_proto!("duckdb.v1");
}

use proto::duck_db_service_server::{DuckDbService, DuckDbServiceServer};
use proto::*;

// ── CLI arguments ────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "duckdb-grpc-server", version = "5.0.0")]
struct Args {
    /// DuckDB database file path, or :memory:
    #[arg(long, default_value = ":memory:")]
    db: String,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// gRPC listen port
    #[arg(long, default_value_t = 17777)]
    port: u16,

    /// Read connection pool size
    #[arg(long, default_value_t = 0)]
    readers: usize,

    /// Write batch window in milliseconds
    #[arg(long, default_value_t = 5)]
    batch_ms: u64,

    /// Max writes per batch
    #[arg(long, default_value_t = 512)]
    batch_max: usize,

    /// Rows per gRPC response message
    #[arg(long, default_value_t = 2048)]
    batch_size: usize,
}

// ── Server state ─────────────────────────────────────────────────────────────

struct DuckDbServerImpl {
    read_pool: Arc<ConnectionPool>,
    writer: Arc<WriteSerializer>,
    batch_size: usize,
    port: u16,

    stat_reads: Arc<AtomicI64>,
    stat_writes: Arc<AtomicI64>,
    stat_errors: Arc<AtomicI64>,
}

// ── gRPC service implementation ──────────────────────────────────────────────

#[tonic::async_trait]
impl DuckDbService for DuckDbServerImpl {
    type QueryStream = tokio_stream::wrappers::ReceiverStream<Result<QueryResponse, Status>>;

    /// Execute a SELECT query and stream columnar results.
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let sql = request.into_inner().sql;
        let pool = self.read_pool.clone();
        let batch_size = self.batch_size;
        let stat_reads = self.stat_reads.clone();
        let stat_errors = self.stat_errors.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::task::spawn_blocking(move || {
            let handle = match pool.borrow() {
                Ok(h) => h,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e)));
                    return;
                }
            };

            // Get column names
            let col_names = match handle.column_names(&sql) {
                Ok(n) => n,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e)));
                    return;
                }
            };
            let col_count = col_names.len();

            // Execute query and get all rows as strings
            let all_rows = match handle.execute_query(&sql) {
                Ok(r) => r,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e)));
                    return;
                }
            };

            let columns: Vec<ColumnMeta> = col_names.iter().map(|name| ColumnMeta {
                name: name.clone(),
                r#type: ColumnType::TypeString as i32,
            }).collect();

            // Stream in columnar batches
            let mut row_buf: Vec<Vec<Option<String>>> = (0..col_count)
                .map(|_| Vec::with_capacity(batch_size))
                .collect();
            let mut rows_in_batch = 0;
            let mut first_batch = true;

            for row in &all_rows {
                for c in 0..col_count {
                    if c < row.len() {
                        row_buf[c].push(row[c].clone());
                    } else {
                        row_buf[c].push(None);
                    }
                }
                rows_in_batch += 1;

                if rows_in_batch >= batch_size {
                    let resp = build_columnar_response(
                        &mut row_buf, rows_in_batch,
                        if first_batch { Some(&columns) } else { None },
                    );
                    first_batch = false;
                    rows_in_batch = 0;
                    if tx.blocking_send(Ok(resp)).is_err() { return; }
                }
            }

            // Flush remaining
            if rows_in_batch > 0 || first_batch {
                let resp = build_columnar_response(
                    &mut row_buf,
                    rows_in_batch,
                    if first_batch { Some(&columns) } else { None },
                );
                let _ = tx.blocking_send(Ok(resp));
            }

            stat_reads.fetch_add(1, Ordering::Relaxed);
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Execute DML/DDL statement.
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let sql = request.into_inner().sql;

        if sql.is_empty() {
            self.stat_errors.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(ExecuteResponse {
                success: false,
                error: "SQL statement is required".into(),
            }));
        }

        match self.writer.submit(&sql) {
            Ok(()) => {
                self.stat_writes.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ExecuteResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => {
                self.stat_errors.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ExecuteResponse {
                    success: false,
                    error: e,
                }))
            }
        }
    }

    /// Bulk insert using DuckDB Appender API.
    async fn bulk_insert(
        &self,
        request: Request<BulkInsertRequest>,
    ) -> Result<Response<BulkInsertResponse>, Status> {
        let req = request.into_inner();
        let writer = self.writer.clone();

        let result = tokio::task::spawn_blocking(move || {
            writer.bulk_insert(&req.table, &req.columns, &req.data, req.row_count as usize)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(count) => {
                self.stat_writes.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(BulkInsertResponse {
                    success: true,
                    error: String::new(),
                    rows_inserted: count as i64,
                }))
            }
            Err(e) => {
                self.stat_errors.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(BulkInsertResponse {
                    success: false,
                    error: e,
                    rows_inserted: 0,
                }))
            }
        }
    }

    /// Liveness check.
    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            message: "pong".into(),
        }))
    }

    /// Server metrics.
    async fn get_stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        Ok(Response::new(StatsResponse {
            queries_read: self.stat_reads.load(Ordering::Relaxed),
            queries_write: self.stat_writes.load(Ordering::Relaxed),
            errors: self.stat_errors.load(Ordering::Relaxed),
            reader_pool_size: self.read_pool.size() as i32,
            port: self.port as i32,
        }))
    }
}

// ── Columnar response builder ────────────────────────────────────────────────

fn build_columnar_response(
    col_bufs: &mut Vec<Vec<Option<String>>>,
    row_count: usize,
    meta: Option<&Vec<ColumnMeta>>,
) -> QueryResponse {
    let mut resp = QueryResponse {
        columns: Vec::new(),
        data: Vec::with_capacity(col_bufs.len()),
        row_count: row_count as i32,
    };

    if let Some(m) = meta {
        resp.columns = m.clone();
    }

    for buf in col_bufs.iter_mut() {
        let mut cd = ColumnData::default();
        let mut null_indices = Vec::new();

        for (i, val) in buf.iter().enumerate() {
            match val {
                Some(s) => cd.string_values.push(s.clone()),
                None => {
                    null_indices.push(i as i32);
                    cd.string_values.push(String::new());
                }
            }
        }

        cd.null_indices = null_indices;
        resp.data.push(cd);
        buf.clear();
    }

    resp
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let readers = if args.readers == 0 {
        num_cpus::get() * 2
    } else {
        args.readers
    };

    let db_path = if args.db == ":memory:" {
        ":memory:".to_string()
    } else {
        args.db.clone()
    };

    // Create connection pool
    let pool = Arc::new(ConnectionPool::new(&db_path, readers)?);

    // Create write serializer
    let writer = Arc::new(WriteSerializer::new(&db_path, args.batch_ms, args.batch_max)?);

    // Apply DuckDB performance tuning
    {
        let handle = pool.borrow().map_err(|e| e)?;
        let _ = handle.execute_batch("PRAGMA enable_object_cache");
        let _ = handle.execute_batch("SET preserve_insertion_order=false");
        let _ = handle.execute_batch("SET checkpoint_threshold='256MB'");
    }

    let server_impl = DuckDbServerImpl {
        read_pool: pool.clone(),
        writer,
        batch_size: args.batch_size,
        port: args.port,
        stat_reads: Arc::new(AtomicI64::new(0)),
        stat_writes: Arc::new(AtomicI64::new(0)),
        stat_errors: Arc::new(AtomicI64::new(0)),
    };

    let addr = format!("{}:{}", args.host, args.port).parse()?;

    println!("[das] DuckDB gRPC Server v5.0.0 (Rust)");
    println!("      address  = {}", addr);
    println!("      readers  = {}", readers);
    println!("      batch    = {}ms / {} max", args.batch_ms, args.batch_max);
    println!("      db       = {}", args.db);
    println!("      protocol = duckdb.v1.DuckDbService (proto/duckdb_service.proto)");

    Server::builder()
        .add_service(DuckDbServiceServer::new(server_impl))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            println!("\n[das] Shutting down...");
        })
        .await?;

    Ok(())
}
