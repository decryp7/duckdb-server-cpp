//! DuckDB gRPC Server — Rust edition (extreme performance)
//!
//! Uses Connection::try_clone() pool for true concurrent reads.
//! Same proto/duckdb_service.proto as C# and C++ servers.

mod pool;
mod writer;

use clap::Parser;
use pool::ConnectionPool;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use writer::WriteSerializer;

pub mod proto {
    tonic::include_proto!("duckdb.v1");
}

use proto::duck_db_service_server::{DuckDbService, DuckDbServiceServer};
use proto::*;

#[derive(Parser, Debug)]
#[command(name = "duckdb-grpc-server", version = "5.0.0")]
struct Args {
    #[arg(long, default_value = ":memory:")]
    db: String,
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 17777)]
    port: u16,
    #[arg(long, default_value_t = 0)]
    readers: usize,
    #[arg(long, default_value_t = 5)]
    batch_ms: u64,
    #[arg(long, default_value_t = 512)]
    batch_max: usize,
    #[arg(long, default_value_t = 2048)]
    batch_size: usize,
}

struct DuckDbServerImpl {
    pool: Arc<ConnectionPool>,
    writer: Arc<WriteSerializer>,
    batch_size: usize,
    port: u16,
    stat_reads: Arc<AtomicI64>,
    stat_writes: Arc<AtomicI64>,
    stat_errors: Arc<AtomicI64>,
}

#[tonic::async_trait]
impl DuckDbService for DuckDbServerImpl {
    type QueryStream = tokio_stream::wrappers::ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(&self, request: Request<QueryRequest>) -> Result<Response<Self::QueryStream>, Status> {
        let sql = request.into_inner().sql;
        let pool = self.pool.clone();
        let batch_size = self.batch_size;
        let stat_reads = self.stat_reads.clone();
        let stat_errors = self.stat_errors.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::task::spawn_blocking(move || {
            let conn = match pool.borrow() {
                Ok(c) => c,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e.to_string())));
                    return;
                }
            };

            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e.to_string())));
                    return;
                }
            };

            // Execute first to populate column metadata
            let rows_result = stmt.query_map([], |row| {
                let col_count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    values.push(row.get::<_, Option<String>>(i).unwrap_or(None));
                }
                Ok(values)
            });

            // Get column names AFTER execution
            let col_count = stmt.column_count();
            let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
            let columns: Vec<ColumnMeta> = col_names.iter().map(|name| ColumnMeta {
                name: name.clone(),
                r#type: ColumnType::TypeString as i32,
            }).collect();

            let rows = match rows_result {
                Ok(r) => r,
                Err(e) => {
                    stat_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.blocking_send(Err(Status::internal(e.to_string())));
                    return;
                }
            };

            let mut row_buf: Vec<Vec<Option<String>>> = (0..col_count)
                .map(|_| Vec::with_capacity(batch_size))
                .collect();
            let mut rows_in_batch = 0;
            let mut first_batch = true;

            for row_result in rows {
                let row = match row_result {
                    Ok(r) => r,
                    Err(e) => {
                        stat_errors.fetch_add(1, Ordering::Relaxed);
                        let _ = tx.blocking_send(Err(Status::internal(e.to_string())));
                        return;
                    }
                };

                for c in 0..col_count {
                    if c < row.len() { row_buf[c].push(row[c].clone()); }
                    else { row_buf[c].push(None); }
                }
                rows_in_batch += 1;

                if rows_in_batch >= batch_size {
                    let resp = build_response(&mut row_buf, rows_in_batch,
                        if first_batch { Some(&columns) } else { None });
                    first_batch = false;
                    rows_in_batch = 0;
                    if tx.blocking_send(Ok(resp)).is_err() { return; }
                }
            }

            if rows_in_batch > 0 || first_batch {
                let resp = build_response(&mut row_buf, rows_in_batch,
                    if first_batch { Some(&columns) } else { None });
                let _ = tx.blocking_send(Ok(resp));
            }

            stat_reads.fetch_add(1, Ordering::Relaxed);
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn execute(&self, request: Request<ExecuteRequest>) -> Result<Response<ExecuteResponse>, Status> {
        let sql = request.into_inner().sql;
        if sql.is_empty() {
            self.stat_errors.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(ExecuteResponse { success: false, error: "SQL required".into() }));
        }
        match self.writer.submit(&sql) {
            Ok(()) => {
                self.stat_writes.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ExecuteResponse { success: true, error: String::new() }))
            }
            Err(e) => {
                self.stat_errors.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ExecuteResponse { success: false, error: e }))
            }
        }
    }

    async fn bulk_insert(&self, request: Request<BulkInsertRequest>) -> Result<Response<BulkInsertResponse>, Status> {
        let req = request.into_inner();
        let writer = self.writer.clone();
        let result = tokio::task::spawn_blocking(move || {
            writer.bulk_insert(&req.table, &req.columns, &req.data, req.row_count as usize)
        }).await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(count) => {
                self.stat_writes.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(BulkInsertResponse { success: true, error: String::new(), rows_inserted: count as i64 }))
            }
            Err(e) => {
                self.stat_errors.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(BulkInsertResponse { success: false, error: e, rows_inserted: 0 }))
            }
        }
    }

    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse { message: "pong".into() }))
    }

    async fn get_stats(&self, _: Request<StatsRequest>) -> Result<Response<StatsResponse>, Status> {
        Ok(Response::new(StatsResponse {
            queries_read: self.stat_reads.load(Ordering::Relaxed),
            queries_write: self.stat_writes.load(Ordering::Relaxed),
            errors: self.stat_errors.load(Ordering::Relaxed),
            reader_pool_size: self.pool.size() as i32,
            port: self.port as i32,
        }))
    }
}

fn build_response(col_bufs: &mut Vec<Vec<Option<String>>>, row_count: usize, meta: Option<&Vec<ColumnMeta>>) -> QueryResponse {
    let mut resp = QueryResponse { columns: Vec::new(), data: Vec::with_capacity(col_bufs.len()), row_count: row_count as i32 };
    if let Some(m) = meta { resp.columns = m.clone(); }
    for buf in col_bufs.iter_mut() {
        let mut cd = ColumnData::default();
        let mut nulls = Vec::new();
        for (i, val) in buf.iter().enumerate() {
            match val {
                Some(s) => cd.string_values.push(s.clone()),
                None => { nulls.push(i as i32); cd.string_values.push(String::new()); }
            }
        }
        cd.null_indices = nulls;
        resp.data.push(cd);
        buf.clear();
    }
    resp
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let readers = if args.readers == 0 { num_cpus::get() * 2 } else { args.readers };

    let pool = Arc::new(ConnectionPool::new(&args.db, readers)?);

    // Apply DuckDB tuning
    {
        let conn = pool.borrow().map_err(|e| e)?;
        let _ = conn.execute_batch("PRAGMA enable_object_cache");
        let _ = conn.execute_batch("SET preserve_insertion_order=false");
        let _ = conn.execute_batch("SET checkpoint_threshold='256MB'");
    }

    // Writer gets its own cloned connections
    let writer = {
        let conn = pool.borrow().map_err(|e| e)?;
        Arc::new(WriteSerializer::from_conn(&conn, args.batch_ms, args.batch_max)?)
    };

    let server_impl = DuckDbServerImpl {
        pool: pool.clone(),
        writer,
        batch_size: args.batch_size,
        port: args.port,
        stat_reads: Arc::new(AtomicI64::new(0)),
        stat_writes: Arc::new(AtomicI64::new(0)),
        stat_errors: Arc::new(AtomicI64::new(0)),
    };

    let addr = format!("{}:{}", args.host, args.port).parse()?;

    println!("[das] DuckDB gRPC Server v5.0.0 (Rust + try_clone pool)");
    println!("      address  = {}", addr);
    println!("      readers  = {}", readers);
    println!("      db       = {}", args.db);

    Server::builder()
        .add_service(DuckDbServiceServer::new(server_impl))
        .serve_with_shutdown(addr, async { tokio::signal::ctrl_c().await.ok(); println!("\n[das] Shutting down..."); })
        .await?;

    Ok(())
}
