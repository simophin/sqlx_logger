mod gelf;

use std::net::SocketAddr;

use anyhow::{anyhow, Context};
use async_shutdown::Shutdown;
use clap::{Parser, ValueEnum};
use derive_more::Display;
use sqlx::{any::AnyStatement, Any, AnyPool, Executor, Pool, Statement, Transaction};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
    select,
    signal::ctrl_c,
    spawn,
};

#[derive(Debug, Display, ValueEnum, Clone)]
enum FilterFormat {
    Json,
    Any,
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The URL to database. e.g. postgres://user:pass@host:port/db. See sqlx's doc
    #[arg(long)]
    db_url: String,

    /// The batch size to run SQL transaction
    #[arg(long, default_value_t = 10)]
    db_batch: usize,

    /// The UDP port to listen on
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// The filter to run on each entry
    #[arg(long, default_value = "any")]
    filter: FilterFormat,

    /// The SQL to run on each entry. The value will be given as parameter :1
    sql: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //std::env::set_var("RUST_LOG", "sqlx_logger=DEBUG");
    std::env::set_var("RUST_LOG", "sqlx_logger=INFO");
    env_logger::init();

    let shutdown = Shutdown::new();

    let mut job = spawn(run_with_args(Args::parse(), shutdown.clone()));

    select! {
        _ = ctrl_c() => {
            shutdown.shutdown();
        }

        _ = &mut job => {}
    };

    job.await
        .map_err(|e| anyhow!("Error joining job: {e:?}"))
        .and_then(|v| v)
}

async fn run_with_args(
    Args {
        db_url,
        db_batch,
        listen,
        sql,
        filter,
    }: Args,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let pool = AnyPool::connect(&db_url)
        .await
        .with_context(|| format!("Connecting to {db_url}"))?;

    pool.prepare(&sql)
        .await
        .with_context(|| format!("Checking SQL: {sql}"))?;

    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("Listening on {listen}"))?;

    log::info!("Listening on tcp://{listen}");
    log::info!("Connected to {db_url}");

    while let Some(v) = shutdown.wrap_cancel(listener.accept()).await {
        let (socket, addr) = v.context("Accepting TCP client")?;
        log::info!("Accepted client from {addr}");

        let shutdown = shutdown.clone();
        let sql = sql.clone();
        let pool = pool.clone();
        let filter = filter.clone();

        spawn(async move {
            if let Err(e) = serve_client(
                socket,
                shutdown.clone(),
                sql.clone(),
                pool.clone(),
                filter.clone(),
                db_batch,
            )
            .await
            {
                log::error!("Error serving client {addr}: {e:?}");
            }
        });
    }

    Ok(())
}

async fn serve_client(
    socket: TcpStream,
    shutdown: Shutdown,
    sql: String,
    pool: Pool<Any>,
    filter: FilterFormat,
    db_batch: usize,
) -> anyhow::Result<()> {
    let mut tx: Option<Transaction<Any>> = None;

    let rs = do_process_log(
        socket,
        shutdown,
        &pool
            .prepare(&sql)
            .await
            .with_context(|| format!("Preparing SQL: \"{sql}\""))?,
        pool,
        filter,
        db_batch,
        &mut tx,
    )
    .await;

    if let Some(tx) = tx {
        log::info!("Committed the pending transactions");
        let _ = tx.commit().await;
    }

    log::debug!("Client serving result: {rs:?}");

    rs
}

async fn do_process_log(
    socket: TcpStream,
    shutdown: Shutdown,
    st: &AnyStatement<'_>,
    pool: Pool<Any>,
    filter: FilterFormat,
    db_batch: usize,
    tx: &mut Option<Transaction<'_, Any>>,
) -> anyhow::Result<()> {
    let mut socket = BufReader::new(socket);
    let mut num_inserts = 0usize;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let buf = match shutdown.wrap_cancel(socket.read_until(0u8, &mut buf)).await {
            None | Some(Ok(0)) => return Ok(()),
            Some(Err(e)) => return Err(e.into()),
            Some(Ok(v)) => &buf[..v - 1],
        };

        let entry = match std::str::from_utf8(buf) {
            Ok(v) if filter.accepts(v) => {
                log::debug!("Accepted: {v}");
                v
            }
            Ok(v) => {
                log::debug!("Filtering out line: {v}");
                continue;
            }
            Err(e) => {
                log::info!("Error converting line to UTF-8: {e:?}");
                continue;
            }
        };

        let t = match tx.as_mut() {
            Some(v) => v,
            None => {
                tx.replace(pool.begin().await.context("Begin transaction")?);
                tx.as_mut().unwrap()
            }
        };

        let r = st
            .query()
            .bind(entry)
            .execute(t)
            .await
            .context("Executing SQL")?;
        log::debug!("Inserted {} rows", r.rows_affected());

        if num_inserts >= db_batch {
            tx.take()
                .unwrap()
                .commit()
                .await
                .context("Committing transactions")?;
            log::debug!("Committed {num_inserts} transactions");
            num_inserts = 0;
        }
    }
}

impl FilterFormat {
    fn accepts(&self, entry: &str) -> bool {
        match self {
            Self::Json => {
                let value: Result<serde::de::IgnoredAny, _> = serde_json::from_str(entry);
                value.is_ok()
            }
            Self::Any => true,
        }
    }
}
