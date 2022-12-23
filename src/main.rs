mod gelf;

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use async_shutdown::Shutdown;
use clap::{Parser, ValueEnum};
use derive_more::Display;
use gelf::GELFState;
use sqlx::{any::AnyStatement, Any, AnyPool, Executor, Pool, Statement, Transaction};
use tokio::{net::UdpSocket, select, signal::ctrl_c, spawn};

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
    // std::env::set_var("RUST_LOG", "sqlx_logger=DEBUG");
    // std::env::set_var("RUST_LOG", "sqlx_logger=INFO");
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

    let st = pool
        .prepare(&sql)
        .await
        .with_context(|| format!("Checking SQL: {sql}"))?;

    let socket = UdpSocket::bind(&listen)
        .await
        .with_context(|| format!("Listening on udp://{listen}"))?;

    log::info!("Listening on udp://{listen}");
    log::info!("Connected to {db_url}");

    let mut tx: Option<Transaction<Any>> = None;

    let rs = do_process_log(socket, shutdown, &st, pool, filter, db_batch, &mut tx).await;

    if let Some(tx) = tx {
        log::info!("Committed pending transactions");
        let _ = tx.commit().await;
    }

    log::debug!("Client serving result: {rs:?}");

    rs
}

const CLEAN_UP_INTERVAL: Duration = Duration::from_secs(60);

async fn do_process_log(
    socket: UdpSocket,
    shutdown: Shutdown,
    st: &AnyStatement<'_>,
    pool: Pool<Any>,
    filter: FilterFormat,
    db_batch: usize,
    tx: &mut Option<Transaction<'_, Any>>,
) -> anyhow::Result<()> {
    let mut state: GELFState = Default::default();
    let mut num_inserts = 0usize;
    let mut buf = vec![0u8; 65536];
    let mut last_cleanup: Option<Instant> = None;
    while let Some(v) = shutdown.wrap_cancel(socket.recv(&mut buf)).await {
        match (&last_cleanup, Instant::now()) {
            (Some(last), n) if n - *last > CLEAN_UP_INTERVAL => {
                log::info!("Cleaning up messages");
                state.clean_up(n);
                last_cleanup.replace(n);
            }
            _ => {}
        }

        let buf = &buf[..v.context("Receiving packet")?];

        let entry = match state.on_data(&buf) {
            Ok(Some(data)) => data,
            Ok(None) => {
                log::debug!("More data needed");
                continue;
            }
            Err(err) => {
                log::error!("Error handling incoming data: {err:?}");
                continue;
            }
        };

        if !filter.accepts(entry.as_ref()) {
            log::debug!("DENIED: {entry}");
            continue;
        } else {
            log::debug!("ACCEPTED: {entry}");
        }

        let t = match tx.as_mut() {
            Some(v) => v,
            None => {
                tx.replace(pool.begin().await.context("Begin transaction")?);
                tx.as_mut().unwrap()
            }
        };

        let r = st
            .query()
            .bind(entry.as_ref())
            .execute(t)
            .await
            .context("Executing SQL")?;
        log::debug!("Inserted {} rows", r.rows_affected());
        num_inserts += 1;

        if num_inserts >= db_batch {
            tx.take()
                .unwrap()
                .commit()
                .await
                .context("Committing transactions")?;
            log::info!("Committed {num_inserts} transactions");
            num_inserts = 0;
        }
    }

    Ok(())
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
