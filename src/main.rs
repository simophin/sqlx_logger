use std::net::SocketAddr;

use anyhow::{anyhow, Context};
use async_shutdown::Shutdown;
use clap::{Parser, ValueEnum};
use derive_more::Display;
use sqlx::{Any, AnyPool, Executor, Statement, Transaction};
use tokio::{net::UdpSocket, signal::ctrl_c, spawn};

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
    std::env::set_var("RUST_LOG", "udp_logger=DEBUG");
    env_logger::init();

    let shutdown = Shutdown::new();

    let job = spawn(run_with_args(Args::parse(), shutdown.clone()));

    let _ = ctrl_c().await;
    shutdown.shutdown();

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
        .with_context(|| format!("Preparing SQL \"{sql}\""))?;

    let socket = UdpSocket::bind(&listen)
        .await
        .with_context(|| format!("Listening on {listen}"))?;

    log::info!("Listening on {listen}");
    log::info!("Connected to {db_url}");

    let mut buf = vec![0u8; 65536];
    let mut err = None;
    let mut tx: Option<Transaction<Any>> = None;
    let mut num_inserts = 0usize;
    loop {
        let buf = match shutdown.wrap_cancel(socket.recv(&mut buf)).await {
            Some(Ok(v)) if v == 0 => {
                log::info!("EOF");
                break;
            }

            Some(Ok(v)) => &buf[..v],
            Some(Err(e)) => {
                err.replace(e.into());
                break;
            }

            None => break,
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
            None => match pool.begin().await {
                Ok(v) => {
                    tx.replace(v);
                    tx.as_mut().unwrap()
                }
                Err(e) => {
                    err.replace(e.into());
                    break;
                }
            },
        };

        match st.query().bind(entry).execute(t).await {
            Ok(v) => {
                log::debug!("Inserted {} rows", v.rows_affected());
                num_inserts += 1;
            }
            Err(e) => {
                err.replace(e.into());
                continue;
            }
        }

        if num_inserts >= db_batch {
            match tx.take().unwrap().commit().await {
                Ok(_) => {
                    log::debug!("Committed {num_inserts} transactions");
                }
                Err(e) => {
                    err.replace(e);
                    break;
                }
            }
            num_inserts = 0;
        }
    }

    if let Some(tx) = tx {
        log::info!("Committed the pending {num_inserts} in tx");
        let _ = tx.commit().await;
    }

    if let Some(err) = err {
        return Err(err).context("Read/Write logs");
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
