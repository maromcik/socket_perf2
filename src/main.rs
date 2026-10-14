use crate::async_net::{run_async_clients, run_async_server};
use crate::config::{Args, ClientConfig, ServerConfig};
use crate::error::AppError;
use clap::Parser;
use log::{error, info, warn};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

mod async_net;
mod config;
mod error;
mod stats;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let args = Args::parse();

    env_logger::Builder::new()
        .filter(None, args.log_level)
        .init();

    let cancel = CancellationToken::new();
    match (ServerConfig::from(&args), ClientConfig::from(&args)) {
        (None, Some(config)) => {
            let fut = run_async_clients(config, cancel.clone());
            spawn_test(fut, cancel.clone()).await;
        }
        (Some(config), None) => {
            let fut = run_async_server(config, cancel.clone());
            spawn_test(fut, cancel.clone()).await;
        }
        (_, _) => warn!("Must specify either --bind (-b) or --connect (-c)"),
    }

    Ok(())
}

/// Runs the benchmark until it finishes on its own or ctrl-c is pressed.
/// On ctrl-c the workers are cancelled and awaited so they can flush and
/// report their final statistics before the process exits.
async fn spawn_test<F>(fut: F, cancel: CancellationToken)
where
    F: Future<Output = Result<(), AppError>> + Send + 'static,
{
    let mut handle = tokio::spawn(async move {
        if let Err(e) = fut.await {
            error!("{e}");
        }
    });

    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            match res {
                Ok(()) => {
                    info!("ctrl-c received, stopping workers...");
                    cancel.cancel();
                }
                Err(e) => error!("Failed to listen for ctrl-c: {e}"),
            }
        }
        // The benchmark finished before any ctrl-c; nothing to shut down.
        _ = &mut handle => return,
    }

    // Give workers a bounded time to drain (flush buffers, print stats).
    match tokio::time::timeout(Duration::from_secs(5), handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!("Worker task failed: {e}"),
        Err(_) => error!("Workers did not stop within 5s, forcing shutdown"),
    }
}
