use crate::config::{ClientConfig, ServerConfig, Stats, calculate_gb, calculate_mb};
use crate::error::AppError;
use log::{error, info};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{self, JoinSet};
use tokio_util::sync::CancellationToken;

pub async fn run_async_server(
    config: ServerConfig,
    cancel: CancellationToken,
) -> Result<(), AppError> {
    let addr = format!("{}:{}", config.bind, config.port);
    let listener = TcpListener::bind(addr.as_str()).await?;
    info!("(async) Server listening on {addr}");

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (socket, peer) = accept?;
                info!("Client connected: {peer}");
                let buffer = config.buffer;
                let cancel = cancel.clone();
                connections.spawn(async move { handle_connection(socket, peer, buffer, cancel).await });
            }
            _ = cancel.cancelled() => {
                info!("Server shutting down, draining {} connection(s)...", connections.len());
                break;
            }
        }
    }

    while connections.join_next().await.is_some() {}
    Ok(())
}

pub async fn handle_connection(
    mut socket: TcpStream,
    peer: SocketAddr,
    buffer: usize,
    cancel: CancellationToken,
) {
    let mut buf = vec![0u8; buffer];
    let mut total_bytes: u64 = 0;
    let mut window_bytes: u64 = 0;
    let start = tokio::time::Instant::now();
    let mut last = start;

    loop {
        let n = tokio::select! {
            res = socket.read(&mut buf) => match res {
                Ok(0) => {
                    info!("{peer} closed the connection");
                    break;
                }
                Ok(n) => n,
                Err(e) => {
                    error!("Read error from {peer}: {e}");
                    break;
                }
            },
            _ = cancel.cancelled() => break,
        };

        total_bytes += n as u64;
        window_bytes += n as u64;

        let dt = last.elapsed().as_secs_f64();
        if dt >= 1.0 {
            let mbps = calculate_mb(window_bytes) / dt;
            info!("{peer}: receiving {mbps:.2} Mbps");
            window_bytes = 0;
            last = tokio::time::Instant::now();
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        let mbps = calculate_mb(total_bytes) / elapsed;
        info!("{peer}: received {total_bytes} bytes in {elapsed:.2}s ({mbps:.2} Mbps avg)");
    }
}

pub async fn run_async_clients(
    config: ClientConfig,
    cancel: CancellationToken,
) -> Result<(), AppError> {
    let barrier = Arc::new(tokio::sync::Barrier::new(config.threads));
    let (tx, mut rx) = tokio::sync::mpsc::channel(config.threads + 10);
    for _ in 0..config.threads {
        let tx = tx.clone();
        let br = barrier.clone();
        let c = config.clone();
        let cancel = cancel.clone();
        task::spawn(async move {
            let res = run_async_client(c, br, cancel).await;
            if let Err(e) = tx.send(res).await {
                error!("Error sending result: {e:?}");
            }
        });
    }
    drop(tx);

    let mut grand_total_bytes: u64 = 0;
    let mut grand_total_messages: u64 = 0;
    let mut aggregate_gbps: f64 = 0.0;
    while let Some(received) = rx.recv().await {
        match received {
            Ok(stat) => {
                grand_total_bytes += stat.total_bytes;
                grand_total_messages += stat.total_messages;
                let secs = stat.elapsed.as_secs_f64();
                if secs > 0.0 {
                    let mbps = calculate_mb(stat.total_bytes) / secs;
                    aggregate_gbps += calculate_gb(stat.total_bytes) / secs;
                    info!(
                        "Stream speed: {mbps:.2} Mbps; stream messages: {:.0}/s (over {secs:.2}s)",
                        stat.total_messages as f64 / secs,
                    );
                } else {
                    info!("Stream stopped before any data was measured");
                }
            }
            Err(e) => {
                error!("Worker error: {e}");
            }
        }
    }

    info!("Total speed in all streams: {aggregate_gbps:.3} Gbps");
    info!("Total: {grand_total_messages} messages, {grand_total_bytes} bytes");
    Ok(())
}

pub async fn run_async_client(
    config: ClientConfig,
    barrier: Arc<tokio::sync::Barrier>,
    cancel: CancellationToken,
) -> Result<Stats, AppError> {
    let addr = format!("{}:{}", config.connect, config.port);
    let stream = TcpStream::connect(addr.as_str()).await?;
    stream.set_nodelay(true)?;
    info!("Connected to server {addr}");

    let mut writer: Box<dyn AsyncWrite + Unpin + Send> = if config.buffer > 0 {
        Box::new(BufWriter::with_capacity(config.buffer, stream))
    } else {
        Box::new(stream)
    };

    let mut packet = vec![0u8; config.size];
    let mut window_bytes: u64 = 0;
    let mut window_messages: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_messages: u64 = 0;
    let mut counter = 0_u128;

    // All streams wait until every connection is established; the
    // measurement window starts immediately after the barrier releases.
    tokio::select! {
        _ = barrier.wait() => {}
        _ = cancel.cancelled() => {
            return Ok(Stats {
                total_bytes: 0,
                total_messages: 0,
                elapsed: Duration::ZERO,
            });
        }
    }
    let start = tokio::time::Instant::now();
    let mut last = start;

    while !cancel.is_cancelled() && start.elapsed() < config.duration {
        if config.variable_data {
            // Mutate the packet in place so the packet length never changes;
            // written bytes must stay identical to the counted bytes.
            let bytes = counter.to_le_bytes();
            let n = packet.len().min(bytes.len());
            packet[..n].copy_from_slice(&bytes[..n]);
            counter += 1;
        }

        writer.write_all(&packet).await?;
        total_bytes += packet.len() as u64;
        window_bytes += packet.len() as u64;
        total_messages += 1;
        window_messages += 1;

        let dt = last.elapsed().as_secs_f64();
        if dt >= 1.0 {
            let mbps = calculate_mb(window_bytes) / dt;
            info!("Sent {mbps:.2} Mbps; {window_messages} messages");
            window_bytes = 0;
            window_messages = 0;
            last = tokio::time::Instant::now();
        }
    }

    // BufWriter does not flush on drop; without this the tail of the data
    // would be counted in the totals but never reach the socket.
    if let Err(e) = writer.flush().await {
        error!("Final flush failed ({e}); totals may include unsent bytes");
    }

    Ok(Stats {
        total_bytes,
        total_messages,
        elapsed: start.elapsed(),
    })
}
