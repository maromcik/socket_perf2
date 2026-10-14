use crate::config::{ClientConfig, ServerConfig};
use crate::error::AppError;
use crate::stats::{Stats, build_table, si_scale};
use crate::token::CancellationAtomic;
use log::{debug, error, info};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{self, JoinSet};

pub async fn run_async_server(
    config: ServerConfig,
    cancel: CancellationAtomic,
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
    cancel: CancellationAtomic,
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
                    debug!("{peer} closed the connection");
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
            let bps = window_bytes as f64 * 8.0 / dt;
            println!("{peer}: Received {}", si_scale(bps, "bps"));
            window_bytes = 0;
            last = tokio::time::Instant::now();
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        let avg_bps = total_bytes as f64 * 8.0 / elapsed;
        println!(
            "{peer}: Received {} in {elapsed:.2}s ({} avg)",
            si_scale(total_bytes as f64, "B"),
            si_scale(avg_bps, "bps"),
        );
    }
}

pub async fn run_async_clients(
    config: ClientConfig,
    cancel: CancellationAtomic,
) -> Result<(), AppError> {
    let barrier = Arc::new(tokio::sync::Barrier::new(config.threads));
    let (tx, mut rx) = tokio::sync::mpsc::channel(config.threads + 10);
    for i in 0..config.threads {
        let tx = tx.clone();
        let br = barrier.clone();
        let c = config.clone();
        let cancel = cancel.clone();
        task::spawn(async move {
            let res = run_async_client(c, br, cancel, i).await;
            if let Err(e) = tx.send(res).await {
                error!("Error sending result: {e:?}");
            }
        });
    }
    drop(tx);

    let mut stream_stats: Vec<Stats> = Vec::new();
    while let Some(received) = rx.recv().await {
        match received {
            Ok(stat) => stream_stats.push(stat),
            Err(e) => error!("Worker error: {e}"),
        }
    }

    if stream_stats.is_empty() {
        info!("No stream produced any measurements");
        return Ok(());
    }
    // Results arrive in completion order; present them by stream id.
    stream_stats.sort_by_key(|s| s.id);

    let headers = [
        "#",
        "Duration",
        "Transferred",
        "Messages",
        "Throughput",
        "Msgs/s",
    ];
    let rows: Vec<Vec<String>> = stream_stats
        .iter()
        .map(|s| {
            let secs = s.elapsed.as_secs_f64();
            let (rate, msgs_per_s) = if secs > 0.0 {
                (
                    si_scale(s.total_bytes as f64 * 8.0 / secs, "bps"),
                    format!("{:.0}", s.total_messages as f64 / secs),
                )
            } else {
                ("-".to_string(), "-".to_string())
            };
            vec![
                s.id.to_string(),
                format!("{secs:.2} s"),
                si_scale(s.total_bytes as f64, "B"),
                s.total_messages.to_string(),
                rate,
                msgs_per_s,
            ]
        })
        .collect();
    println!("=== Per-stream stats ===\n{}", build_table(&headers, &rows));

    let grand_total_bytes: u64 = stream_stats.iter().map(|s| s.total_bytes).sum();
    let grand_total_messages: u64 = stream_stats.iter().map(|s| s.total_messages).sum();
    let total_secs = stream_stats
        .iter()
        .map(|s| s.elapsed)
        .max()
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    let aggregate_bps: f64 = stream_stats
        .iter()
        .map(|s| {
            let secs = s.elapsed.as_secs_f64();
            if secs > 0.0 {
                s.total_bytes as f64 * 8.0 / secs
            } else {
                0.0
            }
        })
        .sum();

    println!("=== Totals ===");
    println!(
        "  Window:      {total_secs:.2} s ({} streams)",
        stream_stats.len()
    );
    println!(
        "  Transferred: {} in {grand_total_messages} messages",
        si_scale(grand_total_bytes as f64, "B"),
    );
    println!("  Throughput:  {}", si_scale(aggregate_bps, "bps"));
    if total_secs > 0.0 {
        println!(
            "  Msg rate:    {:.0} msg/s",
            grand_total_messages as f64 / total_secs
        );
    }
    Ok(())
}

pub async fn run_async_client(
    config: ClientConfig,
    barrier: Arc<tokio::sync::Barrier>,
    cancel: CancellationAtomic,
    id: usize,
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
                id,
                total_bytes: 0,
                total_messages: 0,
                elapsed: Duration::ZERO,
            });
        }
    }
    let start = tokio::time::Instant::now();
    let mut last = start;
    // Pad ids so the columns of the periodic report stay aligned.
    let idw = config.threads.saturating_sub(1).to_string().len();

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
            let bps = window_bytes as f64 * 8.0 / dt;
            println!(
                "[{id:>idw$}] Sent {:>11}; {window_messages} messages",
                si_scale(bps, "bps")
            );
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
        id,
        total_bytes,
        total_messages,
        elapsed: start.elapsed(),
    })
}
