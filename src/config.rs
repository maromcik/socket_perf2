use clap::Parser;
use std::{net::IpAddr, time::Duration};

#[derive(Parser, Debug)]
#[command(name = "socket_perf", about = "Measure TCP throughput")]
pub struct Args {
    #[arg(short = 's', long, default_value = "1500")]
    pub size: usize,

    #[arg(short = 'f')]
    pub buffer: Option<usize>,

    #[arg(short = 'p', long = "port", default_value = "3003")]
    pub port: u16,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        env = "RUST_LOG",
        value_name = "LOG_LEVEL"
    )]
    pub log_level: log::LevelFilter,

    #[arg(short = 'b', long = "bind")]
    pub bind: Option<IpAddr>,

    #[arg(short = 'c', long = "connect")]
    pub connect: Option<IpAddr>,

    #[arg(short = 'v', long = "changing_data", action = clap::ArgAction::SetTrue)]
    pub variable_data: bool,

    #[arg(short = 't', long = "threads", default_value = "1")]
    pub threads: usize,

    #[arg(short = 'd', long = "duration", default_value = "10s", value_parser = humantime::parse_duration)]
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub total_bytes: u64,
    pub total_messages: u64,
    /// Actual measured send window; rates must be derived from this,
    /// never from the configured duration (which is only a deadline).
    pub elapsed: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub connect: IpAddr,
    pub port: u16,
    pub threads: usize,
    pub size: usize,
    pub buffer: usize,
    pub variable_data: bool,
    pub duration: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: IpAddr,
    pub port: u16,
    pub buffer: usize,
}

pub fn calculate_mb(val: u64) -> f64 {
    (val as f64 * 8.0) / 1_000_000.0
}

pub fn calculate_gb(val: u64) -> f64 {
    (val as f64 * 8.0) / 1_000_000_000.0
}

impl ClientConfig {
    pub fn from(args: &Args) -> Option<Self> {
        let connect = args.connect?;
        let config = Self {
            connect,
            port: args.port,
            threads: args.threads,
            size: args.size,
            buffer: args.buffer.unwrap_or(0),
            variable_data: args.variable_data,
            duration: args.duration,
        };
        Some(config)
    }
}

impl ServerConfig {
    pub fn from(args: &Args) -> Option<Self> {
        let bind = args.bind?;
        let config = Self {
            bind,
            port: args.port,
            buffer: args.buffer.unwrap_or(67_108_864),
        };
        Some(config)
    }
}
