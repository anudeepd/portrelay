use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "portrelay",
    version,
    about = "High-performance protocol-agnostic TCP relay",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run one or more administrator-configured TCP relays.
    Serve(ServeArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Listener address. Use :PORT for all IPv4 interfaces.
    #[arg(long, value_name = "HOST:PORT", conflicts_with = "config")]
    pub listen: Option<String>,

    /// Administrator-configured upstream address.
    #[arg(long, value_name = "HOST:PORT", conflicts_with = "config")]
    pub target: Option<String>,

    /// TOML configuration containing one or more relay mappings.
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with_all = ["listen", "target"]
    )]
    pub config: Option<PathBuf>,

    /// Maximum time allowed to establish each upstream connection.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_arg)]
    pub connect_timeout: Option<Duration>,

    /// Close connections after this long without transferred data.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_arg)]
    pub idle_timeout: Option<Duration>,

    /// Maximum time allowed for graceful shutdown.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_arg)]
    pub shutdown_timeout: Option<Duration>,

    /// Maximum concurrent proxied connections in single-relay mode.
    #[arg(long, value_name = "NUMBER", value_parser = parse_positive_usize)]
    pub max_connections: Option<usize>,

    /// Maximum concurrent upstream connection attempts.
    #[arg(long, value_name = "NUMBER", value_parser = parse_positive_usize)]
    pub max_connecting: Option<usize>,

    /// Enable TCP keepalive probes after this idle duration.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_arg)]
    pub tcp_keepalive: Option<Duration>,

    /// Refresh administrator-configured DNS targets at this interval.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_arg)]
    pub dns_refresh: Option<Duration>,

    /// Set TCP receive and send buffer sizes.
    #[arg(long, value_name = "BYTES", value_parser = parse_positive_usize)]
    pub socket_buffer_size: Option<usize>,

    /// Number of bytes in each forwarding buffer.
    #[arg(long, value_name = "BYTES", value_parser = parse_positive_usize)]
    pub buffer_size: Option<usize>,

    /// Number of pooled forwarding buffers shared by connections.
    #[arg(long, value_name = "NUMBER", value_parser = parse_positive_usize)]
    pub buffer_pool_size: Option<usize>,

    /// Sample one connection log event for every N connections.
    #[arg(long, value_name = "NUMBER", value_parser = parse_positive_usize)]
    pub connection_log_sample_rate: Option<usize>,

    /// Forwarding backend: auto, tokio, or splice on Linux.
    #[arg(long, value_name = "BACKEND")]
    pub forward_mode: Option<String>,

    /// Allow multiple processes to bind the same listener with SO_REUSEPORT.
    #[arg(long)]
    pub reuse_port: bool,

    /// Optional Prometheus HTTP endpoint.
    #[arg(long, value_name = "HOST:PORT")]
    pub metrics_listen: Option<String>,

    /// Require this bearer token for metrics requests.
    #[arg(long, value_name = "TOKEN")]
    pub metrics_token: Option<String>,

    /// PEM certificate for HTTPS metrics.
    #[arg(long, value_name = "PATH")]
    pub metrics_tls_cert: Option<PathBuf>,

    /// PEM private key for HTTPS metrics.
    #[arg(long, value_name = "PATH")]
    pub metrics_tls_key: Option<PathBuf>,

    /// Tracing filter, for example info or debug.
    #[arg(long, value_name = "LEVEL")]
    pub log_level: Option<String>,
}

pub fn parse_duration_arg(value: &str) -> Result<Duration, String> {
    let parsed = humantime::parse_duration(value)
        .map_err(|error| format!("invalid duration {value:?}: {error}"))?;
    if parsed.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(parsed)
}

pub fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_owned());
    }
    Ok(parsed)
}

pub fn init_logging(level: &str) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_target(false)
        .init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_relay_options() {
        let cli = Cli::try_parse_from([
            "portrelay",
            "serve",
            "--listen",
            ":13306",
            "--target",
            "db.internal:3306",
            "--connect-timeout",
            "2s",
            "--max-connections",
            "10",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command;
        assert_eq!(args.listen.as_deref(), Some(":13306"));
        assert_eq!(args.target.as_deref(), Some("db.internal:3306"));
        assert_eq!(args.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(args.max_connections, Some(10));
    }

    #[test]
    fn rejects_conflicting_config_and_single_mode() {
        assert!(Cli::try_parse_from([
            "portrelay",
            "serve",
            "--config",
            "portrelay.toml",
            "--listen",
            ":13306",
        ])
        .is_err());
    }

    #[test]
    fn rejects_zero_duration_and_limit() {
        assert!(parse_duration_arg("0s").is_err());
        assert!(parse_positive_usize("0").is_err());
        assert_eq!(
            parse_duration_arg("1500ms").unwrap(),
            Duration::from_millis(1500)
        );
    }
}
