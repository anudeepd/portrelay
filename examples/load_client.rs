use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    task::JoinSet,
    time::{sleep, timeout, Duration},
};
use tokio_util::sync::CancellationToken;

const DEFAULT_PAYLOAD_BYTES: usize = 4 * 1024;
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_LAUNCH_BATCH: usize = 1_000;
const DEFAULT_LAUNCH_DELAY_MS: u64 = 5;

#[derive(Debug, Parser)]
#[command(about = "External TCP load generator for portrelay")]
struct Args {
    /// Relay listener. Repeat or provide a comma-separated list for shards.
    #[arg(
        long = "relay",
        value_name = "HOST:PORT",
        value_delimiter = ',',
        required = true
    )]
    relay: Vec<std::net::SocketAddr>,

    /// Number of client connections to open.
    #[arg(long, default_value_t = 1_000)]
    connections: usize,

    /// Binary payload size sent after all connections are established.
    #[arg(long, default_value_t = DEFAULT_PAYLOAD_BYTES)]
    payload_bytes: usize,

    /// Seconds to hold all connections open before sending the payload.
    #[arg(long, default_value_t = 0)]
    idle_seconds: u64,

    /// Number of request/response rounds per connection.
    #[arg(long, default_value_t = 1)]
    rounds: usize,

    /// Delay between request/response rounds.
    #[arg(long, default_value_t = 0)]
    round_interval_ms: u64,

    /// Seconds to keep connections open after the final response.
    #[arg(long, default_value_t = 0)]
    hold_after_seconds: u64,

    /// Per-connection connect, write, and read timeout.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,

    /// Number of clients launched between pacing delays.
    #[arg(long, default_value_t = DEFAULT_LAUNCH_BATCH)]
    launch_batch: usize,

    /// Delay between launch batches.
    #[arg(long, default_value_t = DEFAULT_LAUNCH_DELAY_MS)]
    launch_delay_ms: u64,
}

struct LoadState {
    start_payload: CancellationToken,
    active_clients: AtomicUsize,
    max_active_clients: AtomicUsize,
    connected_clients: AtomicUsize,
    failed_connections: AtomicUsize,
    connection_ready: Notify,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.connections == 0 {
        bail!("connections must be greater than zero");
    }
    if args.payload_bytes == 0 {
        bail!("payload-bytes must be greater than zero");
    }
    if args.rounds == 0 {
        bail!("rounds must be greater than zero");
    }

    if args.launch_batch == 0 {
        bail!("launch-batch must be greater than zero");
    }
    if args.relay.is_empty() {
        bail!("at least one relay listener is required");
    }

    let timeout_duration = Duration::from_secs(args.timeout_seconds);
    let round_interval = Duration::from_millis(args.round_interval_ms);
    let hold_after = Duration::from_secs(args.hold_after_seconds);

    let payload = Arc::new(vec![0x5a_u8; args.payload_bytes]);
    let state = Arc::new(LoadState {
        start_payload: CancellationToken::new(),
        active_clients: AtomicUsize::new(0),
        max_active_clients: AtomicUsize::new(0),
        connected_clients: AtomicUsize::new(0),
        failed_connections: AtomicUsize::new(0),
        connection_ready: Notify::new(),
    });

    let started = Instant::now();
    let mut clients = JoinSet::new();
    for index in 0..args.connections {
        let relay = args.relay[index % args.relay.len()];
        let payload = Arc::clone(&payload);
        let state = Arc::clone(&state);
        clients.spawn(async move {
            let result = round_trip(
                relay,
                payload,
                state,
                timeout_duration,
                args.rounds,
                round_interval,
                hold_after,
            )
            .await;
            (index, result)
        });
        if (index + 1) % args.launch_batch == 0 {
            sleep(Duration::from_millis(args.launch_delay_ms)).await;
        }
    }

    loop {
        let ready = state.connected_clients.load(Ordering::Relaxed)
            + state.failed_connections.load(Ordering::Relaxed);
        if ready >= args.connections {
            break;
        }
        state.connection_ready.notified().await;
    }

    let connected_before_payload = state.connected_clients.load(Ordering::Relaxed);
    println!("connected_clients_before_payload={connected_before_payload}");
    println!(
        "max_client_tasks_active_before_payload={}",
        state.max_active_clients.load(Ordering::Relaxed)
    );
    if args.idle_seconds > 0 {
        println!("idle_hold_seconds={}", args.idle_seconds);
        sleep(Duration::from_secs(args.idle_seconds)).await;
    }
    state.start_payload.cancel();

    let mut failures = Vec::new();
    while let Some(result) = clients.join_next().await {
        let (index, result) = result?;
        if let Err(error) = result {
            failures.push((index, error.to_string()));
        }
    }

    let elapsed = started.elapsed();
    let successful = args.connections - failures.len();
    let total_bytes = successful as u64 * args.payload_bytes as u64 * 2 * args.rounds as u64;
    let seconds = elapsed.as_secs_f64();
    println!("portrelay_version={}", env!("CARGO_PKG_VERSION"));
    println!("relay_shards={}", args.relay.len());
    println!("requested_connections={}", args.connections);
    println!("successful_connections={successful}");
    println!("failed_connections={}", failures.len());
    println!("payload_bytes={}", args.payload_bytes);
    println!("rounds={}", args.rounds);
    println!("round_interval_ms={}", args.round_interval_ms);
    println!("hold_after_seconds={}", args.hold_after_seconds);
    println!("total_forwarded_bytes={total_bytes}");
    println!("elapsed_seconds={seconds:.6}");
    println!(
        "successful_connections_per_second={:.2}",
        successful as f64 / seconds
    );
    println!(
        "aggregate_throughput_mib_per_second={:.2}",
        total_bytes as f64 / seconds / 1024.0 / 1024.0
    );
    if !failures.is_empty() {
        let details = failures
            .iter()
            .take(10)
            .map(|(index, error)| format!("{index}:{error}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("failure_examples={details}");
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "{} of {} clients failed",
            failures.len(),
            args.connections
        ))
    }
}

async fn round_trip(
    address: std::net::SocketAddr,
    payload: Arc<Vec<u8>>,
    state: Arc<LoadState>,
    timeout_duration: Duration,
    rounds: usize,
    round_interval: Duration,
    hold_after: Duration,
) -> Result<()> {
    let mut client = match timeout(timeout_duration, TcpStream::connect(address)).await {
        Ok(Ok(client)) => client,
        Ok(Err(error)) => return connection_failed(&state, format!("connect: {error}")),
        Err(_) => return connection_failed(&state, "connect timeout".to_owned()),
    };

    state.connected_clients.fetch_add(1, Ordering::Relaxed);
    let active = state.active_clients.fetch_add(1, Ordering::Relaxed) + 1;
    state
        .max_active_clients
        .fetch_max(active, Ordering::Relaxed);
    state.connection_ready.notify_one();
    state.start_payload.cancelled().await;

    let result: Result<()> = async {
        for round in 0..rounds {
            timeout(timeout_duration, client.write_all(&payload))
                .await
                .map_err(|_| anyhow!("write timeout"))?
                .map_err(|error| anyhow!("write: {error}"))?;
            let mut echoed = vec![0_u8; payload.len()];
            timeout(timeout_duration, client.read_exact(&mut echoed))
                .await
                .map_err(|_| anyhow!("read timeout"))?
                .map_err(|error| anyhow!("read: {error}"))?;
            if echoed != *payload {
                bail!("echo payload mismatch");
            }
            if round + 1 < rounds {
                sleep(round_interval).await;
            }
        }
        sleep(hold_after).await;
        Ok(())
    }
    .await;

    state.active_clients.fetch_sub(1, Ordering::Relaxed);
    result
}

fn connection_failed(state: &LoadState, message: String) -> Result<()> {
    state.failed_connections.fetch_add(1, Ordering::Relaxed);
    state.connection_ready.notify_one();
    Err(anyhow!(message))
}
