use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Result};
use portrelay::{
    config::{
        Endpoint, ForwardMode, RelaySpec, Settings, DEFAULT_BUFFER_POOL_SIZE, DEFAULT_BUFFER_SIZE,
        DEFAULT_CONNECTION_LOG_SAMPLE_RATE, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT,
    },
    server::Server,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout, Duration},
};
use tokio_util::sync::CancellationToken;

const PAYLOAD_SIZE: usize = 4 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(120);
const RELAY_SHARDS: usize = 32;
const LOOPBACK_SHARD_START: u8 = 2;
const LAUNCH_BATCH_SIZE: usize = 1_000;
const LAUNCH_DELAY: Duration = Duration::from_millis(5);

struct Target {
    addresses: Vec<SocketAddr>,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
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
    let mut arguments = env::args().skip(1);
    let count = arguments
        .next()
        .unwrap_or_else(|| "1000".to_owned())
        .parse::<usize>()?;
    let forward_mode = arguments
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or_default();
    if arguments.next().is_some() {
        bail!("usage: load [CONNECTIONS] [auto|tokio|splice]");
    }
    if count == 0 {
        bail!("connection count must be greater than zero");
    }

    let target = spawn_target().await?;
    let settings = sharded_settings(&target.addresses, forward_mode)?;
    let server = Server::bind(settings).await?;
    let relay_addresses = server
        .listener_addrs()
        .into_iter()
        .map(|(_, address)| address)
        .collect::<Vec<_>>();
    if relay_addresses.len() != RELAY_SHARDS {
        bail!(
            "expected {RELAY_SHARDS} relay shards, bound {}",
            relay_addresses.len()
        );
    }

    let relay_shutdown = CancellationToken::new();
    let relay_task = tokio::spawn(server.run(relay_shutdown.clone()));
    sleep(Duration::from_millis(100)).await;
    let payload = Arc::new(vec![0x5a_u8; PAYLOAD_SIZE]);

    let started = Instant::now();
    let mut clients = JoinSet::new();
    let state = Arc::new(LoadState {
        start_payload: CancellationToken::new(),
        active_clients: AtomicUsize::new(0),
        max_active_clients: AtomicUsize::new(0),
        connected_clients: AtomicUsize::new(0),
        failed_connections: AtomicUsize::new(0),
        connection_ready: Notify::new(),
    });
    for index in 0..count {
        let payload = Arc::clone(&payload);
        let relay_address = relay_addresses[index % relay_addresses.len()];
        let state = Arc::clone(&state);
        clients.spawn(async move {
            let result = round_trip(relay_address, payload, state).await;
            (index, result)
        });
        if (index + 1) % LAUNCH_BATCH_SIZE == 0 {
            sleep(LAUNCH_DELAY).await;
        }
    }

    loop {
        let ready = state.connected_clients.load(Ordering::Relaxed)
            + state.failed_connections.load(Ordering::Relaxed);
        if ready >= count {
            break;
        }
        state.connection_ready.notified().await;
    }
    let connected_before_payload = state.connected_clients.load(Ordering::Relaxed);
    println!("connected_clients_before_payload={connected_before_payload}");
    state.start_payload.cancel();

    let mut failures = Vec::new();
    while let Some(result) = clients.join_next().await {
        let (index, result) = result?;
        if let Err(error) = result {
            failures.push((index, error.to_string()));
        }
    }

    let elapsed = started.elapsed();
    let successful = count - failures.len();
    let total_bytes = successful as u64 * PAYLOAD_SIZE as u64 * 2;
    let seconds = elapsed.as_secs_f64();
    println!(
        "max_client_tasks_active={}",
        state.max_active_clients.load(Ordering::Relaxed)
    );
    println!("portrelay_version={}", env!("CARGO_PKG_VERSION"));
    println!("forward_mode={forward_mode:?}");
    println!("relay_shards={RELAY_SHARDS}");
    println!("requested_connections={count}");
    println!("successful_connections={successful}");
    println!("failed_connections={}", failures.len());
    println!("payload_bytes={PAYLOAD_SIZE}");
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

    stop_relay(relay_shutdown, relay_task).await?;
    stop_target(target).await?;

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{} of {count} clients failed", failures.len()))
    }
}

fn sharded_settings(
    target_addresses: &[SocketAddr],
    forward_mode: ForwardMode,
) -> Result<Settings> {
    if target_addresses.len() != RELAY_SHARDS {
        bail!(
            "expected {RELAY_SHARDS} target shards, found {}",
            target_addresses.len()
        );
    }
    let relays = (0..RELAY_SHARDS)
        .map(|index| {
            let host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, LOOPBACK_SHARD_START + index as u8));
            Ok(RelaySpec {
                name: format!("benchmark-{index}"),
                listen: Endpoint::parse(&SocketAddr::new(host, 0).to_string(), false, true)?,
                target: Endpoint::parse(&target_addresses[index].to_string(), false, false)?,
                connect_timeout: DEFAULT_CONNECT_TIMEOUT,
                idle_timeout: None,
                max_connections: None,
                max_connecting: None,
                tcp_keepalive: None,
                dns_refresh: None,
                socket_buffer_size: None,
                buffer_size: DEFAULT_BUFFER_SIZE,
                forward_mode,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Settings {
        log_level: "info".to_owned(),
        metrics_listen: None,
        metrics_bearer_token: None,
        metrics_tls_cert: None,
        metrics_tls_key: None,
        shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        global_max_connections: None,
        global_max_connecting: None,
        buffer_pool_size: DEFAULT_BUFFER_POOL_SIZE,
        connection_log_sample_rate: DEFAULT_CONNECTION_LOG_SAMPLE_RATE,
        reuse_port: false,
        relays,
    })
}

async fn round_trip(
    address: SocketAddr,
    payload: Arc<Vec<u8>>,
    state: Arc<LoadState>,
) -> Result<()> {
    let connection = match timeout(CLIENT_TIMEOUT, TcpStream::connect(address)).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(error)) => Err(anyhow!("connect: {error}")),
        Err(_) => Err(anyhow!("connect timeout")),
    };
    let mut client = match connection {
        Ok(client) => client,
        Err(error) => {
            state.failed_connections.fetch_add(1, Ordering::Relaxed);
            state.connection_ready.notify_one();
            return Err(error);
        }
    };

    state.connected_clients.fetch_add(1, Ordering::Relaxed);
    let active = state.active_clients.fetch_add(1, Ordering::Relaxed) + 1;
    state
        .max_active_clients
        .fetch_max(active, Ordering::Relaxed);
    state.connection_ready.notify_one();
    state.start_payload.cancelled().await;

    let result: Result<()> = async {
        timeout(CLIENT_TIMEOUT, client.write_all(&payload))
            .await
            .map_err(|_| anyhow!("write timeout"))?
            .map_err(|error| anyhow!("write: {error}"))?;
        let mut echoed = vec![0_u8; payload.len()];
        timeout(CLIENT_TIMEOUT, client.read_exact(&mut echoed))
            .await
            .map_err(|_| anyhow!("read timeout"))?
            .map_err(|error| anyhow!("read: {error}"))?;
        if echoed != *payload {
            bail!("echo payload mismatch");
        }
        Ok(())
    }
    .await;
    state.active_clients.fetch_sub(1, Ordering::Relaxed);
    result
}

async fn spawn_target() -> Result<Target> {
    let mut listeners = Vec::with_capacity(RELAY_SHARDS);
    let mut addresses = Vec::with_capacity(RELAY_SHARDS);
    for index in 0..RELAY_SHARDS {
        let host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, LOOPBACK_SHARD_START + index as u8));
        let listener = bind_target_listener(SocketAddr::new(host, 0)).await?;
        addresses.push(listener.local_addr()?);
        listeners.push(listener);
    }

    let shutdown = CancellationToken::new();
    let target_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        let mut acceptors = JoinSet::new();
        for listener in listeners {
            let target_shutdown = target_shutdown.clone();
            acceptors.spawn(async move {
                let mut children = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = target_shutdown.cancelled() => break,
                        accepted = listener.accept() => {
                            let Ok((stream, _)) = accepted else { break };
                            children.spawn(echo(stream));
                        }
                    }
                }
                children.abort_all();
                while children.join_next().await.is_some() {}
            });
        }
        while acceptors.join_next().await.is_some() {}
    });
    Ok(Target {
        addresses,
        shutdown,
        task,
    })
}

async fn bind_target_listener(address: SocketAddr) -> Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(65_535)?;
    socket.set_nonblocking(true)?;
    Ok(TcpListener::from_std(socket.into())?)
}

async fn echo(mut stream: TcpStream) {
    let mut buffer = [0_u8; PAYLOAD_SIZE];
    loop {
        let Ok(read) = stream.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        if stream.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
async fn stop_relay(shutdown: CancellationToken, mut task: JoinHandle<Result<()>>) -> Result<()> {
    shutdown.cancel();
    match timeout(Duration::from_secs(45), &mut task).await {
        Ok(result) => result??,
        Err(_) => {
            task.abort();
            let _ = task.await;
            println!("relay_shutdown=forced");
        }
    }
    Ok(())
}

async fn stop_target(mut target: Target) -> Result<()> {
    target.shutdown.cancel();
    match timeout(Duration::from_secs(10), &mut target.task).await {
        Ok(result) => result?,
        Err(_) => {
            target.task.abort();
            let _ = target.task.await;
            println!("target_shutdown=forced");
        }
    }
    Ok(())
}
