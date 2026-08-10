use anyhow::{anyhow, Context, Result};
use rustls::ServerConfig;
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, warn};

use crate::{
    config::{RelaySpec, Settings},
    metrics::{MetricsRegistry, RelayMetrics},
    relay::{self, BufferPool},
};

const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const LISTEN_BACKLOG: i32 = 65_535;
const METRICS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

struct BoundRelay {
    listener: TcpListener,
    spec: Arc<RelaySpec>,
    target: Arc<relay::TargetResolver>,
    metrics: Arc<RelayMetrics>,
    buffer_pool: Arc<BufferPool>,
    per_relay_limit: Option<Arc<Semaphore>>,
    global_limit: Option<Arc<Semaphore>>,
    per_relay_connecting: Option<Arc<Semaphore>>,
    global_connecting: Option<Arc<Semaphore>>,
    connection_log_sample_rate: usize,
}

pub struct Server {
    shutdown_timeout: Duration,
    relays: Vec<BoundRelay>,
    metrics_listener: Option<TcpListener>,
    metrics_tls_acceptor: Option<Arc<TlsAcceptor>>,
    metrics_bearer_token: Option<Arc<str>>,
    metrics_registry: MetricsRegistry,
    connection_tasks: TaskTracker,
    next_connection_id: Arc<AtomicU64>,
}

fn build_metrics_tls_acceptor(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<Option<Arc<TlsAcceptor>>> {
    let (Some(cert_path), Some(key_path)) = (cert_path, key_path) else {
        return Ok(None);
    };

    let mut cert_reader = BufReader::new(File::open(cert_path).with_context(|| {
        format!(
            "cannot open metrics TLS certificate {}",
            cert_path.display()
        )
    })?);
    let certificates = CertificateDer::pem_reader_iter(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("cannot parse metrics TLS certificate")?;
    if certificates.is_empty() {
        return Err(anyhow!(
            "metrics TLS certificate file contains no certificates"
        ));
    }

    let mut key_reader = BufReader::new(
        File::open(key_path)
            .with_context(|| format!("cannot open metrics TLS key {}", key_path.display()))?,
    );
    let private_key = PrivateKeyDer::from_pem_reader(&mut key_reader)
        .context("cannot parse metrics TLS private key")?;
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("invalid metrics TLS certificate/key")?;
    Ok(Some(Arc::new(TlsAcceptor::from(Arc::new(config)))))
}

impl Server {
    pub async fn bind(settings: Settings) -> Result<Self> {
        settings.validate()?;
        let metrics_tls_acceptor = build_metrics_tls_acceptor(
            settings.metrics_tls_cert.as_deref(),
            settings.metrics_tls_key.as_deref(),
        )?;
        let metrics_bearer_token = settings.metrics_bearer_token.clone().map(Arc::<str>::from);
        let global_limit = settings
            .global_max_connections
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let global_connecting = settings
            .global_max_connecting
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let buffer_pool_size = settings.buffer_pool_size;
        let connection_log_sample_rate = settings.connection_log_sample_rate;
        let reuse_port = settings.reuse_port;
        let mut relays = Vec::with_capacity(settings.relays.len());

        for spec in settings.relays {
            let spec = Arc::new(spec);
            let target = Arc::new(relay::TargetResolver::new(
                spec.target.address(),
                spec.dns_refresh.is_some(),
            ));
            initialize_target(&spec, &target).await;
            let metrics = Arc::new(RelayMetrics::new(spec.name.clone()));
            let listener = match bind_listener(spec.listen.address(), reuse_port).await {
                Ok(listener) => listener,
                Err(error) => {
                    error!(
                        relay = %spec.name,
                        listen = %spec.listen,
                        target = %spec.target,
                        error = %error,
                        event = "listener_bind_failed",
                    );
                    continue;
                }
            };
            let local_addr = listener.local_addr()?;
            info!(
                relay = %spec.name,
                listen = %local_addr,
                target = %spec.target,
                event = "listener_started",
            );
            relays.push(BoundRelay {
                listener,
                spec: Arc::clone(&spec),
                target,
                metrics,
                buffer_pool: BufferPool::new(spec.buffer_size, buffer_pool_size),
                per_relay_limit: spec
                    .max_connections
                    .map(|limit| Arc::new(Semaphore::new(limit))),
                global_limit: global_limit.clone(),
                per_relay_connecting: spec
                    .max_connecting
                    .map(|limit| Arc::new(Semaphore::new(limit))),
                global_connecting: global_connecting.clone(),
                connection_log_sample_rate,
            });
        }

        if relays.is_empty() {
            return Err(anyhow!("no relay listeners could be started"));
        }

        let metrics_registry = MetricsRegistry::new(
            relays
                .iter()
                .map(|relay| Arc::clone(&relay.metrics))
                .collect(),
        );
        let metrics_listener = if let Some(endpoint) = settings.metrics_listen {
            match bind_listener(endpoint.address(), reuse_port).await {
                Ok(listener) => {
                    let local_addr = listener.local_addr()?;
                    info!(listen = %local_addr, event = "metrics_listener_started");
                    Some(listener)
                }
                Err(error) => {
                    error!(
                        listen = %endpoint,
                        error = %error,
                        event = "metrics_listener_bind_failed",
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            shutdown_timeout: settings.shutdown_timeout,
            relays,
            metrics_listener,
            metrics_tls_acceptor,
            metrics_bearer_token,
            metrics_registry,
            connection_tasks: TaskTracker::new(),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub fn listener_addrs(&self) -> Vec<(String, std::net::SocketAddr)> {
        self.relays
            .iter()
            .filter_map(|relay| {
                relay
                    .listener
                    .local_addr()
                    .ok()
                    .map(|addr| (relay.spec.name.clone(), addr))
            })
            .collect()
    }

    pub fn metrics_addr(&self) -> Option<std::net::SocketAddr> {
        self.metrics_listener
            .as_ref()
            .and_then(|listener| listener.local_addr().ok())
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let Server {
            shutdown_timeout,
            relays,
            metrics_listener,
            metrics_tls_acceptor,
            metrics_bearer_token,
            metrics_registry,
            connection_tasks,
            next_connection_id,
        } = self;
        let force_shutdown = CancellationToken::new();
        let mut listener_tasks = JoinSet::new();

        for relay in relays {
            if let Some(refresh) = relay.spec.dns_refresh {
                let target = Arc::clone(&relay.target);
                let relay_name = relay.spec.name.clone();
                let target_name = relay.spec.target.to_string();
                let token = shutdown.clone();
                listener_tasks.spawn(async move {
                    refresh_target_loop(target, relay_name, target_name, refresh, token).await;
                });
            }
            let token = shutdown.clone();
            let force = force_shutdown.clone();
            let tracker = connection_tasks.clone();
            let ids = Arc::clone(&next_connection_id);
            listener_tasks.spawn(async move {
                run_relay_listener(relay, token, force, tracker, ids).await;
            });
        }
        if let Some(listener) = metrics_listener {
            let token = shutdown.clone();
            let force = force_shutdown.clone();
            let tracker = connection_tasks.clone();
            let tls_acceptor = metrics_tls_acceptor.clone();
            let bearer_token = metrics_bearer_token.clone();
            listener_tasks.spawn(async move {
                run_metrics_listener(
                    listener,
                    metrics_registry,
                    tls_acceptor,
                    bearer_token,
                    token,
                    force,
                    tracker,
                )
                .await;
            });
        }

        shutdown.cancelled().await;
        shutdown.cancel();

        let graceful = async {
            while let Some(result) = listener_tasks.join_next().await {
                if let Err(error) = result {
                    warn!(error = %error, event = "listener_task_failed");
                }
            }
            connection_tasks.close();
            connection_tasks.wait().await;
        };

        if timeout(shutdown_timeout, graceful).await.is_err() {
            warn!(
                shutdown_timeout_ms = shutdown_timeout.as_millis() as u64,
                event = "shutdown_timeout_reached",
            );
            force_shutdown.cancel();
            listener_tasks.abort_all();
            connection_tasks.close();
            let _ = timeout(Duration::from_secs(1), connection_tasks.wait()).await;
            while let Some(result) = listener_tasks.join_next().await {
                if let Err(error) = result {
                    debug!(error = %error, event = "listener_task_aborted");
                }
            }
        }
        info!(event = "shutdown_complete");
        Ok(())
    }
}

async fn bind_listener(address: &str, reuse_port: bool) -> io::Result<TcpListener> {
    let mut addresses = tokio::net::lookup_host(address).await?;
    let address = addresses.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("no socket address resolved for {address}"),
        )
    })?;
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(unix))]
    let _ = reuse_port;
    socket.bind(&address.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    socket.set_nonblocking(true)?;
    TcpListener::from_std(socket.into())
}

pub async fn run_until_signal(settings: Settings) -> Result<()> {
    let server = Server::bind(settings).await?;
    let shutdown = CancellationToken::new();
    let mut server_task = Box::pin(server.run(shutdown.clone()));

    tokio::select! {
        result = &mut server_task => result,
        signal = wait_for_shutdown_signal() => {
            signal?;
            info!(event = "shutdown_started");
            shutdown.cancel();
            server_task.await
        }
    }
}

async fn initialize_target(spec: &RelaySpec, target: &relay::TargetResolver) {
    match target.refresh().await {
        Ok(0) => warn!(
            relay = %spec.name,
            target = %spec.target,
            event = "target_resolution_empty",
        ),
        Ok(addresses) => debug!(
            relay = %spec.name,
            target = %spec.target,
            addresses,
            event = "target_resolved",
        ),
        Err(error) => warn!(
            relay = %spec.name,
            target = %spec.target,
            error = %error,
            event = "target_resolution_failed",
        ),
    }
}

async fn refresh_target_loop(
    target: Arc<relay::TargetResolver>,
    relay_name: String,
    target_name: String,
    refresh: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(refresh);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                match target.refresh().await {
                    Ok(addresses) => debug!(
                        relay = %relay_name,
                        target = %target_name,
                        addresses,
                        event = "target_resolution_refreshed",
                    ),
                    Err(error) => warn!(
                        relay = %relay_name,
                        target = %target_name,
                        error = %error,
                        event = "target_refresh_failed",
                    ),
                }
            }
        }
    }
}

async fn run_relay_listener(
    bound: BoundRelay,
    shutdown: CancellationToken,
    force_shutdown: CancellationToken,
    connection_tasks: TaskTracker,
    next_connection_id: Arc<AtomicU64>,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            result = bound.listener.accept() => result,
        };
        let (client, peer) = match accepted {
            Ok(connection) => connection,
            Err(error) => {
                error!(
                    relay = %bound.spec.name,
                    error = %error,
                    event = "accept_failed",
                );
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = sleep(ACCEPT_ERROR_BACKOFF) => {}
                }
                continue;
            }
        };
        if shutdown.is_cancelled() {
            drop(client);
            break;
        }

        let global_permit = match acquire_permit(&bound.global_limit) {
            Ok(permit) => permit,
            Err(()) => {
                bound.metrics.rejected();
                drop(client);
                continue;
            }
        };
        let relay_permit = match acquire_permit(&bound.per_relay_limit) {
            Ok(permit) => permit,
            Err(()) => {
                bound.metrics.rejected();
                drop(global_permit);
                drop(client);
                continue;
            }
        };
        let global_connecting_permit = match acquire_permit(&bound.global_connecting) {
            Ok(permit) => permit,
            Err(()) => {
                bound.metrics.rejected();
                drop(relay_permit);
                drop(global_permit);
                drop(client);
                continue;
            }
        };
        let relay_connecting_permit = match acquire_permit(&bound.per_relay_connecting) {
            Ok(permit) => permit,
            Err(()) => {
                bound.metrics.rejected();
                drop(global_connecting_permit);
                drop(relay_permit);
                drop(global_permit);
                drop(client);
                continue;
            }
        };

        let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
        let log_connection = bound.connection_log_sample_rate == 1
            || connection_id % bound.connection_log_sample_rate as u64 == 0;
        bound.metrics.accepted();
        if log_connection {
            debug!(
                relay = %bound.spec.name,
                connection_id,
                peer = %peer,
                event = "connection_opened",
            );
        }

        let spec = Arc::clone(&bound.spec);
        let metrics = Arc::clone(&bound.metrics);
        let target = Arc::clone(&bound.target);
        let buffer_pool = Arc::clone(&bound.buffer_pool);
        let force = force_shutdown.clone();
        connection_tasks.spawn(async move {
            let _global_permit = global_permit;
            let _relay_permit = relay_permit;
            relay::handle_connection(
                client,
                relay::ConnectionContext {
                    peer,
                    connection_id,
                    spec,
                    target_resolver: target,
                    metrics,
                    buffer_pool,
                    force_shutdown: force,
                    log_connection,
                    global_connecting_permit,
                    relay_connecting_permit,
                },
            )
            .await;
        });
    }
    debug!(relay = %bound.spec.name, event = "listener_stopped");
}

fn acquire_permit(
    limit: &Option<Arc<Semaphore>>,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, ()> {
    match limit {
        None => Ok(None),
        Some(semaphore) => semaphore
            .clone()
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| ()),
    }
}

async fn run_metrics_listener(
    listener: TcpListener,
    registry: MetricsRegistry,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    bearer_token: Option<Arc<str>>,
    shutdown: CancellationToken,
    force_shutdown: CancellationToken,
    connection_tasks: TaskTracker,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            result = listener.accept() => result,
        };
        let (stream, _) = match accepted {
            Ok(connection) => connection,
            Err(error) => {
                warn!(error = %error, event = "metrics_accept_failed");
                continue;
            }
        };
        let registry = registry.clone();
        let tls_acceptor = tls_acceptor.clone();
        let bearer_token = bearer_token.clone();
        let force = force_shutdown.clone();
        connection_tasks.spawn(async move {
            if let Some(tls_acceptor) = tls_acceptor {
                let handshake = tokio::select! {
                    _ = force.cancelled() => return,
                    result = timeout(METRICS_REQUEST_TIMEOUT, tls_acceptor.accept(stream)) => result,
                };
                let Ok(Ok(stream)) = handshake else {
                    return;
                };
                serve_metrics_connection(stream, registry, force, bearer_token).await;
            } else {
                serve_metrics_connection(stream, registry, force, bearer_token).await;
            }
        });
    }
}

async fn serve_metrics_connection<S>(
    mut stream: S,
    registry: MetricsRegistry,
    force_shutdown: CancellationToken,
    bearer_token: Option<Arc<str>>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = [0_u8; 4 * 1024];
    let read = tokio::select! {
        _ = force_shutdown.cancelled() => {
            let _ = stream.shutdown().await;
            return;
        }
        result = timeout(
            METRICS_REQUEST_TIMEOUT,
            read_metrics_request(&mut stream, &mut request),
        ) => result,
    };
    let Ok(Ok((size, complete))) = read else {
        let _ = stream.shutdown().await;
        return;
    };
    let request_line = request[..size]
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let is_authorized = bearer_token
        .as_deref()
        .is_none_or(|token| has_valid_bearer_token(&request[..size], token));
    let is_metrics = request_line.starts_with(b"GET /metrics ");
    let (status, content_type, body, auth_header) = if !is_authorized {
        (
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            "unauthorized\n".to_owned(),
            "WWW-Authenticate: Bearer\r\n",
        )
    } else if !complete {
        (
            "400 Bad Request",
            "text/plain; charset=utf-8",
            "bad request\n".to_owned(),
            "",
        )
    } else if is_metrics {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            registry.render(),
            "",
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_owned(),
            "",
        )
    };
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{auth_header}Connection: close\r\n\r\n",
        body.len()
    );
    if stream.write_all(header.as_bytes()).await.is_ok() {
        let _ = stream.write_all(body.as_bytes()).await;
    }
    let _ = stream.shutdown().await;
}
async fn read_metrics_request<S>(stream: &mut S, request: &mut [u8]) -> io::Result<(usize, bool)>
where
    S: AsyncRead + Unpin,
{
    let mut size = 0;
    loop {
        if request[..size]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            return Ok((size, true));
        }
        let read = stream.read(&mut request[size..]).await?;
        if read == 0 {
            return Ok((size, false));
        }
        size += read;
    }
}

fn has_valid_bearer_token(request: &[u8], expected: &str) -> bool {
    request.split(|byte| *byte == b'\n').skip(1).any(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return false;
        };
        if !line[..colon].eq_ignore_ascii_case(b"authorization") {
            return false;
        }
        let value = trim_ascii(&line[colon + 1..]);
        if value.len() < 7
            || !value[..6].eq_ignore_ascii_case(b"bearer")
            || !value[6].is_ascii_whitespace()
        {
            return false;
        }
        let token = trim_ascii(&value[6..]);
        bool::from(token.ct_eq(expected.as_bytes()))
    })
}

fn trim_ascii(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &value[start..end]
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = sigterm.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}
