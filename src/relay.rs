use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    config::{ForwardMode, RelaySpec},
    metrics::RelayMetrics,
};
use arc_swap::ArcSwap;
#[cfg(target_os = "linux")]
use nix::{
    fcntl::{splice, OFlag, SpliceFFlags},
    sys::socket::{shutdown, Shutdown},
    unistd::{dup, pipe2},
};
use socket2::{SockRef, TcpKeepalive};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, OwnedFd};
#[cfg(target_os = "linux")]
use tokio::io::unix::AsyncFd;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct TargetResolver {
    address: String,
    resolved: Option<Arc<ArcSwap<Vec<SocketAddr>>>>,
}

impl TargetResolver {
    pub fn new(address: impl Into<String>, refresh_enabled: bool) -> Self {
        Self {
            address: address.into(),
            resolved: refresh_enabled.then(|| Arc::new(ArcSwap::from_pointee(Vec::new()))),
        }
    }

    pub async fn refresh(&self) -> io::Result<usize> {
        let addresses = tokio::net::lookup_host(&self.address)
            .await?
            .collect::<Vec<_>>();
        if let Some(resolved) = &self.resolved {
            resolved.store(Arc::new(addresses.clone()));
        }
        Ok(addresses.len())
    }

    pub async fn connect(&self) -> io::Result<TcpStream> {
        let Some(resolved) = &self.resolved else {
            return TcpStream::connect(&self.address).await;
        };
        let addresses = resolved.load();
        if !addresses.is_empty() {
            match TcpStream::connect(addresses.as_slice()).await {
                Ok(stream) => return Ok(stream),
                Err(cached_error) => {
                    return TcpStream::connect(&self.address)
                        .await
                        .map_err(|_| cached_error);
                }
            }
        }
        TcpStream::connect(&self.address).await
    }
}

pub struct BufferPool {
    buffer_size: usize,
    buffers: Mutex<Vec<Vec<u8>>>,
    permits: Arc<Semaphore>,
}

impl BufferPool {
    pub fn new(buffer_size: usize, buffer_count: usize) -> Arc<Self> {
        debug_assert!(buffer_size > 0);
        debug_assert!(buffer_count >= 2);
        Arc::new(Self {
            buffer_size,
            buffers: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(buffer_count)),
        })
    }

    async fn acquire_pair(self: &Arc<Self>) -> io::Result<(PooledBuffer, PooledBuffer)> {
        let mut permits = Arc::clone(&self.permits)
            .acquire_many_owned(2)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "buffer pool closed"))?;
        let first_permit = permits.split(1).expect("buffer pool acquired two permits");
        let second_permit = permits;
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|_| io::Error::other("buffer pool lock poisoned"))?;
        let first = buffers.pop().unwrap_or_else(|| vec![0; self.buffer_size]);
        let second = buffers.pop().unwrap_or_else(|| vec![0; self.buffer_size]);
        Ok((
            PooledBuffer {
                pool: Arc::clone(self),
                permit: Some(first_permit),
                buffer: first,
            },
            PooledBuffer {
                pool: Arc::clone(self),
                permit: Some(second_permit),
                buffer: second,
            },
        ))
    }
}

struct PooledBuffer {
    pool: Arc<BufferPool>,
    permit: Option<OwnedSemaphorePermit>,
    buffer: Vec<u8>,
}

impl PooledBuffer {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        if let Ok(mut buffers) = self.pool.buffers.lock() {
            buffers.push(buffer);
        }
        let _ = self.permit.take();
    }
}
pub(crate) struct ConnectionContext {
    pub(crate) peer: SocketAddr,
    pub(crate) connection_id: u64,
    pub(crate) spec: Arc<RelaySpec>,
    pub(crate) target_resolver: Arc<TargetResolver>,
    pub(crate) metrics: Arc<RelayMetrics>,
    pub(crate) buffer_pool: Arc<BufferPool>,
    pub(crate) force_shutdown: CancellationToken,
    pub(crate) log_connection: bool,
    pub(crate) global_connecting_permit: Option<OwnedSemaphorePermit>,
    pub(crate) relay_connecting_permit: Option<OwnedSemaphorePermit>,
}

pub(crate) async fn handle_connection(client: TcpStream, context: ConnectionContext) {
    let ConnectionContext {
        peer,
        connection_id,
        spec,
        target_resolver,
        metrics,
        buffer_pool,
        force_shutdown,
        log_connection,
        global_connecting_permit,
        relay_connecting_permit,
    } = context;
    let connection_started = Instant::now();
    apply_socket_options(
        &client,
        spec.tcp_keepalive,
        spec.socket_buffer_size,
        connection_id,
        "client",
    );

    let connect_started = Instant::now();
    let upstream_result = tokio::select! {
        _ = force_shutdown.cancelled() => {
            metrics.closed(connection_started.elapsed(), 0, 0);
            return;
        }
        result = timeout(
            spec.connect_timeout,
            target_resolver.connect(),
        ) => result,
    };
    let connect_duration = connect_started.elapsed();
    let upstream = match upstream_result {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            metrics.upstream_failure();
            if log_connection {
                debug!(
                    relay = %spec.name,
                    connection_id,
                    peer = %peer,
                    target = %spec.target,
                    error = %error,
                    event = "upstream_connect_failed",
                );
            }
            metrics.closed(connection_started.elapsed(), 0, 0);
            return;
        }
        Err(_) => {
            metrics.upstream_timeout();
            if log_connection {
                debug!(
                    relay = %spec.name,
                    connection_id,
                    peer = %peer,
                    target = %spec.target,
                    timeout_ms = connect_duration.as_millis() as u64,
                    event = "upstream_connect_timeout",
                );
            }
            metrics.closed(connection_started.elapsed(), 0, 0);
            return;
        }
    };
    drop(global_connecting_permit);
    drop(relay_connecting_permit);
    apply_socket_options(
        &upstream,
        spec.tcp_keepalive,
        spec.socket_buffer_size,
        connection_id,
        "upstream",
    );

    let transfer = tokio::select! {
        _ = force_shutdown.cancelled() => {
            metrics.closed(connection_started.elapsed(), 0, 0);
            return;
        }
        result = forward_with_mode(
            client,
            upstream,
            buffer_pool,
            spec.forward_mode,
            spec.idle_timeout,
        ) => result,
    };

    let (client_to_upstream, upstream_to_client) = match transfer {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
            metrics.idle_timeout();
            if log_connection {
                debug!(
                    relay = %spec.name,
                    connection_id,
                    peer = %peer,
                    idle_timeout_ms = spec.idle_timeout.map_or(0, |value| value.as_millis() as u64),
                    event = "connection_idle_timeout",
                );
            }
            (0, 0)
        }
        Err(error) => {
            if log_connection {
                debug!(
                    relay = %spec.name,
                    connection_id,
                    peer = %peer,
                    error = %error,
                    event = "connection_forward_error",
                );
            }
            (0, 0)
        }
    };

    metrics.closed(
        connection_started.elapsed(),
        client_to_upstream,
        upstream_to_client,
    );
    if log_connection {
        debug!(
            relay = %spec.name,
            connection_id,
            peer = %peer,
            duration_ms = connection_started.elapsed().as_millis() as u64,
            client_to_upstream_bytes = client_to_upstream,
            upstream_to_client_bytes = upstream_to_client,
            event = "connection_closed",
        );
    }
}

fn apply_socket_options(
    stream: &TcpStream,
    keepalive: Option<Duration>,
    socket_buffer_size: Option<usize>,
    connection_id: u64,
    side: &str,
) {
    let socket_ref = SockRef::from(stream);
    if let Some(size) = socket_buffer_size {
        if let Err(error) = socket_ref.set_recv_buffer_size(size) {
            warn!(
                connection_id,
                side,
                buffer_size = size,
                error = %error,
                event = "tcp_receive_buffer_failed",
            );
        }
        if let Err(error) = socket_ref.set_send_buffer_size(size) {
            warn!(
                connection_id,
                side,
                buffer_size = size,
                error = %error,
                event = "tcp_send_buffer_failed",
            );
        }
    }
    if let Err(error) = socket_ref.set_tcp_nodelay(true) {
        debug!(connection_id, side, error = %error, event = "tcp_nodelay_failed");
    }
    if let Some(duration) = keepalive {
        let params = TcpKeepalive::new().with_time(duration);
        if let Err(error) = socket_ref.set_tcp_keepalive(&params) {
            warn!(
                connection_id,
                side,
                keepalive_ms = duration.as_millis() as u64,

                error = %error,
                event = "tcp_keepalive_failed",
            );
        }
    }
}
async fn forward_with_mode(
    client: TcpStream,
    upstream: TcpStream,
    buffer_pool: Arc<BufferPool>,
    forward_mode: ForwardMode,
    idle_timeout: Option<Duration>,
) -> io::Result<(u64, u64)> {
    #[cfg(target_os = "linux")]
    if forward_mode == ForwardMode::Splice {
        return forward_with_splice(&client, &upstream, idle_timeout).await;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = forward_mode;

    forward_with_buffer_pool(client, upstream, buffer_pool, idle_timeout).await
}

async fn forward_with_buffer_pool(
    client: TcpStream,
    upstream: TcpStream,
    buffer_pool: Arc<BufferPool>,
    idle_timeout: Option<Duration>,
) -> io::Result<(u64, u64)> {
    let (client_buffer, upstream_buffer) = buffer_pool.acquire_pair().await?;
    let (client_reader, client_writer) = client.into_split();
    let (upstream_reader, upstream_writer) = upstream.into_split();
    let (activity_tx, activity_rx) = watch::channel(());

    let transfer = async move {
        tokio::try_join!(
            copy_direction(
                client_reader,
                upstream_writer,
                client_buffer,
                activity_tx.clone(),
            ),
            copy_direction(upstream_reader, client_writer, upstream_buffer, activity_tx),
        )
    };

    if let Some(idle_timeout) = idle_timeout {
        tokio::pin!(transfer);
        let idle = idle_watchdog(activity_rx, idle_timeout);
        tokio::pin!(idle);
        tokio::select! {
            result = &mut transfer => result,
            _ = &mut idle => Err(io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout")),
        }
    } else {
        transfer.await
    }
}
#[cfg(target_os = "linux")]
const SPLICE_CHUNK_SIZE: usize = 64 * 1024;

#[cfg(target_os = "linux")]
async fn forward_with_splice(
    client: &TcpStream,
    upstream: &TcpStream,
    idle_timeout: Option<Duration>,
) -> io::Result<(u64, u64)> {
    let client_fd = dup(client).map_err(nix_to_io)?;
    let upstream_fd = dup(upstream).map_err(nix_to_io)?;
    let client = AsyncFd::new(client_fd)?;
    let upstream = AsyncFd::new(upstream_fd)?;
    let pipe_flags = OFlag::O_NONBLOCK | OFlag::O_CLOEXEC;
    let (client_pipe_read, client_pipe_write) = pipe2(pipe_flags).map_err(splice_setup_error)?;
    let (upstream_pipe_read, upstream_pipe_write) =
        pipe2(pipe_flags).map_err(splice_setup_error)?;
    let client_pipe_read = AsyncFd::new(client_pipe_read)?;
    let client_pipe_write = AsyncFd::new(client_pipe_write)?;
    let upstream_pipe_read = AsyncFd::new(upstream_pipe_read)?;
    let upstream_pipe_write = AsyncFd::new(upstream_pipe_write)?;
    let (activity_tx, activity_rx) = watch::channel(());

    let transfer = async move {
        tokio::try_join!(
            splice_direction(
                &client,
                &upstream,
                &client_pipe_read,
                &client_pipe_write,
                activity_tx.clone(),
            ),
            splice_direction(
                &upstream,
                &client,
                &upstream_pipe_read,
                &upstream_pipe_write,
                activity_tx,
            ),
        )
    };

    if let Some(idle_timeout) = idle_timeout {
        tokio::pin!(transfer);
        let idle = idle_watchdog(activity_rx, idle_timeout);
        tokio::pin!(idle);
        tokio::select! {
            result = &mut transfer => result,
            _ = &mut idle => Err(io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout")),
        }
    } else {
        transfer.await
    }
}

#[cfg(target_os = "linux")]
async fn splice_direction(
    source: &AsyncFd<OwnedFd>,
    destination: &AsyncFd<OwnedFd>,
    pipe_read: &AsyncFd<OwnedFd>,
    pipe_write: &AsyncFd<OwnedFd>,
    activity: watch::Sender<()>,
) -> io::Result<u64> {
    let mut copied = 0_u64;
    loop {
        let moved = splice_ready(source, pipe_write, SPLICE_CHUNK_SIZE).await?;
        if moved == 0 {
            shutdown(destination.get_ref().as_raw_fd(), Shutdown::Write).map_err(nix_to_io)?;
            return Ok(copied);
        }
        let mut pending = moved;
        while pending > 0 {
            let written = splice_ready(pipe_read, destination, pending).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "splice destination closed",
                ));
            }
            pending -= written;
            copied += written as u64;
            let _ = activity.send(());
        }
    }
}

#[cfg(target_os = "linux")]
async fn splice_ready(
    source: &AsyncFd<OwnedFd>,
    destination: &AsyncFd<OwnedFd>,
    length: usize,
) -> io::Result<usize> {
    let flags =
        SpliceFFlags::SPLICE_F_MOVE | SpliceFFlags::SPLICE_F_MORE | SpliceFFlags::SPLICE_F_NONBLOCK;
    loop {
        tokio::select! {
            source_ready = source.readable() => {
                let mut ready = source_ready?;
                if let Ok(result) = ready.try_io(|_| {
                    splice(source.get_ref(), None, destination.get_ref(), None, length, flags)
                        .map_err(nix_to_io)
                }) {
                    return result;
                }
            }
            destination_ready = destination.writable() => {
                let mut ready = destination_ready?;
                if let Ok(result) = ready.try_io(|_| {
                    splice(source.get_ref(), None, destination.get_ref(), None, length, flags)
                        .map_err(nix_to_io)
                }) {
                    return result;
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn splice_setup_error(error: nix::errno::Errno) -> io::Error {
    if matches!(
        error,
        nix::errno::Errno::ENOSYS | nix::errno::Errno::EINVAL | nix::errno::Errno::EOPNOTSUPP
    ) {
        io::Error::new(io::ErrorKind::Unsupported, error.to_string())
    } else {
        nix_to_io(error)
    }
}

#[cfg(target_os = "linux")]
fn nix_to_io(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

async fn copy_direction<R, W>(
    mut reader: R,
    mut writer: W,
    mut buffer: PooledBuffer,
    activity: watch::Sender<()>,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    loop {
        let read = reader.read(buffer.as_mut_slice()).await?;
        if read == 0 {
            writer.shutdown().await?;
            return Ok(copied);
        }
        writer.write_all(&buffer.as_mut_slice()[..read]).await?;
        copied += read as u64;
        let _ = activity.send(());
    }
}

async fn idle_watchdog(mut activity: watch::Receiver<()>, idle_timeout: Duration) {
    let mut timer = Box::pin(sleep(idle_timeout));
    loop {
        tokio::select! {
            _ = &mut timer => return,
            changed = activity.changed() => {
                if changed.is_err() {
                    return;
                }
                timer.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
            }
        }
    }
}
