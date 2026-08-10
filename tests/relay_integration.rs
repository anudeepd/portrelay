use std::{net::SocketAddr, sync::Arc, time::Duration};

use portrelay::{
    config::{
        Endpoint, ForwardMode, RelaySpec, Settings, DEFAULT_BUFFER_POOL_SIZE, DEFAULT_BUFFER_SIZE,
        DEFAULT_CONNECTION_LOG_SAMPLE_RATE, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT,
    },
    relay::TargetResolver,
    server::Server,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

struct TargetHandle {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

struct RelayHandle {
    listeners: Vec<(String, SocketAddr)>,
    metrics: Option<SocketAddr>,
    shutdown: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
}

async fn spawn_target(banner: &[u8]) -> TargetHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let target_shutdown = shutdown.clone();
    let banner = Arc::new(banner.to_vec());
    let task = tokio::spawn(async move {
        let mut children = JoinSet::new();
        loop {
            tokio::select! {
                _ = target_shutdown.cancelled() => break,
                result = listener.accept() => {
                    let Ok((stream, _)) = result else { break };
                    let banner = Arc::clone(&banner);
                    children.spawn(async move { serve_target(stream, banner).await });
                }
            }
        }
        while children.join_next().await.is_some() {}
    });
    TargetHandle {
        addr,
        shutdown,
        task,
    }
}

async fn serve_target(mut stream: TcpStream, banner: Arc<Vec<u8>>) {
    if !banner.is_empty() {
        stream.write_all(&banner).await.unwrap();
    }
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = stream.read(&mut buffer).await.unwrap();
        if read == 0 {
            return;
        }
        stream.write_all(&buffer[..read]).await.unwrap();
    }
}

fn relay_spec(name: &str, listen: &str, target: SocketAddr) -> RelaySpec {
    RelaySpec {
        name: name.to_owned(),
        listen: Endpoint::parse(listen, true, true).unwrap(),
        target: Endpoint::parse(&target.to_string(), false, false).unwrap(),
        connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        idle_timeout: None,
        max_connections: None,
        max_connecting: None,
        tcp_keepalive: None,
        dns_refresh: None,
        socket_buffer_size: None,
        buffer_size: DEFAULT_BUFFER_SIZE,
        forward_mode: ForwardMode::Tokio,
    }
}

async fn start_relay(settings: Settings) -> RelayHandle {
    let server = Server::bind(settings).await.unwrap();
    let listeners = server.listener_addrs();
    let metrics = server.metrics_addr();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(server.run(shutdown.clone()));
    RelayHandle {
        listeners,
        metrics,
        shutdown,
        task,
    }
}

async fn stop_relay(relay: RelayHandle) {
    relay.shutdown.cancel();
    timeout(Duration::from_secs(2), relay.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

async fn stop_target(target: TargetHandle) {
    target.shutdown.cancel();
    timeout(Duration::from_secs(2), target.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn refreshes_cached_administrator_target() {
    let target = spawn_target(b"resolved").await;
    let resolver = TargetResolver::new(target.addr.to_string(), true);
    assert_eq!(resolver.refresh().await.unwrap(), 1);

    let mut upstream = resolver.connect().await.unwrap();
    let mut banner = [0_u8; 8];
    upstream.read_exact(&mut banner).await.unwrap();
    assert_eq!(&banner, b"resolved");

    drop(upstream);
    stop_target(target).await;
}

#[tokio::test]
async fn forwards_arbitrary_binary_data_and_half_close() {
    let target = spawn_target(&[]).await;
    let relay =
        start_relay(Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap()).await;
    let relay_addr = relay.listeners[0].1;
    let mut client = TcpStream::connect(relay_addr).await.unwrap();
    let payload: Vec<u8> = (0_u8..=u8::MAX).cycle().take(256 * 1024).collect();

    client.write_all(&payload).await.unwrap();
    client.shutdown().await.unwrap();
    let mut received = vec![0_u8; payload.len()];
    client.read_exact(&mut received).await.unwrap();
    assert_eq!(received, payload);
    let mut end = [0_u8; 1];
    assert_eq!(client.read(&mut end).await.unwrap(), 0);

    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn forwards_with_selected_backends() {
    let mut modes = vec![ForwardMode::Auto];
    #[cfg(target_os = "linux")]
    modes.push(ForwardMode::Splice);

    for mode in modes {
        let target = spawn_target(&[]).await;
        let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
        settings.relays[0].forward_mode = mode;
        let relay = start_relay(settings).await;
        let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();
        let payload: Vec<u8> = (0_u8..=u8::MAX)
            .cycle()
            .take(128 * 1024)
            .chain([0, 255, 0, 255])
            .collect();

        client.write_all(&payload).await.unwrap();
        client.shutdown().await.unwrap();
        let mut received = vec![0_u8; payload.len()];
        client.read_exact(&mut received).await.unwrap();
        assert_eq!(received, payload);

        stop_relay(relay).await;
        stop_target(target).await;
    }
}

#[tokio::test]
async fn forwards_unsolicited_upstream_data_and_reverse_traffic() {
    let target = spawn_target(b"target-banner\0").await;
    let relay =
        start_relay(Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap()).await;
    let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();

    let mut banner = vec![0_u8; b"target-banner\0".len()];
    client.read_exact(&mut banner).await.unwrap();
    assert_eq!(banner, b"target-banner\0");

    let payload = b"client-to-target";
    client.write_all(payload).await.unwrap();
    let mut echoed = vec![0_u8; payload.len()];
    client.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, payload);

    drop(client);
    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn routes_multiple_listeners_to_their_configured_targets() {
    let targets = vec![
        spawn_target(b"A").await,
        spawn_target(b"B").await,
        spawn_target(b"C").await,
    ];
    let settings = Settings {
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
        relays: vec![
            relay_spec("alpha", "127.0.0.1:0", targets[0].addr),
            relay_spec("beta", "127.0.0.1:0", targets[1].addr),
            relay_spec("gamma", "127.0.0.1:0", targets[2].addr),
        ],
    };
    let relay = start_relay(settings).await;

    for (name, expected) in [("alpha", b'A'), ("beta", b'B'), ("gamma", b'C')] {
        let addr = relay
            .listeners
            .iter()
            .find_map(|(relay_name, addr)| (relay_name == name).then_some(*addr))
            .unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut banner = [0_u8; 1];
        client.read_exact(&mut banner).await.unwrap();
        assert_eq!(banner[0], expected);
        client.write_all(name.as_bytes()).await.unwrap();
        let mut echoed = vec![0_u8; name.len()];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(echoed, name.as_bytes());
    }

    stop_relay(relay).await;
    for target in targets {
        stop_target(target).await;
    }
}
#[tokio::test]
async fn handles_upstream_disconnect_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = listener.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"closing").await.unwrap();
    });
    let relay =
        start_relay(Settings::single("127.0.0.1:0", &target_addr.to_string()).unwrap()).await;
    let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    let mut response = [0_u8; 7];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"closing");
    let mut end = [0_u8; 1];
    assert_eq!(client.read(&mut end).await.unwrap(), 0);
    target_task.await.unwrap();
    drop(client);
    stop_relay(relay).await;
}

#[tokio::test]
async fn enforces_connection_limit_without_affecting_listener() {
    let target = spawn_target(&[]).await;
    let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
    settings.global_max_connections = Some(2);
    settings.relays[0].max_connections = Some(1);
    let relay = start_relay(settings).await;

    let first = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let mut second = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), second.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(first);

    let third = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    drop(third);
    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn closes_idle_connections() {
    let target = spawn_target(&[]).await;
    let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
    settings.relays[0].idle_timeout = Some(Duration::from_millis(100));
    let relay = start_relay(settings).await;
    let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(
        timeout(Duration::from_secs(2), client.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );

    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn upstream_failure_closes_only_relevant_clients() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    drop(target_listener);
    let relay =
        start_relay(Settings::single("127.0.0.1:0", &target_addr.to_string()).unwrap()).await;

    for _ in 0..2 {
        let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(Duration::from_secs(1), client.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    stop_relay(relay).await;
}

#[tokio::test]
async fn exposes_prometheus_metrics_without_payload_labels() {
    let target = spawn_target(&[]).await;
    let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
    settings.metrics_listen = Some(Endpoint::parse("127.0.0.1:0", true, true).unwrap());
    let relay = start_relay(settings).await;
    let mut client = TcpStream::connect(relay.listeners[0].1).await.unwrap();
    client.write_all(b"metric-payload").await.unwrap();
    let mut echoed = [0_u8; 14];
    client.read_exact(&mut echoed).await.unwrap();
    drop(client);
    sleep(Duration::from_millis(20)).await;

    let mut metrics = TcpStream::connect(relay.metrics.unwrap()).await.unwrap();
    metrics
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), metrics.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.contains("portrelay_active_connections{relay=\"default\"}"));
    assert!(response.contains("portrelay_bytes_client_to_upstream_total{relay=\"default\"} 14"));
    assert!(!response.contains("metric-payload"));

    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn protects_metrics_with_bearer_auth_and_accepts_fragmented_headers() {
    let target = spawn_target(&[]).await;
    let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
    settings.metrics_listen = Some(Endpoint::parse("127.0.0.1:0", true, true).unwrap());
    settings.metrics_bearer_token = Some("secret-token".to_owned());
    let relay = start_relay(settings).await;

    let mut unauthorized = TcpStream::connect(relay.metrics.unwrap()).await.unwrap();
    unauthorized
        .write_all(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearerxsecret-token\r\n\r\n",
        )
        .await
        .unwrap();
    let mut unauthorized_response = Vec::new();
    timeout(
        Duration::from_secs(1),
        unauthorized.read_to_end(&mut unauthorized_response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(unauthorized_response.starts_with(b"HTTP/1.1 401 Unauthorized"));

    let mut authorized = TcpStream::connect(relay.metrics.unwrap()).await.unwrap();
    authorized
        .write_all(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token",
        )
        .await
        .unwrap();
    authorized.write_all(b"\r\n\r\n").await.unwrap();
    let mut authorized_response = Vec::new();
    timeout(
        Duration::from_secs(1),
        authorized.read_to_end(&mut authorized_response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(authorized_response.starts_with(b"HTTP/1.1 200 OK"));
    assert!(authorized_response
        .windows(b"portrelay_active_connections".len())
        .any(|window| window == b"portrelay_active_connections"));

    stop_relay(relay).await;
    stop_target(target).await;
}

#[tokio::test]
async fn graceful_shutdown_has_bounded_timeout() {
    let target = spawn_target(&[]).await;
    let mut settings = Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap();
    settings.shutdown_timeout = Duration::from_millis(100);
    let relay = start_relay(settings).await;
    let _client = TcpStream::connect(relay.listeners[0].1).await.unwrap();

    relay.shutdown.cancel();
    timeout(Duration::from_secs(2), relay.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    stop_target(target).await;
}

#[tokio::test]
async fn handles_many_concurrent_clients() {
    let target = spawn_target(&[]).await;
    let relay =
        start_relay(Settings::single("127.0.0.1:0", &target.addr.to_string()).unwrap()).await;
    let addr = relay.listeners[0].1;
    let mut clients = Vec::new();
    for index in 0..100_u16 {
        clients.push(tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let payload = format!("client-{index}").into_bytes();
            client.write_all(&payload).await.unwrap();
            client.shutdown().await.unwrap();
            let mut echoed = vec![0_u8; payload.len()];
            client.read_exact(&mut echoed).await.unwrap();
            assert_eq!(echoed, payload);
        }));
    }
    for client in clients {
        client.await.unwrap();
    }

    stop_relay(relay).await;
    stop_target(target).await;
}
