# portrelay

`portrelay` is a protocol-agnostic asynchronous TCP relay. Each configured listener maps to exactly one administrator-selected upstream target:

```text
client TCP stream -> portrelay listener -> configured target TCP stream
```

It forwards bytes without parsing, buffering whole messages, terminating TLS, or understanding SSH, databases, HTTP, SMTP, Redis, or custom protocols.

Version: `0.1.1`.

## Install

Install the published release from PyPI:

```bash
pip install portrelay
portrelay --version
portrelay --help
```

The wheel contains the native Rust executable. No networking code runs in Python and the package has no runtime Python dependency.

For an unprivileged installation, use a virtual environment or the user site:

```bash
python3 -m venv "$HOME/.venvs/portrelay"
"$HOME/.venvs/portrelay/bin/python" -m pip install portrelay
"$HOME/.venvs/portrelay/bin/portrelay" --version
```

Alternatively:

```bash
python3 -m pip install --user portrelay
```

No administrator permission is required for package installation or relay operation when listeners use ports `1024` or higher. Ports below `1024` require an operating-system capability; use a load balancer or higher external port instead of granting the relay elevated privileges.

Build locally from source:

```bash
python3 -m pip install maturin
maturin build --release --bindings bin --out dist
python3 -m venv .venv
.venv/bin/pip install dist/portrelay-*.whl
.venv/bin/portrelay --version
```

Release Linux wheels use manylinux2014/glibc 2.17 on x86_64 and
manylinux_2_28/glibc 2.28 on ARM64. See [deployment](docs/deployment.md) for
RHEL 8 requirements and release-wheel commands.

## Single relay

```bash
portrelay serve \
  --listen 0.0.0.0:13306 \
  --target mariadb.internal.company:3306
```

Listener shorthand `:10022` binds IPv4 `0.0.0.0:10022`. Targets accept IPv4, bracketed IPv6, DNS names, and arbitrary TCP ports:

```bash
portrelay serve --listen :10022 --target 10.20.30.40:22
portrelay serve --listen [::]:15432 --target '[2001:db8::20]:5432'
```

Target routing is fixed when the process starts. Remote clients send only application bytes; they cannot select or override target addresses.

## CLI

```text
portrelay serve --listen <HOST:PORT> --target <HOST:PORT>
portrelay serve --config <PATH>
```

Supported `serve` options:

| Option | Meaning |
| --- | --- |
| `--listen HOST:PORT` | Listener for single-relay mode. `:PORT` means `0.0.0.0`. |
| `--target HOST:PORT` | Fixed upstream for single-relay mode. |
| `--config PATH` | TOML file with one or more `[[relay]]` mappings. |
| `--connect-timeout DURATION` | Upstream connect deadline; default `10s`. |
| `--idle-timeout DURATION` | Close after no transferred data; disabled by default. |
| `--shutdown-timeout DURATION` | Graceful shutdown deadline; default `30s`. |
| `--max-connections NUMBER` | Single-relay admission limit. |
| `--max-connecting NUMBER` | Single-relay upstream-connect admission limit. |
| `--tcp-keepalive DURATION` | TCP keepalive probe start delay. |
| `--socket-buffer-size BYTES` | Requested TCP receive/send buffer size. |
| `--buffer-size BYTES` | Per-direction Tokio copy buffer size. |
| `--buffer-pool-size NUMBER` | Maximum pooled copy buffers per relay. |
| `--connection-log-sample-rate NUMBER` | Emit connection debug logs for every Nth connection. |
| `--forward-mode auto|tokio|splice` | Forwarding backend; `splice` is Linux-only and opt-in. |
| `--reuse-port` | Enable Unix `SO_REUSEPORT` for multi-process listeners. |
| `--metrics-listen HOST:PORT` | Optional Prometheus HTTP or HTTPS endpoint. |
| `--metrics-token TOKEN` | Require bearer token for metrics requests. |
| `--metrics-tls-cert PATH` / `--metrics-tls-key PATH` | Serve metrics over TLS with PEM files. |
| `--log-level LEVEL` | Tracing filter, for example `info`, `debug`, or `warn`. |

`--listen` and `--target` must be supplied together without `--config`. They cannot be combined with `--config`. In config mode, relay-specific and security options must be set in TOML; only `--log-level`, `--metrics-listen`, and `--shutdown-timeout` are explicit global command-line overrides.

## Multi-relay configuration

```toml
[global]
log_level = "info"
metrics_listen = "127.0.0.1:9090"
connect_timeout = "10s"
shutdown_timeout = "30s"
max_connections = 100000

[[relay]]
name = "mariadb"
listen = "0.0.0.0:13306"
target = "mariadb.internal:3306"
max_connections = 50000
idle_timeout = "30m"

[[relay]]
name = "ssh"
listen = "0.0.0.0:10022"
target = "linux01.internal:22"

[[relay]]
name = "mongodb"
listen = "0.0.0.0:27018"
target = "mongo01.internal:27017"
max_connections = 50000
```

Run it:
```bash
portrelay serve --config portrelay.toml
```

Each mapping gets its own listener, metrics label, and optional per-relay semaphore. A bind failure for one mapping is logged and does not stop unrelated mappings. The process exits only if no configured listener can start.

## Protocol transparency

The forwarding layer uses the configured streaming backend and never inspects payload bytes. End-to-end application encryption, authentication, TLS, and framing remain between the original client and target.

Examples below are examples only; `portrelay` contains no application-specific logic.

### SSH

```bash
portrelay serve --listen 0.0.0.0:10022 --target linux01.internal:22
ssh -p 10022 user@reachable-server
```

PuTTY: host `reachable-server`, port `10022`.

### MariaDB/MySQL

```bash
portrelay serve --listen 0.0.0.0:13306 --target mariadb.internal:3306
```

Connect the client to `reachable-server:13306`.

### MongoDB

```bash
portrelay serve --listen 0.0.0.0:27018 --target mongo.internal:27017
```

### PostgreSQL

```bash
portrelay serve --listen 0.0.0.0:15432 --target postgres.internal:5432
```

## Metrics

Set `--metrics-listen 127.0.0.1:9090` or `global.metrics_listen` in TOML. Query:

```bash
curl http://127.0.0.1:9090/metrics
```

For a network-reachable endpoint, configure `--metrics-token TOKEN`; add `--metrics-tls-cert cert.pem --metrics-tls-key key.pem` for HTTPS:

```bash
curl --fail --cacert ca.pem \
  -H 'Authorization: Bearer TOKEN' \
  https://metrics.example:9090/metrics
```

Metrics use only the low-cardinality administrator-defined `relay` label. They do not contain connection IDs, client addresses, target payloads, passwords, SQL, or protocol data.

Exported families include:

- `portrelay_active_connections`
- `portrelay_connections_accepted_total`
- `portrelay_connections_rejected_total`
- `portrelay_connections_closed_total`
- `portrelay_upstream_connect_failures_total`
- `portrelay_upstream_connect_timeouts_total`
- `portrelay_bytes_client_to_upstream_total`
- `portrelay_bytes_upstream_to_client_total`
- `portrelay_connection_duration_seconds`
- `portrelay_upstream_connect_duration_seconds`

Bind metrics to loopback or protect it at the network boundary. The endpoint is intentionally small and dependency-free; it implements only the `/metrics` GET needed by Prometheus.

## Architecture and concurrency

- Tokio event-driven networking; no thread per connection.
- One task per accepted proxy connection, with one upstream TCP socket.
- `auto` (default) uses Tokio's bounded bidirectional copy path and preserves TCP backpressure and half-close behavior.
- Explicit Linux `splice` uses kernel pipe forwarding and avoids user-space direction buffers; benchmark before enabling because readiness/pipe overhead can dominate.
- Configured idle timeouts use two bounded direction buffers sized by `buffer_size` and an activity watchdog.
- Global/per-relay connection and upstream-connect semaphores reject excess clients before upstream allocation.
- No process-wide mutex is used in the forwarding path; each relay's bounded buffer pool has a short critical section on buffer reuse.
- Metrics use relaxed atomics and fixed histograms; rendering happens only on metrics requests.
- Graceful shutdown stops listeners, waits for active tasks up to the configured deadline, then cancels remaining connections.

For horizontally scaled deployments, run stateless instances behind a TCP load balancer. Each proxied connection normally consumes two file descriptors and two kernel socket buffers.

## Security model

`portrelay` is a fixed TCP forwarder, not a general proxy. It has no SOCKS negotiation, HTTP CONNECT handling, client-supplied destination parsing, DNS resolution based on client bytes, or payload logging. Administrators control each target through CLI or TOML before the listener accepts traffic.

Target hostnames are resolved at startup for validation where possible and at connection time. Configure `dns_refresh` to periodically replace cached administrator-resolved addresses; client bytes never influence resolution. Resolution or upstream failures close only the affected client connection and are recorded; they do not terminate the process.

## Build, test, and lint

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
cargo audit
```

The integration suite uses local generic TCP echo/hold servers. It covers binary data, reverse traffic, half-close, multiple mappings, limits, connect failures, idle timeouts, metrics, bounded shutdown, and concurrent clients.

## Benchmarking

`examples/load.rs` runs an in-process generic TCP target and relay, then measures concurrent connect/round-trip workload without claiming production capacity. Build and run one point:

```bash
cargo build --release --example load
/usr/bin/time -v target/release/examples/load 1000 auto
/usr/bin/time -v target/release/examples/load 1000 tokio
```

Run 100, 1,000, 10,000, 50,000, and 100,000 only when the host has enough file descriptors, ports, memory, and upstream capacity. Record host/kernel/Rust/version/ulimit/sysctls with each run. Results observed in this workspace are recorded in [docs/benchmarks.md](docs/benchmarks.md); no unmeasured throughput claim is made here.

## RHEL 8 deployment

See [docs/deployment.md](docs/deployment.md) and the unprivileged [user-level systemd template](systemd/user/portrelay@.service). Do not let the application change kernel-wide settings automatically. Capacity planning must include file descriptors, two sockets per connection, listen backlog, TCP buffers, keepalive, TIME_WAIT, ephemeral ports, conntrack, memory, CPU, and NIC bandwidth.

## Known limitations
- `auto` uses bounded Tokio stream copy; explicit Linux `splice` is available but not assumed faster.
- DNS target changes are refreshed only when `dns_refresh` is configured; otherwise each connection uses normal OS/Tokio resolution behavior.
- Metrics are plain HTTP unless TLS and/or bearer authentication are configured; bind unauthenticated metrics to loopback or protect them at the network boundary.
- Per-connection debug logs are sampled by `connection_log_sample_rate`; `info` remains the default.
- A single instance is not promised to support one million connections; horizontal scaling and OS tuning remain deployment responsibilities.
