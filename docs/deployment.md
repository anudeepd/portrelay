# Deployment

## RHEL 8 compatibility

Release Linux wheels use manylinux2014/glibc 2.17 for x86_64 and
manylinux_2_28/glibc 2.28 for ARM64. RHEL 8 provides glibc 2.28, so both
wheel baselines are supported there. Build and test wheels in the release
workflow, not on an arbitrary developer workstation.

Local development wheel:

```bash
maturin build --release --bindings bin --out dist
```

Release-compatible Linux wheel:

```bash
docker run --rm -v "$PWD":/io ghcr.io/pyo3/maturin \
  build --release --bindings bin --compatibility manylinux2014 --out dist
```
A workstation build may receive a newer local manylinux tag (for example
`manylinux_2_34`) and is not a release artifact. Use the release workflow or
the documented manylinux container for publishable Linux wheels.

## GitHub and PyPI release

`.github/workflows/release.yml` builds platform-specific native wheels for Linux
x86_64 and ARM64, macOS Intel and ARM64, and Windows x86_64, plus one source
distribution. A `v*` tag or manual workflow dispatch builds all artifacts, then
the publish job uploads them through PyPI Trusted Publishing.

One-time setup:

1. Create GitHub environment `pypi` under repository settings.
2. In PyPI account or project publishing settings, add a GitHub Actions trusted
   publisher.
3. Set repository owner, repository name, workflow filename `release.yml`, and
   environment `pypi`. PyPI expects filename only; GitHub Actions stores it under
   `.github/workflows/`.
4. If `portrelay` does not exist on PyPI yet, create a pending publisher for
   project name `portrelay`; the first successful publish converts it to an
   active publisher.

No PyPI or uv credential belongs in this repository or workflow. The publish job
receives a short-lived OIDC credential through `id-token: write`. For a manual
local upload only, keep a token in the calling process environment or an OS
secret manager:

```bash
export UV_PUBLISH_TOKEN='pypi-...'
uv publish dist/*
```

Never commit `UV_PUBLISH_TOKEN`, put it in `pyproject.toml`, or store it in a
tracked `.env` file.

The application uses stable Rust, Tokio, and normal TCP sockets. `auto` is the conservative default and currently selects Tokio's bounded bidirectional copy path. Linux also supports explicit `--forward-mode splice`; benchmark that mode against the default on the deployment workload before enabling it.

## Unprivileged deployment

`portrelay` does not require administrator permission for installation or operation when listeners use ports `1024` or higher. Install into a user-owned virtual environment:

```bash
python3 -m venv "$HOME/.venvs/portrelay"
"$HOME/.venvs/portrelay/bin/python" -m pip install portrelay
mkdir -p "$HOME/.config/portrelay"
cp portrelay.toml.example "$HOME/.config/portrelay/default.toml"
```

Edit the user-owned configuration, then run directly:

```bash
"$HOME/.venvs/portrelay/bin/portrelay" \
  serve --config "$HOME/.config/portrelay/default.toml"
```

Alternatively, install with `python3 -m pip install --user portrelay` and invoke `$HOME/.local/bin/portrelay`.

Ports below `1024` require an operating-system capability. Do not grant the relay elevated privileges; expose a higher unprivileged listener port through a load balancer or reverse-proxy layer instead.

### Multiple user-managed instances

The binary starts one process. A TOML file can contain multiple fixed relay mappings, while multiple processes provide horizontal scaling. Use the user-level systemd template [systemd/user/portrelay@.service](../systemd/user/portrelay@.service):

```bash
mkdir -p "$HOME/.config/systemd/user"
cp systemd/user/portrelay@.service "$HOME/.config/systemd/user/"

cp portrelay.toml.example "$HOME/.config/portrelay/db.toml"
cp portrelay.toml.example "$HOME/.config/portrelay/ssh.toml"
# Edit each file with distinct listener ports and administrator-configured targets.

systemctl --user daemon-reload
systemctl --user enable --now portrelay@db.service
systemctl --user enable --now portrelay@ssh.service
systemctl --user status portrelay@db.service
```

Each instance uses `%i.toml`, so instances have independent limits, metrics endpoints, and relay mappings. A user service may stop when the user session ends unless the host already permits persistent user managers. If user-level systemd is unavailable, use a rootless container supervisor, a process supervisor, or an orchestrator such as Kubernetes.

Do not bind the same address and port from multiple processes. Use distinct listener ports/IPs on one host, or let separate hosts/pods bind the same service port behind a TCP load balancer.

The process cannot raise host resource limits. Verify the effective file-descriptor limit before sizing instances:

```bash
systemctl --user show portrelay@db.service --property=LimitNOFILE
cat /proc/"$(pgrep -n -f 'portrelay serve')"/limits | grep "open files"
```

## File descriptors and memory

Each proxied connection normally consumes:

```text
one accepted client socket + one upstream socket
```

Set the effective user-level limit above two times intended concurrent connections, plus listener, metrics, logging, and process overhead. Confirm it without administrator access:

```bash
systemctl --user show portrelay@db.service --property=LimitNOFILE
cat /proc/"$(pgrep -n -f 'portrelay serve')"/limits | grep "open files"
```

The default `auto` path uses Tokio's bounded bidirectional copy buffers. Idle-timeout connections use two bounded direction buffers sized by `buffer_size` (16 KiB by default). Explicit Linux `splice` avoids user-space direction buffers but adds readiness and pipe overhead; it is opt-in because benchmark results are workload-dependent. Kernel receive/send buffers and allocator/task overhead add to RSS; measure on the actual kernel and workload before capacity claims.

Every proxied connection owns one tracked Tokio task and normally two socket descriptors. Set `max_connections` (per relay) and `global.max_connections` below descriptor/RSS budget; set `max_connecting` when the upstream has a connection-accept limit. Excess clients are closed immediately and counted as rejections.

## Network and kernel checklist

Review, measure, and tune through deployment automation outside `portrelay`; the process never changes global kernel settings:

- file-descriptor soft/hard limits and service manager limits
- listen backlog and load-balancer pending-connection queues
- per-socket receive/send buffers and aggregate socket memory
- TCP keepalive policy and middlebox idle limits
- TIME_WAIT behavior under short-lived connection churn
- ephemeral port range for outbound upstream sockets
- conntrack table size and state timeout if firewalls/NAT track flows
- CPU scheduling, Tokio runtime placement, RSS, and memory pressure
- NIC bandwidth, packet rate, MTU, and interrupt/RSS configuration
- upstream service connection limits and failure behavior
The listener requests a 65,535 backlog through socket2. Linux `net.core.somaxconn` and load-balancer queue limits can cap the effective value; tune those outside the process.

A relay target hostname is administrator configuration. Client bytes never select a destination. Bind metrics to loopback or protect the metrics listener with network policy.

## Graceful termination

`SIGINT` and `SIGTERM` stop accepts, then wait for active connections up to `shutdown_timeout`. Remaining connection tasks are canceled at the deadline. This supports systemd and container/Kubernetes termination windows; set `shutdown_timeout` below the platform's hard kill window.

## Horizontal scale

Instances are stateless and can run behind a TCP load balancer. The load balancer must preserve ordinary TCP stream behavior and health-check only a configured listener that has a suitable upstream. Upstream capacity, connection balancing, retries, and cross-zone bandwidth remain deployment concerns.

For multiple instances serving the same public listener, use distinct hosts/pods or enable Unix `reuse_port` on every same-host process. `SO_REUSEPORT` distributes new flows; it does not merge limits, metrics, socket memory, or upstream capacity across processes. Give each process its own metrics endpoint or disable metrics on all but one process.

Use an L4 load balancer, not an HTTP-aware proxy that terminates or retries application streams. A retry after relay acceptance can duplicate opaque application bytes; reconnect at the client/application layer instead. Health checks should target a listener with a deliberately available upstream and should be sized as real upstream connections.

Outbound capacity is per source-address/target-address/target-port tuple. One relay host may exhaust ephemeral ports or upstream connection limits before CPU is saturated; distribute instances across source IPs or hosts and increase upstream capacity only through deployment controls. The relay does not rewrite source addresses or retry failed application sessions.
