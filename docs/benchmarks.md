# Benchmarks

No capacity claim is valid without a run record. This document records measurements made in this workspace and the method used to reproduce them.

## Method

`examples/load.rs` starts 32 local generic TCP echo listeners and one `portrelay::server::Server` containing 32 relay mappings. Each mapping uses a distinct `127.0.0.x` listener and target address. Clients connect to all relay shards, hold every successful connection open at a barrier, then send a fixed binary payload and read the echo. This exercises real concurrent forwarding without involving an application protocol.

The benchmark uses 1,000-connection launch batches with a 5 ms inter-batch delay. The barrier makes the reported `max_client_tasks_active` and `connected_clients_before_payload` values represent simultaneous open client connections, not merely total connections created over time. The relay and benchmark target use an explicit 65,535 socket listen backlog; the host `somaxconn` value still caps its effective kernel backlog.

The 32 loopback shards avoid measuring a single local four-tuple's ephemeral-port ceiling. This is one relay process with 32 administrator-configured mappings, not a claim that one listener and one upstream address support 100,000 connections on one host.

Build once before timing so compilation is excluded:

```bash
cargo build --release --example load
unshare -Urn /bin/bash -c 'ip link set lo up && /usr/bin/time -v target/release/examples/load 10000 auto'
unshare -Urn /bin/bash -c 'ip link set lo up && /usr/bin/time -v target/release/examples/load 50000 auto'
unshare -Urn /bin/bash -c 'ip link set lo up && /usr/bin/time -v target/release/examples/load 100000 auto'
```

Pass `tokio` or (on Linux) `splice` as the second argument to compare forwarding backends.

Fresh user/network namespaces prevent previous benchmark `TIME_WAIT` and ephemeral-port state from contaminating later runs. The namespace requires unprivileged user namespaces and the `ip` utility.

Record for every run:

```text
portrelay version
host CPU and RAM
OS and kernel
Rust version
ulimit -n
relevant sysctl values
relay shard count
requested and connected connection counts
maximum simultaneous client tasks
payload size and total bytes
elapsed time
connections per second
throughput
/usr/bin/time user/system CPU and maximum RSS
errors or admission rejections
```

The local test target is useful for regression detection, not a production capacity claim. Higher points need sufficient file descriptors, ephemeral ports, kernel socket memory, upstream capacity, and test-host RAM. Use a separate target host and a real TCP load generator for deployment sizing.

## Measured workspace run

The release example runs below were executed on 2026-08-10 in fresh user/network namespaces in this workspace after the sharded harness and explicit listener backlog implementation. Values are measured, not extrapolated.

Host record:

```text
CPU: AMD Ryzen 7 7735HS with Radeon Graphics, 16 logical CPUs
RAM: 14 GiB visible to host
OS/kernel: Linux smadayma 7.1.7-200.fc44.x86_64 #1 SMP PREEMPT_DYNAMIC Thu Aug 6 21:13:02 UTC 2026
Rust: rustc 1.97.1 (8bab26f4f 2026-07-14)
ulimit -n: 524288
net.core.somaxconn: 4096
net.ipv4.ip_local_port_range: 32768-60999
net.ipv4.tcp_tw_reuse: 2
net.netfilter.nf_conntrack_max: 262144
```

The example used a 4,096-byte payload, 32 loopback relay/target shards, a simultaneous-connection barrier, full-duplex request/response, and no admission limit. `/usr/bin/time -v` measured the standalone release example after `cargo build --release --example load`.

| Connections | Connected before payload | Max active | Payload | Elapsed | Connections/s | Throughput | CPU time | Max RSS | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1,000 | 1,000 | 1,000 | 4 KiB | 0.074974 s | 13,337.96 | 104.20 MiB/s | 0.06u + 0.41s | 33,588 KiB | pass |
| 10,000 | 10,000 | 10,000 | 4 KiB | 0.621331 s | 16,094.49 | 125.74 MiB/s | 0.65u + 4.08s | 258,596 KiB | pass |
| 50,000 | 50,000 | 50,000 | 4 KiB | 2.414520 s | 20,708.05 | 161.78 MiB/s | 3.18u + 20.41s | 1,449,704 KiB | pass |
| 100,000 | 100,000 | 100,000 | 4 KiB | 5.845026 s | 17,108.56 | 133.66 MiB/s | 6.69u + 43.40s | 3,051,984 KiB | pass |

All requested connections completed with byte-identical echoes. No `EADDRNOTAVAIL`, timeout, reset, or admission failures occurred in these isolated runs.

## Forwarding backend comparison

The same release build was measured on 2026-08-10 in fresh user/network namespaces with 10,000 connections, 4 KiB payloads, 32 loopback shards, and no admission limit:

| Backend | Elapsed | Connections/s | Throughput | User + system CPU | Max RSS | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `auto` (Tokio copy) | 0.516918 s | 19,345.44 | 151.14 MiB/s | 0.64 + 3.98 s | 287,160 KiB | pass |
| `splice` | 0.932086 s | 10,728.63 | 83.82 MiB/s | 0.91 + 6.41 s | 170,812 KiB | pass |

On this workload, explicit `splice` reduced RSS but did not improve throughput or CPU time. `auto` therefore remains mapped to the Tokio copy path; benchmark on production-like payloads and network topology before selecting `splice`.

Current release confirmation: `target/release/examples/load 100000 auto` completed on 2026-08-10 in a fresh user/network namespace with 100,000 connected clients, 100,000 successful round trips, zero failures, 5.288817 s application elapsed time, 18,907.82 connections/s, 147.72 MiB/s, 7.14 s user CPU, 43.51 s system CPU, and 2,846,652 KiB maximum RSS. This confirms the current default backend at the documented sharded topology; it is not a one-listener/one-target or production-host capacity claim.

## External-process validation

The in-process benchmark validates forwarding, but relay capacity needs three independent processes:

```text
load_client host -> native portrelay process -> echo/application target host
```

Build the native relay and the external test tools:

```bash
cargo build --release --bin portrelay --example echo_target --example load_client
```

On the target host, expose 32 independent test ports:

```bash
TARGET_IP=10.20.0.12
LISTEN_ARGS=()
for port in $(seq 23000 23031); do
  LISTEN_ARGS+=(--listen "$TARGET_IP:$port")
done
exec target/release/examples/echo_target "${LISTEN_ARGS[@]}"
```

On the relay host, generate one administrator-configured mapping per target port. Replace `TARGET_IP` with the target host address:

```bash
python3 - <<'PY'
from pathlib import Path

target_host = "10.20.0.12"
lines = [
    "[global]",
    'log_level = "info"',
    'metrics_listen = "127.0.0.1:9090"',
    'connect_timeout = "60s"',
    'shutdown_timeout = "90s"',
    "max_connections = 100000",
    "",
]
for index, port in enumerate(range(23000, 23032)):
    lines.extend([
        "[[relay]]",
        f'name = "load-{index}"',
        f'listen = "0.0.0.0:{24000 + index}"',
        f'target = "{target_host}:{port}"',
        "",
    ])
Path("/tmp/portrelay-load.toml").write_text("\n".join(lines))
PY

target/release/portrelay serve --config /tmp/portrelay-load.toml
```

On the load-generator host, construct the matching relay listener list and run progressively:

```bash
RELAY_IP=10.20.0.11
RELAYS=$(python3 - <<'PY'
relay_host = "10.20.0.11"
print(",".join(f"{relay_host}:{port}" for port in range(24000, 24032)))
PY
)

target/release/examples/load_client \
  --relay "$RELAYS" \
  --connections 10000 \
  --payload-bytes 4096

target/release/examples/load_client \
  --relay "$RELAYS" \
  --connections 50000 \
  --payload-bytes 4096

target/release/examples/load_client \
  --relay "$RELAYS" \
  --connections 100000 \
  --payload-bytes 4096 \
  --idle-seconds 60

target/release/examples/load_client \
  --relay "$RELAYS" \
  --connections 100000 \
  --payload-bytes 4096 \
  --rounds 5 \
  --round-interval-ms 1000 \
  --hold-after-seconds 60
```

`load_client` holds all successful client connections at a barrier before sending the payload. `--idle-seconds` keeps them open before traffic. `--rounds` and `--round-interval-ms` generate sustained request/response traffic. `--hold-after-seconds` keeps connections open after the final response. These modes exercise long-lived connection memory and descriptor usage. The echo target forwards arbitrary bytes and does not model application-protocol work.

For controlled backpressure, add `--response-delay-ms 50` to the target command and use a larger client payload such as `--payload-bytes 65536`.

Monitor the relay during the hold and transfer phases:

```bash
PID="$(pidof portrelay)"
ps -p "$PID" -o pid,pcpu,pmem,rss,vsz,nlwp,etime,cmd
printf 'relay_fds='
printf '%s\n' /proc/"$PID"/fd/* | wc -l
cat /proc/"$PID"/limits | grep "open files"
curl --fail --silent http://127.0.0.1:9090/metrics
ss -s
```

Accept a run only when all requested clients connect, `failed_connections=0`, upstream connect failure/timeout counters remain zero, active connection metrics reach the expected count, file descriptors remain below the service limit, and RSS/CPU/NIC usage stay within the deployment budget. Repeat each level at least three times from clean hosts or fresh network namespaces; do not chain high-churn runs while `TIME_WAIT` state remains.

Measured same-host external-process validation on 2026-08-10:

| Connections | Connected before payload | Successful | Failed | Elapsed | Throughput | Load-client max RSS | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 10,000 | 10,000 | 0 | 0.485784 s | 160.82 MiB/s | 51,880 KiB | pass |
| 100,000 | 100,000 | 100,000 | 0 | 7.265754 s | 107.52 MiB/s | 493,464 KiB | pass |
Fresh user/network-namespace runs used separate target, native relay, and load-generator processes, with no global sysctl changes:

| Test | Connections | Idle before | Rounds | Hold after | Successful | Failed | Elapsed | Throughput | Load-client max RSS | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Capacity | 10,000 | 0 s | 1 | 0 s | 10,000 | 0 | 0.672517 s | 116.17 MiB/s | 51,384 KiB | pass |
| Capacity | 50,000 | 0 s | 1 | 0 s | 50,000 | 0 | 3.101187 s | 125.96 MiB/s | 256,280 KiB | pass |
| Capacity | 100,000 | 0 s | 1 | 0 s | 100,000 | 0 | 6.006352 s | 130.07 MiB/s | 497,952 KiB | pass |
| Idle hold | 100,000 | 60 s | 1 | 0 s | 100,000 | 0 | 65.383981 s | 11.95 MiB/s | 329,768 KiB | pass |
| Sustained traffic | 10,000 | 0 s | 30 | 30 s | 10,000 | 0 | 59.653570 s | 39.29 MiB/s | 75,680 KiB | pass |
| Sustained traffic | 50,000 | 0 s | 10 | 30 s | 50,000 | 0 | 41.474318 s | 94.18 MiB/s | 305,272 KiB | pass |
| Sustained traffic | 100,000 | 0 s | 5 | 60 s | 100,000 | 0 | 69.776149 s | 55.98 MiB/s | 561,728 KiB | pass |

During the clean 100,000-client idle hold, the relay process reached 200,042 file descriptors, 2,192,324 KiB RSS, and 17 threads. The target process reached 100,041 file descriptors. These are same-host process measurements; a production three-host run is still required for NIC, routing, load-balancer, and target-service behavior.

Target-failure isolation also passed in a fresh namespace: the target was terminated during 1,000 active connections, the relay remained alive, the target restarted, and a new 1,000-connection, three-round load completed with zero failures.

Connection churn also passed: ten fresh 5,000-connection cycles completed with zero failures, for 50,000 total connection creations and closures.

Controlled backpressure passed with a 50 ms response delay in each echo target, 10,000 connections, and 64 KiB payloads:

```text
successful_connections=10000
failed_connections=0
total_forwarded_bytes=1310720000
elapsed_seconds=1.876614
aggregate_throughput_mib_per_second=666.09
load_client_max_rss=636936 KiB
```

An earlier non-clean 30-second idle profile reached 100,000 client connections but produced four early EOFs; it is not recorded as a pass. A later attempt was contaminated by 65,536 accumulated `TIME_WAIT` sockets and produced connection timeouts. Fresh namespaces avoided this contamination.

For a production service with one listener and one upstream address, test that exact topology separately. The 32 mappings above intentionally distribute TCP tuples so the load generator can reach 100,000 connections; they do not prove one listener-to-one-target capacity.

## Interpretation

These measurements demonstrate 100,000 simultaneous loopback client connections through one process using 32 independent administrator-configured mappings. They do not prove one listener and one upstream address can sustain 100,000 connections; that topology remains constrained by per-address TCP tuples and upstream capacity. RSS includes benchmark clients, echo targets, relay tasks, socket buffers, and allocator overhead. Production relay-only RSS was not isolated by this run.

Explicit listener backlog removes previous default-backlog bottleneck, while fresh network namespaces remove stale `TIME_WAIT` state. No global sysctl values were changed. The recorded 100,000-connection capacity points use Tokio copy mode; the backend comparison above shows why Linux `splice` remains opt-in. Preserve byte-for-byte and half-close tests when comparing future optimizations.
