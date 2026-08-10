# Changelog

## 0.1.0 - 2026-08-10

- Initial protocol-agnostic asynchronous TCP relay.
- Single-relay CLI and multi-relay TOML configuration.
- Upstream connect, idle, keepalive, admission-limit, metrics, and shutdown controls.
- Structured JSON tracing and Prometheus-compatible metrics.
- Maturin binary wheels with manylinux release workflow.
- Local forwarding, routing, failure, timeout, shutdown, and concurrency tests.
- Explicit TCP listener backlog via socket2.
- 100,000-connection sharded loopback load harness with simultaneous-connection barrier.
- Standalone echo target and external load-generator tools for relay-capacity validation.
- External-process benchmark procedure and measured native-CLI results.
- Sustained multi-round traffic, idle-hold, connection-churn, failure-isolation, and controlled-backpressure load modes.
- Bounded per-relay buffer pools, configurable forwarding backends, DNS refresh, upstream-connect limits, sampled connection logging, and socket-buffer controls.
- Optional bearer-token and TLS protection for the Prometheus endpoint.