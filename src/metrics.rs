use std::{
    fmt::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

const DURATION_BUCKETS_MILLIS: [u64; 12] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000, 30_000, 300_000,
];

struct Histogram {
    buckets: [AtomicU64; DURATION_BUCKETS_MILLIS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    fn observe(&self, duration: Duration) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        for (index, bound) in DURATION_BUCKETS_MILLIS.iter().enumerate() {
            if millis <= *bound {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(
            duration.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    fn render(&self, output: &mut String, metric_name: &str, relay_label: &str) {
        let mut cumulative = 0;
        for (index, bound) in DURATION_BUCKETS_MILLIS.iter().enumerate() {
            cumulative += self.buckets[index].load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "{metric_name}_bucket{{relay=\"{relay_label}\",le=\"{}\"}} {cumulative}",
                seconds_string(*bound),
            );
        }
        let _ = writeln!(
            output,
            "{metric_name}_bucket{{relay=\"{relay_label}\",le=\"+Inf\"}} {}",
            self.count.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "{metric_name}_sum{{relay=\"{relay_label}\"}} {:.6}",
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        );
        let _ = writeln!(
            output,
            "{metric_name}_count{{relay=\"{relay_label}\"}} {}",
            self.count.load(Ordering::Relaxed),
        );
    }
}

fn seconds_string(milliseconds: u64) -> String {
    if milliseconds % 1_000 == 0 {
        (milliseconds / 1_000).to_string()
    } else {
        format!("{:.3}", milliseconds as f64 / 1_000.0)
    }
}

pub struct RelayMetrics {
    name: String,
    label: String,
    active_connections: AtomicU64,
    connections_accepted: AtomicU64,
    connections_rejected: AtomicU64,
    connections_closed: AtomicU64,
    upstream_connect_failures: AtomicU64,
    upstream_connect_timeouts: AtomicU64,
    idle_timeouts: AtomicU64,
    bytes_client_to_upstream: AtomicU64,
    bytes_upstream_to_client: AtomicU64,
    connection_duration: Histogram,
    upstream_connect_duration: Histogram,
}

impl RelayMetrics {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let label = escape_label(&name);
        Self {
            name,
            label,
            active_connections: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            upstream_connect_failures: AtomicU64::new(0),
            upstream_connect_timeouts: AtomicU64::new(0),
            idle_timeouts: AtomicU64::new(0),
            bytes_client_to_upstream: AtomicU64::new(0),
            bytes_upstream_to_client: AtomicU64::new(0),
            connection_duration: Histogram::new(),
            upstream_connect_duration: Histogram::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn closed(&self, duration: Duration, client_to_upstream: u64, upstream_to_client: u64) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
        self.bytes_client_to_upstream
            .fetch_add(client_to_upstream, Ordering::Relaxed);
        self.bytes_upstream_to_client
            .fetch_add(upstream_to_client, Ordering::Relaxed);
        self.connection_duration.observe(duration);
    }

    pub fn upstream_failure(&self) {
        self.upstream_connect_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn upstream_timeout(&self) {
        self.upstream_connect_timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn idle_timeout(&self) {
        self.idle_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_upstream_connect(&self, duration: Duration) {
        self.upstream_connect_duration.observe(duration);
    }

    pub fn render(&self, output: &mut String) {
        let label = &self.label;
        let _ = writeln!(
            output,
            "portrelay_active_connections{{relay=\"{label}\"}} {}",
            self.active_connections.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_connections_accepted_total{{relay=\"{label}\"}} {}",
            self.connections_accepted.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_connections_rejected_total{{relay=\"{label}\"}} {}",
            self.connections_rejected.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_connections_closed_total{{relay=\"{label}\"}} {}",
            self.connections_closed.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_upstream_connect_failures_total{{relay=\"{label}\"}} {}",
            self.upstream_connect_failures.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_upstream_connect_timeouts_total{{relay=\"{label}\"}} {}",
            self.upstream_connect_timeouts.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_idle_timeouts_total{{relay=\"{label}\"}} {}",
            self.idle_timeouts.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_bytes_client_to_upstream_total{{relay=\"{label}\"}} {}",
            self.bytes_client_to_upstream.load(Ordering::Relaxed),
        );
        let _ = writeln!(
            output,
            "portrelay_bytes_upstream_to_client_total{{relay=\"{label}\"}} {}",
            self.bytes_upstream_to_client.load(Ordering::Relaxed),
        );
        self.connection_duration
            .render(output, "portrelay_connection_duration_seconds", label);
        self.upstream_connect_duration.render(
            output,
            "portrelay_upstream_connect_duration_seconds",
            label,
        );
    }
}

fn render_help_type(output: &mut String, name: &str, help: &str, metric_type: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}
fn render_metric_metadata(output: &mut String) {
    render_help_type(
        output,
        "portrelay_active_connections",
        "Active proxied TCP connections.",
        "gauge",
    );
    render_help_type(
        output,
        "portrelay_connections_accepted_total",
        "Accepted proxied TCP connections.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_connections_rejected_total",
        "Connections rejected by admission limits.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_connections_closed_total",
        "Closed proxied TCP connections.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_upstream_connect_failures_total",
        "Upstream connection failures.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_upstream_connect_timeouts_total",
        "Upstream connection timeouts.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_idle_timeouts_total",
        "Connections closed by idle timeout.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_bytes_client_to_upstream_total",
        "Bytes copied from clients to upstreams.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_bytes_upstream_to_client_total",
        "Bytes copied from upstreams to clients.",
        "counter",
    );
    render_help_type(
        output,
        "portrelay_connection_duration_seconds",
        "Proxied TCP connection duration.",
        "histogram",
    );
    render_help_type(
        output,
        "portrelay_upstream_connect_duration_seconds",
        "Upstream TCP connection duration.",
        "histogram",
    );
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[derive(Clone)]
pub struct MetricsRegistry {
    relays: Arc<Vec<Arc<RelayMetrics>>>,
}

impl MetricsRegistry {
    pub fn new(relays: Vec<Arc<RelayMetrics>>) -> Self {
        Self {
            relays: Arc::new(relays),
        }
    }

    pub fn render(&self) -> String {
        let mut output = String::with_capacity(self.relays.len() * 2_048);
        render_metric_metadata(&mut output);
        for relay in self.relays.iter() {
            relay.render(&mut output);
        }

        output
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_metadata_once_for_multiple_relays() {
        let registry = MetricsRegistry::new(vec![
            Arc::new(RelayMetrics::new("first")),
            Arc::new(RelayMetrics::new("second")),
        ]);
        let output = registry.render();
        assert_eq!(
            output
                .matches("# TYPE portrelay_active_connections gauge")
                .count(),
            1
        );
        assert!(output.contains("relay=\"first\""));
        assert!(output.contains("relay=\"second\""));
    }
}
