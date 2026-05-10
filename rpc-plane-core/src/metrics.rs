use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics(Arc<Inner>);

struct Inner {
    registry: Registry,
    requests: CounterVec,
    duration: HistogramVec,
    failovers: CounterVec,
    probe_requests: CounterVec,
    health_score: GaugeVec,
    slot_height: GaugeVec,
    slot_drift: GaugeVec,
    circuit_state: GaugeVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests = CounterVec::new(
            Opts::new("rpc_plane_requests_total", "Total RPC requests routed"),
            &["method", "provider", "status"],
        )
        .unwrap();

        // Latency buckets: 1ms → 2.5s. Covers everything from local loopback
        // to a degraded provider without blowing up the cardinality.
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "rpc_plane_request_duration_seconds",
                "Time spent forwarding requests to providers",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
            &["method", "provider"],
        )
        .unwrap();

        let failovers = CounterVec::new(
            Opts::new(
                "rpc_plane_failover_total",
                "Requests retried on a different provider",
            ),
            &["from_provider", "to_provider"],
        )
        .unwrap();

        let probe_requests = CounterVec::new(
            Opts::new(
                "rpc_plane_probe_requests_total",
                "Health and slot probe requests sent to providers",
            ),
            &["provider", "type", "status"],
        )
        .unwrap();

        let health_score = GaugeVec::new(
            Opts::new(
                "rpc_plane_provider_health_score",
                "Normalised health score (0.0–1.0)",
            ),
            &["provider"],
        )
        .unwrap();

        let slot_height = GaugeVec::new(
            Opts::new(
                "rpc_plane_provider_slot_height",
                "Current slot height reported by provider",
            ),
            &["provider"],
        )
        .unwrap();

        let slot_drift = GaugeVec::new(
            Opts::new("rpc_plane_slot_drift", "Slots behind the network tip"),
            &["provider"],
        )
        .unwrap();

        let circuit_state = GaugeVec::new(
            Opts::new(
                "rpc_plane_circuit_breaker_state",
                "Circuit breaker state (0=closed, 1=open)",
            ),
            &["provider"],
        )
        .unwrap();

        for collector in [
            Box::new(requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(duration.clone()),
            Box::new(failovers.clone()),
            Box::new(probe_requests.clone()),
            Box::new(health_score.clone()),
            Box::new(slot_height.clone()),
            Box::new(slot_drift.clone()),
            Box::new(circuit_state.clone()),
        ] {
            registry.register(collector).unwrap();
        }

        Self(Arc::new(Inner {
            registry,
            requests,
            duration,
            failovers,
            probe_requests,
            health_score,
            slot_height,
            slot_drift,
            circuit_state,
        }))
    }

    pub fn record_request(&self, method: &str, provider: &str, status: &str, latency_ms: f64) {
        self.0
            .requests
            .with_label_values(&[method, provider, status])
            .inc();
        if status == "ok" {
            self.0
                .duration
                .with_label_values(&[method, provider])
                .observe(latency_ms / 1000.0);
        }
    }

    pub fn record_failover(&self, from: &str, to: &str) {
        self.0.failovers.with_label_values(&[from, to]).inc();
    }

    pub fn record_probe(&self, provider: &str, probe_type: &str, status: &str) {
        self.0
            .probe_requests
            .with_label_values(&[provider, probe_type, status])
            .inc();
    }

    /// Called at scrape time to push live health data into the gauge family.
    pub fn update_provider_health(
        &self,
        provider: &str,
        score: f64,
        slot: Option<u64>,
        drift: u64,
        circuit_open: bool,
    ) {
        self.0.health_score.with_label_values(&[provider]).set(score);
        if let Some(s) = slot {
            self.0.slot_height.with_label_values(&[provider]).set(s as f64);
            self.0.slot_drift.with_label_values(&[provider]).set(drift as f64);
        }
        self.0
            .circuit_state
            .with_label_values(&[provider])
            .set(if circuit_open { 1.0 } else { 0.0 });
    }

    /// Render all registered metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.0.registry.gather(), &mut buf)
            .unwrap_or_default();
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_increments_correctly() {
        let m = Metrics::new();
        m.record_request("getSlot", "helius", "ok", 42.0);
        m.record_request("getSlot", "helius", "ok", 58.0);
        m.record_request("getSlot", "triton", "error", 0.0);

        let text = m.render();
        assert!(text.contains(
            "rpc_plane_requests_total{method=\"getSlot\",provider=\"helius\",status=\"ok\"} 2"
        ));
        assert!(text.contains(
            "rpc_plane_requests_total{method=\"getSlot\",provider=\"triton\",status=\"error\"} 1"
        ));
    }

    #[test]
    fn histogram_emits_buckets_and_sum() {
        let m = Metrics::new();
        m.record_request("getSlot", "helius", "ok", 42.0);

        let text = m.render();
        // Prometheus histogram emits _bucket, _sum, _count lines.
        assert!(text.contains("rpc_plane_request_duration_seconds_bucket"));
        assert!(text.contains("rpc_plane_request_duration_seconds_sum"));
        assert!(text.contains("rpc_plane_request_duration_seconds_count"));
    }

    #[test]
    fn duration_not_observed_for_errors() {
        let m = Metrics::new();
        m.record_request("getSlot", "helius", "error", 100.0);

        let text = m.render();
        // No duration lines should appear when no successful requests recorded.
        assert!(!text.contains("rpc_plane_request_duration_seconds_sum"));
    }

    #[test]
    fn failover_counter() {
        let m = Metrics::new();
        m.record_failover("helius", "triton");
        m.record_failover("helius", "triton");

        let text = m.render();
        assert!(text.contains(
            "rpc_plane_failover_total{from_provider=\"helius\",to_provider=\"triton\"} 2"
        ));
    }

    #[test]
    fn health_gauges_update() {
        let m = Metrics::new();
        m.update_provider_health("helius", 0.95, Some(300_000_000), 2, false);

        let text = m.render();
        assert!(text.contains("rpc_plane_provider_health_score{provider=\"helius\"} 0.95"));
        assert!(text.contains("rpc_plane_circuit_breaker_state{provider=\"helius\"} 0"));
    }
}
