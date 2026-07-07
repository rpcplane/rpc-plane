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
    rate_limited: CounterVec,
    probe_requests: CounterVec,
    health_score: GaugeVec,
    slot_height: GaugeVec,
    slot_drift: GaugeVec,
    slot_height_commitment: GaugeVec,
    slot_drift_commitment: GaugeVec,
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

        let rate_limited = CounterVec::new(
            Opts::new(
                "rpc_plane_rate_limited_total",
                "Requests shed from a provider because its max_rps token bucket was empty",
            ),
            &["provider"],
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
            Opts::new(
                "rpc_plane_slot_drift",
                "Slots behind the network tip (worst of processed/confirmed)",
            ),
            &["provider"],
        )
        .unwrap();

        let slot_height_commitment = GaugeVec::new(
            Opts::new(
                "rpc_plane_provider_slot_height_commitment",
                "Slot height reported by provider, per commitment level",
            ),
            &["provider", "commitment"],
        )
        .unwrap();

        let slot_drift_commitment = GaugeVec::new(
            Opts::new(
                "rpc_plane_slot_drift_commitment",
                "Slots behind the per-commitment network tip",
            ),
            &["provider", "commitment"],
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

        let build_info = GaugeVec::new(
            Opts::new("rpc_plane_build_info", "Build information"),
            &["version", "commit", "branch", "rustc"],
        )
        .unwrap();
        build_info
            .with_label_values(&[
                env!("CARGO_PKG_VERSION"),
                option_env!("GIT_COMMIT_HASH").unwrap_or("unknown"),
                option_env!("GIT_BRANCH").unwrap_or("unknown"),
                option_env!("RUSTC_VERSION").unwrap_or("unknown"),
            ])
            .set(1.0);

        for collector in [
            Box::new(requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(duration.clone()),
            Box::new(failovers.clone()),
            Box::new(rate_limited.clone()),
            Box::new(probe_requests.clone()),
            Box::new(health_score.clone()),
            Box::new(slot_height.clone()),
            Box::new(slot_drift.clone()),
            Box::new(slot_height_commitment.clone()),
            Box::new(slot_drift_commitment.clone()),
            Box::new(circuit_state.clone()),
            Box::new(build_info.clone()),
        ] {
            registry.register(collector).unwrap();
        }

        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))
            .unwrap();

        Self(Arc::new(Inner {
            registry,
            requests,
            duration,
            failovers,
            rate_limited,
            probe_requests,
            health_score,
            slot_height,
            slot_drift,
            slot_height_commitment,
            slot_drift_commitment,
            circuit_state,
        }))
    }

    /// Record one forwarded request. `count` is the number of JSON-RPC calls it
    /// represents — 1 for a single request, the element count for a batch — so a
    /// 1000-call batch increments the counter by 1000 (provider-billed volume),
    /// while the latency histogram still gets a single observation for the one
    /// round trip.
    pub fn record_request(
        &self,
        method: &str,
        provider: &str,
        status: &str,
        latency_ms: f64,
        count: u64,
    ) {
        self.0
            .requests
            .with_label_values(&[method, provider, status])
            .inc_by(count as f64);
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

    /// Count one request shed from `provider` because its `max_rps` bucket was
    /// empty (the request routes to a peer, or serves anyway when degraded).
    pub fn record_rate_limited(&self, provider: &str) {
        self.0.rate_limited.with_label_values(&[provider]).inc();
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
        self.0
            .health_score
            .with_label_values(&[provider])
            .set(score);
        if let Some(s) = slot {
            self.0
                .slot_height
                .with_label_values(&[provider])
                .set(s as f64);
            self.0
                .slot_drift
                .with_label_values(&[provider])
                .set(drift as f64);
        }
        self.0
            .circuit_state
            .with_label_values(&[provider])
            .set(if circuit_open { 1.0 } else { 0.0 });
    }

    /// Push per-commitment slot height and drift for one provider. Skipped for a
    /// commitment with no observed slot yet (leaves the series absent rather than
    /// emitting a misleading 0).
    pub fn update_provider_commitment(
        &self,
        provider: &str,
        commitment: &str,
        slot: Option<u64>,
        drift: u64,
    ) {
        if let Some(s) = slot {
            self.0
                .slot_height_commitment
                .with_label_values(&[provider, commitment])
                .set(s as f64);
            self.0
                .slot_drift_commitment
                .with_label_values(&[provider, commitment])
                .set(drift as f64);
        }
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
        m.record_request("getSlot", "helius", "ok", 42.0, 1);
        m.record_request("getSlot", "helius", "ok", 58.0, 1);
        m.record_request("getSlot", "triton", "error", 0.0, 1);

        let text = m.render();
        assert!(text.contains(
            "rpc_plane_requests_total{method=\"getSlot\",provider=\"helius\",status=\"ok\"} 2"
        ));
        assert!(text.contains(
            "rpc_plane_requests_total{method=\"getSlot\",provider=\"triton\",status=\"error\"} 1"
        ));
    }

    #[test]
    fn batch_count_weights_the_counter() {
        let m = Metrics::new();
        // One batch of 1000 getTransaction calls = one round trip, 1000 calls.
        m.record_request("getTransaction", "helius", "ok", 42.0, 1000);

        let text = m.render();
        assert!(text.contains(
            "rpc_plane_requests_total{method=\"getTransaction\",provider=\"helius\",status=\"ok\"} 1000"
        ));
        // Single latency observation despite the 1000-call weight.
        assert!(text.contains("rpc_plane_request_duration_seconds_count{method=\"getTransaction\",provider=\"helius\"} 1"));
    }

    #[test]
    fn histogram_emits_buckets_and_sum() {
        let m = Metrics::new();
        m.record_request("getSlot", "helius", "ok", 42.0, 1);

        let text = m.render();
        // Prometheus histogram emits _bucket, _sum, _count lines.
        assert!(text.contains("rpc_plane_request_duration_seconds_bucket"));
        assert!(text.contains("rpc_plane_request_duration_seconds_sum"));
        assert!(text.contains("rpc_plane_request_duration_seconds_count"));
    }

    #[test]
    fn duration_not_observed_for_errors() {
        let m = Metrics::new();
        m.record_request("getSlot", "helius", "error", 100.0, 1);

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
    fn rate_limited_counter() {
        let m = Metrics::new();
        m.record_rate_limited("helius");
        m.record_rate_limited("helius");
        m.record_rate_limited("triton");

        let text = m.render();
        assert!(text.contains("rpc_plane_rate_limited_total{provider=\"helius\"} 2"));
        assert!(text.contains("rpc_plane_rate_limited_total{provider=\"triton\"} 1"));
    }

    #[test]
    fn health_gauges_update() {
        let m = Metrics::new();
        m.update_provider_health("helius", 0.95, Some(300_000_000), 2, false);

        let text = m.render();
        assert!(text.contains("rpc_plane_provider_health_score{provider=\"helius\"} 0.95"));
        assert!(text.contains("rpc_plane_circuit_breaker_state{provider=\"helius\"} 0"));
    }

    #[test]
    fn per_commitment_gauges_update() {
        let m = Metrics::new();
        m.update_provider_commitment("helius", "confirmed", Some(300_000_000), 4);
        // A commitment with no slot yet emits nothing.
        m.update_provider_commitment("helius", "finalized", None, 0);

        let text = m.render();
        assert!(text.contains(
            "rpc_plane_provider_slot_height_commitment{commitment=\"confirmed\",provider=\"helius\"} 300000000"
        ));
        assert!(text.contains(
            "rpc_plane_slot_drift_commitment{commitment=\"confirmed\",provider=\"helius\"} 4"
        ));
        assert!(
            !text.contains("commitment=\"finalized\""),
            "no series should be emitted for a commitment without data"
        );
    }

    #[test]
    fn build_info_emitted() {
        let text = Metrics::new().render();
        assert!(text.contains("rpc_plane_build_info{"));
        assert!(text.contains(&format!("version=\"{}\"", env!("CARGO_PKG_VERSION"))));
        assert!(text.contains("} 1"));
    }
}
