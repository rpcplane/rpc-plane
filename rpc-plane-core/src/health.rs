use crate::config::{HealthConfig, ProviderConfig};
use crate::metrics::Metrics;
use reqwest::Client;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ── Circuit breaker state ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    HalfOpen,
    Open,
}

// ── Per-provider health snapshot (cheap to clone and pass around) ─────────────

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub name: String,
    /// Normalised health score 0.0–1.0. Open circuit = 0.0.
    pub score: f64,
    /// None until the first successful slot probe.
    pub slot_height: Option<u64>,
    /// Slots behind the network tip. 0 when slot_height is unknown.
    pub slot_drift: u64,
    /// True when slot_drift exceeds the configured threshold.
    pub is_drifting: bool,
    /// EMA latency in milliseconds (0.0 = no data yet).
    pub latency_ms: f64,
    /// Error rate in the sliding window (0.0–1.0).
    pub error_rate: f64,
    pub circuit: CircuitState,
}

impl HealthSnapshot {
    pub fn is_available(&self) -> bool {
        self.circuit != CircuitState::Open
    }
}

// ── Mutable inner state (behind RwLock) ──────────────────────────────────────

struct Inner {
    slot_height: Option<u64>,
    latency_ema_ms: f64,
    consecutive_failures: u32,
    circuit: CircuitState,
    circuit_opened_at: Option<Instant>,
    // (timestamp, success) pairs for the sliding window
    window: VecDeque<(Instant, bool)>,
}

impl Inner {
    fn new() -> Self {
        Self {
            slot_height: None,
            latency_ema_ms: 0.0,
            consecutive_failures: 0,
            circuit: CircuitState::Closed,
            circuit_opened_at: None,
            window: VecDeque::new(),
        }
    }

    fn prune_window(&mut self, window_secs: u64) {
        let cutoff = Instant::now() - Duration::from_secs(window_secs);
        while self
            .window
            .front()
            .map(|(t, _)| *t < cutoff)
            .unwrap_or(false)
        {
            self.window.pop_front();
        }
    }

    fn error_rate(&self) -> f64 {
        if self.window.is_empty() {
            return 0.0;
        }
        let failures = self.window.iter().filter(|(_, ok)| !ok).count();
        failures as f64 / self.window.len() as f64
    }

    fn push_result(&mut self, success: bool, latency_ms: f64, window_secs: u64) {
        self.prune_window(window_secs);
        self.window.push_back((Instant::now(), success));
        if success {
            if self.latency_ema_ms == 0.0 {
                self.latency_ema_ms = latency_ms;
            } else {
                // α = 0.15 — stable EMA, not too slow to adapt
                self.latency_ema_ms = 0.85 * self.latency_ema_ms + 0.15 * latency_ms;
            }
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures += 1;
        }
    }

    /// Compute health score from current state.
    /// Weights are normalised so they don't need to sum to 1.0.
    fn score(&self, slot_tip: u64, cfg: &HealthConfig) -> f64 {
        if self.circuit == CircuitState::Open {
            return 0.0;
        }

        // Sigmoid-like: 0ms→1.0, 200ms→0.5, ∞→0.0
        let latency_score = if self.latency_ema_ms == 0.0 {
            0.5 // no data
        } else {
            200.0 / (200.0 + self.latency_ema_ms)
        };

        let error_rate = self.error_rate();
        let error_score = 1.0 - error_rate;

        // Slot freshness: 0 drift → 1.0; decays as drift grows past threshold
        let slot_score = match (self.slot_height, slot_tip) {
            (Some(h), tip) if tip > 0 => {
                let drift = tip.saturating_sub(h);
                let thr = cfg.slot_drift_threshold as f64;
                thr / (thr + drift as f64)
            }
            _ => 0.5, // no data yet
        };

        // Recent success rate (same window, same metric as error but named separately
        // so the weight can be tuned independently — currently equivalent to error_score)
        let success_score = 1.0 - error_rate;

        let total_w = cfg.w_latency + cfg.w_error + cfg.w_slot + cfg.w_success;
        if total_w <= 0.0 {
            return 0.5;
        }

        (cfg.w_latency * latency_score
            + cfg.w_error * error_score
            + cfg.w_slot * slot_score
            + cfg.w_success * success_score)
            / total_w
    }
}

// ── ProviderHealth ────────────────────────────────────────────────────────────

pub struct ProviderHealth {
    pub name: String,
    inner: RwLock<Inner>,
}

impl ProviderHealth {
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            inner: RwLock::new(Inner::new()),
        })
    }

    /// Read-only snapshot for routing decisions.
    pub async fn snapshot(&self, slot_tip: u64, cfg: &HealthConfig) -> HealthSnapshot {
        let g = self.inner.read().await;
        let (slot_drift, is_drifting) = match g.slot_height {
            Some(h) => {
                let drift = slot_tip.saturating_sub(h);
                (drift, drift > cfg.slot_drift_threshold)
            }
            None => (0, false),
        };
        HealthSnapshot {
            name: self.name.clone(),
            score: g.score(slot_tip, cfg),
            slot_height: g.slot_height,
            slot_drift,
            is_drifting,
            latency_ms: g.latency_ema_ms,
            error_rate: g.error_rate(),
            circuit: g.circuit.clone(),
        }
    }

    /// Record the outcome of any request (probe or real).
    pub async fn record(&self, success: bool, latency_ms: f64, cfg: &HealthConfig) {
        let mut g = self.inner.write().await;
        g.push_result(success, latency_ms, cfg.window_secs);
        apply_circuit_transitions(&mut g, &self.name, cfg);
    }

    pub async fn update_slot(&self, slot: u64) {
        self.inner.write().await.slot_height = Some(slot);
    }
}

fn apply_circuit_transitions(g: &mut Inner, name: &str, cfg: &HealthConfig) {
    match &g.circuit {
        CircuitState::Closed => {
            let should_open = g.consecutive_failures >= cfg.circuit_open_failures
                || g.error_rate() > cfg.circuit_error_threshold;
            if should_open {
                g.circuit = CircuitState::Open;
                g.circuit_opened_at = Some(Instant::now());
                warn!(provider = %name, failures = g.consecutive_failures, "circuit OPEN");
            }
        }
        CircuitState::Open => {
            if let Some(opened) = g.circuit_opened_at {
                if opened.elapsed() >= Duration::from_secs(cfg.circuit_cooldown_secs) {
                    g.circuit = CircuitState::HalfOpen;
                    g.circuit_opened_at = None;
                    info!(provider = %name, "circuit HALF-OPEN (probe)");
                }
            }
        }
        CircuitState::HalfOpen => {
            // The last push_result already updated consecutive_failures.
            // Close on success, re-open on failure.
            let last_success = g.window.back().map(|(_, ok)| *ok).unwrap_or(false);
            if last_success {
                g.circuit = CircuitState::Closed;
                g.circuit_opened_at = None;
                info!(provider = %name, "circuit CLOSED (recovered)");
            } else {
                g.circuit = CircuitState::Open;
                g.circuit_opened_at = Some(Instant::now());
                warn!(provider = %name, "circuit OPEN (probe failed)");
            }
        }
    }
}

// ── HealthMonitor ─────────────────────────────────────────────────────────────

struct ProviderEntry {
    health: Arc<ProviderHealth>,
    /// Set to true to signal the background loops for this provider to exit.
    stop: Arc<AtomicBool>,
}

/// Shared health state for all providers. Cheap to clone (all Arcs inside).
#[derive(Clone)]
pub struct HealthMonitor {
    entries: Arc<std::sync::RwLock<HashMap<String, ProviderEntry>>>,
    pub cfg: Arc<HealthConfig>,
    metrics: Metrics,
}

impl HealthMonitor {
    pub fn new(providers: &[ProviderConfig], cfg: HealthConfig, metrics: Metrics) -> Self {
        let entries = providers
            .iter()
            .map(|p| {
                (
                    p.name.clone(),
                    ProviderEntry {
                        health: ProviderHealth::new(&p.name),
                        stop: Arc::new(AtomicBool::new(false)),
                    },
                )
            })
            .collect();
        Self {
            entries: Arc::new(std::sync::RwLock::new(entries)),
            cfg: Arc::new(cfg),
            metrics,
        }
    }

    /// Spawn health + slot loops for every provider currently in the map.
    pub fn start(&self, client: Arc<Client>, providers: Vec<ProviderConfig>) {
        for provider in providers {
            let (health, stop) = {
                let map = self.entries.read().unwrap();
                let Some(e) = map.get(&provider.name) else {
                    continue;
                };
                (e.health.clone(), e.stop.clone())
            };
            Self::spawn_loops(
                health,
                stop,
                client.clone(),
                provider,
                self.cfg.clone(),
                self.metrics.clone(),
            );
        }
    }

    /// Add a new provider and start its background loops.
    pub fn add_provider(&self, client: Arc<Client>, provider: ProviderConfig) {
        let stop = Arc::new(AtomicBool::new(false));
        let health = ProviderHealth::new(&provider.name);
        Self::spawn_loops(
            health.clone(),
            stop.clone(),
            client,
            provider.clone(),
            self.cfg.clone(),
            self.metrics.clone(),
        );
        self.entries
            .write()
            .unwrap()
            .insert(provider.name.clone(), ProviderEntry { health, stop });
    }

    /// Signal the background loops for this provider to exit and remove it.
    pub fn remove_provider(&self, name: &str) {
        if let Some(entry) = self.entries.write().unwrap().remove(name) {
            entry.stop.store(true, Ordering::Relaxed);
        }
    }

    fn spawn_loops(
        health: Arc<ProviderHealth>,
        stop: Arc<AtomicBool>,
        client: Arc<Client>,
        provider: ProviderConfig,
        cfg: Arc<HealthConfig>,
        metrics: Metrics,
    ) {
        let interval_ms = cfg.slot_interval_ms;

        tokio::spawn({
            let health = health.clone();
            let stop = stop.clone();
            let client = client.clone();
            let provider = provider.clone();
            let metrics = metrics.clone();
            async move { health_loop(health, client, provider, cfg, metrics, stop).await }
        });

        tokio::spawn(crate::slot_tracker::slot_poll_loop(
            health,
            client,
            provider,
            interval_ms,
            metrics,
            stop,
        ));
    }

    /// Network tip = max slot height across all providers.
    pub async fn slot_tip(&self) -> u64 {
        let healths: Vec<Arc<ProviderHealth>> = self
            .entries
            .read()
            .unwrap()
            .values()
            .map(|e| e.health.clone())
            .collect();
        let mut tip = 0u64;
        for h in &healths {
            if let Some(s) = h.inner.read().await.slot_height {
                tip = tip.max(s);
            }
        }
        tip
    }

    /// Current snapshot for every provider.
    pub async fn snapshots(&self) -> Vec<HealthSnapshot> {
        let healths: Vec<Arc<ProviderHealth>> = self
            .entries
            .read()
            .unwrap()
            .values()
            .map(|e| e.health.clone())
            .collect();
        let tip = {
            let mut max = 0u64;
            for h in &healths {
                if let Some(s) = h.inner.read().await.slot_height {
                    max = max.max(s);
                }
            }
            max
        };
        let mut out = Vec::with_capacity(healths.len());
        for h in healths {
            out.push(h.snapshot(tip, &self.cfg).await);
        }
        out
    }

    /// Update the cached slot height for a named provider.
    pub async fn update_slot(&self, provider_name: &str, slot: u64) {
        let health = self
            .entries
            .read()
            .unwrap()
            .get(provider_name)
            .map(|e| e.health.clone());
        if let Some(h) = health {
            h.update_slot(slot).await;
        }
    }

    /// Record a real request outcome (feeds back into health score).
    pub async fn record(&self, provider_name: &str, success: bool, latency_ms: f64) {
        let health = self
            .entries
            .read()
            .unwrap()
            .get(provider_name)
            .map(|e| e.health.clone());
        if let Some(h) = health {
            h.record(success, latency_ms, &self.cfg).await;
        }
    }
}

// ── Background health loop ────────────────────────────────────────────────────

async fn health_loop(
    state: Arc<ProviderHealth>,
    client: Arc<Client>,
    provider: ProviderConfig,
    cfg: Arc<HealthConfig>,
    metrics: Metrics,
    stop: Arc<AtomicBool>,
) {
    let interval = Duration::from_millis(cfg.interval_ms);
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let t0 = Instant::now();
        match probe(&client, &provider).await {
            Ok(slot) => {
                let ms = t0.elapsed().as_secs_f64() * 1000.0;
                state.update_slot(slot).await;
                state.record(true, ms, &cfg).await;
                metrics.record_probe(&provider.name, "health", "ok");
                debug!(
                    provider = %provider.name,
                    slot,
                    latency_ms = format!("{ms:.1}"),
                    "health probe ok"
                );
            }
            Err(e) => {
                let ms = t0.elapsed().as_secs_f64() * 1000.0;
                state.record(false, ms, &cfg).await;
                metrics.record_probe(&provider.name, "health", "error");
                warn!(provider = %provider.name, error = %e, "health probe failed");
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn probe(client: &Client, provider: &ProviderConfig) -> anyhow::Result<u64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "processed"}]
    });
    let resp = client
        .post(&provider.url)
        .header("content-type", "application/json")
        .timeout(Duration::from_secs(5))
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    let json: Value = resp.json().await?;
    json["result"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("unexpected getSlot response"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cfg() -> HealthConfig {
        HealthConfig::default()
    }

    #[test]
    fn score_unknown_provider_is_midpoint() {
        let inner = Inner::new();
        let score = inner.score(0, &default_cfg());
        // latency=0.5, error=1.0, slot=0.5, success=1.0 → weighted average
        assert!(score > 0.0 && score < 1.0, "score={score}");
    }

    #[test]
    fn score_open_circuit_is_zero() {
        let mut inner = Inner::new();
        inner.circuit = CircuitState::Open;
        assert_eq!(inner.score(100, &default_cfg()), 0.0);
    }

    #[test]
    fn score_increases_with_good_latency() {
        let mut low_lat = Inner::new();
        low_lat.latency_ema_ms = 20.0;
        let mut high_lat = Inner::new();
        high_lat.latency_ema_ms = 800.0;
        let cfg = default_cfg();
        assert!(low_lat.score(100, &cfg) > high_lat.score(100, &cfg));
    }

    #[test]
    fn score_decreases_with_slot_drift() {
        let mut fresh = Inner::new();
        fresh.slot_height = Some(1000);
        let mut stale = Inner::new();
        stale.slot_height = Some(950); // 50 slots behind
        let cfg = default_cfg(); // drift_threshold = 10
        assert!(fresh.score(1000, &cfg) > stale.score(1000, &cfg));
    }

    #[test]
    fn error_rate_empty_window_is_zero() {
        let inner = Inner::new();
        assert_eq!(inner.error_rate(), 0.0);
    }

    #[test]
    fn error_rate_counts_failures() {
        let mut inner = Inner::new();
        let cfg = default_cfg();
        inner.push_result(true, 50.0, cfg.window_secs);
        inner.push_result(false, 0.0, cfg.window_secs);
        inner.push_result(false, 0.0, cfg.window_secs);
        assert!((inner.error_rate() - 2.0 / 3.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn circuit_opens_after_consecutive_failures() {
        let ph = ProviderHealth::new("test");
        let cfg = HealthConfig {
            circuit_open_failures: 3,
            ..Default::default()
        };
        for _ in 0..3 {
            ph.record(false, 0.0, &cfg).await;
        }
        let snap = ph.snapshot(0, &cfg).await;
        assert_eq!(snap.circuit, CircuitState::Open);
        assert!(!snap.is_available());
    }

    #[tokio::test]
    async fn slot_drift_and_is_drifting_populated() {
        let ph = ProviderHealth::new("test");
        ph.update_slot(980).await;
        let cfg = default_cfg(); // slot_drift_threshold = 10

        let snap = ph.snapshot(1000, &cfg).await;
        assert_eq!(snap.slot_drift, 20);
        assert!(snap.is_drifting);

        let snap = ph.snapshot(985, &cfg).await;
        assert_eq!(snap.slot_drift, 5);
        assert!(!snap.is_drifting);
    }

    #[test]
    fn latency_ema_first_sample_sets_to_value() {
        let mut inner = Inner::new();
        inner.push_result(true, 120.0, 60);
        assert_eq!(inner.latency_ema_ms, 120.0);
    }

    #[test]
    fn latency_ema_applies_smoothing() {
        let mut inner = Inner::new();
        inner.push_result(true, 100.0, 60);
        inner.push_result(true, 200.0, 60);
        let expected = 0.85 * 100.0 + 0.15 * 200.0;
        assert!((inner.latency_ema_ms - expected).abs() < 1e-9);
    }

    #[test]
    fn latency_ema_unchanged_on_failure() {
        let mut inner = Inner::new();
        inner.push_result(true, 100.0, 60);
        inner.push_result(false, 9999.0, 60);
        assert_eq!(inner.latency_ema_ms, 100.0);
    }

    #[test]
    fn score_worse_with_all_failures_than_all_successes() {
        let cfg = HealthConfig::default();
        let mut bad = Inner::new();
        for _ in 0..10 {
            bad.push_result(false, 0.0, cfg.window_secs);
        }
        let mut good = Inner::new();
        for _ in 0..10 {
            good.push_result(true, 50.0, cfg.window_secs);
        }
        assert!(good.score(100, &cfg) > bad.score(100, &cfg));
    }

    #[tokio::test]
    async fn not_drifting_when_no_slot_data() {
        let ph = ProviderHealth::new("test");
        // slot_height = None (no probe received yet)
        let snap = ph.snapshot(1000, &default_cfg()).await;
        assert!(snap.slot_height.is_none());
        assert_eq!(snap.slot_drift, 0);
        assert!(!snap.is_drifting);
    }

    #[tokio::test]
    async fn circuit_stays_closed_below_threshold() {
        let ph = ProviderHealth::new("test");
        let cfg = HealthConfig {
            circuit_open_failures: 5,
            // Disable the error-rate trigger so only consecutive_failures counts.
            circuit_error_threshold: 1.1,
            ..Default::default()
        };
        for _ in 0..4 {
            ph.record(false, 0.0, &cfg).await;
        }
        let snap = ph.snapshot(0, &cfg).await;
        assert_eq!(snap.circuit, CircuitState::Closed);
    }

    #[tokio::test]
    async fn circuit_full_lifecycle_closed_open_halfopen_closed() {
        let ph = ProviderHealth::new("t");
        // Zero cooldown so Open→HalfOpen happens on the very next record() call.
        let cfg = HealthConfig {
            circuit_open_failures: 1,
            circuit_cooldown_secs: 0,
            circuit_error_threshold: 1.1, // disable error-rate trigger
            ..Default::default()
        };
        ph.record(false, 0.0, &cfg).await;
        assert_eq!(ph.snapshot(0, &cfg).await.circuit, CircuitState::Open);

        // record() with cooldown=0: Open → HalfOpen (elapsed ≥ 0 always true)
        ph.record(true, 10.0, &cfg).await;
        assert_eq!(ph.snapshot(0, &cfg).await.circuit, CircuitState::HalfOpen);

        // Probe success → Closed
        ph.record(true, 10.0, &cfg).await;
        assert_eq!(ph.snapshot(0, &cfg).await.circuit, CircuitState::Closed);
        assert!(ph.snapshot(0, &cfg).await.is_available());
    }

    #[tokio::test]
    async fn circuit_halfopen_failure_reopens() {
        let ph = ProviderHealth::new("t");
        let cfg = HealthConfig {
            circuit_open_failures: 1,
            circuit_cooldown_secs: 0,
            circuit_error_threshold: 1.1,
            ..Default::default()
        };
        ph.record(false, 0.0, &cfg).await; // Closed → Open
        ph.record(false, 0.0, &cfg).await; // Open → HalfOpen (cooldown=0)
        assert_eq!(ph.snapshot(0, &cfg).await.circuit, CircuitState::HalfOpen);
        ph.record(false, 0.0, &cfg).await; // HalfOpen → Open (probe failed)
        assert_eq!(ph.snapshot(0, &cfg).await.circuit, CircuitState::Open);
        assert!(!ph.snapshot(0, &cfg).await.is_available());
    }

    #[tokio::test]
    async fn monitor_update_slot_propagates() {
        let provider = crate::config::ProviderConfig {
            name: "p".to_string(),
            url: "http://127.0.0.1:1".to_string(),
            weight: 1,
            pricing: None,
        };
        let monitor = HealthMonitor::new(
            std::slice::from_ref(&provider),
            HealthConfig::default(),
            Metrics::new(),
        );
        // Manually replace the entry with a fresh ProviderHealth so background loops
        // are not started (avoids real HTTP probes in the unit test).
        {
            let stop = Arc::new(AtomicBool::new(false));
            let health = ProviderHealth::new("p");
            monitor
                .entries
                .write()
                .unwrap()
                .insert("p".to_string(), ProviderEntry { health, stop });
        }
        monitor.update_slot("p", 42_000).await;
        assert_eq!(monitor.slot_tip().await, 42_000);
    }

    #[tokio::test]
    async fn add_and_remove_provider() {
        let monitor = HealthMonitor::new(&[], HealthConfig::default(), Metrics::new());
        assert_eq!(monitor.snapshots().await.len(), 0);

        let provider = ProviderConfig {
            name: "test".to_string(),
            url: "http://localhost:8899".to_string(),
            weight: 1,
            pricing: None,
        };
        // add_provider spawns background tasks, so we need a tokio runtime
        // We don't call add_provider here (needs real HTTP) — just verify map ops
        {
            let stop = Arc::new(AtomicBool::new(false));
            let health = ProviderHealth::new(&provider.name);
            monitor
                .entries
                .write()
                .unwrap()
                .insert(provider.name.clone(), ProviderEntry { health, stop });
        }
        assert_eq!(monitor.snapshots().await.len(), 1);

        monitor.remove_provider("test");
        assert_eq!(monitor.snapshots().await.len(), 0);
    }
}
