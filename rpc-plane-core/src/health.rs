use crate::config::{HealthConfig, ProviderConfig};
use crate::metrics::Metrics;
use crate::proxy::Clients;
use parking_lot::RwLock;
use reqwest::Client;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ── Circuit breaker state ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    HalfOpen,
    Open,
}

/// One outcome in a provider's sliding health window.
///
/// `Throttle` (HTTP 429) is deliberately distinct from `Failure`: a rate limit is
/// a load signal, not a fault. It demotes the score (so traffic sheds to peers)
/// but is neutral to the circuit breaker — a throttled-but-healthy provider stays
/// eligible and reabsorbs load the instant the burst passes, instead of being
/// excluded for a full cooldown and cascading its traffic onto everyone else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sample {
    Success,
    Failure,
    Throttle,
}

// ── Commitment isolation ─────────────────────────────────────────────────────

/// The three Solana commitment levels probed each cycle. `processed` is the hot
/// path a provider serves cheapest; `confirmed`/`finalized` can lag on an
/// entirely different storage tier, so drift is tracked per commitment against a
/// per-commitment tip (finalized trails ~32 slots by design — comparing it to
/// the processed tip would make every provider look drifting).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

impl Commitment {
    /// The read commitments expected to track the tip. `finalized` is excluded:
    /// it legitimately lags, so it feeds drift metrics but not the score.
    const READ: [Commitment; 2] = [Commitment::Processed, Commitment::Confirmed];

    pub fn as_str(self) -> &'static str {
        match self {
            Commitment::Processed => "processed",
            Commitment::Confirmed => "confirmed",
            Commitment::Finalized => "finalized",
        }
    }
}

/// Last slot a provider reported at each commitment (None until first probe).
#[derive(Debug, Clone, Copy, Default)]
pub struct CommitmentSlots {
    pub processed: Option<u64>,
    pub confirmed: Option<u64>,
    pub finalized: Option<u64>,
}

impl CommitmentSlots {
    fn get(&self, c: Commitment) -> Option<u64> {
        match c {
            Commitment::Processed => self.processed,
            Commitment::Confirmed => self.confirmed,
            Commitment::Finalized => self.finalized,
        }
    }

    /// Overlay freshly-probed slots, keeping prior values for any commitment
    /// whose probe leg failed this cycle (e.g. a stalled `finalized` backend).
    fn merge(&mut self, new: CommitmentSlots) {
        if new.processed.is_some() {
            self.processed = new.processed;
        }
        if new.confirmed.is_some() {
            self.confirmed = new.confirmed;
        }
        if new.finalized.is_some() {
            self.finalized = new.finalized;
        }
    }
}

/// Network tip per commitment = max slot across all providers at that level.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotTips {
    pub processed: u64,
    pub confirmed: u64,
    pub finalized: u64,
}

impl SlotTips {
    fn get(&self, c: Commitment) -> u64 {
        match c {
            Commitment::Processed => self.processed,
            Commitment::Confirmed => self.confirmed,
            Commitment::Finalized => self.finalized,
        }
    }

    fn observe(&mut self, slots: &CommitmentSlots) {
        self.processed = self.processed.max(slots.processed.unwrap_or(0));
        self.confirmed = self.confirmed.max(slots.confirmed.unwrap_or(0));
        self.finalized = self.finalized.max(slots.finalized.unwrap_or(0));
    }
}

/// Per-commitment slice of a provider's snapshot: slot, drift vs the
/// commitment's tip, and whether that drift crossed the threshold.
#[derive(Debug, Clone, Copy, Default)]
pub struct CommitmentHealth {
    pub slot: Option<u64>,
    pub drift: u64,
    pub is_drifting: bool,
}

// ── Per-provider health snapshot (cheap to clone and pass around) ─────────────

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub name: Arc<str>,
    /// Normalised health score 0.0–1.0. Open circuit = 0.0.
    pub score: f64,
    /// Last observed `processed` slot. None until the first successful probe.
    pub slot_height: Option<u64>,
    /// Worst drift across the read commitments (processed/confirmed). 0 when no
    /// slot data yet. This is the headline drift; per-commitment detail lives in
    /// the `processed`/`confirmed`/`finalized` fields below.
    pub slot_drift: u64,
    /// True when a read commitment's drift exceeds the configured threshold.
    pub is_drifting: bool,
    /// EMA latency in milliseconds (0.0 = no data yet).
    pub latency_ms: f64,
    /// Error rate in the sliding window (0.0–1.0).
    pub error_rate: f64,
    pub circuit: CircuitState,
    /// Per-commitment slot + drift. `finalized` is observed for visibility but
    /// excluded from the score (it legitimately trails the tip).
    pub processed: CommitmentHealth,
    pub confirmed: CommitmentHealth,
    pub finalized: CommitmentHealth,
}

impl HealthSnapshot {
    pub fn is_available(&self) -> bool {
        self.circuit != CircuitState::Open
    }
}

// ── Mutable inner state (behind RwLock) ──────────────────────────────────────

struct Inner {
    slots: CommitmentSlots,
    latency_ema_ms: f64,
    consecutive_failures: u32,
    circuit: CircuitState,
    circuit_opened_at: Option<Instant>,
    // (timestamp, outcome) pairs for the sliding window
    window: VecDeque<(Instant, Sample)>,
}

impl Inner {
    fn new() -> Self {
        Self {
            slots: CommitmentSlots::default(),
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

    /// Fraction of non-successful outcomes in the window — hard failures **and**
    /// throttles. Drives the health *score*, so a rate-limited provider is demoted
    /// and best-score routing sheds its traffic to peers with headroom.
    fn error_rate(&self) -> f64 {
        if self.window.is_empty() {
            return 0.0;
        }
        let bad = self
            .window
            .iter()
            .filter(|(_, s)| *s != Sample::Success)
            .count();
        bad as f64 / self.window.len() as f64
    }

    /// Fraction of *hard* failures in the window — throttles excluded. Drives the
    /// circuit *breaker*, so a 429 storm never trips a healthy-but-capped provider
    /// open (which would exclude it for the full cooldown and cascade load).
    fn hard_error_rate(&self) -> f64 {
        if self.window.is_empty() {
            return 0.0;
        }
        let hard = self
            .window
            .iter()
            .filter(|(_, s)| *s == Sample::Failure)
            .count();
        hard as f64 / self.window.len() as f64
    }

    /// Push one outcome into the window. `update_latency` gates whether a success
    /// feeds the scoring EMA — only health probes do (see [`ProviderHealth::record`]).
    fn push_sample(
        &mut self,
        sample: Sample,
        latency_ms: f64,
        update_latency: bool,
        window_secs: u64,
    ) {
        self.prune_window(window_secs);
        self.window.push_back((Instant::now(), sample));
        match sample {
            Sample::Success => {
                // Only probe successes move the scoring EMA. Live successes span a
                // cheap getSlot to a 10k-account getProgramAccounts scan, so folding
                // their latency in makes the score oscillate; live latency is still
                // exported to the Prometheus histograms.
                if update_latency {
                    if self.latency_ema_ms == 0.0 {
                        self.latency_ema_ms = latency_ms;
                    } else {
                        // α = 0.15 — stable EMA, not too slow to adapt
                        self.latency_ema_ms = 0.85 * self.latency_ema_ms + 0.15 * latency_ms;
                    }
                }
                self.consecutive_failures = 0;
            }
            // Hard failure: extend the consecutive-failure streak that trips the circuit.
            Sample::Failure => self.consecutive_failures += 1,
            // Throttle (429): neutral to the circuit — it neither extends nor resets
            // the hard-failure streak, and does not move the latency EMA (a fast 429
            // would otherwise flatter the provider). It only lands in the window,
            // demoting the score via `error_rate`.
            Sample::Throttle => {}
        }
    }

    /// Back-compat helper: push a boolean success/failure sample. `update_latency`
    /// gates whether a success feeds the scoring EMA (probes only).
    fn push_result(
        &mut self,
        success: bool,
        latency_ms: f64,
        update_latency: bool,
        window_secs: u64,
    ) {
        let sample = if success {
            Sample::Success
        } else {
            Sample::Failure
        };
        self.push_sample(sample, latency_ms, update_latency, window_secs);
    }

    /// Worst drift across the read commitments (processed/confirmed), each
    /// compared to its own tip. None until at least one has slot data.
    fn worst_read_drift(&self, tips: &SlotTips) -> Option<u64> {
        Commitment::READ
            .into_iter()
            .filter_map(|c| self.slots.get(c).map(|h| tips.get(c).saturating_sub(h)))
            .max()
    }

    /// Compute health score from current state.
    /// Weights are normalised so they don't need to sum to 1.0.
    fn score(&self, tips: &SlotTips, cfg: &HealthConfig) -> f64 {
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

        // Slot freshness: 0 drift → 1.0; decays as drift grows past threshold.
        // Scored off the worst read commitment so a provider whose `confirmed`
        // stalls while `processed` stays fresh is still demoted.
        let slot_score = match self.worst_read_drift(tips) {
            Some(drift) => {
                let thr = cfg.slot_drift_threshold as f64;
                thr / (thr + drift as f64)
            }
            None => 0.5, // no data yet
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
    pub name: Arc<str>,
    inner: RwLock<Inner>,
}

impl ProviderHealth {
    pub fn new(name: impl Into<Arc<str>>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            inner: RwLock::new(Inner::new()),
        })
    }

    /// Read-only snapshot for routing decisions.
    pub fn snapshot(&self, tips: &SlotTips, cfg: &HealthConfig) -> HealthSnapshot {
        let g = self.inner.read();
        let commitment = |c: Commitment| match g.slots.get(c) {
            Some(h) => {
                let drift = tips.get(c).saturating_sub(h);
                CommitmentHealth {
                    slot: Some(h),
                    drift,
                    is_drifting: drift > cfg.slot_drift_threshold,
                }
            }
            None => CommitmentHealth::default(),
        };
        let processed = commitment(Commitment::Processed);
        let confirmed = commitment(Commitment::Confirmed);
        let finalized = commitment(Commitment::Finalized);
        // Headline drift = worst of the read commitments (finalized excluded).
        let slot_drift = processed.drift.max(confirmed.drift);
        let is_drifting = processed.is_drifting || confirmed.is_drifting;
        HealthSnapshot {
            name: self.name.clone(),
            score: g.score(tips, cfg),
            slot_height: processed.slot,
            slot_drift,
            is_drifting,
            latency_ms: g.latency_ema_ms,
            error_rate: g.error_rate(),
            circuit: g.circuit.clone(),
            processed,
            confirmed,
            finalized,
        }
    }

    /// Record the outcome of a request. `is_probe` marks a health-probe outcome:
    /// only probes feed the latency EMA that drives the score, because live calls
    /// range from a cheap getSlot to a huge getProgramAccounts scan and would make
    /// the score oscillate. Live successes/failures still feed the error window and
    /// circuit breaker; live latency is tracked in the Prometheus histograms.
    pub fn record(&self, success: bool, latency_ms: f64, is_probe: bool, cfg: &HealthConfig) {
        let mut g = self.inner.write();
        g.push_result(success, latency_ms, is_probe, cfg.window_secs);
        apply_circuit_transitions(&mut g, &self.name, cfg);
    }

    /// Record a rate-limit (HTTP 429) outcome. Demotes the score so traffic sheds
    /// to peers, but stays neutral to the circuit breaker — the provider is
    /// healthy, just capped, so opening it would only make the overload worse.
    pub fn record_throttled(&self, latency_ms: f64, cfg: &HealthConfig) {
        let mut g = self.inner.write();
        // A throttle never moves the latency EMA (a fast 429 would flatter the
        // provider), so `update_latency` is always false regardless of source.
        g.push_sample(Sample::Throttle, latency_ms, false, cfg.window_secs);
        apply_circuit_transitions(&mut g, &self.name, cfg);
    }

    /// Convenience: set only the `processed` slot (single-commitment callers).
    pub fn update_slot(&self, slot: u64) {
        self.inner.write().slots.processed = Some(slot);
    }

    /// Merge a freshly-probed set of per-commitment slots.
    pub fn update_slots(&self, slots: CommitmentSlots) {
        self.inner.write().slots.merge(slots);
    }
}

fn apply_circuit_transitions(g: &mut Inner, name: &str, cfg: &HealthConfig) {
    match &g.circuit {
        CircuitState::Closed => {
            // Throttles are excluded from both triggers (`hard_error_rate`,
            // `consecutive_failures`) so a rate limit never opens the circuit.
            let should_open = g.consecutive_failures >= cfg.circuit_open_failures
                || g.hard_error_rate() > cfg.circuit_error_threshold;
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
            // Close on a genuine success, reopen on a hard failure. A throttle is
            // neutral — it neither confirms recovery nor counts as a fault — so the
            // circuit stays half-open awaiting a decisive next outcome.
            match g.window.back().map(|(_, s)| *s) {
                Some(Sample::Success) => {
                    g.circuit = CircuitState::Closed;
                    g.circuit_opened_at = None;
                    info!(provider = %name, "circuit CLOSED (recovered)");
                }
                Some(Sample::Failure) => {
                    g.circuit = CircuitState::Open;
                    g.circuit_opened_at = Some(Instant::now());
                    warn!(provider = %name, "circuit OPEN (probe failed)");
                }
                Some(Sample::Throttle) | None => {}
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
    entries: Arc<RwLock<HashMap<String, ProviderEntry>>>,
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
                        health: ProviderHealth::new(p.name.as_str()),
                        stop: Arc::new(AtomicBool::new(false)),
                    },
                )
            })
            .collect();
        Self {
            entries: Arc::new(RwLock::new(entries)),
            cfg: Arc::new(cfg),
            metrics,
        }
    }

    /// Spawn health + slot loops for every provider currently in the map.
    pub fn start(&self, clients: Clients, providers: Vec<ProviderConfig>) {
        for provider in providers {
            let client = {
                let map = clients.read();
                let Some(c) = map.get(&provider.name) else {
                    continue;
                };
                c.clone()
            };
            let (health, stop) = {
                let map = self.entries.read();
                let Some(e) = map.get(&provider.name) else {
                    continue;
                };
                (e.health.clone(), e.stop.clone())
            };
            Self::spawn_loops(
                health,
                stop,
                client,
                provider,
                self.cfg.clone(),
                self.metrics.clone(),
            );
        }
    }

    /// Add a new provider and start its background loops.
    pub fn add_provider(&self, client: Arc<Client>, provider: ProviderConfig) {
        let stop = Arc::new(AtomicBool::new(false));
        let health = ProviderHealth::new(provider.name.as_str());
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
            .insert(provider.name.clone(), ProviderEntry { health, stop });
    }

    /// Signal the background loops for this provider to exit and remove it.
    pub fn remove_provider(&self, name: &str) {
        if let Some(entry) = self.entries.write().remove(name) {
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
        tokio::spawn({
            async move { health_loop(health, client, provider, cfg, metrics, stop).await }
        });
    }

    /// Per-commitment network tips = max slot across all providers at each level.
    pub fn slot_tips(&self) -> SlotTips {
        let mut tips = SlotTips::default();
        for e in self.entries.read().values() {
            tips.observe(&e.health.inner.read().slots);
        }
        tips
    }

    /// Network tip at `processed` = max processed slot across all providers.
    pub fn slot_tip(&self) -> u64 {
        self.slot_tips().processed
    }

    /// Current snapshot for every provider. Hot path:
    /// parking_lot::RwLock allows unlimited concurrent readers with no
    /// scheduler involvement; the lock is held for nanoseconds (pure CPU
    /// score computation), so 1000s of concurrent requests proceed without
    /// queuing on each other.
    pub fn snapshots(&self) -> Vec<HealthSnapshot> {
        let healths: Vec<Arc<ProviderHealth>> = self
            .entries
            .read()
            .values()
            .map(|e| e.health.clone())
            .collect();
        let mut tips = SlotTips::default();
        for h in &healths {
            tips.observe(&h.inner.read().slots);
        }
        healths
            .iter()
            .map(|h| h.snapshot(&tips, &self.cfg))
            .collect()
    }

    /// Update the cached slot height for a named provider.
    pub fn update_slot(&self, provider_name: &str, slot: u64) {
        if let Some(h) = self
            .entries
            .read()
            .get(provider_name)
            .map(|e| e.health.clone())
        {
            h.update_slot(slot);
        }
    }

    /// Record a request outcome (feeds back into health score). `is_probe` marks a
    /// health-probe outcome — only those feed the scoring latency EMA. Live outcomes
    /// still feed the error window and circuit breaker. See [`ProviderHealth::record`].
    pub fn record(&self, provider_name: &str, success: bool, latency_ms: f64, is_probe: bool) {
        if let Some(h) = self
            .entries
            .read()
            .get(provider_name)
            .map(|e| e.health.clone())
        {
            h.record(success, latency_ms, is_probe, &self.cfg);
        }
    }

    /// Record a rate-limit (429) outcome for a named provider — demotes its score
    /// without opening its circuit. See [`ProviderHealth::record_throttled`].
    pub fn record_throttled(&self, provider_name: &str, latency_ms: f64) {
        if let Some(h) = self
            .entries
            .read()
            .get(provider_name)
            .map(|e| e.health.clone())
        {
            h.record_throttled(latency_ms, &self.cfg);
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
    // Submit-only providers (e.g. a transaction-landing service scoped to
    // `methods = ["sendTransaction"]`) can't answer the getSlot probe. Skip the
    // probe loop for them; their health is driven entirely by real request
    // outcomes recorded on the routing path.
    if !provider.supports("getSlot") {
        debug!(
            provider = %provider.name,
            "health probe skipped (provider does not support getSlot); \
             health tracked via live request outcomes"
        );
        return;
    }

    let interval = Duration::from_millis(cfg.interval_ms);
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let t0 = Instant::now();
        match probe(&client, &provider).await {
            Ok(res) => {
                // Score latency off the `processed` call alone — confirmed/finalized
                // hit slower storage tiers and would inflate the EMA.
                let ms = res.processed_latency_ms;
                let confirmed = res.slots.confirmed;
                let finalized = res.slots.finalized;
                let processed = res.slots.processed.unwrap_or_default();
                state.update_slots(res.slots);
                state.record(true, ms, /* is_probe */ true, &cfg);
                metrics.record_probe(&provider.name, "health", "ok");
                debug!(
                    provider = %provider.name,
                    processed,
                    confirmed = ?confirmed,
                    finalized = ?finalized,
                    latency_ms = format!("{ms:.1}"),
                    "health probe ok"
                );
            }
            Err(e) => {
                let ms = t0.elapsed().as_secs_f64() * 1000.0;
                if e.downcast_ref::<RateLimited>().is_some() {
                    // The provider throttled even the cheap probe — a load signal,
                    // not a fault. Demote the score but leave the circuit closed.
                    state.record_throttled(ms, &cfg);
                    metrics.record_probe(&provider.name, "health", "rate_limited");
                    warn!(provider = %provider.name, "health probe rate limited (429)");
                } else {
                    state.record(false, ms, /* is_probe */ true, &cfg);
                    metrics.record_probe(&provider.name, "health", "error");
                    warn!(provider = %provider.name, error = %e, "health probe failed");
                }
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Result of one probe cycle: the per-commitment slots plus the latency of the
/// `processed` call (the liveness signal used for scoring).
struct ProbeResult {
    slots: CommitmentSlots,
    processed_latency_ms: f64,
}

/// Probe a provider's slot at all three commitments.
///
/// Three separate `getSlot` requests are issued *concurrently* rather than as a
/// JSON-RPC batch. Batches can traverse extra provider-side machinery (batch
/// splitting at the load balancer, an un-batching hop) and some providers cap or
/// disable them; single requests reuse the warm connection pool and mirror the
/// shape of real client traffic. `processed` is the liveness signal — the probe
/// fails only if it fails; a stalled `confirmed`/`finalized` surfaces as drift,
/// not a probe error (its slot is simply left unchanged this cycle).
async fn probe(client: &Client, provider: &ProviderConfig) -> anyhow::Result<ProbeResult> {
    let (processed, confirmed, finalized) = tokio::join!(
        get_slot(client, provider, Commitment::Processed),
        get_slot(client, provider, Commitment::Confirmed),
        get_slot(client, provider, Commitment::Finalized),
    );
    let (processed_slot, processed_latency_ms) = processed?;
    Ok(ProbeResult {
        slots: CommitmentSlots {
            processed: Some(processed_slot),
            confirmed: confirmed.ok().map(|(s, _)| s),
            finalized: finalized.ok().map(|(s, _)| s),
        },
        processed_latency_ms,
    })
}

/// Marker error: a probe leg received HTTP 429. Carried as a typed error (rather
/// than flattened into a generic failure string) so the health loop can score it
/// as a throttle — demoting the score without opening the circuit.
#[derive(Debug)]
struct RateLimited;

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "provider returned HTTP 429 (rate limited)")
    }
}

impl std::error::Error for RateLimited {}

/// Single `getSlot` at one commitment. Returns the slot and its round-trip ms.
async fn get_slot(
    client: &Client,
    provider: &ProviderConfig,
    commitment: Commitment,
) -> anyhow::Result<(u64, f64)> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": commitment.as_str()}]
    });
    let mut req = client
        .post(&provider.url)
        .header("content-type", "application/json")
        .timeout(Duration::from_secs(5))
        .json(&body);
    if provider.http3 {
        req = req.version(reqwest::Version::HTTP_3);
    }
    let t0 = Instant::now();
    let resp = req.send().await?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    // Surface a 429 as a typed error so the health loop scores it as a throttle
    // (score demotion, no circuit trip) rather than a generic probe failure.
    if resp.status().as_u16() == 429 {
        return Err(anyhow::Error::new(RateLimited));
    }
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    let json: Value = resp.json().await?;
    let slot = json["result"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("unexpected getSlot response"))?;
    Ok((slot, ms))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cfg() -> HealthConfig {
        HealthConfig::default()
    }

    /// Uniform tip across all commitments — for tests that only exercise the
    /// (single-commitment) `processed` slot via `update_slot`.
    fn tips(slot: u64) -> SlotTips {
        SlotTips {
            processed: slot,
            confirmed: slot,
            finalized: slot,
        }
    }

    #[test]
    fn score_unknown_provider_is_midpoint() {
        let inner = Inner::new();
        let score = inner.score(&tips(0), &default_cfg());
        // latency=0.5, error=1.0, slot=0.5, success=1.0 → weighted average
        assert!(score > 0.0 && score < 1.0, "score={score}");
    }

    #[test]
    fn score_open_circuit_is_zero() {
        let mut inner = Inner::new();
        inner.circuit = CircuitState::Open;
        assert_eq!(inner.score(&tips(100), &default_cfg()), 0.0);
    }

    #[test]
    fn score_increases_with_good_latency() {
        let mut low_lat = Inner::new();
        low_lat.latency_ema_ms = 20.0;
        let mut high_lat = Inner::new();
        high_lat.latency_ema_ms = 800.0;
        let cfg = default_cfg();
        assert!(low_lat.score(&tips(100), &cfg) > high_lat.score(&tips(100), &cfg));
    }

    #[test]
    fn score_decreases_with_slot_drift() {
        let mut fresh = Inner::new();
        fresh.slots.processed = Some(1000);
        let mut stale = Inner::new();
        stale.slots.processed = Some(950); // 50 slots behind
        let cfg = default_cfg(); // drift_threshold = 10
        assert!(fresh.score(&tips(1000), &cfg) > stale.score(&tips(1000), &cfg));
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
        inner.push_result(true, 50.0, true, cfg.window_secs);
        inner.push_result(false, 0.0, true, cfg.window_secs);
        inner.push_result(false, 0.0, true, cfg.window_secs);
        assert!((inner.error_rate() - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn circuit_opens_after_consecutive_failures() {
        let ph = ProviderHealth::new("test");
        let cfg = HealthConfig {
            circuit_open_failures: 3,
            ..Default::default()
        };
        for _ in 0..3 {
            ph.record(false, 0.0, true, &cfg);
        }
        let snap = ph.snapshot(&tips(0), &cfg);
        assert_eq!(snap.circuit, CircuitState::Open);
        assert!(!snap.is_available());
    }

    #[test]
    fn slot_drift_and_is_drifting_populated() {
        let ph = ProviderHealth::new("test");
        ph.update_slot(980);
        let cfg = default_cfg(); // slot_drift_threshold = 10

        let snap = ph.snapshot(&tips(1000), &cfg);
        assert_eq!(snap.slot_drift, 20);
        assert!(snap.is_drifting);

        let snap = ph.snapshot(&tips(985), &cfg);
        assert_eq!(snap.slot_drift, 5);
        assert!(!snap.is_drifting);
    }

    #[test]
    fn latency_ema_first_sample_sets_to_value() {
        let mut inner = Inner::new();
        inner.push_result(true, 120.0, true, 60);
        assert_eq!(inner.latency_ema_ms, 120.0);
    }

    #[test]
    fn latency_ema_applies_smoothing() {
        let mut inner = Inner::new();
        inner.push_result(true, 100.0, true, 60);
        inner.push_result(true, 200.0, true, 60);
        let expected = 0.85 * 100.0 + 0.15 * 200.0;
        assert!((inner.latency_ema_ms - expected).abs() < 1e-9);
    }

    #[test]
    fn latency_ema_unchanged_on_failure() {
        let mut inner = Inner::new();
        inner.push_result(true, 100.0, true, 60);
        inner.push_result(false, 9999.0, true, 60);
        assert_eq!(inner.latency_ema_ms, 100.0);
    }

    #[test]
    fn latency_ema_ignores_live_outcomes() {
        // A live outcome (update_latency = false) lands in the window but must not
        // move the scoring EMA — a heavy getProgramAccounts scan should not make a
        // provider look slow for routing. See `record`'s `is_probe` flag.
        let mut inner = Inner::new();
        inner.push_result(true, 100.0, true, 60); // probe seeds the EMA
        inner.push_result(true, 900.0, false, 60); // heavy live success, ignored
        assert_eq!(
            inner.latency_ema_ms, 100.0,
            "live latency must not move the EMA"
        );
        // ...but the live success is still in the window (error rate stays clean).
        assert_eq!(inner.error_rate(), 0.0);
        assert_eq!(
            inner.window.len(),
            2,
            "live outcome still recorded in the window"
        );
    }

    #[test]
    fn score_worse_with_all_failures_than_all_successes() {
        let cfg = HealthConfig::default();
        let mut bad = Inner::new();
        for _ in 0..10 {
            bad.push_result(false, 0.0, true, cfg.window_secs);
        }
        let mut good = Inner::new();
        for _ in 0..10 {
            good.push_result(true, 50.0, true, cfg.window_secs);
        }
        assert!(good.score(&tips(100), &cfg) > bad.score(&tips(100), &cfg));
    }

    #[test]
    fn not_drifting_when_no_slot_data() {
        let ph = ProviderHealth::new("test");
        // slots all None (no probe received yet)
        let snap = ph.snapshot(&tips(1000), &default_cfg());
        assert!(snap.slot_height.is_none());
        assert_eq!(snap.slot_drift, 0);
        assert!(!snap.is_drifting);
    }

    #[test]
    fn circuit_stays_closed_below_threshold() {
        let ph = ProviderHealth::new("test");
        let cfg = HealthConfig {
            circuit_open_failures: 5,
            // Disable the error-rate trigger so only consecutive_failures counts.
            circuit_error_threshold: 1.1,
            ..Default::default()
        };
        for _ in 0..4 {
            ph.record(false, 0.0, true, &cfg);
        }
        let snap = ph.snapshot(&tips(0), &cfg);
        assert_eq!(snap.circuit, CircuitState::Closed);
    }

    #[test]
    fn circuit_full_lifecycle_closed_open_halfopen_closed() {
        let ph = ProviderHealth::new("t");
        // Zero cooldown so Open→HalfOpen happens on the very next record() call.
        let cfg = HealthConfig {
            circuit_open_failures: 1,
            circuit_cooldown_secs: 0,
            circuit_error_threshold: 1.1, // disable error-rate trigger
            ..Default::default()
        };
        ph.record(false, 0.0, true, &cfg);
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::Open);

        // record() with cooldown=0: Open → HalfOpen (elapsed ≥ 0 always true)
        ph.record(true, 10.0, true, &cfg);
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::HalfOpen);

        // Probe success → Closed
        ph.record(true, 10.0, true, &cfg);
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::Closed);
        assert!(ph.snapshot(&tips(0), &cfg).is_available());
    }

    #[test]
    fn circuit_halfopen_failure_reopens() {
        let ph = ProviderHealth::new("t");
        let cfg = HealthConfig {
            circuit_open_failures: 1,
            circuit_cooldown_secs: 0,
            circuit_error_threshold: 1.1,
            ..Default::default()
        };
        ph.record(false, 0.0, true, &cfg); // Closed → Open
        ph.record(false, 0.0, true, &cfg); // Open → HalfOpen (cooldown=0)
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::HalfOpen);
        ph.record(false, 0.0, true, &cfg); // HalfOpen → Open (probe failed)
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::Open);
        assert!(!ph.snapshot(&tips(0), &cfg).is_available());
    }

    #[test]
    fn throttle_demotes_score_without_opening_circuit() {
        let cfg = HealthConfig {
            circuit_open_failures: 3,
            circuit_error_threshold: 0.5,
            ..Default::default()
        };
        // 10 throttles — would trip both circuit triggers if counted as failures.
        let throttled = ProviderHealth::new("throttled");
        for _ in 0..10 {
            throttled.record_throttled(0.0, &cfg);
        }
        let ts = throttled.snapshot(&tips(0), &cfg);
        assert_eq!(
            ts.circuit,
            CircuitState::Closed,
            "throttles must not open the circuit"
        );
        assert!(ts.is_available(), "throttled provider stays eligible");

        // But the score is demoted relative to a clean provider, so best-score
        // routing sheds traffic to peers.
        let clean = ProviderHealth::new("clean");
        for _ in 0..10 {
            clean.record(true, 50.0, true, &cfg);
        }
        assert!(
            ts.score < clean.snapshot(&tips(0), &cfg).score,
            "throttles must demote the score (throttled={}, clean={})",
            ts.score,
            clean.snapshot(&tips(0), &cfg).score
        );
    }

    #[test]
    fn throttle_is_neutral_to_the_hard_failure_streak() {
        // A throttle interleaved in a hard-failure run neither resets nor extends
        // the streak: F, T, F, F must still reach circuit_open_failures = 3.
        let ph = ProviderHealth::new("t");
        let cfg = HealthConfig {
            circuit_open_failures: 3,
            circuit_error_threshold: 1.1, // disable the rate trigger
            ..Default::default()
        };
        ph.record(false, 0.0, true, &cfg); // hard #1
        ph.record_throttled(0.0, &cfg); // neutral
        ph.record(false, 0.0, true, &cfg); // hard #2
        assert_eq!(
            ph.snapshot(&tips(0), &cfg).circuit,
            CircuitState::Closed,
            "two hard failures (throttle between) < threshold"
        );
        ph.record(false, 0.0, true, &cfg); // hard #3 → open
        assert_eq!(ph.snapshot(&tips(0), &cfg).circuit, CircuitState::Open);
    }

    #[test]
    fn throttle_storm_never_trips_error_rate_circuit() {
        // Error-rate trigger at 0.5: an all-throttle window has hard_error_rate 0,
        // so the circuit stays closed however high the throttle rate climbs.
        let ph = ProviderHealth::new("t");
        let cfg = HealthConfig {
            circuit_open_failures: 100, // disable the consecutive trigger
            circuit_error_threshold: 0.5,
            ..Default::default()
        };
        for _ in 0..20 {
            ph.record_throttled(0.0, &cfg);
        }
        let snap = ph.snapshot(&tips(0), &cfg);
        assert_eq!(snap.circuit, CircuitState::Closed);
        // Scoring error rate reflects the throttles (all non-success)...
        assert_eq!(snap.error_rate, 1.0);
    }

    #[test]
    fn live_outcomes_feed_circuit_but_not_the_scoring_ema() {
        let cfg = HealthConfig {
            circuit_open_failures: 3,
            ..Default::default()
        };
        let ph = ProviderHealth::new("t");
        // A probe establishes the scoring EMA baseline.
        ph.record(true, 40.0, /* is_probe */ true, &cfg);
        // A slow live success must not drag the EMA up (probes-only EMA)...
        ph.record(true, 5_000.0, /* is_probe */ false, &cfg);
        assert_eq!(
            ph.snapshot(&tips(0), &cfg).latency_ms,
            40.0,
            "live latency must not move the scoring EMA"
        );
        // ...but live failures still count toward the circuit breaker.
        for _ in 0..3 {
            ph.record(false, 0.0, /* is_probe */ false, &cfg);
        }
        assert_eq!(
            ph.snapshot(&tips(0), &cfg).circuit,
            CircuitState::Open,
            "live failures must still open the circuit"
        );
    }

    #[test]
    fn monitor_update_slot_propagates() {
        let provider = crate::config::ProviderConfig {
            name: "p".to_string(),
            url: "http://127.0.0.1:1".to_string(),
            weight: 1,
            http3: false,
            methods: None,
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
                .insert("p".to_string(), ProviderEntry { health, stop });
        }
        monitor.update_slot("p", 42_000);
        assert_eq!(monitor.slot_tip(), 42_000);
    }

    #[test]
    fn add_and_remove_provider() {
        let monitor = HealthMonitor::new(&[], HealthConfig::default(), Metrics::new());
        assert_eq!(monitor.snapshots().len(), 0);

        let provider = ProviderConfig {
            name: "test".to_string(),
            url: "http://localhost:8899".to_string(),
            weight: 1,
            http3: false,
            methods: None,
        };
        // We don't call add_provider here (needs real HTTP) — just verify map ops
        {
            let stop = Arc::new(AtomicBool::new(false));
            let health = ProviderHealth::new(provider.name.as_str());
            monitor
                .entries
                .write()
                .insert(provider.name.clone(), ProviderEntry { health, stop });
        }
        assert_eq!(monitor.snapshots().len(), 1);

        monitor.remove_provider("test");
        assert_eq!(monitor.snapshots().len(), 0);
    }

    // ── Memory bounds: sliding window ──────────────────────────────────────────

    #[test]
    fn window_prunes_entries_older_than_window_secs() {
        let mut inner = Inner::new();
        // Directly inject entries with timestamps 120 seconds in the past.
        let old = Instant::now() - Duration::from_secs(120);
        inner.window.push_back((old, Sample::Success));
        inner.window.push_back((old, Sample::Failure));
        assert_eq!(inner.window.len(), 2);

        // prune_window(60) should evict both entries (120s > 60s cutoff).
        inner.prune_window(60);
        assert_eq!(
            inner.window.len(),
            0,
            "entries older than window_secs must be evicted"
        );
    }

    #[test]
    fn window_keeps_entries_within_window_secs() {
        let mut inner = Inner::new();
        // Inject an entry 10 seconds old — within a 60-second window.
        let recent = Instant::now() - Duration::from_secs(10);
        inner.window.push_back((recent, Sample::Success));

        inner.prune_window(60);
        assert_eq!(inner.window.len(), 1, "recent entries must be retained");
    }

    #[test]
    fn window_evicts_old_but_keeps_new_entries() {
        let mut inner = Inner::new();
        let old = Instant::now() - Duration::from_secs(90);
        let recent = Instant::now() - Duration::from_secs(5);
        inner.window.push_back((old, Sample::Failure));
        inner.window.push_back((old, Sample::Success));
        inner.window.push_back((recent, Sample::Success));

        inner.prune_window(60);
        // Only the recent entry survives.
        assert_eq!(inner.window.len(), 1);
        assert_eq!(
            inner.window[0].1,
            Sample::Success,
            "surviving entry should be the recent success"
        );
    }

    #[test]
    fn window_length_bounded_by_time_not_call_count() {
        let mut inner = Inner::new();
        // Simulate 1 000 records all arriving "now" within a 1-second window.
        // At a real 1s probe interval only ~1 entry would accumulate per second,
        // but this verifies there is no panic or unbounded allocation under burst.
        for i in 0..1_000 {
            inner.push_result(i % 2 == 0, 50.0, true, 60);
        }
        // All 1 000 are within the window — they're all retained, but the important
        // property is that push_result always calls prune_window, so old entries
        // are flushed each time records arrive. No leak possible.
        assert_eq!(inner.window.len(), 1_000);
        assert!(
            inner.error_rate() >= 0.0 && inner.error_rate() <= 1.0,
            "error_rate must remain a valid probability"
        );
    }

    // ── Commitment isolation ──────────────────────────────────────────────────

    #[test]
    fn finalized_drift_measured_against_finalized_tip() {
        let ph = ProviderHealth::new("p");
        ph.update_slots(CommitmentSlots {
            processed: Some(1000),
            confirmed: Some(998),
            finalized: Some(968), // ~32 behind processed by design, but at the finalized tip
        });
        let t = SlotTips {
            processed: 1000,
            confirmed: 1000,
            finalized: 968,
        };
        let snap = ph.snapshot(&t, &default_cfg()); // threshold 10
                                                    // Against the *finalized* tip it is not behind at all — comparing it
                                                    // to the processed tip (1000) would falsely show a 32-slot lag.
        assert_eq!(snap.finalized.drift, 0);
        assert!(!snap.finalized.is_drifting);
        // Headline drift folds processed(0) + confirmed(2) but never finalized.
        assert_eq!(snap.slot_drift, 2);
        assert!(!snap.is_drifting);
    }

    #[test]
    fn confirmed_stall_demotes_score_and_flags_drift() {
        let cfg = default_cfg(); // threshold 10
        let t = SlotTips {
            processed: 1000,
            confirmed: 1000,
            finalized: 968,
        };

        // Fresh at both read commitments.
        let healthy = ProviderHealth::new("healthy");
        healthy.update_slots(CommitmentSlots {
            processed: Some(1000),
            confirmed: Some(999),
            finalized: Some(968),
        });

        // processed fresh, but confirmed frozen 40 slots back — the failure the
        // single processed probe could never see.
        let stalled = ProviderHealth::new("stalled");
        stalled.update_slots(CommitmentSlots {
            processed: Some(1000),
            confirmed: Some(960),
            finalized: Some(968),
        });

        let hs = healthy.snapshot(&t, &cfg);
        let ss = stalled.snapshot(&t, &cfg);

        assert!(!hs.is_drifting);
        assert!(ss.confirmed.is_drifting, "confirmed 40 behind must flag");
        assert!(ss.is_drifting, "headline drift folds confirmed");
        assert_eq!(ss.slot_drift, 40);
        assert!(
            ss.score < hs.score,
            "a stalled confirmed pipeline must score below a healthy one \
             (stalled={}, healthy={})",
            ss.score,
            hs.score
        );
    }

    #[test]
    fn slot_tips_are_computed_per_commitment() {
        let monitor = HealthMonitor::new(&[], HealthConfig::default(), Metrics::new());
        let insert = |name: &str, slots: CommitmentSlots| {
            let health = ProviderHealth::new(name);
            health.update_slots(slots);
            monitor.entries.write().insert(
                name.to_string(),
                ProviderEntry {
                    health,
                    stop: Arc::new(AtomicBool::new(false)),
                },
            );
        };
        insert(
            "a",
            CommitmentSlots {
                processed: Some(1000),
                confirmed: Some(990),
                finalized: Some(950),
            },
        );
        insert(
            "b",
            CommitmentSlots {
                processed: Some(998),
                confirmed: Some(995),
                finalized: Some(970),
            },
        );
        let tips = monitor.slot_tips();
        assert_eq!(tips.processed, 1000, "max processed from a");
        assert_eq!(tips.confirmed, 995, "max confirmed from b");
        assert_eq!(tips.finalized, 970, "max finalized from b");
        assert_eq!(monitor.slot_tip(), 1000);
    }

    #[test]
    fn update_slots_merges_partial_probe() {
        let ph = ProviderHealth::new("p");
        ph.update_slots(CommitmentSlots {
            processed: Some(1000),
            confirmed: Some(990),
            finalized: Some(950),
        });
        // A later probe returns only processed (confirmed/finalized legs failed).
        ph.update_slots(CommitmentSlots {
            processed: Some(1005),
            ..Default::default()
        });
        let t = SlotTips {
            processed: 1005,
            confirmed: 990,
            finalized: 950,
        };
        let snap = ph.snapshot(&t, &default_cfg());
        assert_eq!(snap.processed.slot, Some(1005));
        assert_eq!(
            snap.confirmed.slot,
            Some(990),
            "prior confirmed retained across a partial probe"
        );
        assert_eq!(snap.finalized.slot, Some(950));
    }
}
