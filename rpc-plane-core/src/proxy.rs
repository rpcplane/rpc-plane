use crate::config::{Config, ProviderConfig};
use crate::health::{CircuitState, CommitmentHealth, HealthMonitor};
use crate::metrics::Metrics;
use crate::router::{
    extract_rpc_error_code, is_client_error, is_retryable_http, is_retryable_rpc_code, route,
};
use crate::telemetry::{NoopReporter, Reporter, TelemetryEvent};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;

pub type Clients = Arc<parking_lot::RwLock<HashMap<String, Arc<Client>>>>;
use std::time::Instant;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

// ── State ─────────────────────────────────────────────────────────────────────

pub fn build_client(provider: &ProviderConfig, pool_max_idle_per_host: usize) -> Client {
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(std::time::Duration::from_secs(60))
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .pool_idle_timeout(std::time::Duration::from_secs(90));
    if provider.http3 {
        builder = builder.http3_prior_knowledge().http3_send_grease(false);
    }
    builder.build().expect("failed to build HTTP client")
}

fn build_clients(providers: &[ProviderConfig], pool_max_idle_per_host: usize) -> Clients {
    let map = providers
        .iter()
        .map(|p| {
            (
                p.name.clone(),
                Arc::new(build_client(p, pool_max_idle_per_host)),
            )
        })
        .collect();
    Arc::new(parking_lot::RwLock::new(map))
}

#[derive(Clone)]
pub struct ProxyState {
    /// Live config — swapped atomically on hot reload. Read the Arc cheaply
    /// at the start of each handler; never hold the lock across an await.
    config: Arc<parking_lot::RwLock<Arc<Config>>>,
    pub clients: Clients,
    pub monitor: HealthMonitor,
    pub metrics: Metrics,
    pub reporter: Arc<dyn Reporter>,
}

impl ProxyState {
    /// Build with a `NoopReporter` (Prometheus-only mode).
    pub fn new(config: Config) -> Self {
        Self::new_with_reporter(config, Arc::new(NoopReporter))
    }

    /// Build with a custom reporter (e.g. `RemoteReporter` for telemetry).
    pub fn new_with_reporter(config: Config, reporter: Arc<dyn Reporter>) -> Self {
        let clients = build_clients(&config.providers, config.server.pool_max_idle_per_host);
        let metrics = Metrics::new();
        let monitor = HealthMonitor::new(&config.providers, config.health.clone(), metrics.clone());
        monitor.start(clients.clone(), config.providers.clone());
        Self {
            config: Arc::new(parking_lot::RwLock::new(Arc::new(config))),
            clients,
            monitor,
            metrics,
            reporter,
        }
    }

    /// Clone of the Arc wrapping the live config. Pass to the hot-reload watcher.
    pub fn config_handle(&self) -> Arc<parking_lot::RwLock<Arc<Config>>> {
        self.config.clone()
    }

    /// Clone of the clients map. Pass to the hot-reload watcher so it can
    /// insert/remove per-provider clients on config changes.
    pub fn clients_handle(&self) -> Clients {
        self.clients.clone()
    }

    fn client_for(&self, name: &str) -> Option<Arc<Client>> {
        self.clients.read().get(name).cloned()
    }

    /// Cheaply snapshot the current config (clones an Arc, not the Config).
    fn current_config(&self) -> Arc<Config> {
        self.config.read().clone()
    }

    /// The cheap Arc-backed handles needed to record a request outcome, detached
    /// from the rest of `ProxyState`. Built once per request and cloned into the
    /// detached straggler drain, which must own its handles (`'static`).
    fn recorder(&self) -> OutcomeRecorder {
        OutcomeRecorder {
            metrics: self.metrics.clone(),
            reporter: self.reporter.clone(),
            monitor: self.monitor.clone(),
        }
    }
}

/// How one forwarded leg is scored. Separates the metrics label from the health
/// effect so a client-attributable 4xx is visible without counting against the
/// provider, and a 429 demotes the score without opening the circuit.
#[derive(Clone, Copy)]
enum Outcome {
    /// A real success — 2xx with no JSON-RPC `error` body. Feeds health as a
    /// success and is the only outcome that ends a broadcast race.
    Ok,
    /// Provider-attributable failure — a 5xx, an auth 4xx, a 2xx carrying an
    /// error body, an upstream `Err`, or a retryable JSON-RPC error. Feeds health
    /// as a hard failure.
    ProviderError,
    /// HTTP 429 — a load signal, not a fault. Fails over like a 5xx, but demotes
    /// the provider's score without opening its circuit (see [`HealthMonitor`]).
    RateLimited,
    /// Client-attributable 4xx — recorded for visibility, never fed to health.
    ClientError,
}

impl Outcome {
    /// Classify from the HTTP status alone (sequential path — the status has
    /// already been separated from any 2xx error body by earlier branches).
    fn from_status(status: u16) -> Self {
        if (200..300).contains(&status) {
            Outcome::Ok
        } else if status == 429 {
            Outcome::RateLimited
        } else if is_client_error(status) {
            Outcome::ClientError
        } else {
            Outcome::ProviderError
        }
    }

    /// Classify a broadcast leg, which is a success only on a 2xx with no
    /// JSON-RPC error body ([`leg_succeeded`]); a 2xx carrying an error body is a
    /// provider failure (a preflight -32002), not a success.
    fn from_leg(status: u16, bytes: &Bytes) -> Self {
        if leg_succeeded(status, bytes) {
            Outcome::Ok
        } else if status == 429 {
            Outcome::RateLimited
        } else if is_client_error(status) {
            Outcome::ClientError
        } else {
            Outcome::ProviderError
        }
    }

    /// The `status` label written to metrics/telemetry.
    fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::ProviderError => "error",
            Outcome::RateLimited => "rate_limited",
            Outcome::ClientError => "client_error",
        }
    }
}

/// Bundles the cheap Arc-backed handles a request outcome touches so both the
/// hot path and the detached straggler drain record through one place.
#[derive(Clone)]
struct OutcomeRecorder {
    metrics: Metrics,
    reporter: Arc<dyn Reporter>,
    monitor: HealthMonitor,
}

impl OutcomeRecorder {
    /// Record one forwarded leg across metrics, telemetry, and provider health.
    /// The health effect is per-outcome: success/failure feed the circuit, a 429
    /// demotes the score only, and a client-attributable 4xx is health-neutral.
    fn record(&self, method: &str, name: &str, outcome: Outcome, latency_ms: f64, count: u64) {
        let label = outcome.label();
        self.metrics
            .record_request(method, name, label, latency_ms, count);
        self.reporter
            .record_request(method, name, label, latency_ms, count);
        match outcome {
            Outcome::Ok => self.monitor.record(name, true, latency_ms),
            Outcome::ProviderError => self.monitor.record(name, false, latency_ms),
            Outcome::RateLimited => self.monitor.record_throttled(name, latency_ms),
            Outcome::ClientError => {}
        }
    }
}

// ── Routers ───────────────────────────────────────────────────────────────────

pub fn build_router(state: ProxyState) -> Router {
    Router::new()
        .route("/", post(handle_rpc))
        .route("/health", get(handle_health))
        // Turns any handler panic into HTTP 500 instead of a silent connection drop.
        // (parking_lot locks can't poison, so this only guards stray panics/unwrap()s.)
        .layer(CatchPanicLayer::new())
        .with_state(state)
}

pub fn build_metrics_router(state: ProxyState) -> Router {
    Router::new()
        .route("/metrics", get(handle_metrics))
        .with_state(state)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_health(State(state): State<ProxyState>) -> impl IntoResponse {
    let snaps = state.monitor.snapshots();
    let commitment = |c: &CommitmentHealth| {
        serde_json::json!({
            "slot": c.slot,
            "drift": c.drift,
            "is_drifting": c.is_drifting,
        })
    };
    let providers: Vec<_> = snaps
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name.as_ref(),
                "score": (s.score * 1000.0).round() / 1000.0,
                "slot": s.slot_height,
                "slot_drift": s.slot_drift,
                "is_drifting": s.is_drifting,
                "latency_ms": (s.latency_ms * 10.0).round() / 10.0,
                "error_rate": (s.error_rate * 1000.0).round() / 1000.0,
                "circuit": format!("{:?}", s.circuit).to_lowercase(),
                "available": s.is_available(),
                "commitments": {
                    "processed": commitment(&s.processed),
                    "confirmed": commitment(&s.confirmed),
                    "finalized": commitment(&s.finalized),
                },
            })
        })
        .collect();
    axum::Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "providers": providers,
    }))
}

async fn handle_metrics(State(state): State<ProxyState>) -> impl IntoResponse {
    let snaps = state.monitor.snapshots();
    for s in &snaps {
        state.metrics.update_provider_health(
            s.name.as_ref(),
            s.score,
            s.slot_height,
            s.slot_drift,
            s.circuit == CircuitState::Open,
        );
        for (level, ch) in [
            ("processed", &s.processed),
            ("confirmed", &s.confirmed),
            ("finalized", &s.finalized),
        ] {
            state
                .metrics
                .update_provider_commitment(s.name.as_ref(), level, ch.slot, ch.drift);
        }
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
}

async fn handle_rpc(State(state): State<ProxyState>, headers: HeaderMap, body: Bytes) -> Response {
    let request_id = Uuid::new_v4();
    // `call_count` is the number of JSON-RPC calls (batch size) — used to weight
    // metrics/telemetry so a 1000-call batch is billed as 1000, not 1.
    let (method, call_count) = extract_method(&body).unwrap_or_else(|| ("unknown".to_string(), 1));

    let config = state.current_config();
    let snapshots = state.monitor.snapshots();
    let decision = route(
        &method,
        &snapshots,
        &config.routing.strategy,
        &config.providers,
        config.routing.broadcast_writes,
        &config.routing.write_methods,
    );

    let ctx = RequestCtx {
        method: &method,
        headers: &headers,
        body: &body,
        request_id,
        count: call_count,
    };

    if decision.broadcast {
        broadcast(&state, &config, &decision.providers, ctx).await
    } else {
        sequential(&state, &config, &decision.providers, ctx).await
    }
}

/// Per-request routing context shared by the sequential and broadcast paths.
/// Bundled so the hot-path fns keep a small, stable signature as fields grow.
struct RequestCtx<'a> {
    method: &'a str,
    headers: &'a HeaderMap,
    body: &'a Bytes,
    request_id: Uuid,
    /// JSON-RPC call count (batch size) — weights metrics/telemetry.
    count: u64,
}

/// Result of one provider forward: (provider name, Ok(status, body) | Err(msg), latency_ms).
type ForwardOutcome = (Arc<str>, Result<(u16, Bytes), String>, f64);

// ── Sequential (reads + failover) ────────────────────────────────────────────

async fn sequential(
    state: &ProxyState,
    config: &Config,
    providers: &[Arc<str>],
    ctx: RequestCtx<'_>,
) -> Response {
    let RequestCtx {
        method,
        headers,
        body,
        request_id,
        count,
    } = ctx;
    let max_attempts = (config.routing.max_retries + 1).min(providers.len());
    let mut prev_failed: Option<(Arc<str>, &'static str)> = None; // (name, reason)
    let recorder = state.recorder();

    for (attempt, name) in providers.iter().enumerate().take(max_attempts) {
        if attempt > 0 {
            if let Some((ref from, reason)) = prev_failed {
                state.metrics.record_failover(from, name);
                state.reporter.emit(TelemetryEvent::Failover {
                    from_provider: from.to_string(),
                    to_provider: name.to_string(),
                    reason: reason.to_string(),
                });
            }
        }

        let Some(provider) = config
            .providers
            .iter()
            .find(|p| p.name.as_str() == name.as_ref())
        else {
            continue;
        };

        let Some(client) = state.client_for(name) else {
            continue;
        };
        let t0 = Instant::now();
        let result = forward(&client, provider, headers, body).await;
        let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok((status, bytes)) => {
                // Retryable HTTP (429 or 5xx): fail over to the next provider. A
                // 5xx is a provider fault (`ProviderError`); a 429 is a load signal
                // (`RateLimited`) that demotes the provider's score so traffic
                // sheds to peers but leaves its circuit closed. On exhaustion the
                // loop falls through to the generic "all providers failed" 502.
                if is_retryable_http(status) {
                    let outcome = Outcome::from_status(status);
                    warn!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        attempt,
                        status,
                        outcome = outcome.label(),
                        "retryable upstream status, trying next provider"
                    );
                    recorder.record(method, name, outcome, latency_ms, count);
                    prev_failed = Some((name.clone(), "retryable_http"));
                    continue;
                }

                // Non-2xx that wasn't retryable. Pass the status through to the
                // client instead of replying 200, and do not fail over — it is
                // non-retryable by construction. A client-attributable 4xx
                // (malformed body, unknown route, oversized payload …) is the
                // caller's fault, so it is recorded as `client_error` and left out
                // of provider health; otherwise a buggy client loop could open
                // every circuit. Auth failures (401/403) still score as errors —
                // a revoked key makes the provider genuinely unusable.
                if !(200..300).contains(&status) {
                    let outcome = Outcome::from_status(status);
                    warn!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        attempt,
                        status,
                        outcome = outcome.label(),
                        "upstream non-2xx, passing status through"
                    );
                    recorder.record(method, name, outcome, latency_ms, count);
                    return (
                        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                        [("content-type", "application/json")],
                        bytes,
                    )
                        .into_response();
                }

                if let Some(code) = extract_rpc_error_code(&bytes) {
                    if is_retryable_rpc_code(code) && attempt + 1 < max_attempts {
                        warn!(
                            request_id = %request_id,
                            provider = %name,
                            method,
                            code,
                            attempt,
                            "retryable RPC error, trying next provider"
                        );
                        recorder.record(method, name, Outcome::ProviderError, latency_ms, count);
                        prev_failed = Some((name.clone(), "retryable_rpc_error"));
                        continue;
                    }
                }
                info!(
                    request_id = %request_id,
                    provider = %name,
                    method,
                    attempt,
                    latency_ms = format!("{latency_ms:.1}"),
                    "request ok"
                );
                recorder.record(method, name, Outcome::Ok, latency_ms, count);
                return (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    bytes,
                )
                    .into_response();
            }
            Err(e) => {
                warn!(
                    request_id = %request_id,
                    provider = %name,
                    method,
                    attempt,
                    error = %e,
                    "provider error, trying next"
                );
                recorder.record(method, name, Outcome::ProviderError, latency_ms, count);
                prev_failed = Some((name.clone(), "provider_error"));
            }
        }
    }

    error!(%request_id, method, "all providers failed");
    json_error_response(StatusCode::BAD_GATEWAY, -32603, "all providers failed")
}

// ── Broadcast (writes + parallel race) ────────────────────────────────────────

async fn broadcast(
    state: &ProxyState,
    config: &Config,
    providers: &[Arc<str>],
    ctx: RequestCtx<'_>,
) -> Response {
    let RequestCtx {
        method,
        headers,
        body,
        request_id,
        count,
    } = ctx;
    if providers.is_empty() {
        return json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            -32603,
            "no providers available",
        );
    }

    let mut set: JoinSet<ForwardOutcome> = JoinSet::new();

    for name in providers {
        let Some(provider) = config
            .providers
            .iter()
            .find(|p| p.name.as_str() == name.as_ref())
        else {
            continue;
        };
        let Some(client) = state.client_for(name) else {
            continue;
        };
        let provider = provider.clone();
        let headers = headers.clone();
        let body = body.clone();
        let name = name.clone();

        set.spawn(async move {
            let t0 = Instant::now();
            let result = forward(&client, &provider, &headers, &body).await;
            let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
            (name, result.map_err(|e| e.to_string()), latency_ms)
        });
    }

    // Return on the FIRST real success rather than draining the whole JoinSet —
    // otherwise client latency tracks the slowest provider, defeating the entire
    // point of broadcast/ParallelRace. A leg succeeds only on HTTP 2xx *and* a body
    // with no JSON-RPC `error` member: a `sendTransaction` that fails preflight
    // returns HTTP 200 + error -32002 and is the *fastest* leg precisely because it
    // did no work — if that won the race the client would see a failure, re-sign,
    // and double-execute while a slower provider actually landed the tx. Stragglers
    // are handed to a detached task (below) so every provider's outcome still feeds
    // metrics and health scoring.
    //
    // `best_error` keeps the most informative failed-leg body so that, if no leg
    // succeeds, the client gets a real upstream error (a deterministic -32602/-32002
    // it can act on) instead of a generic "all providers failed".
    let mut best_error: Option<(u16, Bytes, u8)> = None; // (status, body, rank)
    let recorder = state.recorder();

    while let Some(res) = set.join_next().await {
        match res {
            Ok((name, Ok((status, bytes)), latency_ms)) => {
                let outcome = Outcome::from_leg(status, &bytes);
                recorder.record(method, &name, outcome, latency_ms, count);
                if matches!(outcome, Outcome::Ok) {
                    info!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        latency_ms = format!("{latency_ms:.1}"),
                        "broadcast first success"
                    );
                    spawn_straggler_drain(recorder, method, request_id, count, set);
                    return (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        bytes,
                    )
                        .into_response();
                }
                let rpc_code = extract_rpc_error_code(&bytes);
                warn!(request_id = %request_id, provider = %name, method, status, code = ?rpc_code, outcome = outcome.label(), "broadcast leg failed");
                let rank = broadcast_error_rank(status, rpc_code);
                if best_error.as_ref().is_none_or(|(_, _, best)| rank > *best) {
                    best_error = Some((status, bytes, rank));
                }
            }
            Ok((name, Err(e), latency_ms)) => {
                warn!(request_id = %request_id, provider = %name, method, error = %e, "broadcast provider failed");
                recorder.record(method, &name, Outcome::ProviderError, latency_ms, count);
            }
            Err(e) => {
                warn!(request_id = %request_id, error = %e, "broadcast task panicked");
            }
        }
    }

    // No provider returned a real success — every outcome was already recorded
    // synchronously above. Surface a captured upstream error body (a preflight
    // -32002 rides on HTTP 200; a non-retryable 4xx passes its status through)
    // rather than masking it as a generic -32603.
    if let Some((status, bytes, _)) = best_error {
        error!(%request_id, method, "all broadcast providers failed; returning upstream error");
        return (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            [("content-type", "application/json")],
            bytes,
        )
            .into_response();
    }
    error!(%request_id, method, "all broadcast providers failed");
    json_error_response(StatusCode::BAD_GATEWAY, -32603, "all providers failed")
}

/// A broadcast leg counts as a success only on HTTP 2xx *and* a body with no
/// JSON-RPC `error` member. A 2xx carrying an error body — e.g. a `sendTransaction`
/// preflight failure (-32002) — is a failed leg, not a success; see `broadcast()`.
fn leg_succeeded(status: u16, bytes: &Bytes) -> bool {
    (200..300).contains(&status) && extract_rpc_error_code(bytes).is_none()
}

/// Rank a failed broadcast leg for selection as the fallback error returned to the
/// client when no leg succeeds. Higher wins. A non-retryable JSON-RPC error (bad
/// params, failed preflight) is deterministic and identical across providers, so
/// it's the most useful thing to hand back; a retryable RPC error is next; a bare
/// HTTP error with no JSON-RPC body is last.
fn broadcast_error_rank(status: u16, rpc_code: Option<i64>) -> u8 {
    match rpc_code {
        Some(code) if !is_retryable_rpc_code(code) => 3,
        Some(_) => 2,
        None if !(200..300).contains(&status) => 1,
        None => 0,
    }
}

/// Drain the remaining in-flight broadcast tasks in the background after the
/// client has already been answered, so late providers still feed metrics and
/// health. The `recorder` owns the cheap Arc-backed handles it needs; the
/// JoinSet is moved in and owns everything else ('static).
fn spawn_straggler_drain(
    recorder: OutcomeRecorder,
    method: &str,
    request_id: Uuid,
    count: u64,
    mut set: JoinSet<ForwardOutcome>,
) {
    let method = method.to_string();
    tokio::spawn(async move {
        while let Some(res) = set.join_next().await {
            let (name, outcome, latency_ms) = match res {
                Ok((name, Ok((status, bytes)), latency_ms)) => {
                    (name, Outcome::from_leg(status, &bytes), latency_ms)
                }
                Ok((name, Err(e), latency_ms)) => {
                    warn!(request_id = %request_id, provider = %name, error = %e, "broadcast straggler failed");
                    (name, Outcome::ProviderError, latency_ms)
                }
                Err(e) => {
                    warn!(request_id = %request_id, error = %e, "broadcast straggler task panicked");
                    continue;
                }
            };
            recorder.record(&method, &name, outcome, latency_ms, count);
        }
    });
}

// ── HTTP forwarding ───────────────────────────────────────────────────────────

async fn forward(
    client: &Client,
    provider: &ProviderConfig,
    incoming_headers: &HeaderMap,
    body: &Bytes,
) -> anyhow::Result<(u16, Bytes)> {
    let mut builder = client
        .post(&provider.url)
        .header("content-type", "application/json")
        .header("accept", "application/json");

    if provider.http3 {
        builder = builder.version(reqwest::Version::HTTP_3);
    }

    for name in &["x-request-id", "x-trace-id", "traceparent", "tracestate"] {
        if let Some(value) = incoming_headers.get(*name) {
            builder = builder.header(*name, value);
        }
    }

    let resp = builder.body(body.clone()).send().await?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await?;
    // Return the status and body for every HTTP response and let the caller
    // decide: retryable statuses (429/5xx) fail over to the next provider,
    // non-retryable 4xx pass through. Only a transport-level error (the `?`s
    // above) is an `Err`. This keeps the 429 → rate-limit classification intact
    // on both the sequential and broadcast paths.
    Ok((status, bytes))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Typed struct skips all irrelevant fields and borrows the method string directly,
// avoiding the full Value allocation that the naive from_slice::<Value> approach incurs.
#[derive(serde::Deserialize)]
struct MethodField<'a> {
    method: Option<&'a str>,
}

/// Max distinct method names rendered into a batch label before the remainder is
/// collapsed into a `+N` suffix.
const MAX_BATCH_LABEL_METHODS: usize = 5;
/// Hard ceiling on the rendered method label (a Prometheus label value and a
/// telemetry-aggregator map key). Defensive against pathologically long names.
const MAX_METHOD_LABEL_LEN: usize = 128;

/// Returns the (bounded method label, JSON-RPC call count) for a request body.
/// A single request is one call; a batch's call count is the number of
/// method-bearing elements. The label is bounded so a large/varied batch can't
/// mint a giant, unbounded-cardinality metrics label / aggregator key (the raw
/// `join(",")` let a 1000-element batch produce a ~15 KB never-evicted key); the
/// count weights metrics/telemetry so that batch is billed as its true volume.
pub fn extract_method(body: &[u8]) -> Option<(String, u64)> {
    let first = body.iter().find(|&&b| !b.is_ascii_whitespace())?;
    if *first == b'{' {
        // Single request: parse only the method field, skip everything else.
        let req: MethodField<'_> = serde_json::from_slice(body).ok()?;
        return req.method.map(|m| (m.to_owned(), 1));
    }
    // Batch request. Dedup method names (first-seen order), render up to
    // MAX_BATCH_LABEL_METHODS distinct names, collapse the rest into `+N`. A
    // homogeneous batch of 1000 `getTransaction` therefore labels as
    // `getTransaction` — identical to the single request, so dashboards group
    // them — while `count` carries the real 1000-call volume.
    let arr: Vec<MethodField<'_>> = serde_json::from_slice(body).ok()?;
    let mut uniques: Vec<&str> = Vec::new();
    let mut calls: u64 = 0;
    for m in arr.iter().filter_map(|r| r.method) {
        calls += 1;
        if !uniques.contains(&m) {
            uniques.push(m);
        }
    }
    if uniques.is_empty() {
        return None;
    }
    let shown = uniques.len().min(MAX_BATCH_LABEL_METHODS);
    let mut label = uniques[..shown].join(",");
    if uniques.len() > shown {
        label.push_str(&format!("+{}", uniques.len() - shown));
    }
    if label.len() > MAX_METHOD_LABEL_LEN {
        let mut end = MAX_METHOD_LABEL_LEN;
        while !label.is_char_boundary(end) {
            end -= 1;
        }
        label.truncate(end);
    }
    Some((label, calls))
}

fn json_error_response(status: StatusCode, code: i64, message: &str) -> Response {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": null,
    })
    .to_string();
    (status, [("content-type", "application/json")], body).into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_method_works() {
        assert_eq!(
            extract_method(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#),
            Some(("getSlot".to_string(), 1))
        );
    }

    #[test]
    fn extract_method_missing() {
        assert_eq!(extract_method(br#"{"jsonrpc":"2.0","id":1}"#), None);
    }

    #[test]
    fn extract_method_invalid_json() {
        assert_eq!(extract_method(b"not json"), None);
    }

    #[test]
    fn extract_method_batch_distinct_methods_kept() {
        let batch = br#"[{"jsonrpc":"2.0","id":1,"method":"getSlot"},{"jsonrpc":"2.0","id":2,"method":"getBalance"}]"#;
        assert_eq!(
            extract_method(batch),
            Some(("getSlot,getBalance".to_string(), 2))
        );
    }

    #[test]
    fn extract_method_homogeneous_batch_collapses_to_single_label() {
        // 1000× getTransaction: label collapses to the bare method (matching a
        // single request so dashboards group them), count carries the volume.
        let one = r#"{"jsonrpc":"2.0","id":1,"method":"getTransaction"}"#;
        let batch = format!("[{}]", vec![one; 1000].join(","));
        let (label, count) = extract_method(batch.as_bytes()).unwrap();
        assert_eq!(label, "getTransaction");
        assert_eq!(count, 1000);
        // The label is bounded regardless of batch size.
        assert!(label.len() <= MAX_METHOD_LABEL_LEN);
    }

    #[test]
    fn extract_method_many_distinct_methods_capped_with_suffix() {
        // 8 distinct methods → first 5 rendered, remainder collapsed to `+3`.
        let methods = [
            "getSlot",
            "getBalance",
            "getAccountInfo",
            "getBlock",
            "getTransaction",
            "getSignatureStatuses",
            "getEpochInfo",
            "getVersion",
        ];
        let body = format!(
            "[{}]",
            methods
                .iter()
                .enumerate()
                .map(|(i, m)| format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"{m}"}}"#))
                .collect::<Vec<_>>()
                .join(",")
        );
        let (label, count) = extract_method(body.as_bytes()).unwrap();
        assert_eq!(
            label,
            "getSlot,getBalance,getAccountInfo,getBlock,getTransaction+3"
        );
        assert_eq!(count, 8);
    }

    #[test]
    fn extract_method_batch_array_no_methods_returns_none() {
        let batch = br#"[{"jsonrpc":"2.0","id":1},{"jsonrpc":"2.0","id":2}]"#;
        assert_eq!(extract_method(batch), None);
    }
}
