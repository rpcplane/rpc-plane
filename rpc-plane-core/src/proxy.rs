use crate::config::{Config, ProviderConfig};
use crate::health::{CircuitState, HealthMonitor};
use crate::metrics::Metrics;
use crate::router::{extract_rpc_error_code, is_retryable_http, is_retryable_rpc_code, route};
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
                // Non-2xx that wasn't retryable (retryable HTTP already became an
                // Err above): a permanent provider error such as a revoked-key 401.
                // Record it as an error so health scoring reflects real traffic, and
                // pass the status through to the client instead of replying 200.
                // Do not fail over — it is non-retryable by construction.
                if !(200..300).contains(&status) {
                    warn!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        attempt,
                        status,
                        "upstream non-2xx, passing status through"
                    );
                    state
                        .metrics
                        .record_request(method, name, "error", latency_ms, count);
                    state
                        .reporter
                        .record_request(method, name, "error", latency_ms, count);
                    state.monitor.record(name, false, latency_ms);
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
                        state
                            .metrics
                            .record_request(method, name, "error", latency_ms, count);
                        state
                            .reporter
                            .record_request(method, name, "error", latency_ms, count);
                        state.monitor.record(name, false, latency_ms);
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
                state
                    .metrics
                    .record_request(method, name, "ok", latency_ms, count);
                state
                    .reporter
                    .record_request(method, name, "ok", latency_ms, count);
                state.monitor.record(name, true, latency_ms);
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
                state
                    .metrics
                    .record_request(method, name, "error", latency_ms, count);
                state
                    .reporter
                    .record_request(method, name, "error", latency_ms, count);
                state.monitor.record(name, false, latency_ms);
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

    // Return on the FIRST 2xx success rather than draining the whole JoinSet —
    // otherwise client latency tracks the slowest provider, defeating the entire
    // point of broadcast/ParallelRace. Stragglers are handed to a detached task
    // (below) so every provider's outcome still feeds metrics and health scoring.
    while let Some(res) = set.join_next().await {
        match res {
            Ok((name, Ok((status, bytes)), latency_ms)) => {
                let success = (200..300).contains(&status);
                record_broadcast_outcome(state, method, &name, success, latency_ms, count);
                if success {
                    info!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        latency_ms = format!("{latency_ms:.1}"),
                        "broadcast first success"
                    );
                    spawn_straggler_drain(state, method, request_id, count, set);
                    return (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        bytes,
                    )
                        .into_response();
                }
                warn!(request_id = %request_id, provider = %name, method, status, "broadcast provider non-2xx");
            }
            Ok((name, Err(e), latency_ms)) => {
                warn!(request_id = %request_id, provider = %name, method, error = %e, "broadcast provider failed");
                record_broadcast_outcome(state, method, &name, false, latency_ms, count);
            }
            Err(e) => {
                warn!(request_id = %request_id, error = %e, "broadcast task panicked");
            }
        }
    }

    // Reached only when no provider produced a 2xx — every outcome was already
    // recorded synchronously above.
    error!(%request_id, method, "all broadcast providers failed");
    json_error_response(StatusCode::BAD_GATEWAY, -32603, "all providers failed")
}

/// Record one broadcast provider outcome across metrics, telemetry, and health.
fn record_broadcast_outcome(
    state: &ProxyState,
    method: &str,
    name: &str,
    success: bool,
    latency_ms: f64,
    count: u64,
) {
    let status = if success { "ok" } else { "error" };
    state
        .metrics
        .record_request(method, name, status, latency_ms, count);
    state
        .reporter
        .record_request(method, name, status, latency_ms, count);
    state.monitor.record(name, success, latency_ms);
}

/// Drain the remaining in-flight broadcast tasks in the background after the
/// client has already been answered, so late providers still feed metrics and
/// health. Clones only the cheap Arc-backed handles off `ProxyState`; the
/// JoinSet is moved in and owns everything it needs ('static).
fn spawn_straggler_drain(
    state: &ProxyState,
    method: &str,
    request_id: Uuid,
    count: u64,
    mut set: JoinSet<ForwardOutcome>,
) {
    let metrics = state.metrics.clone();
    let reporter = state.reporter.clone();
    let monitor = state.monitor.clone();
    let method = method.to_string();
    tokio::spawn(async move {
        while let Some(res) = set.join_next().await {
            let (name, success, latency_ms) = match res {
                Ok((name, Ok((status, _)), latency_ms)) => {
                    (name, (200..300).contains(&status), latency_ms)
                }
                Ok((name, Err(e), latency_ms)) => {
                    warn!(request_id = %request_id, provider = %name, error = %e, "broadcast straggler failed");
                    (name, false, latency_ms)
                }
                Err(e) => {
                    warn!(request_id = %request_id, error = %e, "broadcast straggler task panicked");
                    continue;
                }
            };
            let status = if success { "ok" } else { "error" };
            metrics.record_request(&method, &name, status, latency_ms, count);
            reporter.record_request(&method, &name, status, latency_ms, count);
            monitor.record(&name, success, latency_ms);
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
    // Retryable statuses (429/5xx) become an Err so the caller fails over to the
    // next provider. Every other status — including non-retryable 4xx like a
    // revoked-key 401 — is returned with its code so the caller can pass it
    // through to the client and score it correctly, instead of masking it as 200.
    if is_retryable_http(status) {
        anyhow::bail!("provider returned HTTP {status}");
    }
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
