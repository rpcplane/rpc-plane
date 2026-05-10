use crate::config::{Config, ProviderConfig};
use crate::health::{CircuitState, HealthMonitor};
use crate::metrics::Metrics;
use crate::router::{extract_rpc_error_code, is_retryable_rpc_code, route};
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
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ProxyState {
    /// Live config — swapped atomically on hot reload. Read the Arc cheaply
    /// at the start of each handler; never hold the lock across an await.
    config: Arc<std::sync::RwLock<Arc<Config>>>,
    pub client: Arc<Client>,
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
        let client = Arc::new(
            Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .tcp_keepalive(std::time::Duration::from_secs(60))
                .pool_max_idle_per_host(32)
                .build()
                .expect("failed to build HTTP client"),
        );
        let metrics = Metrics::new();
        let monitor = HealthMonitor::new(&config.providers, config.health.clone(), metrics.clone());
        monitor.start(client.clone(), config.providers.clone());
        Self {
            config: Arc::new(std::sync::RwLock::new(Arc::new(config))),
            client,
            monitor,
            metrics,
            reporter,
        }
    }

    /// Clone of the Arc wrapping the live config. Pass to the hot-reload watcher.
    pub fn config_handle(&self) -> Arc<std::sync::RwLock<Arc<Config>>> {
        self.config.clone()
    }

    /// Cheaply snapshot the current config (clones an Arc, not the Config).
    fn current_config(&self) -> Arc<Config> {
        self.config.read().unwrap().clone()
    }
}

// ── Routers ───────────────────────────────────────────────────────────────────

pub fn build_router(state: ProxyState) -> Router {
    Router::new()
        .route("/", post(handle_rpc))
        .route("/health", get(handle_health))
        .with_state(state)
}

pub fn build_metrics_router(state: ProxyState) -> Router {
    Router::new()
        .route("/metrics", get(handle_metrics))
        .with_state(state)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_health(State(state): State<ProxyState>) -> impl IntoResponse {
    let snaps = state.monitor.snapshots().await;
    let providers: Vec<_> = snaps
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
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
    let snaps = state.monitor.snapshots().await;
    for s in &snaps {
        state.metrics.update_provider_health(
            &s.name,
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
    let method = extract_method(&body).unwrap_or_else(|| "unknown".to_string());

    let config = state.current_config();
    let snapshots = state.monitor.snapshots().await;
    let config_weights: Vec<(String, u32)> = config
        .providers
        .iter()
        .map(|p| (p.name.clone(), p.weight))
        .collect();
    let decision = route(
        &method,
        &snapshots,
        &config.routing.strategy,
        &config_weights,
    );

    if decision.broadcast {
        broadcast(
            &state,
            &config,
            &decision.providers,
            &method,
            &headers,
            &body,
            request_id,
        )
        .await
    } else {
        sequential(
            &state,
            &config,
            &decision.providers,
            &method,
            &headers,
            &body,
            request_id,
        )
        .await
    }
}

// ── Sequential (reads + failover) ────────────────────────────────────────────

async fn sequential(
    state: &ProxyState,
    config: &Config,
    providers: &[String],
    method: &str,
    headers: &HeaderMap,
    body: &Bytes,
    request_id: Uuid,
) -> Response {
    let max_attempts = (config.routing.max_retries + 1).min(providers.len());
    let mut prev_failed: Option<(String, &'static str)> = None; // (name, reason)

    for (attempt, name) in providers.iter().enumerate().take(max_attempts) {
        if attempt > 0 {
            if let Some((ref from, reason)) = prev_failed {
                state.metrics.record_failover(from, name);
                state.reporter.emit(TelemetryEvent::Failover {
                    from_provider: from.clone(),
                    to_provider: name.clone(),
                    reason: reason.to_string(),
                });
            }
        }

        let Some(provider) = config.providers.iter().find(|p| p.name == *name) else {
            continue;
        };

        let t0 = Instant::now();
        let result = forward(&state.client, provider, headers, body).await;
        let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(bytes) => {
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
                        state.metrics.record_request(method, name, "error", latency_ms);
                        state.reporter.emit(TelemetryEvent::Request {
                            id: request_id.to_string(),
                            method: method.to_string(),
                            provider: name.clone(),
                            latency_ms,
                            status: "error".to_string(),
                            commitment: None,
                            estimated_cost: None,
                        });
                        state.monitor.record(name, false, latency_ms).await;
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
                state.metrics.record_request(method, name, "ok", latency_ms);
                state.reporter.emit(TelemetryEvent::Request {
                    id: request_id.to_string(),
                    method: method.to_string(),
                    provider: name.clone(),
                    latency_ms,
                    status: "ok".to_string(),
                    commitment: None,
                    estimated_cost: None,
                });
                state.monitor.record(name, true, latency_ms).await;
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
                state.metrics.record_request(method, name, "error", latency_ms);
                state.reporter.emit(TelemetryEvent::Request {
                    id: request_id.to_string(),
                    method: method.to_string(),
                    provider: name.clone(),
                    latency_ms,
                    status: "error".to_string(),
                    commitment: None,
                    estimated_cost: None,
                });
                state.monitor.record(name, false, latency_ms).await;
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
    providers: &[String],
    method: &str,
    headers: &HeaderMap,
    body: &Bytes,
    request_id: Uuid,
) -> Response {
    if providers.is_empty() {
        return json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            -32603,
            "no providers available",
        );
    }

    let mut set: JoinSet<(String, Result<Bytes, String>, f64)> = JoinSet::new();

    for name in providers {
        let Some(provider) = config.providers.iter().find(|p| p.name == *name) else {
            continue;
        };
        let client = state.client.clone();
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

    let mut first_success: Option<Bytes> = None;

    while let Some(res) = set.join_next().await {
        match res {
            Ok((name, Ok(bytes), latency_ms)) => {
                state.metrics.record_request(method, &name, "ok", latency_ms);
                state.reporter.emit(TelemetryEvent::Request {
                    id: request_id.to_string(),
                    method: method.to_string(),
                    provider: name.clone(),
                    latency_ms,
                    status: "ok".to_string(),
                    commitment: None,
                    estimated_cost: None,
                });
                state.monitor.record(&name, true, latency_ms).await;
                if first_success.is_none() {
                    info!(
                        request_id = %request_id,
                        provider = %name,
                        method,
                        latency_ms = format!("{latency_ms:.1}"),
                        "broadcast first success"
                    );
                    first_success = Some(bytes);
                }
            }
            Ok((name, Err(e), latency_ms)) => {
                warn!(request_id = %request_id, provider = %name, method, error = %e, "broadcast provider failed");
                state.metrics.record_request(method, &name, "error", latency_ms);
                state.reporter.emit(TelemetryEvent::Request {
                    id: request_id.to_string(),
                    method: method.to_string(),
                    provider: name.clone(),
                    latency_ms,
                    status: "error".to_string(),
                    commitment: None,
                    estimated_cost: None,
                });
                state.monitor.record(&name, false, latency_ms).await;
            }
            Err(e) => {
                warn!(request_id = %request_id, error = %e, "broadcast task panicked");
            }
        }
    }

    match first_success {
        Some(bytes) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            bytes,
        )
            .into_response(),
        None => {
            error!(%request_id, method, "all broadcast providers failed");
            json_error_response(StatusCode::BAD_GATEWAY, -32603, "all providers failed")
        }
    }
}

// ── HTTP forwarding ───────────────────────────────────────────────────────────

async fn forward(
    client: &Client,
    provider: &ProviderConfig,
    incoming_headers: &HeaderMap,
    body: &Bytes,
) -> anyhow::Result<Bytes> {
    let mut builder = client
        .post(&provider.url)
        .header("content-type", "application/json")
        .header("accept", "application/json");

    for name in &["x-request-id", "x-trace-id", "traceparent", "tracestate"] {
        if let Some(value) = incoming_headers.get(*name) {
            builder = builder.header(*name, value);
        }
    }

    let resp = builder.body(body.clone()).send().await?;
    Ok(resp.bytes().await?)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn extract_method(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    if let Some(method) = v.get("method").and_then(Value::as_str) {
        return Some(method.to_owned());
    }
    // Batch request: extract methods from array elements.
    let arr = v.as_array()?;
    let methods: Vec<&str> = arr
        .iter()
        .filter_map(|req| req.get("method")?.as_str())
        .collect();
    if methods.is_empty() {
        return None;
    }
    Some(methods.join(","))
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
            Some("getSlot".to_string())
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
    fn extract_method_batch_array_joins_methods() {
        let batch = br#"[{"jsonrpc":"2.0","id":1,"method":"getSlot"},{"jsonrpc":"2.0","id":2,"method":"getBalance"}]"#;
        assert_eq!(
            extract_method(batch),
            Some("getSlot,getBalance".to_string())
        );
    }

    #[test]
    fn extract_method_batch_array_no_methods_returns_none() {
        let batch = br#"[{"jsonrpc":"2.0","id":1},{"jsonrpc":"2.0","id":2}]"#;
        assert_eq!(extract_method(batch), None);
    }
}
