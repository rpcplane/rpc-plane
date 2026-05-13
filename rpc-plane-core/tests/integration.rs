/// Integration tests: spin up mock HTTP provider servers, run the full proxy
/// stack (ProxyState → axum Router), send real requests via `tower::ServiceExt`,
/// and assert end-to-end behaviour.
///
/// Each test gets its own tokio runtime; spawned tasks are aborted on drop.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    routing::post,
    Router,
};
use http_body_util::BodyExt;
use rpc_plane_core::{
    config::{Config, HealthConfig, ProviderConfig, RoutingConfig, RoutingStrategy, ServerConfig},
    proxy::{build_router, ProxyState},
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tower::ServiceExt;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Bind to port 0, return the URL, then drop the listener so the port is free.
/// Connecting to this URL will get an immediate connection-refused.
fn refused_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    // listener drops here — port is immediately unreachable
    format!("http://{addr}")
}

/// Start an axum server on a random port. Returns (url, abort_handle).
/// The server is cancelled when the abort handle is dropped or `.abort()` is called.
async fn start_mock(router: Router) -> (String, tokio::task::AbortHandle) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}"), handle.abort_handle())
}

/// Build a test Config that disables background health/slot probes (huge intervals)
/// so they don't interfere with state we set explicitly.
fn test_config(
    providers: &[(&str, &str)],
    strategy: RoutingStrategy,
    max_retries: usize,
    circuit_open_failures: u32,
) -> Config {
    Config {
        server: ServerConfig::default(),
        health: HealthConfig {
            interval_ms: 999_999,
            circuit_open_failures,
            // Disable error-rate trigger — use only consecutive_failures.
            circuit_error_threshold: 1.1,
            circuit_cooldown_secs: 30,
            ..Default::default()
        },
        routing: RoutingConfig {
            strategy,
            max_retries,
            ..Default::default()
        },
        providers: providers
            .iter()
            .map(|(name, url)| ProviderConfig {
                name: name.to_string(),
                url: url.to_string(),
                weight: 1,
                pricing: None,
                http3: false,
            })
            .collect(),
        reporting: None,
    }
}

/// Send a POST / to the proxy router and return (status, parsed json body).
async fn proxy_request(router: Router, body: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, json)
}

const GET_SLOT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
const SEND_TX: &str =
    r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["dummybase64"]}"#;

fn slot_response(slot: u64) -> axum::Json<Value> {
    axum::Json(json!({ "jsonrpc": "2.0", "result": slot, "id": 1 }))
}

fn rpc_error_response(code: i64, msg: &'static str) -> axum::Json<Value> {
    axum::Json(json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": msg },
        "id": 1
    }))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// When all providers are unreachable, the proxy must return 502 Bad Gateway
/// with a JSON-RPC error body — not hang, not panic.
#[tokio::test]
async fn all_providers_down_returns_bad_gateway() {
    let url_a = refused_url();
    let url_b = refused_url();

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        2, // max_retries — tries a then b
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let code = body["error"]["code"].as_i64();
    assert!(code.is_some(), "expected JSON-RPC error in body: {body}");
}

/// With FailoverOrdered, the proxy tries provider A first. When A is unreachable
/// (connection refused) it retries on B, which succeeds.
#[tokio::test]
async fn sequential_failover_on_connection_refused() {
    let url_a = refused_url();
    let mock_b = Router::new().route("/", post(|| async { slot_response(999) }));
    let (url_b, _abort) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        2,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(999));
}

/// A retryable JSON-RPC error code (-32603) from provider A must cause the proxy
/// to retry the request on provider B.
#[tokio::test]
async fn retryable_rpc_error_causes_retry_to_next_provider() {
    let mock_a = Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32603, "internal error") }),
    );
    let mock_b = Router::new().route("/", post(|| async { slot_response(777) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    // Give A a higher score so BestScore picks it first, then falls back to B.
    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        2,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(777));
}

/// sendTransaction must be broadcast to every healthy provider simultaneously.
/// Both providers' request counts must be 1 after a single write request.
#[tokio::test]
async fn write_method_broadcasts_to_all_providers() {
    let count_a = Arc::new(AtomicUsize::new(0));
    let count_b = Arc::new(AtomicUsize::new(0));

    // Only count requests that actually contain sendTransaction.
    // Background health probes send getSlot and must not be counted.
    let cnt_a = count_a.clone();
    let mock_a = Router::new().route(
        "/",
        post(move |body: axum::body::Bytes| {
            let c = cnt_a.clone();
            async move {
                if std::str::from_utf8(&body).is_ok_and(|s| s.contains("sendTransaction")) {
                    c.fetch_add(1, Ordering::Relaxed);
                }
                slot_response(1)
            }
        }),
    );

    let cnt_b = count_b.clone();
    let mock_b = Router::new().route(
        "/",
        post(move |body: axum::body::Bytes| {
            let c = cnt_b.clone();
            async move {
                if std::str::from_utf8(&body).is_ok_and(|s| s.contains("sendTransaction")) {
                    c.fetch_add(1, Ordering::Relaxed);
                }
                slot_response(1)
            }
        }),
    );

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, _) = proxy_request(router, SEND_TX).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(count_a.load(Ordering::Relaxed), 1, "provider A not reached");
    assert_eq!(count_b.load(Ordering::Relaxed), 1, "provider B not reached");
}

/// When provider A's circuit is open (pre-seeded with enough failures) it must
/// be excluded from routing; all traffic goes to B.
#[tokio::test]
async fn circuit_open_provider_excluded_from_routing() {
    // A is unreachable — connection-refused means health probes also fail, helping open the circuit.
    let url_a = refused_url();
    let mock_b = Router::new().route("/", post(|| async { slot_response(555) }));
    let (url_b, _abort) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered, // A first in config order
        0,                                // no retries — if A were tried, it would fail
        3,
    );
    let state = ProxyState::new(cfg);

    // Pre-open A's circuit by recording 3 consecutive failures.
    for _ in 0..3 {
        state.monitor.record("a", false, 1000.0).await;
    }

    let router = build_router(state);
    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(555));
}

/// Even when one broadcast provider fails (connection refused), the proxy
/// must return a successful response from the other provider.
#[tokio::test]
async fn broadcast_partial_failure_still_succeeds() {
    let url_a = refused_url(); // A is completely down
    let mock_b = Router::new().route("/", post(|| async { slot_response(321) }));
    let (url_b, _abort) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    // sendTransaction → broadcast to A and B; A fails, B succeeds.
    let (status, body) = proxy_request(router, SEND_TX).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(321));
}

/// GET /health must return 200 with a "providers" array.
#[tokio::test]
async fn health_endpoint_returns_provider_list() {
    let mock = Router::new().route("/", post(|| async { slot_response(1) }));
    let (url, _abort) = start_mock(mock).await;

    let cfg = test_config(&[("p", &url)], RoutingStrategy::BestScore, 0, 5);
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"].as_str(), Some("ok"));
    let providers = body["providers"].as_array().unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["name"].as_str(), Some("p"));
}
