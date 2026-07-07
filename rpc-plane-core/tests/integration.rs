/// Integration tests: spin up mock HTTP provider servers, run the full proxy
/// stack (ProxyState → axum Router), send real requests via `tower::ServiceExt`,
/// and assert end-to-end behaviour.
///
/// Each test gets its own tokio runtime; spawned tasks are aborted on drop.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
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
use std::time::Duration;
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

/// Start a mock that sleeps `delay` before answering every POST with `slot`.
/// Used to model a degraded-but-eventually-successful provider so broadcast /
/// ParallelRace early-return can be observed against the wall clock.
async fn start_slow_mock(slot: u64, delay: Duration) -> (String, tokio::task::AbortHandle) {
    let router = Router::new().route(
        "/",
        post(move || async move {
            tokio::time::sleep(delay).await;
            slot_response(slot)
        }),
    );
    start_mock(router).await
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
                http3: false,
                methods: None,
                max_rps: None,
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

    let mut cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
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

    // Pre-open A's circuit by recording 3 consecutive live failures.
    for _ in 0..3 {
        state.monitor.record("a", false, 1000.0, false);
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

    let mut cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
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

/// Provider A returns HTTP 429 (rate limited); proxy must fail over to B.
#[tokio::test]
async fn rate_limited_provider_fails_over_to_next() {
    let mock_a = Router::new().route("/", post(|| async { (StatusCode::TOO_MANY_REQUESTS, "") }));
    let mock_b = Router::new().route("/", post(|| async { slot_response(888) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        1,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(888));
}

/// Provider A has a stale slot (100 behind tip); BestScore routes exclusively to B.
///
/// A: slot=900, B: slot=1000 (tip). slot_score(A) << slot_score(B) so B wins.
/// Background health probes may also call mock_a, so we assert only on the returned
/// slot value — A returns 900, B returns 1000.
#[tokio::test]
async fn drifting_provider_deprioritized_by_best_score() {
    let mock_a = Router::new().route("/", post(|| async { slot_response(900) }));
    let mock_b = Router::new().route("/", post(|| async { slot_response(1000) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    let state = ProxyState::new(cfg);
    // Pre-seed slot heights: A at 900, B at 1000 (network tip = 1000, A drifts 100 slots).
    // drift_threshold=10 → A's slot_score = 10/(10+100) ≈ 0.09; B's = 1.0.
    state.monitor.update_slot("a", 900);
    state.monitor.update_slot("b", 1000);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::OK);
    // B's response (slot=1000) confirms the fresh provider was selected.
    // If A were chosen, the result would be 900.
    assert_eq!(body["result"].as_u64(), Some(1000));
}

/// Provider A is pre-seeded with high-latency history; BestScore prefers B.
///
/// latency_score(A) = 200/(200+800) = 0.2; B has no latency data → defaults to 0.5.
/// A returns slot=1, B returns slot=2 — response value reveals which was picked.
/// Background health probes may call mock_a, but only the proxy-request result matters.
#[tokio::test]
async fn slow_provider_deprioritized_by_latency_score() {
    let mock_a = Router::new().route("/", post(|| async { slot_response(1) }));
    let mock_b = Router::new().route("/", post(|| async { slot_response(2) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    let state = ProxyState::new(cfg);
    // Simulate provider A's probes having been consistently slow (800ms average).
    // Only probe outcomes feed the scoring EMA, so these are recorded as probes.
    // latency_score(A) ≈ 0.2; latency_score(B) = 0.5 (no data) — B wins on latency.
    for _ in 0..10 {
        state.monitor.record("a", true, 800.0, true);
    }
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(status, StatusCode::OK);
    // B returns slot=2; if A were selected the result would be 1.
    assert_eq!(
        body["result"].as_u64(),
        Some(2),
        "B (lower latency) should be selected"
    );
}

/// Circuit opens after consecutive failures, then manual probe recovery closes it,
/// and FailoverOrdered sends traffic back to A (which is now the primary).
#[tokio::test]
async fn circuit_recovers_traffic_resumes() {
    let mock_a = Router::new().route("/", post(|| async { slot_response(42) }));
    let mock_b = Router::new().route("/", post(|| async { slot_response(99) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    // circuit_cooldown_secs=0 so Open→HalfOpen transitions happen immediately on the next record().
    let cfg = Config {
        server: ServerConfig::default(),
        health: HealthConfig {
            interval_ms: 999_999,
            circuit_open_failures: 2,
            circuit_cooldown_secs: 0,
            circuit_error_threshold: 1.1,
            ..HealthConfig::default()
        },
        routing: RoutingConfig {
            strategy: RoutingStrategy::FailoverOrdered,
            max_retries: 0,
            ..RoutingConfig::default()
        },
        providers: vec![
            ProviderConfig {
                name: "a".into(),
                url: url_a,
                weight: 1,
                http3: false,
                methods: None,
                max_rps: None,
            },
            ProviderConfig {
                name: "b".into(),
                url: url_b,
                weight: 1,
                http3: false,
                methods: None,
                max_rps: None,
            },
        ],
        reporting: None,
    };
    let state = ProxyState::new(cfg);

    // Open A's circuit with 2 consecutive live failures.
    state.monitor.record("a", false, 100.0, false);
    state.monitor.record("a", false, 100.0, false);

    // Verify A is excluded: FailoverOrdered with A first, but A's circuit is open.
    // Traffic must go to B.
    {
        let (status, body) = proxy_request(build_router(state.clone()), GET_SLOT).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["result"].as_u64(),
            Some(99),
            "B should handle when A is circuit-open"
        );
    }

    // Simulate A recovering: cooldown=0, so Open→HalfOpen on first record(),
    // then HalfOpen→Closed on the next success.
    state.monitor.record("a", true, 10.0, false); // Open → HalfOpen
    state.monitor.record("a", true, 10.0, false); // HalfOpen → Closed

    // Now A's circuit is Closed again. FailoverOrdered picks A first.
    {
        let (status, body) = proxy_request(build_router(state), GET_SLOT).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["result"].as_u64(),
            Some(42),
            "A should handle traffic after recovering"
        );
    }
}

/// A non-retryable RPC error code (-32700) from provider A is returned immediately.
/// Provider B must not be called even when max_retries allows a retry.
#[tokio::test]
async fn non_retryable_rpc_error_not_retried() {
    let mock_a = Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32700, "parse error") }),
    );
    let mock_b = Router::new().route("/", post(|| async { slot_response(999) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        1, // max_retries=1 — would retry if the error were retryable
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, GET_SLOT).await;

    // Proxy returns A's error as-is (no retry on non-retryable code).
    // If B had been called for the proxy request the result would be slot=999, not an error.
    assert_eq!(status, StatusCode::OK);
    assert!(body["error"].is_object(), "expected error body: {body}");
    assert_eq!(body["error"]["code"].as_i64(), Some(-32700));
}

/// Chaos: provider A returns retryable errors on every request, accumulating
/// consecutive failures until the circuit opens. After circuit_open_failures,
/// A is excluded and subsequent requests go directly to B without touching A.
#[tokio::test]
async fn chaos_consecutive_errors_open_circuit_and_reroute() {
    let a_count = Arc::new(AtomicUsize::new(0));
    let ac = a_count.clone();
    let mock_a = Router::new().route(
        "/",
        post(move || {
            let c = ac.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                rpc_error_response(-32603, "internal error") // retryable
            }
        }),
    );
    let mock_b = Router::new().route("/", post(|| async { slot_response(42) }));

    let (url_a, _abort_a) = start_mock(mock_a).await;
    let (url_b, _abort_b) = start_mock(mock_b).await;

    // circuit_open_failures=3, max_retries=1: each request tries A (fails), then B (succeeds).
    // After 3 consecutive failures on A, A's circuit opens.
    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        1,
        3,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    // Requests 1–3: A fails (retryable), B handles it. A accumulates 3 failures → circuit opens.
    for i in 0..3 {
        let (status, body) = proxy_request(router.clone(), GET_SLOT).await;
        assert_eq!(status, StatusCode::OK, "request {i} should succeed via B");
        assert_eq!(body["result"].as_u64(), Some(42));
    }

    // Request 4: A's circuit is now open → A excluded, B handles directly.
    let calls_before = a_count.load(Ordering::Relaxed);
    let (status, body) = proxy_request(router.clone(), GET_SLOT).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(42));
    assert_eq!(
        a_count.load(Ordering::Relaxed),
        calls_before,
        "A should be excluded (circuit open) — no new calls after circuit opens"
    );
}

/// A panic inside a handler must return HTTP 500, not silently drop the connection.
///
/// CatchPanicLayer wraps build_router; this verifies it is wired up correctly.
/// Real-world trigger: a stray panic or unwrap() on the request path (parking_lot
/// locks can't poison, so a poisoned-lock cascade is no longer possible).
#[tokio::test]
async fn handler_panic_returns_500_not_connection_drop() {
    use tower_http::catch_panic::CatchPanicLayer;

    // Minimal router: one route that always panics, wrapped with CatchPanicLayer.
    let router = Router::new()
        .route(
            "/panic",
            get(|| async {
                panic!("intentional test panic");
                #[allow(unreachable_code)]
                ()
            }),
        )
        .layer(CatchPanicLayer::new());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/panic")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "panic should yield 500, not a connection drop"
    );
}

// ── Contract tests ──────────────────────────────────────────────────────────────
//
// These pin documented routing/forwarding behaviour. They began life #[ignore]d
// and failing (documenting bugs the code didn't yet honour); the Milestone 1
// fixes — broadcast early-return and upstream non-2xx passthrough — landed, so
// they now run as regression gates.

/// CONTRACT: broadcast must respond on the FIRST provider success, not after the
/// slowest provider finishes. With
/// `broadcast_writes`, `sendTransaction` fans out to every provider; a fast
/// provider and a degraded one (5 s) race, and the client must see the fast
/// success well under 1 s. The slow provider's outcome is still recorded by the
/// detached straggler drain, but off the client's critical path.
#[tokio::test]
async fn broadcast_returns_on_first_success_before_slow_provider() {
    let (url_fast, _abort_fast) =
        start_mock(Router::new().route("/", post(|| async { slot_response(111) }))).await;
    let (url_slow, _abort_slow) = start_slow_mock(222, Duration::from_secs(5)).await;

    let mut cfg = test_config(
        &[("fast", &url_fast), ("slow", &url_slow)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let started = std::time::Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(1), proxy_request(router, SEND_TX)).await;
    let elapsed = started.elapsed();

    let (status, body) =
        outcome.expect("broadcast did not respond within 1s — it waited for the slow provider");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["result"].as_u64(),
        Some(111),
        "should return the fast provider's response"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "broadcast took {elapsed:?}; expected first-success well under 1s"
    );
}

/// CONTRACT: ParallelRace is documented to return the fastest success. It routes
/// reads through the broadcast path, so a read (`getSlot`) raced across a fast and
/// a 5 s-degraded provider must return the fast result well under 1 s — the same
/// early-return property, on the read path.
#[tokio::test]
async fn parallel_race_returns_first_success_not_slowest() {
    let (url_fast, _abort_fast) =
        start_mock(Router::new().route("/", post(|| async { slot_response(111) }))).await;
    let (url_slow, _abort_slow) = start_slow_mock(222, Duration::from_secs(5)).await;

    let cfg = test_config(
        &[("fast", &url_fast), ("slow", &url_slow)],
        RoutingStrategy::ParallelRace,
        0,
        5,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let started = std::time::Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(1), proxy_request(router, GET_SLOT)).await;
    let elapsed = started.elapsed();

    let (status, body) = outcome
        .expect("ParallelRace did not respond within 1s — it waited for the slowest provider");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"].as_u64(), Some(111));
    assert!(
        elapsed < Duration::from_secs(1),
        "ParallelRace took {elapsed:?}; expected first-success well under 1s"
    );
}

/// CONTRACT: on the broadcast path a leg is a success only on HTTP 2xx *and* a body
/// with no JSON-RPC error. A `sendTransaction` that fails preflight returns HTTP 200
/// with error -32002 and is the *fastest* leg precisely because it did no work — if
/// that error won the race the client would see a failure, re-sign, and
/// double-execute while a slower provider actually landed the tx. The slower real
/// success must win.
#[tokio::test]
async fn broadcast_slower_success_beats_faster_preflight_error() {
    // "err" answers immediately with a JSON-RPC preflight error (HTTP 200 + -32002).
    // "ok" is slower but returns a real success.
    let (url_err, _abort_err) = start_mock(Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32002, "blockhash not found") }),
    ))
    .await;
    let (url_ok, _abort_ok) = start_slow_mock(12345, Duration::from_millis(150)).await;

    let mut cfg = test_config(
        &[("err", &url_err), ("ok", &url_ok)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, SEND_TX).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body["error"].is_null(),
        "must not return the fast preflight error — that causes re-sign/double-execute: {body}"
    );
    assert_eq!(
        body["result"].as_u64(),
        Some(12345),
        "must return the landing provider's success"
    );
}

/// CONTRACT: when every broadcast leg fails with a JSON-RPC error body, the proxy
/// must surface a real upstream error (the -32002 preflight failure, which rides on
/// HTTP 200) rather than masking it as a generic -32603 "all providers failed".
#[tokio::test]
async fn broadcast_all_error_bodies_passthrough_not_generic() {
    let (url_a, _abort_a) = start_mock(Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32002, "blockhash not found") }),
    ))
    .await;
    let (url_b, _abort_b) = start_mock(Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32002, "blockhash not found") }),
    ))
    .await;

    let mut cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, SEND_TX).await;

    // Preflight errors are HTTP 200 with a JSON-RPC error body.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["error"]["code"].as_i64(),
        Some(-32002),
        "should surface the upstream preflight error, not synthetic -32603: {body}"
    );
}

/// CONTRACT: when no broadcast leg succeeds, the returned error prefers a
/// deterministic client error (-32602 invalid params, identical across providers)
/// over a transient one (-32005 node behind), regardless of arrival order.
#[tokio::test]
async fn broadcast_prefers_deterministic_error_over_transient() {
    let (url_transient, _abort_t) = start_mock(Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32005, "node is behind") }),
    ))
    .await;
    let (url_deterministic, _abort_d) = start_mock(Router::new().route(
        "/",
        post(|| async { rpc_error_response(-32602, "invalid params") }),
    ))
    .await;

    let mut cfg = test_config(
        &[
            ("transient", &url_transient),
            ("deterministic", &url_deterministic),
        ],
        RoutingStrategy::BestScore,
        0,
        5,
    );
    cfg.routing.broadcast_writes = true;
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, body) = proxy_request(router, SEND_TX).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["error"]["code"].as_i64(),
        Some(-32602),
        "should prefer the deterministic invalid-params error: {body}"
    );
}

/// CONTRACT: a non-retryable upstream 4xx (e.g. a revoked key → 401) must be
/// surfaced to the client as that status, not masked as HTTP 200. Chosen
/// semantics: pass the provider status code through to the client.
///
/// Single provider, no retry — there is nowhere to fail over, so the client must
/// simply see the 401 (and the provider is scored as an error, not healthy).
#[tokio::test]
async fn upstream_4xx_status_passed_through_not_200() {
    let mock = Router::new().route(
        "/",
        post(|| async { (StatusCode::UNAUTHORIZED, r#"{"error":"invalid api key"}"#) }),
    );
    let (url, _abort) = start_mock(mock).await;

    let cfg = test_config(&[("a", &url)], RoutingStrategy::FailoverOrdered, 0, 5);
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    let (status, _body) = proxy_request(router, GET_SLOT).await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "upstream 401 must pass through, not be masked as 200"
    );
}

const GET_BALANCE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["addr"]}"#;

/// A mock that answers `getSlot` health probes normally (so probe failures don't
/// muddy the health signal) but returns `status` for the real `getBalance` call.
async fn start_status_on_get_balance(status: StatusCode) -> (String, tokio::task::AbortHandle) {
    let router = Router::new().route(
        "/",
        post(move |body: axum::body::Bytes| async move {
            if std::str::from_utf8(&body).is_ok_and(|s| s.contains("getBalance")) {
                (
                    status,
                    r#"{"jsonrpc":"2.0","error":{"code":0,"message":"x"},"id":1}"#,
                )
                    .into_response()
            } else {
                slot_response(1).into_response()
            }
        }),
    );
    start_mock(router).await
}

/// CONTRACT: a client-attributable 4xx (400) is passed through to the caller but
/// must NOT count against provider health — a buggy client loop that keeps
/// tripping a 400 cannot open the circuit and paint the provider as down.
#[tokio::test]
async fn client_4xx_passed_through_without_touching_health() {
    let (url, _abort) = start_status_on_get_balance(StatusCode::BAD_REQUEST).await;

    // circuit_open_failures = 2: two *counted* failures would open the circuit.
    let cfg = test_config(&[("a", &url)], RoutingStrategy::FailoverOrdered, 0, 2);
    let state = ProxyState::new(cfg);
    let router = build_router(state.clone());

    for _ in 0..5 {
        let (status, _body) = proxy_request(router.clone(), GET_BALANCE).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "400 must pass through");
    }

    let snap = state.monitor.snapshots().pop().unwrap();
    assert_eq!(
        snap.circuit,
        rpc_plane_core::health::CircuitState::Closed,
        "client 4xx must not open the circuit"
    );
    assert_eq!(snap.error_rate, 0.0, "client 4xx must not raise error rate");
}

/// CONTRACT: an auth 4xx (401) is a genuine provider problem (revoked key) — it
/// must count against health and, with enough failures, open the circuit.
#[tokio::test]
async fn auth_4xx_counts_against_health_and_opens_circuit() {
    let (url, _abort) = start_status_on_get_balance(StatusCode::UNAUTHORIZED).await;

    let cfg = test_config(&[("a", &url)], RoutingStrategy::FailoverOrdered, 0, 2);
    let state = ProxyState::new(cfg);
    let router = build_router(state.clone());

    for _ in 0..3 {
        let (status, _body) = proxy_request(router.clone(), GET_BALANCE).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "401 must pass through");
    }

    let snap = state.monitor.snapshots().pop().unwrap();
    assert_eq!(
        snap.circuit,
        rpc_plane_core::health::CircuitState::Open,
        "auth 4xx must open the circuit"
    );
}

/// CONTRACT (429 handling): a rate-limited provider fails over *and* is demoted
/// so traffic sheds to peers, but its circuit stays CLOSED — a 429 is a load
/// signal, not a fault. Provider A 429s every real call (its probes still 200);
/// B serves. Even with `circuit_open_failures = 2`, A must never open.
#[tokio::test]
async fn rate_limited_provider_demoted_but_circuit_stays_closed() {
    let (url_a, _abort_a) = start_status_on_get_balance(StatusCode::TOO_MANY_REQUESTS).await;
    let mock_b = Router::new().route("/", post(|| async { slot_response(7) }));
    let (url_b, _abort_b) = start_mock(mock_b).await;

    // max_retries=1 so A→B failover happens; circuit_open_failures=2 would open A
    // fast if 429s were counted as failures.
    let cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        1,
        2,
    );
    let state = ProxyState::new(cfg);
    let router = build_router(state.clone());

    for _ in 0..8 {
        let (status, body) = proxy_request(router.clone(), GET_BALANCE).await;
        assert_eq!(status, StatusCode::OK, "B must serve after A is throttled");
        assert_eq!(body["result"].as_u64(), Some(7));
    }

    let snaps = state.monitor.snapshots();
    let a = snaps.iter().find(|s| s.name.as_ref() == "a").unwrap();
    let b = snaps.iter().find(|s| s.name.as_ref() == "b").unwrap();
    assert_eq!(
        a.circuit,
        rpc_plane_core::health::CircuitState::Closed,
        "429 must not open the circuit"
    );
    assert!(a.is_available(), "throttled provider stays eligible");
    assert!(
        a.score < b.score,
        "throttled provider must be demoted (a={}, b={})",
        a.score,
        b.score
    );
}

// ── Per-provider max_rps rate limiting ──────────────────────────────────────────

/// A mock answering every POST with `slot`, counting only the POSTs whose body
/// contains `needle`. Reads use `getBalance` as the needle so the background
/// `getSlot` health probe (which never touches the rate bucket) isn't counted.
async fn start_counting_mock(
    slot: u64,
    needle: &'static str,
) -> (String, Arc<AtomicUsize>, tokio::task::AbortHandle) {
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let router = Router::new().route(
        "/",
        post(move |body: axum::body::Bytes| {
            let c = c.clone();
            async move {
                if std::str::from_utf8(&body).is_ok_and(|s| s.contains(needle)) {
                    c.fetch_add(1, Ordering::Relaxed);
                }
                slot_response(slot)
            }
        }),
    );
    let (url, abort) = start_mock(router).await;
    (url, count, abort)
}

/// A provider at its `max_rps` cap is treated as unavailable: once its one-token
/// bucket drains, the next read sheds to an uncapped peer rather than hammering
/// the capped provider. FailoverOrdered keeps A preferred while it has capacity.
#[tokio::test]
async fn max_rps_sheds_request_to_uncapped_peer() {
    let (url_a, count_a, _aa) = start_counting_mock(1, "getBalance").await;
    let (url_b, count_b, _ab) = start_counting_mock(2, "getBalance").await;

    let mut cfg = test_config(
        &[("a", &url_a), ("b", &url_b)],
        RoutingStrategy::FailoverOrdered,
        2, // allow a retry to the peer
        5,
    );
    cfg.providers[0].max_rps = Some(1);
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    // Request 1: A has its starting token → served by A.
    let (s1, _) = proxy_request(router.clone(), GET_BALANCE).await;
    assert_eq!(s1, StatusCode::OK);
    // Request 2: A's bucket is empty → A is unavailable, the read sheds to B.
    let (s2, _) = proxy_request(router.clone(), GET_BALANCE).await;
    assert_eq!(s2, StatusCode::OK);

    assert_eq!(
        count_a.load(Ordering::Relaxed),
        1,
        "A serves exactly one read before its cap"
    );
    assert_eq!(
        count_b.load(Ordering::Relaxed),
        1,
        "the second read sheds to the uncapped peer"
    );
}

/// With only one provider, its `max_rps` cap must never hard-fail a request:
/// there is no peer to shed to, so the degraded path forwards it anyway rather
/// than returning 502. The cap is a load-shedding hint, not a hard throttle.
#[tokio::test]
async fn max_rps_lone_provider_still_serves_when_capped() {
    let (url_a, count_a, _aa) = start_counting_mock(9, "getBalance").await;

    let mut cfg = test_config(&[("a", &url_a)], RoutingStrategy::FailoverOrdered, 2, 5);
    cfg.providers[0].max_rps = Some(1);
    let state = ProxyState::new(cfg);
    let router = build_router(state);

    // Two reads back to back: the second finds an empty bucket but, with no peer,
    // the router degrades and serves it anyway.
    let (s1, _) = proxy_request(router.clone(), GET_BALANCE).await;
    let (s2, b2) = proxy_request(router.clone(), GET_BALANCE).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(
        s2,
        StatusCode::OK,
        "a lone capped provider must still serve, not 502"
    );
    assert_eq!(b2["result"].as_u64(), Some(9));
    assert_eq!(
        count_a.load(Ordering::Relaxed),
        2,
        "both reads reach the lone provider"
    );
}
