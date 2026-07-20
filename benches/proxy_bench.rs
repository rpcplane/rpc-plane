#![allow(dead_code, unused_imports)]

#[path = "../src/config.rs"]
mod config;
#[path = "../src/health.rs"]
mod health;
#[path = "../src/historical.rs"]
mod historical;
#[path = "../src/metrics.rs"]
mod metrics;
#[path = "../src/proxy.rs"]
mod proxy;
#[path = "../src/router.rs"]
mod router;
#[path = "../src/telemetry.rs"]
mod telemetry;
#[path = "../src/tx.rs"]
mod tx;

use crate::{
    config::{HistoricalAnalyticsConfig, ProviderConfig, RoutingStrategy},
    health::{CircuitState, CommitmentHealth, HealthSnapshot, ObservedSlotTips},
    historical::{
        parse_response, AnalyzerCore, Fingerprint, HistoricalAnalytics, HistoricalCommitment,
        HistoricalRequestClass,
    },
    proxy::extract_method,
    router::{extract_rpc_error_code, route},
    telemetry::{NoopReporter, Reporter},
};
use axum::body::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ── Fixtures ──────────────────────────────────────────────────────────────────

fn snap(name: &str, score: f64) -> HealthSnapshot {
    HealthSnapshot {
        name: Arc::from(name),
        score,
        slot_height: Some(100),
        slot_drift: 0,
        is_drifting: false,
        latency_ms: 10.0,
        error_rate: 0.0,
        circuit: CircuitState::Closed,
        processed: CommitmentHealth::default(),
        confirmed: CommitmentHealth::default(),
        finalized: CommitmentHealth::default(),
        rate_limited: false,
    }
}

fn prov(name: &str) -> ProviderConfig {
    ProviderConfig {
        name: name.to_string(),
        url: "http://localhost:9090".to_string(),
        weight: 1,
        http3: false,
        methods: None,
        max_rps: None,
    }
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

fn bench_extract_method(c: &mut Criterion) {
    let single = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let batch = br#"[{"jsonrpc":"2.0","id":1,"method":"getSlot"},{"jsonrpc":"2.0","id":2,"method":"getBalance","params":["9bT...", {"commitment":"confirmed"}]}]"#;

    let mut g = c.benchmark_group("extract_method");
    g.bench_function("single", |b| b.iter(|| extract_method(black_box(single))));
    g.bench_function("batch_2", |b| b.iter(|| extract_method(black_box(batch))));
    g.finish();
}

fn bench_route(c: &mut Criterion) {
    let snapshots = vec![
        snap("helius", 0.9),
        snap("quicknode", 0.7),
        snap("alchemy", 0.5),
    ];
    let providers = vec![prov("helius"), prov("quicknode"), prov("alchemy")];
    let writes = vec![
        "sendTransaction".to_string(),
        "simulateTransaction".to_string(),
    ];

    let mut g = c.benchmark_group("route");
    g.bench_function("best_score/3_providers", |b| {
        b.iter(|| {
            route(
                black_box("getSlot"),
                black_box(&snapshots),
                black_box(&RoutingStrategy::BestScore),
                black_box(&providers),
                false,
                black_box(&writes),
            )
        })
    });
    g.bench_function("weighted_random/3_providers", |b| {
        b.iter(|| {
            route(
                black_box("getSlot"),
                black_box(&snapshots),
                black_box(&RoutingStrategy::WeightedRandom),
                black_box(&providers),
                false,
                black_box(&writes),
            )
        })
    });
    g.bench_function("broadcast_write/3_providers", |b| {
        b.iter(|| {
            route(
                black_box("sendTransaction"),
                black_box(&snapshots),
                black_box(&RoutingStrategy::BestScore),
                black_box(&providers),
                true,
                black_box(&writes),
            )
        })
    });
    g.finish();
}

fn bench_extract_rpc_error_code(c: &mut Criterion) {
    let ok = br#"{"jsonrpc":"2.0","result":{"context":{"slot":100},"value":1000},"id":1}"#;
    let err = br#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":1}"#;

    let mut g = c.benchmark_group("extract_rpc_error_code");
    g.bench_function("success_response", |b| {
        b.iter(|| extract_rpc_error_code(black_box(ok)))
    });
    g.bench_function("error_response", |b| {
        b.iter(|| extract_rpc_error_code(black_box(err)))
    });
    g.finish();
}

// ── Historical analytics ──────────────────────────────────────────────────────
//
// The analyzer must cost effectively nothing when the `[historical_analytics]`
// block is absent, and must never parse a body on the request task when it is
// present. `completion_path` covers both halves of the acceptance criteria:
// `disabled` is the `Option` check the proxy performs per request, `enqueue` is
// the full enabled hot path (bounds check, permit acquire, two `Bytes` clones,
// `try_send`). Parsing, normalization, and state transitions are benched
// separately because they run on the worker, not on the request task.

const GET_TRANSACTION_REQUEST: &[u8] = br#"{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":["5wHu1qwD4kLwYqBBiVLLxpDRTLNTvBhnvbSMPCkFYVjNSFPdKtxJmzZuLZ3g7VeMYS8Fw7yEbKAvbLHDrDF3xrf7",{"encoding":"json","commitment":"confirmed","maxSupportedTransactionVersion":0}]}"#;
const FOUND_RESPONSE: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"slot":312894001,"blockTime":1731000000,"meta":{"err":null,"fee":5000,"preBalances":[100000,200000],"postBalances":[95000,200000]},"transaction":{"signatures":["5wHu1qwD4kLwYqBBiVLLxpDRTLNTvBhnvbSMPCkFYVjNSFPdKtxJmzZuLZ3g7VeMYS8Fw7yEbKAvbLHDrDF3xrf7"],"message":{"accountKeys":["11111111111111111111111111111111"]}}}}"#;
const NULL_RESPONSE: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":null}"#;

fn bench_historical_completion_path(c: &mut Criterion) {
    let request = Bytes::from_static(GET_TRANSACTION_REQUEST);
    let response = Bytes::from_static(FOUND_RESPONSE);
    let tips = ObservedSlotTips::default();

    // Multi-threaded so the worker actually drains the queue; a saturated queue
    // would measure the drop path instead of the enqueue path.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

    let disabled: Option<HistoricalAnalytics> = None;
    let reporter: Arc<dyn Reporter> = Arc::new(NoopReporter);
    let enabled = Some(HistoricalAnalytics::new(
        HistoricalAnalyticsConfig::default(),
        reporter,
    ));

    let mut g = c.benchmark_group("historical/completion_path");
    g.bench_function("disabled", |b| {
        b.iter(|| {
            if let Some(analytics) = black_box(&disabled) {
                analytics.finish(
                    HistoricalRequestClass::Single,
                    &request,
                    &response,
                    200,
                    tips,
                );
            }
        })
    });
    g.bench_function("enqueue", |b| {
        b.iter(|| {
            if let Some(analytics) = black_box(&enabled) {
                analytics.finish(
                    black_box(HistoricalRequestClass::Single),
                    black_box(&request),
                    black_box(&response),
                    200,
                    black_box(tips),
                );
            }
        })
    });
    g.bench_function("ineligible_method", |b| {
        b.iter(|| {
            if let Some(analytics) = black_box(&enabled) {
                analytics.finish(
                    black_box(HistoricalRequestClass::None),
                    black_box(&request),
                    black_box(&response),
                    200,
                    black_box(tips),
                );
            }
        })
    });
    g.finish();
}

fn bench_historical_parse_response(c: &mut Criterion) {
    let mut g = c.benchmark_group("historical/parse_response");
    g.bench_function("found", |b| {
        b.iter(|| parse_response(black_box(200), black_box(FOUND_RESPONSE)))
    });
    g.bench_function("null", |b| {
        b.iter(|| parse_response(black_box(200), black_box(NULL_RESPONSE)))
    });
    g.finish();
}

fn bench_historical_fingerprint(c: &mut Criterion) {
    let core = AnalyzerCore::new(250_000, 172_800);
    let request: serde_json::Value = serde_json::from_slice(GET_TRANSACTION_REQUEST).unwrap();

    let mut g = c.benchmark_group("historical/fingerprint");
    // Worker-side cost of turning a request body into a keyed fingerprint: the
    // JSON parse the request task deliberately avoids, plus normalize + hash.
    g.bench_function("parse_and_hash", |b| {
        b.iter(|| {
            let value: serde_json::Value =
                serde_json::from_slice(black_box(GET_TRANSACTION_REQUEST)).unwrap();
            core.fingerprint(black_box(&value))
        })
    });
    g.bench_function("hash_only", |b| {
        b.iter(|| core.fingerprint(black_box(&request)))
    });
    g.finish();
}

fn bench_historical_transition(c: &mut Criterion) {
    let commitment = HistoricalCommitment::Confirmed;
    let start = Instant::now();

    let mut g = c.benchmark_group("historical/transition");
    // At capacity every insert also evicts, which is the steady state for a
    // long-running proxy whose working set exceeds `state_capacity`.
    g.bench_function("at_capacity_with_eviction", |b| {
        let capacity = 10_000;
        let mut core = AnalyzerCore::new(capacity, 172_800);
        for i in 0..capacity as u64 {
            core.transition(
                Fingerprint(i, i),
                commitment,
                false,
                None,
                start + Duration::from_millis(i),
            );
        }
        let mut i = capacity as u64;
        b.iter(|| {
            i += 1;
            core.transition(
                black_box(Fingerprint(i, i)),
                commitment,
                false,
                None,
                start + Duration::from_millis(i),
            );
        })
    });
    // The found -> found repeat is the strong positive-cache signal, so its
    // per-observation cost is the one that matters most at high reuse rates.
    g.bench_function("found_repeat", |b| {
        let mut core = AnalyzerCore::new(10_000, 172_800);
        let fingerprint = Fingerprint(7, 7);
        core.transition(fingerprint, commitment, true, Some(1), start);
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            core.transition(
                black_box(fingerprint),
                commitment,
                true,
                Some(1),
                start + Duration::from_millis(i),
            );
        })
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_extract_method,
    bench_route,
    bench_extract_rpc_error_code,
    bench_historical_completion_path,
    bench_historical_parse_response,
    bench_historical_fingerprint,
    bench_historical_transition
);
criterion_main!(benches);
