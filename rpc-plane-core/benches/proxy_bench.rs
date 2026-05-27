use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rpc_plane_core::{
    config::{ProviderConfig, RoutingStrategy},
    health::{CircuitState, HealthSnapshot},
    proxy::extract_method,
    router::{extract_rpc_error_code, route},
};
use std::sync::Arc;

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
    }
}

fn prov(name: &str) -> ProviderConfig {
    ProviderConfig {
        name: name.to_string(),
        url: "http://localhost:9090".to_string(),
        weight: 1,
        http3: false,
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

    let mut g = c.benchmark_group("route");
    g.bench_function("best_score/3_providers", |b| {
        b.iter(|| {
            route(
                black_box("getSlot"),
                black_box(&snapshots),
                black_box(&RoutingStrategy::BestScore),
                black_box(&providers),
                false,
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

criterion_group!(
    benches,
    bench_extract_method,
    bench_route,
    bench_extract_rpc_error_code
);
criterion_main!(benches);
