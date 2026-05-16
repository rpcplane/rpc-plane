/// Minimal Solana JSON-RPC server for local load testing.
///
/// Responds to any POST request with a fixed JSON-RPC success response.
/// Does NOT require a real Solana node.
///
/// Usage:
///   cargo run -p dummy-rpc -- --port 9901 --delay-ms 5
///
/// Then point rpc-plane at it:
///   [[providers]]
///   name = "dummy"
///   url  = "http://127.0.0.1:9901"
///
/// Why --delay-ms matters for load testing:
///   Without artificial delay the server responds in <0.1ms, which makes proxy
///   overhead look enormous in relative terms and under-stresses concurrency
///   handling. A realistic RPC node takes 3–15ms. Setting --delay-ms=5 (default)
///   means the baseline and proxy measurements reflect real-world queue depth,
///   and proxy overhead = (proxy p99) - (baseline p99) is a fair comparison.
use axum::{body::Bytes, response::IntoResponse, routing::post, Router};
use clap::Parser;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Parser)]
#[command(
    name = "dummy-rpc",
    about = "Minimal Solana JSON-RPC server for load testing"
)]
struct Cli {
    #[arg(long, default_value = "9901", help = "Port to listen on")]
    port: u16,

    #[arg(
        long,
        default_value = "100000000",
        help = "Slot number returned for getSlot"
    )]
    slot: u64,

    /// Simulated provider latency in milliseconds.
    ///
    /// Mimics real RPC node response time so proxy overhead measurements are
    /// meaningful. A real Solana RPC node typically responds in 3–15ms.
    /// Set to 0 to measure raw proxy throughput capacity instead.
    #[arg(long, default_value = "5", help = "Simulated latency per request (ms)")]
    delay_ms: u64,

    #[arg(
        long,
        default_value = "4096",
        help = "TCP listen backlog (OS caps at net.core.somaxconn)"
    )]
    backlog: u32,
}

#[derive(Clone)]
struct State {
    slot: u64,
    delay: Duration,
    request_count: Arc<AtomicU64>,
    started_at: Instant,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state = State {
        slot: cli.slot,
        delay: Duration::from_millis(cli.delay_ms),
        request_count: Arc::new(AtomicU64::new(0)),
        started_at: Instant::now(),
    };

    let request_count = state.request_count.clone();
    let started_at = state.started_at;

    // Log request rate every 5 seconds.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await; // skip immediate first tick
        loop {
            interval.tick().await;
            let total = request_count.load(Ordering::Relaxed);
            let elapsed = started_at.elapsed().as_secs_f64();
            info!(
                total_requests = total,
                rps = format!("{:.0}", total as f64 / elapsed.max(1.0)),
                "stats"
            );
        }
    });

    let router = Router::new().route("/", post(handle)).with_state(state);

    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", cli.port)
        .parse()
        .expect("invalid address");
    let socket = tokio::net::TcpSocket::new_v4().expect("TcpSocket::new_v4");
    socket.set_reuseaddr(true).expect("SO_REUSEADDR");
    socket
        .bind(addr)
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));
    let listener = socket
        .listen(cli.backlog)
        .unwrap_or_else(|e| panic!("failed to listen on {addr}: {e}"));

    info!(
        addr = %addr,
        slot = cli.slot,
        delay_ms = cli.delay_ms,
        backlog = cli.backlog,
        "dummy-rpc listening"
    );

    axum::serve(listener, router).await.unwrap();
}

async fn handle(
    axum::extract::State(state): axum::extract::State<State>,
    body: Bytes,
) -> impl IntoResponse {
    state.request_count.fetch_add(1, Ordering::Relaxed);

    if !state.delay.is_zero() {
        tokio::time::sleep(state.delay).await;
    }

    // Echo the request id back so clients don't reject the response.
    let id = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("id").cloned())
        .unwrap_or(json!(1));

    axum::Json(json!({
        "jsonrpc": "2.0",
        "result": state.slot,
        "id": id,
    }))
}
