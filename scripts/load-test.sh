#!/usr/bin/env bash
# Load test: measure proxy overhead vs direct provider latency.
#
# Requires:
#   oha  — cargo install oha
#
# Usage:
#   ./scripts/load-test.sh [requests] [concurrency] [delay_ms]
#
# Examples:
#   ./scripts/load-test.sh                    # 50K req, 200 concurrent, 5ms delay
#   ./scripts/load-test.sh 100000 500 10      # 100K req, 500 concurrent, 10ms delay
#   ./scripts/load-test.sh 200000 200 0       # raw throughput (no simulated latency)
#
# How to read the results:
#   Baseline  — oha directly to dummy-rpc (no proxy). This is the "provider" latency.
#   Via proxy — oha through rpc-plane → dummy-rpc. This is proxy + provider latency.
#   Overhead  — (proxy p99) - (baseline p99). Target: < 1 ms.
#
# Why --delay-ms matters:
#   Without it dummy-rpc responds in <0.1ms, making proxy overhead look enormous
#   in relative terms and not stressing concurrency the way real providers do.
#   Default 5ms mimics a fast-but-real Solana RPC node.

set -euo pipefail

REQUESTS="${1:-50000}"
CONCURRENCY="${2:-200}"
DELAY_MS="${3:-5}"
MOCK_PORT=9901
PROXY_PORT=9400
BODY='{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Dependency checks ──────────────────────────────────────────────────────────

if ! command -v oha &>/dev/null; then
    echo "oha not found. Install with:"
    echo "  cargo install oha"
    exit 1
fi

# ── Build ──────────────────────────────────────────────────────────────────────

echo "==> Building release binaries..."
cargo build --release -p dummy-rpc -p rpc-plane --quiet

DUMMY_RPC="$WORKSPACE/target/release/dummy-rpc"
PROXY="$WORKSPACE/target/release/rpc-plane"

# ── Config for the proxy ───────────────────────────────────────────────────────

TMPCONFIG=$(mktemp /tmp/rpcplane-loadtest-XXXXXX.toml)
trap 'rm -f "$TMPCONFIG"; kill $(jobs -p) 2>/dev/null || true' EXIT

cat >"$TMPCONFIG" <<TOML
[server]
listen         = "127.0.0.1:${PROXY_PORT}"
metrics_listen = "127.0.0.1:9401"

[health]
interval_ms = 2000   # slow probes so they don't skew latency measurements

[[providers]]
name = "dummy"
url  = "http://127.0.0.1:${MOCK_PORT}"
TOML

# ── Start dummy-rpc ────────────────────────────────────────────────────────────

echo "==> Starting dummy-rpc on :${MOCK_PORT} (delay=${DELAY_MS}ms)..."
RUST_LOG=warn "$DUMMY_RPC" --port "$MOCK_PORT" --delay-ms "$DELAY_MS" &>/tmp/dummy-rpc.log &
sleep 0.3

# ── Start proxy ────────────────────────────────────────────────────────────────

echo "==> Starting rpc-plane on :${PROXY_PORT}..."
RUST_LOG=warn "$PROXY" run -c "$TMPCONFIG" &>/tmp/rpcplane-loadtest.log &
sleep 0.5

# ── Warm up both paths ─────────────────────────────────────────────────────────

OHA_ARGS=(-m POST -H "Content-Type: application/json" -d "$BODY")

echo "==> Warming up..."
oha -n 500 -c 20 "${OHA_ARGS[@]}" "http://127.0.0.1:${MOCK_PORT}"   &>/dev/null || true
oha -n 500 -c 20 "${OHA_ARGS[@]}" "http://127.0.0.1:${PROXY_PORT}"  &>/dev/null || true
sleep 0.2

# ── Baseline: direct to dummy-rpc ─────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  BASELINE — direct to dummy-rpc (simulated provider, delay=${DELAY_MS}ms)"
echo "══════════════════════════════════════════════════════════"
oha -n "$REQUESTS" -c "$CONCURRENCY" "${OHA_ARGS[@]}" "http://127.0.0.1:${MOCK_PORT}"

# ── Via proxy ─────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  VIA PROXY — rpc-plane → dummy-rpc"
echo "══════════════════════════════════════════════════════════"
oha -n "$REQUESTS" -c "$CONCURRENCY" "${OHA_ARGS[@]}" "http://127.0.0.1:${PROXY_PORT}"

echo ""
echo "==> Done."
echo "    Overhead = (proxy p99) - (baseline p99). Target: < 1 ms"
