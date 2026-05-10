# RPC Plane

Solana RPC proxy with intelligent multi-provider routing, automatic failover, and slot-aware health scoring. A single binary that sits between your app and your RPC providers.

```
Your App → http://localhost:9400 → [Helius / QuickNode / Triton]
```

## Quick start

```bash
# 1. Download the binary
curl -sSf https://rpcplane.dev/install.sh | sh

# 2. Generate a config
rpc-plane init

# 3. Add your provider URLs and run
rpc-plane run
```

Your app replaces its provider URL with `http://localhost:9400`. No other changes needed.

## What it does

- **Routes reads** to the healthiest provider based on latency, error rate, and slot freshness
- **Broadcasts writes** (`sendTransaction`) to all healthy providers simultaneously — maximizes landing probability
- **Circuit breaker** per provider: opens on failure, probes for recovery, resumes traffic automatically
- **Slot tracker**: tracks slot height across providers, deprioritizes drifting nodes
- **Auto-retry** on transient errors (429, 503, timeout) — tries the next-best provider
- **Hot reload**: edit the config file, changes apply without restart
- **Prometheus metrics** on `:9401/metrics`
- **Zero infrastructure**: single binary, single config file, no databases, no Redis

## Install

**Script (Linux / macOS):**
```bash
curl -sSf https://rpcplane.dev/install.sh | sh
```

**Manual download:** grab the binary for your platform from [GitHub Releases](https://github.com/rpcplane/rpc-plane/releases).

| Platform | Binary |
|----------|--------|
| Linux x86_64 | `rpc-plane-x86_64-unknown-linux-gnu` |
| Linux aarch64 | `rpc-plane-aarch64-unknown-linux-gnu` |
| macOS x86_64 | `rpc-plane-x86_64-apple-darwin` |
| macOS arm64 | `rpc-plane-aarch64-apple-darwin` |

The Linux binaries are built against **glibc 2.35** (Ubuntu 22.04). They run on Ubuntu 22.04+, Debian 12+, Amazon Linux 2023, and Rocky/RHEL 9+. For older systems (RHEL/Rocky 8, Debian 11), use the Docker image or build from source.

Each release includes a `.sha256` checksum and a `.cosign.bundle` Sigstore signature. The install script verifies both automatically when `cosign` is installed; manual verification:

```bash
cosign verify-blob \
  --bundle rpc-plane-x86_64-unknown-linux-gnu.cosign.bundle \
  --certificate-identity-regexp '^https://github\.com/rpcplane/rpc-plane/\.github/workflows/release\.yml@refs/tags/v[0-9].*$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  rpc-plane-x86_64-unknown-linux-gnu
```

**Docker:**
```bash
docker run -v $(pwd)/rpc-plane.toml:/etc/rpc-plane.toml ghcr.io/rpcplane/rpc-plane
```

**From source:**
```bash
cargo install --git https://github.com/rpcplane/rpc-plane rpc-plane
```

## Configuration

Minimal config (`rpc-plane.toml`):

```toml
[[providers]]
name = "helius"
url  = "https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}"

[[providers]]
name = "quicknode"
url  = "https://your-endpoint.quiknode.pro/${QUICKNODE_API_KEY}"
```

Run `rpc-plane init` to generate a full config with all options and their defaults.

See the [configuration reference](https://docs.rpcplane.dev/configuration) for every option.

## CLI

```
rpc-plane run        # start the proxy (default)
rpc-plane check      # validate config and test provider connectivity
rpc-plane status     # show live provider health (proxy must be running)
rpc-plane init       # generate a starter config file
```

```
rpc-plane --help
rpc-plane -c /path/to/config.toml run
```

## Routing strategies

| Strategy | Description |
|----------|-------------|
| `best_score` | Route reads to the highest-scoring provider (default) |
| `weighted_random` | Probabilistic selection by config weight × health score |
| `failover_ordered` | Try providers in config order, skip open circuits |
| `parallel_race` | Send to all healthy providers, return fastest success |

Writes (`sendTransaction`, `simulateTransaction`) always broadcast to all healthy providers regardless of strategy.

## Observability

**Health endpoint:**
```bash
curl http://localhost:9400/health | jq
```

**Provider status:**
```bash
rpc-plane status
#   NAME        SCORE          SLOT   DRIFT     LATENCY  CIRCUIT
#   --------  -------  ------------  ------  ----------  -------
#   helius      0.912   341892471       0      23.4ms     closed
#   quicknode   0.841   341892469       2      31.1ms     closed
#   triton      0.000           —       —           —     open
```

**Prometheus:**
```
http://localhost:9401/metrics
```

Key metrics: `rpc_plane_requests_total`, `rpc_plane_request_duration_seconds`, `rpc_plane_provider_health_score`, `rpc_plane_slot_drift`, `rpc_plane_circuit_breaker_state`, `rpc_plane_failover_total`.

## Example configs

See the [`examples/`](examples/) directory:

- [`helius-quicknode-triton.toml`](examples/helius-quicknode-triton.toml) — standard three-provider setup
- [`single-provider.toml`](examples/single-provider.toml) — single provider with health monitoring
- [`trading-bot.toml`](examples/trading-bot.toml) — write-path optimized for transaction landing

## Architecture

See the [architecture overview](https://docs.rpcplane.dev/architecture) for how routing decisions are made.

## License

[Elastic License 2.0](LICENSE) — source-available; you can use, modify, and self-host. You can't offer it as a hosted/managed service to third parties.
