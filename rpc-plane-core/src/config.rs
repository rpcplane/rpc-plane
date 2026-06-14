use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(rename = "providers", default)]
    pub providers: Vec<ProviderConfig>,
    #[serde(default)]
    pub reporting: Option<ReportingConfig>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {}", path.display()))?;
        let (expanded, unset_vars) = expand_env_vars(&raw);
        for var in &unset_vars {
            tracing::warn!(
                "config references unset environment variable `${{{var}}}`; it expanded \
                 to an empty string — any provider URL using it is now missing that value"
            );
        }
        let config: Config = toml::from_str(&expanded)
            .with_context(|| format!("failed to parse config: {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.providers.is_empty(),
            "config must have at least one [[providers]] entry"
        );
        let mut seen = std::collections::HashSet::new();
        for p in &self.providers {
            anyhow::ensure!(!p.url.is_empty(), "provider '{}' has an empty url", p.name);
            anyhow::ensure!(
                seen.insert(p.name.as_str()),
                "duplicate provider name '{}'; provider names must be unique",
                p.name
            );
            if let Some(methods) = &p.methods {
                anyhow::ensure!(
                    !methods.is_empty(),
                    "provider '{}' has an empty methods list; omit the key to serve all methods",
                    p.name
                );
                anyhow::ensure!(
                    methods.iter().all(|m| !m.trim().is_empty()),
                    "provider '{}' has an empty entry in methods",
                    p.name
                );
            }
        }
        anyhow::ensure!(
            self.routing
                .write_methods
                .iter()
                .all(|m| !m.trim().is_empty()),
            "routing.write_methods must not contain empty entries"
        );
        if let Some(r) = &self.reporting {
            anyhow::ensure!(
                !r.endpoint.is_empty(),
                "reporting.endpoint must not be empty"
            );
            anyhow::ensure!(
                r.flush_interval_ms >= 10_000,
                "reporting.flush_interval_ms must be >= 10000 (10 s); got {}. \
                 Values below 10 s produce excessive telemetry volume.",
                r.flush_interval_ms
            );
        }
        Ok(())
    }
}

/// Expand `${VAR}` / `$VAR` references against the process environment.
///
/// Unset variables expand to an empty string (preserving prior behaviour) but
/// their names are collected and returned, sorted and de-duplicated, so callers
/// can warn about likely typos that would otherwise silently produce a broken
/// URL (e.g. `${HELIUS_API_KY}` → `...?api-key=`). Configs that hardcode the
/// full URL+token reference no variables and so report nothing.
pub fn expand_env_vars(input: &str) -> (String, Vec<String>) {
    expand_env_vars_with(input, |var| std::env::var(var).ok())
}

/// Core of [`expand_env_vars`], parameterised over the variable lookup.
///
/// `lookup` resolves a variable name to its value (`None` = unset). Production
/// passes a closure over the process environment; tests pass an in-memory map so
/// they never mutate the shared process env (which would race under the parallel
/// test runner).
fn expand_env_vars_with(
    input: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> (String, Vec<String>) {
    use std::cell::RefCell;
    let unset: RefCell<std::collections::BTreeSet<String>> = RefCell::new(Default::default());
    let expanded = shellexpand::env_with_context_no_errors(input, |var| {
        Some(lookup(var).unwrap_or_else(|| {
            unset.borrow_mut().insert(var.to_string());
            String::new()
        }))
    })
    .into_owned();
    (expanded, unset.into_inner().into_iter().collect())
}

// ── Server ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,
    /// OS TCP listen backlog for the proxy and metrics sockets.
    /// The kernel clamps this to net.core.somaxconn (raise that too for 1000+ concurrency).
    #[serde(default = "default_listen_backlog")]
    pub listen_backlog: u32,
    /// Max idle connections kept in the outbound pool per provider.
    /// Set to at least your expected peak concurrency to avoid cold TCP handshakes.
    #[serde(default = "default_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,
    /// Number of Tokio worker threads. Defaults to the number of logical CPUs.
    /// Set this to dedicate a fixed core count when sharing a host with other services.
    #[serde(default)]
    pub worker_threads: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            metrics_listen: default_metrics_listen(),
            listen_backlog: default_listen_backlog(),
            pool_max_idle_per_host: default_pool_max_idle_per_host(),
            worker_threads: None,
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:9400".to_string()
}
fn default_metrics_listen() -> String {
    "127.0.0.1:9401".to_string()
}
fn default_listen_backlog() -> u32 {
    4096
}
fn default_pool_max_idle_per_host() -> usize {
    512
}

// ── Health ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct HealthConfig {
    /// How often to probe each provider (ms).
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    /// Sliding window for error rate calculation (s).
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
    /// Slots behind network tip before a provider is considered drifting.
    #[serde(default = "default_slot_drift_threshold")]
    pub slot_drift_threshold: u64,
    /// Consecutive probe failures before the circuit opens.
    #[serde(default = "default_circuit_open_failures")]
    pub circuit_open_failures: u32,
    /// Error-rate threshold (0.0–1.0) that also triggers circuit open.
    #[serde(default = "default_circuit_error_threshold")]
    pub circuit_error_threshold: f64,
    /// Seconds to wait before moving Open → HalfOpen.
    #[serde(default = "default_circuit_cooldown_secs")]
    pub circuit_cooldown_secs: u64,
    // Score weights — must sum to > 0. Automatically normalised.
    #[serde(default = "default_w_latency")]
    pub w_latency: f64,
    #[serde(default = "default_w_error")]
    pub w_error: f64,
    #[serde(default = "default_w_slot")]
    pub w_slot: f64,
    #[serde(default = "default_w_success")]
    pub w_success: f64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_interval_ms(),
            window_secs: default_window_secs(),
            slot_drift_threshold: default_slot_drift_threshold(),
            circuit_open_failures: default_circuit_open_failures(),
            circuit_error_threshold: default_circuit_error_threshold(),
            circuit_cooldown_secs: default_circuit_cooldown_secs(),
            w_latency: default_w_latency(),
            w_error: default_w_error(),
            w_slot: default_w_slot(),
            w_success: default_w_success(),
        }
    }
}

fn default_interval_ms() -> u64 {
    1000
}
fn default_window_secs() -> u64 {
    60
}
fn default_slot_drift_threshold() -> u64 {
    10
}
fn default_circuit_open_failures() -> u32 {
    5
}
fn default_circuit_error_threshold() -> f64 {
    0.5
}
fn default_circuit_cooldown_secs() -> u64 {
    30
}
fn default_w_latency() -> f64 {
    0.4
}
fn default_w_error() -> f64 {
    0.3
}
fn default_w_slot() -> f64 {
    0.2
}
fn default_w_success() -> f64 {
    0.1
}

// ── Routing ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    /// Route every read to the provider with the highest health score.
    #[default]
    BestScore,
    /// Probabilistic selection weighted by config weight × health score.
    WeightedRandom,
    /// Try providers in config order; skip circuit-open ones.
    FailoverOrdered,
    /// Send to N best providers simultaneously; return fastest success.
    ParallelRace,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoutingConfig {
    #[serde(default)]
    pub strategy: RoutingStrategy,
    /// Maximum provider retries on retryable errors (per request).
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    /// Broadcast write methods to all healthy providers simultaneously. Off by
    /// default — writes are routed like reads (sequential failover).
    #[serde(default)]
    pub broadcast_writes: bool,
    /// JSON-RPC methods treated as writes. Defaults to `sendTransaction` and
    /// `simulateTransaction` so simulations route on the fast write path; override
    /// to reclassify (e.g. drop `simulateTransaction` to route it like a read).
    #[serde(default = "default_write_methods")]
    pub write_methods: Vec<String>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            strategy: RoutingStrategy::default(),
            max_retries: default_max_retries(),
            broadcast_writes: false,
            write_methods: default_write_methods(),
        }
    }
}

fn default_max_retries() -> usize {
    2
}

fn default_write_methods() -> Vec<String> {
    // simulateTransaction is read-only (no on-chain mutation) and is not in the
    // fast-landing path, so it routes like a read by default. Add it here to have
    // it broadcast under broadcast_writes.
    vec!["sendTransaction".to_string()]
}

// ── Providers ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub name: String,
    pub url: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Use HTTP/3 (QUIC) for outbound connections to this provider.
    #[serde(default)]
    pub http3: bool,
    /// Restrict this provider to a specific set of JSON-RPC methods. When unset
    /// (the default) the provider serves every method. Set it to scope a
    /// submission-only endpoint (e.g. a transaction-landing service that only
    /// supports `sendTransaction`) so it never receives reads it can't answer —
    /// `methods = ["sendTransaction"]`. Health-probed only if `getSlot` is in the
    /// list; otherwise health is driven by real request outcomes.
    #[serde(default)]
    pub methods: Option<Vec<String>>,
}

impl ProviderConfig {
    /// Whether this provider is allowed to serve `method`. Unrestricted
    /// providers (no `methods` list) serve everything.
    pub fn supports(&self, method: &str) -> bool {
        match &self.methods {
            Some(list) if !list.is_empty() => list.iter().any(|m| m == method),
            _ => true,
        }
    }
}

fn default_weight() -> u32 {
    1
}

// ── Reporting ────────────────────────────────────────────────────────────────

/// Optional `[reporting]` TOML block. Presence activates `RemoteReporter`;
/// absence leaves the binary in Prometheus-only mode (`NoopReporter`).
#[derive(Debug, Deserialize, Clone)]
pub struct ReportingConfig {
    /// HTTP endpoint that accepts `POST { "events": [...] }`.
    pub endpoint: String,
    /// Sent as `x-api-key` header when present.
    pub api_key: Option<String>,
    /// How often to flush buffered events to the endpoint (ms).
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Maximum number of events buffered in the async channel.
    /// Events are silently dropped when the channel is full.
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    /// Maximum events per HTTP POST.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_flush_interval_ms() -> u64 {
    60_000
}
fn default_buffer_size() -> usize {
    1000
}
fn default_batch_size() -> usize {
    100
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_config(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn parses_minimal_config() {
        let f = write_config(
            r#"
[[providers]]
name = "test"
url = "http://localhost:8899"
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        assert_eq!(cfg.providers.len(), 1);
        assert_eq!(cfg.providers[0].name, "test");
        assert_eq!(cfg.server.listen, "127.0.0.1:9400");
        assert_eq!(cfg.health.interval_ms, 1000);
        assert_eq!(cfg.routing.max_retries, 2);
    }

    #[test]
    fn parses_full_config() {
        let f = write_config(
            r#"
[health]
interval_ms = 1000
circuit_open_failures = 3

[routing]
strategy = "weighted_random"
max_retries = 1

[[providers]]
name = "a"
url = "http://localhost:8899"
weight = 2
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        assert_eq!(cfg.health.interval_ms, 1000);
        assert_eq!(cfg.health.circuit_open_failures, 3);
        assert_eq!(cfg.routing.strategy, RoutingStrategy::WeightedRandom);
        assert_eq!(cfg.routing.max_retries, 1);
        assert_eq!(cfg.providers[0].weight, 2);
    }

    #[test]
    fn rejects_empty_providers() {
        let f = write_config("[server]\nlisten = \"127.0.0.1:9400\"\n");
        assert!(Config::load(f.path()).is_err());
    }

    #[test]
    fn rejects_empty_url() {
        let f = write_config(
            r#"
[[providers]]
name = "bad"
url = ""
"#,
        );
        assert!(Config::load(f.path()).is_err());
    }

    #[test]
    fn rejects_duplicate_provider_names() {
        let f = write_config(
            r#"
[[providers]]
name = "helius"
url = "http://localhost:8899"

[[providers]]
name = "helius"
url = "http://localhost:8900"
"#,
        );
        let err = Config::load(f.path()).unwrap_err().to_string();
        assert!(err.contains("duplicate provider name"), "got: {err}");
        assert!(err.contains("helius"), "got: {err}");
    }

    #[test]
    fn default_write_methods_is_send_only() {
        let f = write_config(
            r#"
[[providers]]
name = "p"
url = "http://localhost:8899"
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        assert_eq!(
            cfg.routing.write_methods,
            vec!["sendTransaction".to_string()]
        );
    }

    #[test]
    fn parses_provider_methods_allowlist() {
        let f = write_config(
            r#"
[[providers]]
name = "general"
url = "http://localhost:8899"

[[providers]]
name = "submit"
url = "http://localhost:9000"
methods = ["sendTransaction"]
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        let general = &cfg.providers[0];
        let submit = &cfg.providers[1];
        assert!(general.methods.is_none());
        assert!(general.supports("getSlot"));
        assert!(submit.supports("sendTransaction"));
        assert!(!submit.supports("getSlot"));
    }

    #[test]
    fn rejects_empty_methods_list() {
        let f = write_config(
            r#"
[[providers]]
name = "submit"
url = "http://localhost:9000"
methods = []
"#,
        );
        let err = Config::load(f.path()).unwrap_err().to_string();
        assert!(err.contains("empty methods list"), "got: {err}");
    }

    #[test]
    fn expand_env_vars_reports_unset_references() {
        let (expanded, unset) =
            expand_env_vars("url = \"https://rpc.example/?api-key=${RPC_PLANE_DEFINITELY_UNSET}\"");
        assert_eq!(expanded, "url = \"https://rpc.example/?api-key=\"");
        assert_eq!(unset, vec!["RPC_PLANE_DEFINITELY_UNSET".to_string()]);
    }

    #[test]
    fn expand_env_vars_reports_nothing_when_hardcoded() {
        // A fully hardcoded URL+token references no variables → no warnings.
        let (expanded, unset) =
            expand_env_vars("url = \"https://rpc.example/?api-key=hardcoded-token\"");
        assert_eq!(
            expanded,
            "url = \"https://rpc.example/?api-key=hardcoded-token\""
        );
        assert!(unset.is_empty());
    }

    #[test]
    fn parses_all_routing_strategies() {
        let cases = [
            ("best_score", RoutingStrategy::BestScore),
            ("weighted_random", RoutingStrategy::WeightedRandom),
            ("failover_ordered", RoutingStrategy::FailoverOrdered),
            ("parallel_race", RoutingStrategy::ParallelRace),
        ];
        for (s, expected) in cases {
            let toml = format!(
                "[routing]\nstrategy = \"{s}\"\n[[providers]]\nname=\"p\"\nurl=\"http://x\"\n"
            );
            let f = write_config(&toml);
            let cfg = Config::load(f.path()).unwrap();
            assert_eq!(cfg.routing.strategy, expected, "strategy={s}");
        }
    }

    // The expansion logic is tested against an in-memory lookup rather than the
    // process env: `std::env::set_var` mutates global state shared by every test
    // in the binary, which races under the parallel runner.
    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |var| map.get(var).cloned()
    }

    #[test]
    fn expands_env_vars_braced() {
        let (out, unset) = expand_env_vars_with(
            "url = \"https://rpc.example.com/?key=${TEST_RPC_KEY}\"",
            lookup(&[("TEST_RPC_KEY", "abc123")]),
        );
        assert_eq!(out, "url = \"https://rpc.example.com/?key=abc123\"");
        assert!(unset.is_empty());
    }

    #[test]
    fn parses_reporting_block() {
        let f = write_config(
            r#"
[[providers]]
name = "p"
url = "http://localhost:8899"

[reporting]
endpoint = "http://localhost:3000/api/ingest"
api_key = "rp_live_test"
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        let r = cfg.reporting.expect("reporting should be present");
        assert_eq!(r.endpoint, "http://localhost:3000/api/ingest");
        assert_eq!(r.api_key.as_deref(), Some("rp_live_test"));
        assert_eq!(r.flush_interval_ms, 60_000);
        assert_eq!(r.buffer_size, 1000);
        assert_eq!(r.batch_size, 100);
    }

    #[test]
    fn reporting_absent_is_none() {
        let f = write_config("[[providers]]\nname=\"p\"\nurl=\"http://x\"\n");
        let cfg = Config::load(f.path()).unwrap();
        assert!(cfg.reporting.is_none());
    }

    #[test]
    fn rejects_empty_reporting_endpoint() {
        let f = write_config(
            r#"
[[providers]]
name = "p"
url = "http://localhost:8899"
[reporting]
endpoint = ""
"#,
        );
        assert!(Config::load(f.path()).is_err());
    }

    #[test]
    fn rejects_flush_interval_below_floor() {
        let f = write_config(
            r#"
[[providers]]
name = "p"
url = "http://localhost:8899"
[reporting]
endpoint = "http://localhost:3000/api/ingest"
flush_interval_ms = 1000
"#,
        );
        let err = Config::load(f.path()).unwrap_err();
        assert!(err.to_string().contains("flush_interval_ms"), "{err}");
    }

    #[test]
    fn expands_env_vars_unbraced() {
        let (out, unset) = expand_env_vars_with(
            "url = \"https://rpc.example.com/?key=$TEST_UNBRACED_KEY\"",
            lookup(&[("TEST_UNBRACED_KEY", "xyz789")]),
        );
        assert_eq!(out, "url = \"https://rpc.example.com/?key=xyz789\"");
        assert!(unset.is_empty());
    }

    #[test]
    fn unset_env_var_expands_empty_and_is_reported() {
        let (out, unset) = expand_env_vars_with("url = \"http://x/${MISSING_VAR}\"", lookup(&[]));
        assert_eq!(out, "url = \"http://x/\"");
        assert_eq!(unset, vec!["MISSING_VAR".to_string()]);
    }

    // Proves the full load path applies env expansion. Uses a variable that is
    // never set by any test (so it stays unset under parallel execution); the
    // reference collapses to an empty string, leaving a valid non-empty URL.
    #[test]
    fn load_applies_env_expansion() {
        let f = write_config(
            r#"
[[providers]]
name = "test"
url = "https://rpc.example.com/path${RPC_PLANE_UNSET_TEST_VAR}"
"#,
        );
        let cfg = Config::load(f.path()).unwrap();
        assert_eq!(cfg.providers[0].url, "https://rpc.example.com/path");
    }
}
