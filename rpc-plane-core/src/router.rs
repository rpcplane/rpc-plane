use crate::config::{ProviderConfig, RoutingStrategy};
use crate::health::HealthSnapshot;
use rand::Rng;
use std::sync::Arc;

// ── Method classification ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Clone)]
pub enum MethodClass {
    /// Standard read — route to single best provider with fallback.
    Read,
    /// Mutating — broadcast to all healthy providers simultaneously.
    Write,
}

/// Classify a method as a read or write given the configured write-method list.
///
/// The list is operator-controlled (`routing.write_methods`); it defaults to
/// `sendTransaction` + `simulateTransaction` so simulations route on the fast
/// write path, but it can be overridden to add or remove methods.
pub fn classify(method: &str, write_methods: &[String]) -> MethodClass {
    if write_methods.iter().any(|m| m == method) {
        MethodClass::Write
    } else {
        MethodClass::Read
    }
}

// ── Retryability ──────────────────────────────────────────────────────────────

/// HTTP-level errors worth retrying on the next provider.
pub fn is_retryable_http(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// JSON-RPC error codes that may succeed on a different provider.
pub fn is_retryable_rpc_code(code: i64) -> bool {
    // -32003 = transaction simulation failed / node not ready
    // -32005 = node is behind
    // -32603 = internal error (transient)
    matches!(code, -32003 | -32005 | -32603)
}

pub fn extract_rpc_error_code(body: &[u8]) -> Option<i64> {
    #[derive(serde::Deserialize)]
    struct RpcErr {
        code: Option<i64>,
    }
    #[derive(serde::Deserialize)]
    struct Wrapper {
        error: Option<RpcErr>,
    }
    serde_json::from_slice::<Wrapper>(body).ok()?.error?.code
}

// ── Routing decision ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RouteDecision {
    /// Provider names to try, in priority order.
    /// For broadcasts all are contacted concurrently.
    pub providers: Vec<Arc<str>>,
    pub broadcast: bool,
}

/// Select providers given current health snapshots.
///
/// `providers`: config entries in config order — `weight` drives
/// `WeightedRandom` and the order is the stable tiebreak for `FailoverOrdered`.
/// Borrowed directly from the live config so nothing is allocated per request.
pub fn route(
    method: &str,
    snapshots: &[HealthSnapshot],
    strategy: &RoutingStrategy,
    providers: &[ProviderConfig],
    broadcast_writes: bool,
    write_methods: &[String],
) -> RouteDecision {
    let class = classify(method, write_methods);

    // A provider serves this request only if its optional `methods` allowlist
    // permits the method (unrestricted providers permit everything).
    let supports = |name: &Arc<str>| {
        providers
            .iter()
            .find(|p| p.name.as_str() == name.as_ref())
            .is_none_or(|p| p.supports(method))
    };

    let mut available: Vec<&HealthSnapshot> = snapshots
        .iter()
        .filter(|s| s.is_available() && supports(&s.name))
        .collect();

    if available.is_empty() {
        // No healthy provider serves this method. Fall back to every provider
        // that *supports* it, even circuit-open — degraded but not dead. If none
        // support it (misconfiguration), the list is empty and the proxy errors.
        return RouteDecision {
            providers: snapshots
                .iter()
                .filter(|s| supports(&s.name))
                .map(|s| s.name.clone())
                .collect(),
            broadcast: broadcast_writes && class == MethodClass::Write,
        };
    }

    if class == MethodClass::Write && broadcast_writes {
        return RouteDecision {
            providers: available.iter().map(|s| s.name.clone()).collect(),
            broadcast: true,
        };
    }

    // Reads: apply strategy.
    match strategy {
        RoutingStrategy::BestScore => {
            available.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            RouteDecision {
                providers: available.iter().map(|s| s.name.clone()).collect(),
                broadcast: false,
            }
        }

        RoutingStrategy::WeightedRandom => {
            let weights: Vec<f64> = available
                .iter()
                .map(|s| {
                    let w = providers
                        .iter()
                        .find(|p| p.name.as_str() == s.name.as_ref())
                        .map_or(1, |p| p.weight) as f64;
                    (w * s.score).max(1e-9) // keep non-zero so circuit-open is never picked
                })
                .collect();

            let total: f64 = weights.iter().sum();
            let roll = rand::rng().random::<f64>() * total;
            let mut cum = 0.0;
            let mut primary = 0;
            for (i, w) in weights.iter().enumerate() {
                cum += w;
                if roll <= cum {
                    primary = i;
                    break;
                }
            }

            // Primary first, rest sorted by score as fallbacks.
            let primary_snap = available.remove(primary);
            available.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let mut result = vec![primary_snap.name.clone()];
            result.extend(available.iter().map(|s| s.name.clone()));

            RouteDecision {
                providers: result,
                broadcast: false,
            }
        }

        RoutingStrategy::FailoverOrdered => {
            // Preserve config order, skipping circuit-open.
            let mut ordered: Vec<&HealthSnapshot> = providers
                .iter()
                .filter_map(|p| {
                    available
                        .iter()
                        .find(|s| s.name.as_ref() == p.name.as_str())
                        .copied()
                })
                .collect();
            // Append any providers not in the config list at the end.
            for s in &available {
                if !providers.iter().any(|p| p.name.as_str() == s.name.as_ref()) {
                    ordered.push(s);
                }
            }
            RouteDecision {
                providers: ordered.iter().map(|s| s.name.clone()).collect(),
                broadcast: false,
            }
        }

        RoutingStrategy::ParallelRace => {
            // Best N providers in parallel; proxy returns the first success.
            available.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            RouteDecision {
                providers: available.iter().map(|s| s.name.clone()).collect(),
                broadcast: true, // use broadcast path but return on first success
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;
    use crate::health::CircuitState;

    fn snap(name: &str, score: f64) -> HealthSnapshot {
        HealthSnapshot {
            name: name.into(),
            score,
            slot_height: Some(100),
            slot_drift: 0,
            is_drifting: false,
            latency_ms: 50.0,
            error_rate: 0.0,
            circuit: CircuitState::Closed,
        }
    }

    fn snap_open(name: &str) -> HealthSnapshot {
        HealthSnapshot {
            name: name.into(),
            score: 0.0,
            slot_height: None,
            slot_drift: 0,
            is_drifting: false,
            latency_ms: 0.0,
            error_rate: 1.0,
            circuit: CircuitState::Open,
        }
    }

    fn providers(names: &[&str]) -> Vec<ProviderConfig> {
        names
            .iter()
            .map(|n| ProviderConfig {
                name: n.to_string(),
                url: "http://x".to_string(),
                weight: 1,
                http3: false,
                methods: None,
            })
            .collect()
    }

    fn names(d: &RouteDecision) -> Vec<&str> {
        d.providers.iter().map(|p| p.as_ref()).collect()
    }

    /// Like `providers`, but restricts `name` to a single method (submit-only).
    fn providers_scoped(names: &[&str], scoped: &str, method: &str) -> Vec<ProviderConfig> {
        let mut p = providers(names);
        for cfg in &mut p {
            if cfg.name == scoped {
                cfg.methods = Some(vec![method.to_string()]);
            }
        }
        p
    }

    fn writes() -> Vec<String> {
        vec!["sendTransaction".into(), "simulateTransaction".into()]
    }

    #[test]
    fn best_score_picks_highest() {
        let snaps = vec![snap("b", 0.6), snap("a", 0.9), snap("c", 0.3)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b", "c"]),
            false,
            &writes(),
        );
        assert_eq!(&*d.providers[0], "a");
        assert_eq!(&*d.providers[1], "b");
        assert!(!d.broadcast);
    }

    #[test]
    fn write_routes_sequentially_by_default() {
        let snaps = vec![snap("a", 0.9), snap("b", 0.7)];
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b"]),
            false,
            &writes(),
        );
        assert!(!d.broadcast);
        assert_eq!(&*d.providers[0], "a");
    }

    #[test]
    fn write_broadcasts_when_enabled() {
        let snaps = vec![snap("a", 0.9), snap("b", 0.7)];
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b"]),
            true,
            &writes(),
        );
        assert!(d.broadcast);
        assert_eq!(d.providers.len(), 2);
    }

    #[test]
    fn open_circuit_excluded_from_reads() {
        let snaps = vec![snap("a", 0.9), snap_open("b"), snap("c", 0.5)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b", "c"]),
            false,
            &writes(),
        );
        assert!(!d.providers.iter().any(|p| p.as_ref() == "b"));
    }

    #[test]
    fn open_circuit_excluded_from_writes() {
        let snaps = vec![snap("a", 0.9), snap_open("b"), snap("c", 0.5)];
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b", "c"]),
            true,
            &writes(),
        );
        assert!(!d.providers.iter().any(|p| p.as_ref() == "b"));
        assert_eq!(d.providers.len(), 2);
    }

    #[test]
    fn all_open_returns_all_providers_as_fallback() {
        let snaps = vec![snap_open("a"), snap_open("b")];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b"]),
            false,
            &writes(),
        );
        assert_eq!(d.providers.len(), 2);
    }

    #[test]
    fn failover_ordered_respects_config_order() {
        let snaps = vec![snap("a", 0.3), snap("b", 0.9), snap("c", 0.6)];
        let cfg = providers(&["c", "a", "b"]);
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::FailoverOrdered,
            &cfg,
            false,
            &writes(),
        );
        assert_eq!(names(&d), ["c", "a", "b"]);
    }

    #[test]
    fn classify_write_methods() {
        let w = writes();
        assert_eq!(classify("sendTransaction", &w), MethodClass::Write);
        assert_eq!(classify("simulateTransaction", &w), MethodClass::Write);
    }

    #[test]
    fn classify_read_methods() {
        let w = writes();
        assert_eq!(classify("getSlot", &w), MethodClass::Read);
        assert_eq!(classify("getAccountInfo", &w), MethodClass::Read);
        assert_eq!(classify("getBalance", &w), MethodClass::Read);
    }

    #[test]
    fn classify_honors_custom_write_list() {
        // Drop simulateTransaction so simulations route like reads.
        let w = vec!["sendTransaction".to_string()];
        assert_eq!(classify("simulateTransaction", &w), MethodClass::Read);
        assert_eq!(classify("sendTransaction", &w), MethodClass::Write);
    }

    #[test]
    fn retryable_http_status_codes() {
        for code in [429u16, 500, 502, 503, 504] {
            assert!(is_retryable_http(code), "expected {code} to be retryable");
        }
    }

    #[test]
    fn non_retryable_http_status_codes() {
        for code in [200u16, 400, 401, 403, 404, 422] {
            assert!(
                !is_retryable_http(code),
                "expected {code} to be non-retryable"
            );
        }
    }

    #[test]
    fn retryable_rpc_error_codes() {
        for code in [-32003i64, -32005, -32603] {
            assert!(
                is_retryable_rpc_code(code),
                "expected {code} to be retryable"
            );
        }
    }

    #[test]
    fn non_retryable_rpc_error_codes() {
        for code in [-32700i64, -32600, -32601, -32602, 0, 1] {
            assert!(
                !is_retryable_rpc_code(code),
                "expected {code} to be non-retryable"
            );
        }
    }

    #[test]
    fn parallel_race_sets_broadcast_true() {
        let snaps = vec![snap("a", 0.9), snap("b", 0.7)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::ParallelRace,
            &providers(&["a", "b"]),
            false,
            &writes(),
        );
        assert!(d.broadcast);
        assert_eq!(names(&d), ["a", "b"]);
    }

    #[test]
    fn single_available_provider_used_when_other_open() {
        let snaps = vec![snap_open("a"), snap("b", 0.8)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &providers(&["a", "b"]),
            false,
            &writes(),
        );
        assert_eq!(names(&d), ["b"]);
        assert!(!d.broadcast);
    }

    #[test]
    fn submit_only_provider_excluded_from_reads() {
        // "send" only supports sendTransaction; a read must not route to it.
        let snaps = vec![snap("read", 0.9), snap("send", 0.95)];
        let cfg = providers_scoped(&["read", "send"], "send", "sendTransaction");
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &cfg,
            true,
            &writes(),
        );
        assert_eq!(names(&d), ["read"]);
        assert!(!d.broadcast);
    }

    #[test]
    fn submit_only_provider_included_in_write_broadcast() {
        let snaps = vec![snap("read", 0.9), snap("send", 0.95)];
        let cfg = providers_scoped(&["read", "send"], "send", "sendTransaction");
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &cfg,
            true,
            &writes(),
        );
        assert!(d.broadcast);
        let mut got = names(&d);
        got.sort_unstable();
        assert_eq!(got, ["read", "send"]);
    }

    #[test]
    fn unsupported_method_yields_no_providers() {
        // Only provider is submit-only; a read it can't serve routes nowhere.
        let snaps = vec![snap("send", 0.95)];
        let cfg = providers_scoped(&["send"], "send", "sendTransaction");
        let d = route(
            "getAccountInfo",
            &snaps,
            &RoutingStrategy::BestScore,
            &cfg,
            true,
            &writes(),
        );
        assert!(d.providers.is_empty());
    }

    #[test]
    fn submit_only_provider_used_when_others_open() {
        // All circuits open: fall back to providers that support the method only.
        let snaps = vec![snap_open("read"), snap_open("send")];
        let cfg = providers_scoped(&["read", "send"], "send", "sendTransaction");
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &cfg,
            true,
            &writes(),
        );
        // getSlot can't go to the submit-only provider even in the degraded path.
        assert_eq!(names(&d), ["read"]);
    }

    #[test]
    fn extract_rpc_error_code_works() {
        let body =
            br#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":1}"#;
        assert_eq!(extract_rpc_error_code(body), Some(-32603));
    }

    #[test]
    fn extract_rpc_error_code_missing() {
        let body = br#"{"jsonrpc":"2.0","result":100,"id":1}"#;
        assert_eq!(extract_rpc_error_code(body), None);
    }
}
