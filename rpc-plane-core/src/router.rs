use crate::config::RoutingStrategy;
use crate::health::HealthSnapshot;
use rand::Rng;

// ── Method classification ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Clone)]
pub enum MethodClass {
    /// Standard read — route to single best provider with fallback.
    Read,
    /// Mutating — broadcast to all healthy providers simultaneously.
    Write,
}

pub fn classify(method: &str) -> MethodClass {
    match method {
        "sendTransaction" | "simulateTransaction" => MethodClass::Write,
        _ => MethodClass::Read,
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
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("error")?.get("code")?.as_i64()
}

// ── Routing decision ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RouteDecision {
    /// Provider names to try, in priority order.
    /// For broadcasts all are contacted concurrently.
    pub providers: Vec<String>,
    pub broadcast: bool,
}

/// Select providers given current health snapshots.
///
/// `config_weights`: `(name, weight)` pairs in config order — used for
/// `WeightedRandom` and as the stable tiebreak for `FailoverOrdered`.
pub fn route(
    method: &str,
    snapshots: &[HealthSnapshot],
    strategy: &RoutingStrategy,
    config_weights: &[(String, u32)],
) -> RouteDecision {
    let class = classify(method);

    let mut available: Vec<&HealthSnapshot> =
        snapshots.iter().filter(|s| s.is_available()).collect();

    if available.is_empty() {
        // All circuits open: try every provider anyway — degraded but not dead.
        return RouteDecision {
            providers: snapshots.iter().map(|s| s.name.clone()).collect(),
            broadcast: class == MethodClass::Write,
        };
    }

    if class == MethodClass::Write {
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
                    let w = config_weights
                        .iter()
                        .find(|(n, _)| n == &s.name)
                        .map_or(1, |(_, w)| *w) as f64;
                    (w * s.score).max(1e-9) // keep non-zero so circuit-open is never picked
                })
                .collect();

            let total: f64 = weights.iter().sum();
            let roll = rand::thread_rng().gen::<f64>() * total;
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
            let mut providers = vec![primary_snap.name.clone()];
            providers.extend(available.iter().map(|s| s.name.clone()));

            RouteDecision {
                providers,
                broadcast: false,
            }
        }

        RoutingStrategy::FailoverOrdered => {
            // Preserve config order, skipping circuit-open.
            let mut ordered: Vec<&HealthSnapshot> = config_weights
                .iter()
                .filter_map(|(name, _)| available.iter().find(|s| s.name == *name).copied())
                .collect();
            // Append any providers not in config_weights at the end.
            for s in &available {
                if !config_weights.iter().any(|(n, _)| n == &s.name) {
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
    use crate::health::CircuitState;

    fn snap(name: &str, score: f64) -> HealthSnapshot {
        HealthSnapshot {
            name: name.to_string(),
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
            name: name.to_string(),
            score: 0.0,
            slot_height: None,
            slot_drift: 0,
            is_drifting: false,
            latency_ms: 0.0,
            error_rate: 1.0,
            circuit: CircuitState::Open,
        }
    }

    fn weights(names: &[&str]) -> Vec<(String, u32)> {
        names.iter().map(|n| (n.to_string(), 1u32)).collect()
    }

    #[test]
    fn best_score_picks_highest() {
        let snaps = vec![snap("b", 0.6), snap("a", 0.9), snap("c", 0.3)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &weights(&["a", "b", "c"]),
        );
        assert_eq!(d.providers[0], "a");
        assert_eq!(d.providers[1], "b");
        assert!(!d.broadcast);
    }

    #[test]
    fn write_method_broadcasts() {
        let snaps = vec![snap("a", 0.9), snap("b", 0.7)];
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &weights(&["a", "b"]),
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
            &weights(&["a", "b", "c"]),
        );
        assert!(!d.providers.contains(&"b".to_string()));
    }

    #[test]
    fn open_circuit_excluded_from_writes() {
        let snaps = vec![snap("a", 0.9), snap_open("b"), snap("c", 0.5)];
        let d = route(
            "sendTransaction",
            &snaps,
            &RoutingStrategy::BestScore,
            &weights(&["a", "b", "c"]),
        );
        assert!(!d.providers.contains(&"b".to_string()));
        assert_eq!(d.providers.len(), 2);
    }

    #[test]
    fn all_open_returns_all_providers_as_fallback() {
        let snaps = vec![snap_open("a"), snap_open("b")];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &weights(&["a", "b"]),
        );
        assert_eq!(d.providers.len(), 2);
    }

    #[test]
    fn failover_ordered_respects_config_order() {
        let snaps = vec![snap("a", 0.3), snap("b", 0.9), snap("c", 0.6)];
        let cw = vec![
            ("c".to_string(), 1u32),
            ("a".to_string(), 1),
            ("b".to_string(), 1),
        ];
        let d = route("getSlot", &snaps, &RoutingStrategy::FailoverOrdered, &cw);
        assert_eq!(d.providers, vec!["c", "a", "b"]);
    }

    #[test]
    fn classify_write_methods() {
        assert_eq!(classify("sendTransaction"), MethodClass::Write);
        assert_eq!(classify("simulateTransaction"), MethodClass::Write);
    }

    #[test]
    fn classify_read_methods() {
        assert_eq!(classify("getSlot"), MethodClass::Read);
        assert_eq!(classify("getAccountInfo"), MethodClass::Read);
        assert_eq!(classify("getBalance"), MethodClass::Read);
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
            assert!(!is_retryable_http(code), "expected {code} to be non-retryable");
        }
    }

    #[test]
    fn retryable_rpc_error_codes() {
        for code in [-32003i64, -32005, -32603] {
            assert!(is_retryable_rpc_code(code), "expected {code} to be retryable");
        }
    }

    #[test]
    fn non_retryable_rpc_error_codes() {
        for code in [-32700i64, -32600, -32601, -32602, 0, 1] {
            assert!(!is_retryable_rpc_code(code), "expected {code} to be non-retryable");
        }
    }

    #[test]
    fn parallel_race_sets_broadcast_true() {
        let snaps = vec![snap("a", 0.9), snap("b", 0.7)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::ParallelRace,
            &weights(&["a", "b"]),
        );
        assert!(d.broadcast);
        assert_eq!(d.providers, vec!["a", "b"]);
    }

    #[test]
    fn single_available_provider_used_when_other_open() {
        let snaps = vec![snap_open("a"), snap("b", 0.8)];
        let d = route(
            "getSlot",
            &snaps,
            &RoutingStrategy::BestScore,
            &weights(&["a", "b"]),
        );
        assert_eq!(d.providers, vec!["b"]);
        assert!(!d.broadcast);
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
