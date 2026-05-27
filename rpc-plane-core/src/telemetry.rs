use crate::config::ReportingConfig;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, warn};

// ── Event types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TelemetryEvent {
    /// Pre-aggregated request stats for one (method, provider, status) bucket
    /// covering a single flush window. Replaces per-request events to keep
    /// Analytics Engine write volume independent of RPS.
    RequestAggregate {
        window_start_ms: u64,
        window_end_ms: u64,
        method: String,
        provider: String,
        /// "ok" | "error"
        status: String,
        count: u64,
        latency_p50_ms: f64,
        latency_p95_ms: f64,
        latency_p99_ms: f64,
        latency_avg_ms: f64,
    },
    ProviderHealth {
        provider: String,
        score: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        slot_height: Option<u64>,
        slot_drift: u64,
        circuit_state: String,
    },
    Failover {
        from_provider: String,
        to_provider: String,
        reason: String,
    },
    Divergence {
        method: String,
        /// (provider_name, slot_height) pairs
        providers: Vec<(String, u64)>,
        resolution: String,
    },
}

// ── Reporter trait ────────────────────────────────────────────────────────────

pub trait Reporter: Send + Sync {
    /// Emit a raw event (Failover, Divergence, ProviderHealth). Must never block.
    fn emit(&self, event: TelemetryEvent);

    /// Record a single request outcome into the aggregate accumulator.
    /// Default implementation is a no-op (used by NoopReporter).
    fn record_request(&self, _method: &str, _provider: &str, _status: &str, _latency_ms: f64) {}

    /// Signal the reporter to flush any buffered events. Fire-and-forget.
    fn flush(&self);
}

// ── NoopReporter ──────────────────────────────────────────────────────────────

/// Default reporter. Zero-cost — all calls compile away.
pub struct NoopReporter;

impl Reporter for NoopReporter {
    fn emit(&self, _event: TelemetryEvent) {}
    fn flush(&self) {}
}

// ── Aggregator ────────────────────────────────────────────────────────────────

/// Accumulates per-request latency samples into (method, provider, status) buckets.
/// Drained at each flush interval to produce `RequestAggregate` events.
struct Aggregator {
    state: Mutex<AggState>,
}

struct AggState {
    // key: (method, provider, status)
    buckets: HashMap<(String, String, String), Vec<f64>>,
    window_start_ms: u64,
}

impl Aggregator {
    fn new() -> Self {
        Self {
            state: Mutex::new(AggState {
                buckets: HashMap::new(),
                window_start_ms: now_ms(),
            }),
        }
    }

    fn record(&self, method: &str, provider: &str, status: &str, latency_ms: f64) {
        let mut s = self.state.lock().unwrap();
        s.buckets
            .entry((method.to_string(), provider.to_string(), status.to_string()))
            .or_default()
            .push(latency_ms);
    }

    /// Drain all buckets into `RequestAggregate` events and reset the window.
    fn drain(&self) -> Vec<TelemetryEvent> {
        let now = now_ms();
        let mut s = self.state.lock().unwrap();
        let window_start = s.window_start_ms;

        let events = s
            .buckets
            .drain()
            .filter(|(_, v)| !v.is_empty())
            .map(|((method, provider, status), mut latencies)| {
                let sum: f64 = latencies.iter().sum();
                let count = latencies.len() as u64;
                let latency_avg_ms = sum / count as f64;
                latencies.sort_unstable_by(|a, b| a.total_cmp(b));
                TelemetryEvent::RequestAggregate {
                    window_start_ms: window_start,
                    window_end_ms: now,
                    method,
                    provider,
                    status,
                    count,
                    latency_p50_ms: percentile(&latencies, 50.0),
                    latency_p95_ms: percentile(&latencies, 95.0),
                    latency_p99_ms: percentile(&latencies, 99.0),
                    latency_avg_ms,
                }
            })
            .collect();

        s.window_start_ms = now;
        events
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    // nearest-rank: rank = ceil(p/100 * n), 1-indexed → 0-indexed = rank - 1
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

// ── RemoteReporter ────────────────────────────────────────────────────────────

/// Buffers raw events in a bounded channel and aggregates request data in-memory.
/// At each flush interval: drains the aggregator (producing `RequestAggregate` events)
/// and sends all buffered events to the remote endpoint in batches.
pub struct RemoteReporter {
    tx: mpsc::Sender<TelemetryEvent>,
    flush_signal: Arc<Notify>,
    aggregator: Arc<Aggregator>,
}

impl RemoteReporter {
    /// Spawns the background flush task. Must be called inside a Tokio runtime.
    pub fn new(config: ReportingConfig, client: Arc<Client>) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let flush_signal = Arc::new(Notify::new());
        let aggregator = Arc::new(Aggregator::new());

        tokio::spawn(flush_task(
            rx,
            flush_signal.clone(),
            aggregator.clone(),
            config,
            client,
        ));

        Self {
            tx,
            flush_signal,
            aggregator,
        }
    }
}

impl Reporter for RemoteReporter {
    fn emit(&self, event: TelemetryEvent) {
        // try_send never blocks; silently drop if the buffer is full.
        let _ = self.tx.try_send(event);
    }

    fn record_request(&self, method: &str, provider: &str, status: &str, latency_ms: f64) {
        self.aggregator.record(method, provider, status, latency_ms);
    }

    fn flush(&self) {
        self.flush_signal.notify_one();
    }
}

// ── Background flush task ─────────────────────────────────────────────────────

async fn flush_task(
    mut rx: mpsc::Receiver<TelemetryEvent>,
    flush_signal: Arc<Notify>,
    aggregator: Arc<Aggregator>,
    config: ReportingConfig,
    client: Arc<Client>,
) {
    let mut batch: Vec<TelemetryEvent> = Vec::with_capacity(config.batch_size);
    let mut interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(e) => {
                        batch.push(e);
                        if batch.len() >= config.batch_size {
                            send_batch(&client, &config, &mut batch).await;
                        }
                    }
                    None => break, // all senders dropped — exit
                }
            }
            _ = interval.tick() => {
                batch.extend(aggregator.drain());
                while let Ok(e) = rx.try_recv() {
                    batch.push(e);
                }
                if !batch.is_empty() {
                    send_batch(&client, &config, &mut batch).await;
                }
            }
            _ = flush_signal.notified() => {
                batch.extend(aggregator.drain());
                while let Ok(e) = rx.try_recv() {
                    batch.push(e);
                }
                if !batch.is_empty() {
                    send_batch(&client, &config, &mut batch).await;
                }
            }
        }
    }

    // Final drain after the channel is closed (graceful shutdown).
    batch.extend(aggregator.drain());
    while let Ok(e) = rx.try_recv() {
        batch.push(e);
    }
    if !batch.is_empty() {
        send_batch(&client, &config, &mut batch).await;
    }
}

async fn send_batch(client: &Client, config: &ReportingConfig, batch: &mut Vec<TelemetryEvent>) {
    let events = std::mem::take(batch);
    let count = events.len();

    let mut req = client
        .post(&config.endpoint)
        .json(&serde_json::json!({ "events": events }));

    if let Some(key) = &config.api_key {
        req = req.header("x-api-key", key.as_str());
    }

    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!(count, "telemetry batch flushed");
        }
        Ok(resp) => {
            warn!(status = %resp.status(), count, "telemetry flush non-2xx");
        }
        Err(e) => {
            warn!(error = %e, count, "telemetry flush failed");
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    type RequestLog = Arc<Mutex<Vec<(String, String, String, f64)>>>;

    struct RecordingReporter {
        events: Arc<Mutex<Vec<TelemetryEvent>>>,
        requests: RequestLog,
        flush_count: Arc<AtomicUsize>,
    }

    impl RecordingReporter {
        fn new() -> (
            Self,
            Arc<Mutex<Vec<TelemetryEvent>>>,
            RequestLog,
            Arc<AtomicUsize>,
        ) {
            let events = Arc::new(Mutex::new(Vec::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let flush_count = Arc::new(AtomicUsize::new(0));
            let reporter = Self {
                events: events.clone(),
                requests: requests.clone(),
                flush_count: flush_count.clone(),
            };
            (reporter, events, requests, flush_count)
        }
    }

    impl Reporter for RecordingReporter {
        fn emit(&self, event: TelemetryEvent) {
            self.events.lock().unwrap().push(event);
        }
        fn record_request(&self, method: &str, provider: &str, status: &str, latency_ms: f64) {
            self.requests.lock().unwrap().push((
                method.to_string(),
                provider.to_string(),
                status.to_string(),
                latency_ms,
            ));
        }
        fn flush(&self) {
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn noop_reporter_is_zero_cost() {
        let r = NoopReporter;
        r.emit(TelemetryEvent::Failover {
            from_provider: "a".into(),
            to_provider: "b".into(),
            reason: "test".into(),
        });
        r.record_request("getSlot", "helius", "ok", 1.0);
        r.flush();
        // no panic = pass
    }

    #[test]
    fn recording_reporter_captures_events() {
        let (reporter, events, requests, flush_count) = RecordingReporter::new();
        reporter.emit(TelemetryEvent::Failover {
            from_provider: "a".into(),
            to_provider: "b".into(),
            reason: "test".into(),
        });
        reporter.record_request("getSlot", "helius", "ok", 12.5);
        reporter.flush();

        assert_eq!(events.lock().unwrap().len(), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert_eq!(flush_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn telemetry_event_serializes_tagged() {
        let ev = TelemetryEvent::Failover {
            from_provider: "a".into(),
            to_provider: "b".into(),
            reason: "provider_error".into(),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "failover");
        assert_eq!(json["from_provider"], "a");
    }

    #[test]
    fn request_aggregate_serializes_correctly() {
        let ev = TelemetryEvent::RequestAggregate {
            window_start_ms: 1000,
            window_end_ms: 61000,
            method: "getSlot".into(),
            provider: "helius".into(),
            status: "ok".into(),
            count: 5000,
            latency_p50_ms: 10.0,
            latency_p95_ms: 45.0,
            latency_p99_ms: 90.0,
            latency_avg_ms: 15.0,
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "request_aggregate");
        assert_eq!(json["count"], 5000);
    }

    #[test]
    fn aggregator_drain_produces_correct_percentiles() {
        let agg = Aggregator::new();
        // 100 samples: 1.0, 2.0, ..., 100.0
        for i in 1..=100 {
            agg.record("getSlot", "helius", "ok", i as f64);
        }
        let events = agg.drain();
        assert_eq!(events.len(), 1);
        if let TelemetryEvent::RequestAggregate {
            count,
            latency_p50_ms,
            latency_p95_ms,
            latency_p99_ms,
            latency_avg_ms,
            ..
        } = &events[0]
        {
            assert_eq!(*count, 100);
            assert_eq!(*latency_p50_ms, 50.0);
            assert_eq!(*latency_p95_ms, 95.0);
            assert_eq!(*latency_p99_ms, 99.0);
            // avg of 1..=100 = 50.5
            assert!((latency_avg_ms - 50.5).abs() < 0.001);
        } else {
            panic!("expected RequestAggregate");
        }
    }

    #[test]
    fn aggregator_drain_resets_window() {
        let agg = Aggregator::new();
        agg.record("getSlot", "helius", "ok", 10.0);
        let first = agg.drain();
        assert_eq!(first.len(), 1);
        // second drain with no new data should be empty
        let second = agg.drain();
        assert!(second.is_empty());
    }

    #[test]
    fn aggregator_buckets_by_key() {
        let agg = Aggregator::new();
        agg.record("getSlot", "helius", "ok", 10.0);
        agg.record("getSlot", "quicknode", "ok", 20.0);
        agg.record("getBalance", "helius", "ok", 5.0);
        agg.record("getSlot", "helius", "error", 100.0);
        let events = agg.drain();
        assert_eq!(events.len(), 4);
    }
}
