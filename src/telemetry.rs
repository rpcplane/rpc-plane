use crate::config::ReportingConfig;
use crate::tx::TransactionInfo;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Notify};
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
    TransactionSubmit {
        window_start_ms: u64,
        window_end_ms: u64,
        provider: String,
        accepted_count: u64,
        rejected_count: u64,
        parsed_count: u64,
        unparsed_count: u64,
        unsupported_count: u64,
        invalid_budget_count: u64,
        batch_unsupported_count: u64,
        decode_queue_drop_count: u64,
        fee_sample_count: u64,
        cu_limit_p50: u64,
        cu_limit_p90: u64,
        cu_limit_p95: u64,
        cu_price_micro_lamports_p50: u64,
        cu_price_micro_lamports_p90: u64,
        cu_price_micro_lamports_p95: u64,
        requested_priority_fee_lamports_p50: u64,
        requested_priority_fee_lamports_p90: u64,
        requested_priority_fee_lamports_p95: u64,
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

    /// Record a single request outcome into the aggregate accumulator. `count`
    /// is the number of JSON-RPC calls the request represents (1 for a single
    /// request, the batch element count otherwise) so aggregate `count` reflects
    /// provider-billed call volume, while latency stats use the one round-trip
    /// sample. Default implementation is a no-op (used by NoopReporter).
    fn record_request(
        &self,
        _method: &str,
        _provider: &str,
        _status: &str,
        _latency_ms: f64,
        _count: u64,
    ) {
    }

    fn record_transaction_submission(&self, _provider: &str, _accepted: bool) {}

    fn record_transaction_decode(
        &self,
        _provider: &str,
        _result: &str,
        _sample: Option<&TransactionInfo>,
    ) {
    }

    /// Record outcome and decode data atomically in one aggregation window.
    fn record_transaction_result(
        &self,
        provider: &str,
        accepted: bool,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        self.record_transaction_submission(provider, accepted);
        self.record_transaction_decode(provider, result, sample);
    }

    /// Signal the reporter to flush any buffered events. Fire-and-forget.
    fn flush(&self);

    /// Flush and wait for the current remote write attempt to finish. Reporters
    /// without buffering complete immediately.
    fn flush_and_wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.flush();
        Box::pin(async {})
    }
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
    buckets: HashMap<(String, String, String), Bucket>,
    tx_buckets: HashMap<String, TransactionBucket>,
    window_start_ms: u64,
}

/// One aggregation bucket. `count` is provider-billed call volume (batch-weighted
/// and may exceed `latencies.len()`); `latencies` holds one sample per round trip,
/// so a 1000-call batch contributes 1000 to `count` but a single latency sample —
/// keeping percentiles meaningful and the Vec bounded.
#[derive(Default)]
struct Bucket {
    count: u64,
    latencies: Vec<f64>,
}

#[derive(Default)]
struct TransactionBucket {
    accepted_count: u64,
    rejected_count: u64,
    parsed_count: u64,
    unparsed_count: u64,
    unsupported_count: u64,
    invalid_budget_count: u64,
    batch_unsupported_count: u64,
    decode_queue_drop_count: u64,
    cu_limits: Vec<u64>,
    cu_prices: Vec<u64>,
    fees: Vec<u64>,
}

impl Aggregator {
    fn new() -> Self {
        Self {
            state: Mutex::new(AggState {
                buckets: HashMap::new(),
                tx_buckets: HashMap::new(),
                window_start_ms: now_ms(),
            }),
        }
    }

    fn record(&self, method: &str, provider: &str, status: &str, latency_ms: f64, count: u64) {
        let mut s = self.state.lock().unwrap();
        let bucket = s
            .buckets
            .entry((method.to_string(), provider.to_string(), status.to_string()))
            .or_default();
        bucket.count += count;
        bucket.latencies.push(latency_ms);
    }

    fn record_transaction_submission(&self, provider: &str, accepted: bool) {
        let mut s = self.state.lock().unwrap();
        let bucket = s.tx_buckets.entry(provider.to_string()).or_default();
        if accepted {
            bucket.accepted_count += 1;
        } else {
            bucket.rejected_count += 1;
        }
    }

    fn record_transaction_decode(
        &self,
        provider: &str,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        let mut s = self.state.lock().unwrap();
        let bucket = s.tx_buckets.entry(provider.to_string()).or_default();
        Self::record_transaction_decode_in_bucket(bucket, result, sample);
    }

    fn record_transaction_result(
        &self,
        provider: &str,
        accepted: bool,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        let mut s = self.state.lock().unwrap();
        let bucket = s.tx_buckets.entry(provider.to_string()).or_default();
        if accepted {
            bucket.accepted_count += 1;
        } else {
            bucket.rejected_count += 1;
        }
        Self::record_transaction_decode_in_bucket(bucket, result, sample);
    }

    fn record_transaction_decode_in_bucket(
        bucket: &mut TransactionBucket,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        match result {
            "parsed" => bucket.parsed_count += 1,
            "unparsed" => bucket.unparsed_count += 1,
            "unsupported" => bucket.unsupported_count += 1,
            "invalid_budget" => bucket.invalid_budget_count += 1,
            "batch_unsupported" => bucket.batch_unsupported_count += 1,
            "queue_dropped" => bucket.decode_queue_drop_count += 1,
            _ => return,
        }
        if let Some(info) = sample {
            bucket.cu_limits.push(info.cu_limit);
            bucket
                .cu_prices
                .push(info.cu_price_micro_lamports.unwrap_or(0));
            bucket
                .fees
                .push(info.requested_priority_fee_lamports.unwrap_or(0));
        }
    }

    /// Drain all buckets into `RequestAggregate` events and reset the window.
    fn drain(&self) -> Vec<TelemetryEvent> {
        let now = now_ms();
        let mut s = self.state.lock().unwrap();
        let window_start = s.window_start_ms;

        let mut events: Vec<_> = s
            .buckets
            .drain()
            .filter(|(_, b)| !b.latencies.is_empty())
            .map(|((method, provider, status), bucket)| {
                let Bucket {
                    count,
                    mut latencies,
                } = bucket;
                let sum: f64 = latencies.iter().sum();
                // Average over latency samples (round trips), not the call-weighted
                // `count`, so a batch's single sample isn't divided by its call volume.
                let latency_avg_ms = sum / latencies.len() as f64;
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

        events.extend(s.tx_buckets.drain().map(|(provider, mut bucket)| {
            bucket.cu_limits.sort_unstable();
            bucket.cu_prices.sort_unstable();
            bucket.fees.sort_unstable();
            TelemetryEvent::TransactionSubmit {
                window_start_ms: window_start,
                window_end_ms: now,
                provider,
                accepted_count: bucket.accepted_count,
                rejected_count: bucket.rejected_count,
                parsed_count: bucket.parsed_count,
                unparsed_count: bucket.unparsed_count,
                unsupported_count: bucket.unsupported_count,
                invalid_budget_count: bucket.invalid_budget_count,
                batch_unsupported_count: bucket.batch_unsupported_count,
                decode_queue_drop_count: bucket.decode_queue_drop_count,
                fee_sample_count: bucket.cu_limits.len() as u64,
                cu_limit_p50: percentile_u64(&bucket.cu_limits, 50),
                cu_limit_p90: percentile_u64(&bucket.cu_limits, 90),
                cu_limit_p95: percentile_u64(&bucket.cu_limits, 95),
                cu_price_micro_lamports_p50: percentile_u64(&bucket.cu_prices, 50),
                cu_price_micro_lamports_p90: percentile_u64(&bucket.cu_prices, 90),
                cu_price_micro_lamports_p95: percentile_u64(&bucket.cu_prices, 95),
                requested_priority_fee_lamports_p50: percentile_u64(&bucket.fees, 50),
                requested_priority_fee_lamports_p90: percentile_u64(&bucket.fees, 90),
                requested_priority_fee_lamports_p95: percentile_u64(&bucket.fees, 95),
            }
        }));

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

fn percentile_u64(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

// ── RemoteReporter ────────────────────────────────────────────────────────────

/// Buffers raw events in a bounded channel and aggregates request data in-memory.
/// At each flush interval: drains the aggregator (producing `RequestAggregate` events)
/// and sends all buffered events to the remote endpoint in batches.
pub struct RemoteReporter {
    tx: mpsc::Sender<TelemetryEvent>,
    flush_signal: Arc<Notify>,
    flush_waiters: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
    aggregator: Arc<Aggregator>,
}

impl RemoteReporter {
    /// Spawns the background flush task. Must be called inside a Tokio runtime.
    pub fn new(config: ReportingConfig, client: Arc<Client>) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let flush_signal = Arc::new(Notify::new());
        let flush_waiters = Arc::new(Mutex::new(Vec::new()));
        let aggregator = Arc::new(Aggregator::new());

        tokio::spawn(flush_task(
            rx,
            flush_signal.clone(),
            flush_waiters.clone(),
            aggregator.clone(),
            config,
            client,
        ));

        Self {
            tx,
            flush_signal,
            flush_waiters,
            aggregator,
        }
    }
}

impl Reporter for RemoteReporter {
    fn emit(&self, event: TelemetryEvent) {
        // try_send never blocks; silently drop if the buffer is full.
        let _ = self.tx.try_send(event);
    }

    fn record_request(
        &self,
        method: &str,
        provider: &str,
        status: &str,
        latency_ms: f64,
        count: u64,
    ) {
        self.aggregator
            .record(method, provider, status, latency_ms, count);
    }

    fn record_transaction_submission(&self, provider: &str, accepted: bool) {
        self.aggregator
            .record_transaction_submission(provider, accepted);
    }

    fn record_transaction_decode(
        &self,
        provider: &str,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        self.aggregator
            .record_transaction_decode(provider, result, sample);
    }

    fn record_transaction_result(
        &self,
        provider: &str,
        accepted: bool,
        result: &str,
        sample: Option<&TransactionInfo>,
    ) {
        self.aggregator
            .record_transaction_result(provider, accepted, result, sample);
    }

    fn flush(&self) {
        self.flush_signal.notify_one();
    }

    fn flush_and_wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let (done, receiver) = oneshot::channel();
        self.flush_waiters.lock().unwrap().push(done);
        self.flush_signal.notify_one();
        Box::pin(async move {
            let _ = receiver.await;
        })
    }
}

// ── Background flush task ─────────────────────────────────────────────────────

async fn flush_task(
    mut rx: mpsc::Receiver<TelemetryEvent>,
    flush_signal: Arc<Notify>,
    flush_waiters: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
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
                for waiter in std::mem::take(&mut *flush_waiters.lock().unwrap()) {
                    let _ = waiter.send(());
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
    for waiter in std::mem::take(&mut *flush_waiters.lock().unwrap()) {
        let _ = waiter.send(());
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

    type RequestLog = Arc<Mutex<Vec<(String, String, String, f64, u64)>>>;

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
        fn record_request(
            &self,
            method: &str,
            provider: &str,
            status: &str,
            latency_ms: f64,
            count: u64,
        ) {
            self.requests.lock().unwrap().push((
                method.to_string(),
                provider.to_string(),
                status.to_string(),
                latency_ms,
                count,
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
        r.record_request("getSlot", "helius", "ok", 1.0, 1);
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
        reporter.record_request("getSlot", "helius", "ok", 12.5, 1);
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
            agg.record("getSlot", "helius", "ok", i as f64, 1);
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
    fn batch_weighted_count_keeps_single_latency_sample() {
        let agg = Aggregator::new();
        // One batch of 1000 getTransaction calls: 1000 to count, one 200ms sample.
        agg.record("getTransaction", "helius", "ok", 200.0, 1000);
        let events = agg.drain();
        assert_eq!(events.len(), 1);
        if let TelemetryEvent::RequestAggregate {
            count,
            latency_avg_ms,
            latency_p50_ms,
            ..
        } = &events[0]
        {
            // Call volume is weighted by batch size...
            assert_eq!(*count, 1000);
            // ...but latency is the one round-trip sample, not 200/1000.
            assert_eq!(*latency_avg_ms, 200.0);
            assert_eq!(*latency_p50_ms, 200.0);
        } else {
            panic!("expected RequestAggregate");
        }
    }

    #[test]
    fn aggregator_drain_resets_window() {
        let agg = Aggregator::new();
        agg.record("getSlot", "helius", "ok", 10.0, 1);
        let first = agg.drain();
        assert_eq!(first.len(), 1);
        // second drain with no new data should be empty
        let second = agg.drain();
        assert!(second.is_empty());
    }

    #[test]
    fn aggregator_buckets_by_key() {
        let agg = Aggregator::new();
        agg.record("getSlot", "helius", "ok", 10.0, 1);
        agg.record("getSlot", "quicknode", "ok", 20.0, 1);
        agg.record("getBalance", "helius", "ok", 5.0, 1);
        agg.record("getSlot", "helius", "error", 100.0, 1);
        let events = agg.drain();
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn transaction_aggregates_counts_and_accepted_percentiles() {
        let agg = Aggregator::new();
        agg.record_transaction_submission("helius", true);
        agg.record_transaction_submission("helius", false);
        for limit in [100, 200, 300, 400] {
            let info = TransactionInfo {
                cu_limit: limit,
                cu_limit_defaulted: false,
                cu_price_micro_lamports: Some(limit * 10),
                requested_priority_fee_lamports: Some(limit * 100),
                num_instructions: 1,
                num_signatures: 1,
                is_versioned: false,
            };
            agg.record_transaction_decode("helius", "parsed", Some(&info));
        }
        agg.record_transaction_decode("helius", "invalid_budget", None);
        let events = agg.drain();
        let event = events
            .iter()
            .find(|event| matches!(event, TelemetryEvent::TransactionSubmit { .. }))
            .unwrap();
        let TelemetryEvent::TransactionSubmit {
            accepted_count,
            rejected_count,
            parsed_count,
            invalid_budget_count,
            cu_limit_p50,
            cu_limit_p90,
            cu_limit_p95,
            ..
        } = event
        else {
            unreachable!()
        };
        assert_eq!((*accepted_count, *rejected_count), (1, 1));
        assert_eq!((*parsed_count, *invalid_budget_count), (4, 1));
        assert_eq!(
            (*cu_limit_p50, *cu_limit_p90, *cu_limit_p95),
            (200, 400, 400)
        );
        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "transaction_submit");
    }
}
