use crate::config::HistoricalAnalyticsConfig;
use crate::health::ObservedSlotTips;
use crate::telemetry::{
    Reporter, TelemetryEvent, HISTORICAL_FOUND_REUSE_BUCKET_COUNT,
    HISTORICAL_GET_TRANSACTION_AGGREGATE_VERSION, HISTORICAL_POLLS_BEFORE_FOUND_BUCKET_COUNT,
    HISTORICAL_SLOT_AGE_BUCKET_COUNT, HISTORICAL_TIME_TO_FOUND_BUCKET_COUNT,
};
use axum::body::Bytes;
use std::collections::{hash_map::RandomState, BTreeSet, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

const QUEUE_BYTE_UNIT: usize = 4096;
const REUSE_EPOCH_OBSERVATION_SECS: u64 = 172_800;

// ── Classification vocabulary ────────────────────────────────────────────────
//
// These are the fixed classifications the analyzer reports. They are aggregated
// into the versioned `HistoricalGetTransactionAggregate` telemetry event and
// are deliberately not exported as Prometheus metrics: historical workload
// analysis is a hosted-dashboard feature, so the aggregate event is the single
// delivery path. Adding a variant is a telemetry schema change — bump
// `HISTORICAL_GET_TRANSACTION_AGGREGATE_VERSION` when the meaning or order of
// any reported bucket changes.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalCommitment {
    Processed,
    Confirmed,
    Finalized,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalErrorKind {
    Rpc,
    Parse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalTransition {
    FirstObservationFound,
    FirstObservationNull,
    NullRepeat,
    NullToFound,
    FoundRepeat,
    FoundToNullRegression,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalAnalyzerDropReason {
    QueueFull,
    ByteBudgetExhausted,
    OversizedJob,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalStateEvictionReason {
    Capacity,
    Ttl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalState {
    NullSeen,
    Found,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoricalRequestClass {
    None,
    Single,
    Batch { eligible_calls: u64 },
}

impl HistoricalRequestClass {
    fn eligible_calls(self) -> u64 {
        match self {
            Self::None => 0,
            Self::Single => 1,
            Self::Batch { eligible_calls } => eligible_calls,
        }
    }
}

#[derive(Clone)]
pub struct HistoricalAnalytics {
    sender: mpsc::Sender<Message>,
    byte_budget: Arc<Semaphore>,
    max_job_bytes: usize,
    quality: Arc<QualityCounters>,
}

struct Job {
    request: Bytes,
    response: Bytes,
    status: u16,
    observed_at: Instant,
    tips: ObservedSlotTips,
    class: HistoricalRequestClass,
    _permit: OwnedSemaphorePermit,
}

enum Message {
    Analyze(Job),
    Flush(oneshot::Sender<()>),
}

#[derive(Default)]
struct QualityCounters {
    dropped_logical: AtomicU64,
    queue_full: AtomicU64,
    byte_budget: AtomicU64,
    oversized: AtomicU64,
    resets: AtomicU64,
}

impl HistoricalAnalytics {
    pub fn new(config: HistoricalAnalyticsConfig, reporter: Arc<dyn Reporter>) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let permit_count = (config.max_queued_bytes / QUEUE_BYTE_UNIT).max(1);
        let byte_budget = Arc::new(Semaphore::new(permit_count));
        let quality = Arc::new(QualityCounters::default());
        quality.resets.store(1, Ordering::Relaxed);

        tokio::spawn(worker_loop(
            receiver,
            config.clone(),
            reporter,
            quality.clone(),
        ));

        Self {
            sender,
            byte_budget,
            max_job_bytes: config.max_job_bytes,
            quality,
        }
    }

    pub fn finish(
        &self,
        class: HistoricalRequestClass,
        request: &Bytes,
        response: &Bytes,
        status: u16,
        tips: ObservedSlotTips,
    ) {
        let logical = class.eligible_calls();
        if logical == 0 {
            return;
        }

        let Some(job_bytes) = request.len().checked_add(response.len()) else {
            self.drop_job(HistoricalAnalyzerDropReason::OversizedJob, logical);
            return;
        };
        if job_bytes > self.max_job_bytes {
            self.drop_job(HistoricalAnalyzerDropReason::OversizedJob, logical);
            return;
        }

        let units = job_bytes.div_ceil(QUEUE_BYTE_UNIT).max(1);
        let Ok(units) = u32::try_from(units) else {
            self.drop_job(HistoricalAnalyzerDropReason::ByteBudgetExhausted, logical);
            return;
        };
        let Ok(permit) = self.byte_budget.clone().try_acquire_many_owned(units) else {
            self.drop_job(HistoricalAnalyzerDropReason::ByteBudgetExhausted, logical);
            return;
        };

        let job = Job {
            request: request.clone(),
            response: response.clone(),
            status,
            observed_at: Instant::now(),
            tips,
            class,
            _permit: permit,
        };
        if self.sender.try_send(Message::Analyze(job)).is_err() {
            self.drop_job(HistoricalAnalyzerDropReason::QueueFull, logical);
        }
    }

    fn drop_job(&self, reason: HistoricalAnalyzerDropReason, logical: u64) {
        self.quality
            .dropped_logical
            .fetch_add(logical, Ordering::Relaxed);
        match reason {
            HistoricalAnalyzerDropReason::QueueFull => &self.quality.queue_full,
            HistoricalAnalyzerDropReason::ByteBudgetExhausted => &self.quality.byte_budget,
            HistoricalAnalyzerDropReason::OversizedJob => &self.quality.oversized,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub async fn flush(&self) {
        let (done, receiver) = oneshot::channel();
        if self.sender.send(Message::Flush(done)).await.is_ok() {
            let _ = receiver.await;
        }
    }
}

async fn worker_loop(
    mut receiver: mpsc::Receiver<Message>,
    config: HistoricalAnalyticsConfig,
    reporter: Arc<dyn Reporter>,
    quality: Arc<QualityCounters>,
) {
    let mut core = AnalyzerCore::new(config.state_capacity, config.state_ttl_secs);
    let mut interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            message = receiver.recv() => match message {
                Some(Message::Analyze(job)) => core.process(job),
                Some(Message::Flush(done)) => {
                    core.expire(Instant::now());
                    core.flush(&reporter, &quality, config.state_capacity);
                    let _ = done.send(());
                }
                None => break,
            },
            _ = interval.tick() => {
                core.expire(Instant::now());
                core.flush(&reporter, &quality, config.state_capacity);
            }
        }
    }
    core.expire(Instant::now());
    core.flush(&reporter, &quality, config.state_capacity);
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Fingerprint(pub(crate) u64, pub(crate) u64);

enum Lifecycle {
    NullSeen {
        first_null_at: Instant,
        null_poll_count: u64,
    },
    Found {
        last_found_at: Instant,
    },
}

struct StateEntry {
    lifecycle: Lifecycle,
    commitment: HistoricalCommitment,
    last_seen_at: Instant,
}

pub(crate) struct AnalyzerCore {
    state: HashMap<Fingerprint, StateEntry>,
    lru: BTreeSet<(Instant, Fingerprint)>,
    capacity: usize,
    ttl: Duration,
    hash_a: RandomState,
    hash_b: RandomState,
    windows: [Window; 4],
    window_start_ms: u64,
}

impl AnalyzerCore {
    pub(crate) fn new(capacity: usize, ttl_secs: u64) -> Self {
        Self {
            state: HashMap::with_capacity(capacity.min(16_384)),
            lru: BTreeSet::new(),
            capacity,
            ttl: Duration::from_secs(ttl_secs),
            hash_a: RandomState::new(),
            hash_b: RandomState::new(),
            windows: std::array::from_fn(|_| Window::default()),
            window_start_ms: now_ms(),
        }
    }

    fn process(&mut self, job: Job) {
        self.expire(job.observed_at);
        match job.class {
            HistoricalRequestClass::None => {}
            HistoricalRequestClass::Single => self.process_single(job),
            HistoricalRequestClass::Batch { eligible_calls } => {
                self.process_batch(&job.request, eligible_calls)
            }
        }
    }

    fn process_batch(&mut self, request: &[u8], eligible_calls: u64) {
        let mut observed = 0u64;
        if let Ok(serde_json::Value::Array(calls)) = serde_json::from_slice(request) {
            for call in calls {
                if call.get("method").and_then(|v| v.as_str()) != Some("getTransaction") {
                    continue;
                }
                observed += 1;
                let commitment = commitment_from_request(&call);
                let window = self.window(commitment);
                window.logical_request_count += 1;
                window.unsupported_batch_count += 1;
            }
        }
        for _ in observed..eligible_calls {
            let window = self.window(HistoricalCommitment::Unknown);
            window.logical_request_count += 1;
            window.unsupported_batch_count += 1;
        }
    }

    fn process_single(&mut self, job: Job) {
        let request: serde_json::Value = match serde_json::from_slice(&job.request) {
            Ok(value) => value,
            Err(_) => {
                self.record_error(HistoricalCommitment::Unknown, HistoricalErrorKind::Parse);
                return;
            }
        };
        let commitment = commitment_from_request(&request);
        let Some(fingerprint) = self.fingerprint(&request) else {
            self.record_error(commitment, HistoricalErrorKind::Parse);
            return;
        };
        let outcome = parse_response(job.status, &job.response);
        match outcome {
            ParsedOutcome::RpcError => self.record_error(commitment, HistoricalErrorKind::Rpc),
            ParsedOutcome::ParseError => self.record_error(commitment, HistoricalErrorKind::Parse),
            ParsedOutcome::Null => {
                let window = self.window(commitment);
                window.logical_request_count += 1;
                window.analyzed_request_count += 1;
                window.null_count += 1;
                self.transition(fingerprint, commitment, false, None, job.observed_at);
            }
            ParsedOutcome::Found(slot) => {
                let window = self.window(commitment);
                window.logical_request_count += 1;
                window.analyzed_request_count += 1;
                window.found_count += 1;
                self.record_slot_age(commitment, slot, job.tips);
                self.transition(fingerprint, commitment, true, slot, job.observed_at);
            }
        }
    }

    fn record_error(&mut self, commitment: HistoricalCommitment, kind: HistoricalErrorKind) {
        let window = self.window(commitment);
        window.logical_request_count += 1;
        window.analyzed_request_count += 1;
        match kind {
            HistoricalErrorKind::Rpc => window.rpc_error_count += 1,
            HistoricalErrorKind::Parse => window.parse_error_count += 1,
        }
    }

    pub(crate) fn fingerprint(&self, request: &serde_json::Value) -> Option<Fingerprint> {
        let object = request.as_object()?;
        if object.get("method")?.as_str()? != "getTransaction" {
            return None;
        }
        let params = object.get("params")?.as_array()?;
        params.first()?.as_str()?;
        let mut a = self.hash_a.build_hasher();
        let mut b = self.hash_b.build_hasher();
        hash_json(&serde_json::Value::String("getTransaction".into()), &mut a);
        hash_json(&serde_json::Value::String("getTransaction".into()), &mut b);
        hash_json(object.get("params")?, &mut a);
        hash_json(object.get("params")?, &mut b);
        Some(Fingerprint(a.finish(), b.finish()))
    }

    pub(crate) fn transition(
        &mut self,
        fingerprint: Fingerprint,
        commitment: HistoricalCommitment,
        found: bool,
        _slot: Option<u64>,
        now: Instant,
    ) {
        let previous = self.state.remove(&fingerprint).inspect(|entry| {
            self.lru.remove(&(entry.last_seen_at, fingerprint));
        });

        let lifecycle = match (previous, found) {
            (None, false) => {
                self.record_transition(commitment, HistoricalTransition::FirstObservationNull);
                Lifecycle::NullSeen {
                    first_null_at: now,
                    null_poll_count: 1,
                }
            }
            (None, true) => {
                self.record_transition(commitment, HistoricalTransition::FirstObservationFound);
                self.window(commitment).polls_before_found_bucket_counts[0] += 1;
                Lifecycle::Found { last_found_at: now }
            }
            (Some(entry), false) => match entry.lifecycle {
                Lifecycle::NullSeen {
                    first_null_at,
                    null_poll_count,
                } => {
                    self.record_transition(commitment, HistoricalTransition::NullRepeat);
                    Lifecycle::NullSeen {
                        first_null_at,
                        null_poll_count: null_poll_count.saturating_add(1),
                    }
                }
                Lifecycle::Found { last_found_at } => {
                    self.record_transition(commitment, HistoricalTransition::FoundToNullRegression);
                    Lifecycle::Found { last_found_at }
                }
            },
            (Some(entry), true) => match entry.lifecycle {
                Lifecycle::NullSeen {
                    first_null_at,
                    null_poll_count,
                } => {
                    self.record_transition(commitment, HistoricalTransition::NullToFound);
                    let elapsed = now.saturating_duration_since(first_null_at);
                    let window = self.window(commitment);
                    window.polls_before_found_bucket_counts[polls_bucket(null_poll_count)] += 1;
                    window.time_to_found_bucket_counts
                        [duration_bucket(elapsed, &[1, 2, 5, 15, 60])] += 1;
                    Lifecycle::Found { last_found_at: now }
                }
                Lifecycle::Found { last_found_at } => {
                    self.record_transition(commitment, HistoricalTransition::FoundRepeat);
                    let elapsed = now.saturating_duration_since(last_found_at);
                    self.window(commitment).found_reuse_bucket_counts[duration_bucket(
                        elapsed,
                        &[300, 3_600, 86_400, REUSE_EPOCH_OBSERVATION_SECS],
                    )] += 1;
                    Lifecycle::Found { last_found_at: now }
                }
            },
        };

        self.state.insert(
            fingerprint,
            StateEntry {
                lifecycle,
                commitment,
                last_seen_at: now,
            },
        );
        self.lru.insert((now, fingerprint));
        while self.state.len() > self.capacity {
            self.evict_oldest(HistoricalStateEvictionReason::Capacity);
        }
    }

    fn record_transition(
        &mut self,
        commitment: HistoricalCommitment,
        transition: HistoricalTransition,
    ) {
        let window = self.window(commitment);
        match transition {
            HistoricalTransition::FirstObservationFound => {
                window.first_observation_found_count += 1
            }
            HistoricalTransition::FirstObservationNull => window.first_observation_null_count += 1,
            HistoricalTransition::NullRepeat => window.null_repeat_count += 1,
            HistoricalTransition::NullToFound => window.null_to_found_count += 1,
            HistoricalTransition::FoundRepeat => window.found_repeat_count += 1,
            HistoricalTransition::FoundToNullRegression => {
                window.found_to_null_regression_count += 1
            }
        }
    }

    fn record_slot_age(
        &mut self,
        commitment: HistoricalCommitment,
        returned_slot: Option<u64>,
        tips: ObservedSlotTips,
    ) {
        let tip = match commitment {
            HistoricalCommitment::Processed => tips.processed,
            HistoricalCommitment::Confirmed => tips.confirmed,
            HistoricalCommitment::Finalized => tips.finalized,
            HistoricalCommitment::Unknown => None,
        };
        match (returned_slot, tip) {
            (Some(slot), Some(tip)) if slot <= tip => {
                let age = tip - slot;
                self.window(commitment).slot_age_bucket_counts[slot_age_bucket(age)] += 1;
            }
            (Some(_), Some(_)) => {
                self.window(commitment).slot_age_tip_behind_count += 1;
            }
            _ => {
                self.window(commitment).slot_age_unknown_count += 1;
            }
        }
    }

    fn expire(&mut self, now: Instant) {
        while let Some((last_seen, _)) = self.lru.first().copied() {
            if now.saturating_duration_since(last_seen) < self.ttl {
                break;
            }
            self.evict_oldest(HistoricalStateEvictionReason::Ttl);
        }
    }

    fn evict_oldest(&mut self, reason: HistoricalStateEvictionReason) {
        let Some((last_seen, fingerprint)) = self.lru.pop_first() else {
            return;
        };
        let Some(entry) = self.state.remove(&fingerprint) else {
            return;
        };
        debug_assert_eq!(entry.last_seen_at, last_seen);
        let state = match entry.lifecycle {
            Lifecycle::NullSeen { .. } => HistoricalState::NullSeen,
            Lifecycle::Found { .. } => HistoricalState::Found,
        };
        let window = self.window(entry.commitment);
        match reason {
            HistoricalStateEvictionReason::Capacity => {
                window.state_capacity_eviction_count += 1;
                if state == HistoricalState::NullSeen {
                    window.unresolved_null_capacity_eviction_count += 1;
                }
            }
            HistoricalStateEvictionReason::Ttl => {
                window.state_ttl_expiration_count += 1;
                if state == HistoricalState::NullSeen {
                    window.unresolved_null_ttl_expiration_count += 1;
                }
            }
        }
    }

    fn window(&mut self, commitment: HistoricalCommitment) -> &mut Window {
        &mut self.windows[commitment_index(commitment)]
    }

    fn flush(
        &mut self,
        reporter: &Arc<dyn Reporter>,
        quality: &QualityCounters,
        configured_capacity: usize,
    ) {
        let end = now_ms();
        let dropped_logical = quality.dropped_logical.swap(0, Ordering::Relaxed);
        let queue_full = quality.queue_full.swap(0, Ordering::Relaxed);
        let byte_budget = quality.byte_budget.swap(0, Ordering::Relaxed);
        let oversized = quality.oversized.swap(0, Ordering::Relaxed);
        let resets = quality.resets.swap(0, Ordering::Relaxed);

        let quality_present = dropped_logical + queue_full + byte_budget + oversized + resets > 0;
        for (index, window) in self.windows.iter_mut().enumerate() {
            let is_unknown = index == commitment_index(HistoricalCommitment::Unknown);
            if window.is_empty() && !(is_unknown && quality_present) {
                continue;
            }
            if is_unknown {
                window.logical_request_count += dropped_logical;
            }
            let event = window.take_event(
                commitment_for_index(index),
                self.window_start_ms,
                end,
                if is_unknown { queue_full } else { 0 },
                if is_unknown { byte_budget } else { 0 },
                if is_unknown { oversized } else { 0 },
                if is_unknown { resets } else { 0 },
                self.state.len(),
                configured_capacity,
            );
            reporter.emit(event);
        }
        self.window_start_ms = end;
    }
}

#[derive(Default)]
struct Window {
    logical_request_count: u64,
    analyzed_request_count: u64,
    found_count: u64,
    null_count: u64,
    rpc_error_count: u64,
    parse_error_count: u64,
    first_observation_found_count: u64,
    first_observation_null_count: u64,
    null_repeat_count: u64,
    null_to_found_count: u64,
    found_repeat_count: u64,
    found_to_null_regression_count: u64,
    slot_age_bucket_counts: [u64; HISTORICAL_SLOT_AGE_BUCKET_COUNT],
    slot_age_unknown_count: u64,
    slot_age_tip_behind_count: u64,
    polls_before_found_bucket_counts: [u64; HISTORICAL_POLLS_BEFORE_FOUND_BUCKET_COUNT],
    time_to_found_bucket_counts: [u64; HISTORICAL_TIME_TO_FOUND_BUCKET_COUNT],
    found_reuse_bucket_counts: [u64; HISTORICAL_FOUND_REUSE_BUCKET_COUNT],
    unsupported_batch_count: u64,
    state_capacity_eviction_count: u64,
    state_ttl_expiration_count: u64,
    unresolved_null_capacity_eviction_count: u64,
    unresolved_null_ttl_expiration_count: u64,
}

impl Window {
    fn is_empty(&self) -> bool {
        self.logical_request_count == 0
            && self.state_capacity_eviction_count == 0
            && self.state_ttl_expiration_count == 0
    }

    #[allow(clippy::too_many_arguments)]
    fn take_event(
        &mut self,
        commitment: HistoricalCommitment,
        start: u64,
        end: u64,
        queue_full: u64,
        byte_budget: u64,
        oversized: u64,
        resets: u64,
        entries: usize,
        capacity: usize,
    ) -> TelemetryEvent {
        let window = std::mem::take(self);
        TelemetryEvent::HistoricalGetTransactionAggregate {
            schema_version: HISTORICAL_GET_TRANSACTION_AGGREGATE_VERSION,
            window_start_ms: start,
            window_end_ms: end,
            commitment: commitment_name(commitment).to_string(),
            logical_request_count: window.logical_request_count,
            analyzed_request_count: window.analyzed_request_count,
            found_count: window.found_count,
            null_count: window.null_count,
            rpc_error_count: window.rpc_error_count,
            parse_error_count: window.parse_error_count,
            first_observation_found_count: window.first_observation_found_count,
            first_observation_null_count: window.first_observation_null_count,
            null_repeat_count: window.null_repeat_count,
            null_to_found_count: window.null_to_found_count,
            found_repeat_count: window.found_repeat_count,
            found_to_null_regression_count: window.found_to_null_regression_count,
            slot_age_bucket_counts: Box::new(window.slot_age_bucket_counts),
            slot_age_unknown_count: window.slot_age_unknown_count,
            slot_age_tip_behind_count: window.slot_age_tip_behind_count,
            polls_before_found_bucket_counts: Box::new(window.polls_before_found_bucket_counts),
            time_to_found_bucket_counts: Box::new(window.time_to_found_bucket_counts),
            found_reuse_bucket_counts: Box::new(window.found_reuse_bucket_counts),
            unsupported_batch_count: window.unsupported_batch_count,
            queue_full_drop_count: queue_full,
            byte_budget_exhausted_drop_count: byte_budget,
            oversized_job_drop_count: oversized,
            state_capacity_eviction_count: window.state_capacity_eviction_count,
            state_ttl_expiration_count: window.state_ttl_expiration_count,
            unresolved_null_capacity_eviction_count: window.unresolved_null_capacity_eviction_count,
            unresolved_null_ttl_expiration_count: window.unresolved_null_ttl_expiration_count,
            state_reset_count: resets,
            current_state_entries: entries as u64,
            configured_state_capacity: capacity as u64,
        }
    }
}

pub(crate) enum ParsedOutcome {
    Found(Option<u64>),
    Null,
    RpcError,
    ParseError,
}

pub(crate) fn parse_response(status: u16, response: &[u8]) -> ParsedOutcome {
    if !(200..300).contains(&status) {
        return ParsedOutcome::RpcError;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(response) else {
        return ParsedOutcome::ParseError;
    };
    let Some(object) = value.as_object() else {
        return ParsedOutcome::ParseError;
    };
    if object.get("error").is_some_and(|error| !error.is_null()) {
        return ParsedOutcome::RpcError;
    }
    match object.get("result") {
        Some(result) if result.is_null() => ParsedOutcome::Null,
        Some(result) => ParsedOutcome::Found(result.get("slot").and_then(|slot| slot.as_u64())),
        None => ParsedOutcome::ParseError,
    }
}

fn commitment_from_request(request: &serde_json::Value) -> HistoricalCommitment {
    let value = request
        .get("params")
        .and_then(|params| params.as_array())
        .and_then(|params| params.get(1))
        .and_then(|config| config.get("commitment"))
        .and_then(|commitment| commitment.as_str());
    match value {
        Some("processed") => HistoricalCommitment::Processed,
        Some("confirmed") => HistoricalCommitment::Confirmed,
        Some("finalized") => HistoricalCommitment::Finalized,
        _ => HistoricalCommitment::Unknown,
    }
}

fn hash_json(value: &serde_json::Value, hasher: &mut impl Hasher) {
    match value {
        serde_json::Value::Null => hasher.write_u8(0),
        serde_json::Value::Bool(value) => {
            hasher.write_u8(1);
            hasher.write_u8(*value as u8);
        }
        serde_json::Value::Number(value) => {
            hasher.write_u8(2);
            hasher.write(value.to_string().as_bytes());
        }
        serde_json::Value::String(value) => {
            hasher.write_u8(3);
            hasher.write_usize(value.len());
            hasher.write(value.as_bytes());
        }
        serde_json::Value::Array(values) => {
            hasher.write_u8(4);
            hasher.write_usize(values.len());
            for value in values {
                hash_json(value, hasher);
            }
        }
        serde_json::Value::Object(values) => {
            hasher.write_u8(5);
            hasher.write_usize(values.len());
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            for key in keys {
                hasher.write_usize(key.len());
                hasher.write(key.as_bytes());
                hash_json(&values[key], hasher);
            }
        }
    }
}

fn slot_age_bucket(age: u64) -> usize {
    [
        149, 749, 8_999, 53_999, 215_999, 431_999, 2_159_999, 4_319_999,
    ]
    .iter()
    .position(|upper| age <= *upper)
    .unwrap_or(HISTORICAL_SLOT_AGE_BUCKET_COUNT - 1)
}

fn polls_bucket(polls: u64) -> usize {
    match polls {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=5 => 3,
        6..=10 => 4,
        _ => 5,
    }
}

fn duration_bucket(duration: Duration, upper_exclusive_secs: &[u64]) -> usize {
    upper_exclusive_secs
        .iter()
        .position(|upper| duration < Duration::from_secs(*upper))
        .unwrap_or(upper_exclusive_secs.len())
}

fn commitment_index(commitment: HistoricalCommitment) -> usize {
    match commitment {
        HistoricalCommitment::Processed => 0,
        HistoricalCommitment::Confirmed => 1,
        HistoricalCommitment::Finalized => 2,
        HistoricalCommitment::Unknown => 3,
    }
}

fn commitment_for_index(index: usize) -> HistoricalCommitment {
    match index {
        0 => HistoricalCommitment::Processed,
        1 => HistoricalCommitment::Confirmed,
        2 => HistoricalCommitment::Finalized,
        _ => HistoricalCommitment::Unknown,
    }
}

fn commitment_name(commitment: HistoricalCommitment) -> &'static str {
    match commitment {
        HistoricalCommitment::Processed => "processed",
        HistoricalCommitment::Confirmed => "confirmed",
        HistoricalCommitment::Finalized => "finalized",
        HistoricalCommitment::Unknown => "unknown",
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingReporter(Mutex<Vec<TelemetryEvent>>);

    impl Reporter for RecordingReporter {
        fn emit(&self, event: TelemetryEvent) {
            self.0.lock().unwrap().push(event);
        }

        fn flush(&self) {}
    }

    #[test]
    fn response_outcomes_are_classified() {
        assert!(matches!(
            parse_response(200, br#"{"result":null}"#),
            ParsedOutcome::Null
        ));
        assert!(matches!(
            parse_response(200, br#"{"result":{"slot":42}}"#),
            ParsedOutcome::Found(Some(42))
        ));
        assert!(matches!(
            parse_response(200, br#"{"error":{"code":-1}}"#),
            ParsedOutcome::RpcError
        ));
        assert!(matches!(
            parse_response(200, b"bad"),
            ParsedOutcome::ParseError
        ));
    }

    #[test]
    fn fingerprint_excludes_id_and_canonicalizes_object_order() {
        let core = AnalyzerCore::new(10, 60);
        let a = serde_json::json!({
            "id": 1,
            "method": "getTransaction",
            "params": ["signature", {"encoding":"json", "commitment":"confirmed"}]
        });
        let b = serde_json::json!({
            "id": "different",
            "method": "getTransaction",
            "params": ["signature", {"commitment":"confirmed", "encoding":"json"}]
        });
        assert_eq!(core.fingerprint(&a), core.fingerprint(&b));

        let c = serde_json::json!({
            "method": "getTransaction",
            "params": ["signature", {"commitment":"finalized", "encoding":"json"}]
        });
        assert_ne!(core.fingerprint(&a), core.fingerprint(&c));
    }

    #[test]
    fn bucket_boundaries_match_schema() {
        assert_eq!(slot_age_bucket(149), 0);
        assert_eq!(slot_age_bucket(150), 1);
        assert_eq!(slot_age_bucket(4_320_000), 8);
        assert_eq!(polls_bucket(0), 0);
        assert_eq!(polls_bucket(3), 3);
        assert_eq!(polls_bucket(11), 5);
        assert_eq!(duration_bucket(Duration::from_millis(999), &[1, 2]), 0);
        assert_eq!(duration_bucket(Duration::from_secs(1), &[1, 2]), 1);
    }

    #[test]
    fn lifecycle_transitions_and_regression_preserve_last_found_time() {
        let mut core = AnalyzerCore::new(10, 10_000);
        let fingerprint = Fingerprint(1, 2);
        let commitment = HistoricalCommitment::Confirmed;
        let start = Instant::now();

        core.transition(fingerprint, commitment, false, None, start);
        core.transition(
            fingerprint,
            commitment,
            false,
            None,
            start + Duration::from_secs(1),
        );
        core.transition(
            fingerprint,
            commitment,
            true,
            Some(1),
            start + Duration::from_secs(2),
        );
        core.transition(
            fingerprint,
            commitment,
            false,
            None,
            start + Duration::from_secs(402),
        );
        core.transition(
            fingerprint,
            commitment,
            true,
            Some(1),
            start + Duration::from_secs(502),
        );

        let window = &core.windows[commitment_index(commitment)];
        assert_eq!(window.first_observation_null_count, 1);
        assert_eq!(window.null_repeat_count, 1);
        assert_eq!(window.null_to_found_count, 1);
        assert_eq!(window.found_to_null_regression_count, 1);
        assert_eq!(window.found_repeat_count, 1);
        assert_eq!(window.polls_before_found_bucket_counts[2], 1);
        assert_eq!(window.found_reuse_bucket_counts[1], 1);
    }

    #[test]
    fn capacity_and_ttl_evictions_count_unresolved_nulls() {
        let start = Instant::now();
        let mut core = AnalyzerCore::new(1, 10);
        core.transition(
            Fingerprint(1, 1),
            HistoricalCommitment::Processed,
            false,
            None,
            start,
        );
        core.transition(
            Fingerprint(2, 2),
            HistoricalCommitment::Processed,
            true,
            None,
            start + Duration::from_secs(1),
        );
        let window = &core.windows[commitment_index(HistoricalCommitment::Processed)];
        assert_eq!(window.state_capacity_eviction_count, 1);
        assert_eq!(window.unresolved_null_capacity_eviction_count, 1);

        let mut ttl_core = AnalyzerCore::new(2, 10);
        ttl_core.transition(
            Fingerprint(3, 3),
            HistoricalCommitment::Finalized,
            false,
            None,
            start,
        );
        ttl_core.expire(start + Duration::from_secs(10));
        let window = &ttl_core.windows[commitment_index(HistoricalCommitment::Finalized)];
        assert_eq!(window.state_ttl_expiration_count, 1);
        assert_eq!(window.unresolved_null_ttl_expiration_count, 1);
        assert!(ttl_core.state.is_empty());
    }

    #[test]
    fn aggregate_flush_emits_only_counts() {
        let mut core = AnalyzerCore::new(10, 60);
        let reporter = Arc::new(RecordingReporter::default());
        let reporter_trait: Arc<dyn Reporter> = reporter.clone();
        let quality = QualityCounters::default();
        quality.resets.store(1, Ordering::Relaxed);
        core.transition(
            Fingerprint(9, 9),
            HistoricalCommitment::Processed,
            true,
            None,
            Instant::now(),
        );
        core.window(HistoricalCommitment::Processed)
            .logical_request_count = 1;
        core.flush(&reporter_trait, &quality, 10);

        let events = reporter.0.lock().unwrap();
        assert_eq!(events.len(), 2); // processed data plus unknown reset quality
        let json = serde_json::to_string(&*events).unwrap();
        assert!(!json.contains("Fingerprint"));
        assert!(!json.contains("signature"));
        assert!(json.contains("historical_get_transaction_aggregate"));
    }
}
