use crate::config::ReportingConfig;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, warn};

// ── Event types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TelemetryEvent {
    Request {
        id: String,
        method: String,
        provider: String,
        latency_ms: f64,
        /// "ok" | "error"
        status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        commitment: Option<String>,
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
    CacheEvent {
        method: String,
        hit: bool,
    },
}

// ── Reporter trait ────────────────────────────────────────────────────────────

pub trait Reporter: Send + Sync {
    /// Emit an event. Must never block the caller — implementations that do
    /// async work must buffer internally.
    fn emit(&self, event: TelemetryEvent);

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

// ── RemoteReporter ────────────────────────────────────────────────────────────

/// Buffers events in a bounded channel and flushes batches to a remote endpoint
/// on a configurable interval or when the buffer reaches `batch_size`.
///
/// `emit` is always non-blocking: if the channel is full the event is silently
/// dropped rather than stalling the proxy hot path.
pub struct RemoteReporter {
    tx: mpsc::Sender<TelemetryEvent>,
    flush_signal: Arc<Notify>,
}

impl RemoteReporter {
    /// Spawns the background flush task. Must be called inside a Tokio runtime.
    pub fn new(config: ReportingConfig, client: Arc<Client>) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let flush_signal = Arc::new(Notify::new());

        tokio::spawn(flush_task(rx, flush_signal.clone(), config, client));

        Self { tx, flush_signal }
    }
}

impl Reporter for RemoteReporter {
    fn emit(&self, event: TelemetryEvent) {
        // try_send never blocks; silently drop if the buffer is full.
        let _ = self.tx.try_send(event);
    }

    fn flush(&self) {
        self.flush_signal.notify_one();
    }
}

// ── Background flush task ─────────────────────────────────────────────────────

async fn flush_task(
    mut rx: mpsc::Receiver<TelemetryEvent>,
    flush_signal: Arc<Notify>,
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
                if !batch.is_empty() {
                    send_batch(&client, &config, &mut batch).await;
                }
            }
            _ = flush_signal.notified() => {
                // Drain everything currently in the channel then send.
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

    struct RecordingReporter {
        events: Arc<Mutex<Vec<TelemetryEvent>>>,
        flush_count: Arc<AtomicUsize>,
    }

    impl RecordingReporter {
        fn new() -> (Self, Arc<Mutex<Vec<TelemetryEvent>>>, Arc<AtomicUsize>) {
            let events = Arc::new(Mutex::new(Vec::new()));
            let flush_count = Arc::new(AtomicUsize::new(0));
            let reporter = Self {
                events: events.clone(),
                flush_count: flush_count.clone(),
            };
            (reporter, events, flush_count)
        }
    }

    impl Reporter for RecordingReporter {
        fn emit(&self, event: TelemetryEvent) {
            self.events.lock().unwrap().push(event);
        }
        fn flush(&self) {
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn noop_reporter_is_zero_cost() {
        let r = NoopReporter;
        r.emit(TelemetryEvent::CacheEvent {
            method: "getSlot".into(),
            hit: true,
        });
        r.flush();
        // no panic = pass
    }

    #[test]
    fn recording_reporter_captures_events() {
        let (reporter, events, flush_count) = RecordingReporter::new();
        reporter.emit(TelemetryEvent::Request {
            id: "abc".into(),
            method: "getSlot".into(),
            provider: "helius".into(),
            latency_ms: 12.5,
            status: "ok".into(),
            commitment: None,
        });
        reporter.flush();

        let locked = events.lock().unwrap();
        assert_eq!(locked.len(), 1);
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
    fn request_event_skips_none_fields() {
        let ev = TelemetryEvent::Request {
            id: "x".into(),
            method: "getSlot".into(),
            provider: "p".into(),
            latency_ms: 1.0,
            status: "ok".into(),
            commitment: None,
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert!(json.get("commitment").is_none());
        assert!(json.get("estimated_cost").is_none());
    }
}
