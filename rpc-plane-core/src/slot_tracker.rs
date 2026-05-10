use crate::config::ProviderConfig;
use crate::health::ProviderHealth;
use crate::metrics::Metrics;
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub(crate) async fn slot_poll_loop(
    state: Arc<ProviderHealth>,
    client: Arc<Client>,
    provider: ProviderConfig,
    interval_ms: u64,
    metrics: Metrics,
    stop: Arc<AtomicBool>,
) {
    let interval = Duration::from_millis(interval_ms);
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        match get_slot(&client, &provider.url).await {
            Ok(slot) => {
                state.update_slot(slot).await;
                metrics.record_probe(&provider.name, "slot", "ok");
                debug!(provider = %provider.name, slot, "slot tracker update");
            }
            Err(e) => {
                metrics.record_probe(&provider.name, "slot", "error");
                warn!(provider = %provider.name, error = %e, "slot tracker probe failed");
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn get_slot(client: &Client, url: &str) -> anyhow::Result<u64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "processed"}]
    });
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .timeout(Duration::from_secs(5))
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    let json: Value = resp.json().await?;
    json["result"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("unexpected getSlot response: {}", json))
}
