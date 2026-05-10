use anyhow::Result;
use clap::{Parser, Subcommand};
use rpc_plane_core::telemetry::{NoopReporter, RemoteReporter, Reporter, TelemetryEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    name = "rpc-plane",
    version,
    about = "Solana RPC proxy — intelligent multi-provider routing"
)]
struct Cli {
    #[arg(
        short,
        long,
        default_value = "rpc-plane.toml",
        global = true,
        help = "Path to config file"
    )]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the proxy (default when no subcommand given)
    Run,
    /// Validate config and test provider connectivity
    Check,
    /// Show current provider health (requires the proxy to be running)
    Status,
    /// Generate a starter config file
    Init,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run(cli.config).await,
        Command::Check => check(cli.config).await,
        Command::Status => status(cli.config).await,
        Command::Init => init(cli.config).await,
    }
}

async fn run(config_path: PathBuf) -> Result<()> {
    let config = rpc_plane_core::config::Config::load(&config_path)?;
    let listen = config.server.listen.clone();
    let metrics_listen = config.server.metrics_listen.clone();

    info!(
        listen = %listen,
        metrics_listen = %metrics_listen,
        providers = config.providers.len(),
        version = env!("CARGO_PKG_VERSION"),
        "rpc-plane starting"
    );
    for p in &config.providers {
        info!(name = %p.name, url = %p.url, weight = p.weight, "provider registered");
    }

    // Build reporter: RemoteReporter when [reporting] is configured, NoopReporter otherwise.
    let reporter: Arc<dyn Reporter> = match &config.reporting {
        Some(rc) => {
            let client = Arc::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?,
            );
            info!(endpoint = %rc.endpoint, "telemetry reporting enabled");
            Arc::new(RemoteReporter::new(rc.clone(), client))
        }
        None => Arc::new(NoopReporter),
    };

    let state = rpc_plane_core::proxy::ProxyState::new_with_reporter(config, reporter.clone());
    let proxy_router = rpc_plane_core::proxy::build_router(state.clone());
    let metrics_router = rpc_plane_core::proxy::build_metrics_router(state.clone());

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    let metrics_listener = tokio::net::TcpListener::bind(&metrics_listen).await?;

    info!("proxy listening on http://{listen}");
    info!("replace your provider URL with http://{listen} in your Solana app");
    info!("metrics listening on http://{metrics_listen}/metrics");

    // Hot-reload watcher
    let config_handle = state.config_handle();
    let monitor = state.monitor.clone();
    let client = state.client.clone();
    tokio::spawn(watch_config(config_path, config_handle, monitor, client));

    // ProviderHealth telemetry emitter — emits a snapshot for every provider every 10s.
    let health_reporter = reporter.clone();
    let health_monitor = state.monitor.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let snaps = health_monitor.snapshots().await;
            for s in &snaps {
                health_reporter.emit(TelemetryEvent::ProviderHealth {
                    provider: s.name.clone(),
                    score: s.score,
                    slot_height: s.slot_height,
                    slot_drift: s.slot_drift,
                    circuit_state: format!("{:?}", s.circuit).to_lowercase(),
                });
            }
        }
    });

    tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, metrics_router).await {
            tracing::error!("metrics server error: {e}");
        }
    });

    axum::serve(listener, proxy_router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Flush any buffered telemetry before exiting.
    reporter.flush();
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("shutdown complete");
    Ok(())
}

async fn check(config_path: PathBuf) -> Result<()> {
    let config = rpc_plane_core::config::Config::load(&config_path)?;
    println!(
        "Config OK — {} provider(s) configured\n",
        config.providers.len()
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let mut all_ok = true;
    for provider in &config.providers {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth",
        });
        match client.post(&provider.url).json(&body).send().await {
            Ok(resp) => {
                let status = resp.status();
                let marker = if status.is_success() { "OK  " } else { "WARN" };
                println!("[{marker}] {} — HTTP {status}", provider.name);
                if !status.is_success() {
                    all_ok = false;
                }
            }
            Err(e) => {
                println!("[FAIL] {} — {e}", provider.name);
                all_ok = false;
            }
        }
    }

    if all_ok {
        println!("\nAll providers reachable.");
    } else {
        warn!("one or more providers failed the connectivity check");
        std::process::exit(1);
    }
    Ok(())
}

async fn status(config_path: PathBuf) -> Result<()> {
    let config = rpc_plane_core::config::Config::load(&config_path)?;
    let url = format!("http://{}/health", config.server.listen);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let resp = client.get(&url).send().await.map_err(|e| {
        anyhow::anyhow!("could not reach proxy at {url}: {e}\nIs rpc-plane running?")
    })?;

    let body: serde_json::Value = resp.json().await?;

    let version = body["version"].as_str().unwrap_or("unknown");
    let providers = body["providers"].as_array().cloned().unwrap_or_default();

    println!("RPC Plane v{version} — {} provider(s)\n", providers.len());

    if providers.is_empty() {
        println!("No providers configured.");
        return Ok(());
    }

    // Column widths
    let name_w = providers
        .iter()
        .filter_map(|p| p["name"].as_str())
        .map(str::len)
        .max()
        .unwrap_or(4)
        .max(4);

    println!(
        "  {:<name_w$}  {:>6}  {:>12}  {:>6}  {:>10}  {}",
        "NAME", "SCORE", "SLOT", "DRIFT", "LATENCY", "CIRCUIT"
    );
    println!(
        "  {:<name_w$}  {:>6}  {:>12}  {:>6}  {:>10}  {}",
        "-".repeat(name_w),
        "------",
        "------------",
        "------",
        "----------",
        "-------"
    );

    for p in &providers {
        let name = p["name"].as_str().unwrap_or("?");
        let score = p["score"].as_f64().unwrap_or(0.0);
        let slot = p["slot"].as_u64().unwrap_or(0);
        let drift = p["slot_drift"].as_u64().unwrap_or(0);
        let latency = p["latency_ms"].as_f64().unwrap_or(0.0);
        let circuit = p["circuit"].as_str().unwrap_or("?");

        let latency_str = if latency == 0.0 {
            "—".to_string()
        } else {
            format!("{latency:.1}ms")
        };
        let slot_str = if slot == 0 {
            "—".to_string()
        } else {
            slot.to_string()
        };

        println!(
            "  {:<name_w$}  {:>6.3}  {:>12}  {:>6}  {:>10}  {}",
            name, score, slot_str, drift, latency_str, circuit
        );
    }

    Ok(())
}

async fn init(config_path: PathBuf) -> Result<()> {
    if config_path.exists() {
        anyhow::bail!(
            "config file already exists: {}\nDelete it first or choose a different path with -c",
            config_path.display()
        );
    }

    let example = include_str!("../../config.example.toml");
    std::fs::write(&config_path, example)?;
    println!("Created {}", config_path.display());
    println!("Edit the file to add your provider API keys, then run:");
    println!("  rpc-plane run");
    Ok(())
}

// ── Hot reload ────────────────────────────────────────────────────────────────

async fn watch_config(
    path: PathBuf,
    config: Arc<std::sync::RwLock<Arc<rpc_plane_core::config::Config>>>,
    monitor: rpc_plane_core::health::HealthMonitor,
    client: Arc<reqwest::Client>,
) {
    let mut last_mtime = mtime(&path);
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    ticker.tick().await; // first tick fires immediately — skip it

    loop {
        ticker.tick().await;

        let current = mtime(&path);
        if current == last_mtime {
            continue;
        }
        last_mtime = current;

        // Brief pause so editors that do write-rename don't catch a partial file.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let new_config = match rpc_plane_core::config::Config::load(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("config reload failed, keeping old config: {e}");
                continue;
            }
        };

        // Snapshot old provider map for diffing.
        let old_providers: HashMap<String, String> = config
            .read()
            .unwrap()
            .providers
            .iter()
            .map(|p| (p.name.clone(), p.url.clone()))
            .collect();

        let new_providers: HashMap<String, _> = new_config
            .providers
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        // Removed providers.
        for name in old_providers.keys() {
            if !new_providers.contains_key(name) {
                monitor.remove_provider(name);
                info!(provider = %name, "hot reload: provider removed");
            }
        }

        // Added providers + URL-changed providers (treat as remove then add).
        for (name, new_p) in &new_providers {
            match old_providers.get(name) {
                Some(old_url) if old_url == &new_p.url => {
                    // Unchanged — keep existing health state.
                }
                Some(_) => {
                    monitor.remove_provider(name);
                    monitor.add_provider(client.clone(), new_p.clone());
                    info!(provider = %name, "hot reload: provider URL updated");
                }
                None => {
                    monitor.add_provider(client.clone(), new_p.clone());
                    info!(provider = %name, "hot reload: provider added");
                }
            }
        }

        // Warn about settings that require a restart.
        {
            let old = config.read().unwrap();
            if old.server.listen != new_config.server.listen
                || old.server.metrics_listen != new_config.server.metrics_listen
            {
                warn!("server.listen / metrics_listen changed — restart required to take effect");
            }
        }

        *config.write().unwrap() = Arc::new(new_config);
        info!(path = %path.display(), "config reloaded");
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

// ── Shutdown ──────────────────────────────────────────────────────────────────

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl+C"),
        _ = terminate => info!("received SIGTERM"),
    }
}
