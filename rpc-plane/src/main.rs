use anyhow::Result;
use clap::{Parser, Subcommand};
use rpc_plane_core::telemetry::{NoopReporter, RemoteReporter, Reporter, TelemetryEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

fn is_unix_path(s: &str) -> bool {
    s.starts_with('/') || s.starts_with("./")
}

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let worker_threads = match cli.command {
        Some(Command::Run) | None => peek_worker_threads(&cli.config),
        _ => None,
    };

    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = worker_threads {
        rt.worker_threads(n);
    }
    let rt = rt.enable_all().build()?;

    rt.block_on(async move {
        match cli.command.unwrap_or(Command::Run) {
            Command::Run => run(cli.config).await,
            Command::Check => check(cli.config).await,
            Command::Status => status(cli.config).await,
            Command::Init => init(cli.config).await,
        }
    })
}

fn peek_worker_threads(path: &Path) -> Option<usize> {
    #[derive(serde::Deserialize, Default)]
    struct Peek {
        #[serde(default)]
        server: PeekServer,
    }
    #[derive(serde::Deserialize, Default)]
    struct PeekServer {
        worker_threads: Option<usize>,
    }
    let raw = std::fs::read_to_string(path).ok()?;
    toml::from_str::<Peek>(&raw).ok()?.server.worker_threads
}

async fn run(config_path: PathBuf) -> Result<()> {
    #[cfg(unix)]
    raise_nofile_limit();
    let config = rpc_plane_core::config::Config::load(&config_path)?;
    let listen = config.server.listen.clone();
    let metrics_listen = config.server.metrics_listen.clone();
    let listen_backlog = config.server.listen_backlog;

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

    // Background tasks common to both TCP and UDS paths.
    let config_handle = state.config_handle();
    let monitor = state.monitor.clone();
    let clients = state.clients_handle();
    tokio::spawn(watch_config(config_path, config_handle, monitor, clients));

    let health_reporter = reporter.clone();
    let health_monitor = state.monitor.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let snaps = health_monitor.snapshots();
            for s in &snaps {
                health_reporter.emit(TelemetryEvent::ProviderHealth {
                    provider: s.name.to_string(),
                    score: s.score,
                    slot_height: s.slot_height,
                    slot_drift: s.slot_drift,
                    circuit_state: format!("{:?}", s.circuit).to_lowercase(),
                });
            }
        }
    });

    let metrics_listener = tcp_listen(&metrics_listen, listen_backlog).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, metrics_router).await {
            tracing::error!("metrics server error: {e}");
        }
    });

    // Serve on unix socket or TCP depending on the listen address.
    if is_unix_path(&listen) {
        #[cfg(not(unix))]
        anyhow::bail!("unix socket paths are only supported on unix");

        #[cfg(unix)]
        {
            info!("proxy listening on unix:{listen}");
            info!("configure your Solana app to connect via unix socket at {listen}");
            info!("metrics listening on http://{metrics_listen}/metrics");
            let listener = unix_listen(&listen)?;
            serve_unix(listener, proxy_router).await?;
        }
    } else {
        let listener = tcp_listen(&listen, listen_backlog).await?;
        info!("proxy listening on http://{listen}");
        info!("replace your provider URL with http://{listen} in your Solana app");
        info!("metrics listening on http://{metrics_listen}/metrics");
        axum::serve(listener, proxy_router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }

    // Flush any buffered telemetry before exiting.
    reporter.flush();
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("shutdown complete");
    Ok(())
}

async fn check(config_path: PathBuf) -> Result<()> {
    // Re-expand the raw file first so we can flag references to unset env vars
    // (e.g. a typo'd `${HELIUS_API_KY}`) that silently collapse to empty and
    // would otherwise show up only as runtime 401s.
    if let Ok(raw) = std::fs::read_to_string(&config_path) {
        let (_, unset_vars) = rpc_plane_core::config::expand_env_vars(&raw);
        for var in &unset_vars {
            println!(
                "[WARN] environment variable `${var}` is unset — it expanded to an empty \
                 string; any provider URL using it is missing that value"
            );
        }
        if !unset_vars.is_empty() {
            println!();
        }
    }

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
    let listen = &config.server.listen;

    if is_unix_path(listen) {
        println!("Proxy is listening on a unix socket. To check health:");
        println!("  curl --unix-socket {listen} http://localhost/health | jq");
        return Ok(());
    }

    let url = format!("http://{listen}/health");

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
        "  {:<name_w$}  {:>6}  {:>12}  {:>6}  {:>10}  CIRCUIT",
        "NAME", "SCORE", "SLOT", "DRIFT", "LATENCY"
    );
    println!(
        "  {:<name_w$}  {:>6}  {:>12}  {:>6}  {:>10}  -------",
        "-".repeat(name_w),
        "------",
        "------------",
        "------",
        "----------"
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
    config: Arc<parking_lot::RwLock<Arc<rpc_plane_core::config::Config>>>,
    monitor: rpc_plane_core::health::HealthMonitor,
    clients: rpc_plane_core::proxy::Clients,
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

        // Snapshot old config for diffing (clone out so we don't hold the lock).
        let (old_providers, old_server) = {
            let old = config.read();
            let providers: HashMap<String, rpc_plane_core::config::ProviderConfig> = old
                .providers
                .iter()
                .map(|p| (p.name.clone(), p.clone()))
                .collect();
            (providers, old.server.clone())
        };

        // pool_max_idle_per_host feeds every outbound client, so a change forces
        // a full client rebuild — otherwise the new pool size never takes effect.
        let pool_size_changed =
            old_server.pool_max_idle_per_host != new_config.server.pool_max_idle_per_host;
        if pool_size_changed {
            info!(
                old = old_server.pool_max_idle_per_host,
                new = new_config.server.pool_max_idle_per_host,
                "hot reload: pool_max_idle_per_host changed — rebuilding all provider clients"
            );
        }

        let new_providers: HashMap<String, _> = new_config
            .providers
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        // Removed providers.
        for name in old_providers.keys() {
            if !new_providers.contains_key(name) {
                clients.write().remove(name);
                monitor.remove_provider(name);
                info!(provider = %name, "hot reload: provider removed");
            }
        }

        // Added providers + providers whose client inputs changed. The reqwest
        // client is rebuilt whenever its inputs change: the URL, the `http3`
        // flag, or the global pool size. weight is routing-only and is picked up
        // live when the config swaps below, so it needs no rebuild.
        for (name, new_p) in &new_providers {
            let Some(reason) =
                client_rebuild_reason(old_providers.get(name), new_p, pool_size_changed)
            else {
                continue; // client inputs unchanged — keep existing health state
            };

            // Rebuild = remove-then-add for an existing provider; plain add otherwise.
            if old_providers.contains_key(name) {
                clients.write().remove(name);
                monitor.remove_provider(name);
            }
            let client = Arc::new(rpc_plane_core::proxy::build_client(
                new_p,
                new_config.server.pool_max_idle_per_host,
            ));
            clients.write().insert(name.clone(), client.clone());
            monitor.add_provider(client, new_p.clone());
            info!(provider = %name, reason, "hot reload: provider client (re)built");
        }

        // Warn about settings that need a restart to take effect (the runtime
        // and listening sockets are already built).
        if old_server.listen != new_config.server.listen
            || old_server.metrics_listen != new_config.server.metrics_listen
            || old_server.listen_backlog != new_config.server.listen_backlog
            || old_server.worker_threads != new_config.server.worker_threads
        {
            warn!(
                "a restart-only server setting changed (listen / metrics_listen / \
                 listen_backlog / worker_threads) — restart required to take effect"
            );
        }

        *config.write() = Arc::new(new_config);
        info!(path = %path.display(), "config reloaded");
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Decide whether a provider's outbound reqwest client must be rebuilt on hot
/// reload. Returns a short human-readable reason, or `None` when none of the
/// client's inputs (URL, `http3`, or the global pool size) changed. `weight` is
/// deliberately excluded: it only affects routing, which reads the live config.
fn client_rebuild_reason(
    old: Option<&rpc_plane_core::config::ProviderConfig>,
    new: &rpc_plane_core::config::ProviderConfig,
    pool_size_changed: bool,
) -> Option<&'static str> {
    match old {
        None => Some("added"),
        Some(_) if pool_size_changed => Some("pool size changed"),
        Some(o) if o.url != new.url => Some("URL updated"),
        Some(o) if o.http3 != new.http3 => Some("http3 toggled"),
        Some(_) => None,
    }
}

// ── Networking helpers ────────────────────────────────────────────────────────

async fn tcp_listen(addr: &str, backlog: u32) -> Result<tokio::net::TcpListener> {
    let addr: std::net::SocketAddr = addr.parse()?;
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    #[cfg(unix)]
    socket.set_reuseport(true)?;
    socket.bind(addr)?;
    Ok(socket.listen(backlog)?)
}

#[cfg(unix)]
fn unix_listen(path: &str) -> Result<tokio::net::UnixListener> {
    // Remove a stale socket file so re-binding works without a restart.
    let _ = std::fs::remove_file(path);
    Ok(tokio::net::UnixListener::bind(path)?)
}

// axum 0.7's serve() only accepts TcpListener, so UDS connections are served
// through hyper directly. GracefulShutdown coordinates in-flight draining.
#[cfg(unix)]
async fn serve_unix(
    listener: tokio::net::UnixListener,
    router: axum::Router,
) -> std::io::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::server::graceful::GracefulShutdown;
    use tower::Service as _;

    let builder = Builder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    let mut sig = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, _addr) = match res {
                    Ok(v) => v,
                    Err(e) => { tracing::error!("unix socket accept error: {e}"); continue; }
                };
                let io = TokioIo::new(stream);
                let app = router.clone();
                let conn = builder.serve_connection_with_upgrades(
                    io,
                    hyper::service::service_fn(move |req| app.clone().call(req)),
                );
                let conn = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("unix socket connection error: {e}");
                    }
                });
            }
            _ = &mut sig => break,
        }
    }

    graceful.shutdown().await;
    Ok(())
}

// ── System limits ─────────────────────────────────────────────────────────────

#[cfg(unix)]
fn raise_nofile_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 && rlim.rlim_cur < 65535 {
            rlim.rlim_cur = rlim.rlim_max.min(65535);
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                tracing::warn!(
                    limit = rlim.rlim_cur,
                    "could not raise fd limit; set LimitNOFILE=65535 in the systemd unit"
                );
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rpc_plane_core::config::ProviderConfig;

    fn provider(url: &str, http3: bool) -> ProviderConfig {
        ProviderConfig {
            name: "p".to_string(),
            url: url.to_string(),
            weight: 1,
            http3,
            methods: None,
        }
    }

    #[test]
    fn rebuild_when_provider_added() {
        let new = provider("http://a", false);
        assert_eq!(client_rebuild_reason(None, &new, false), Some("added"));
    }

    #[test]
    fn no_rebuild_when_inputs_unchanged() {
        let old = provider("http://a", false);
        let new = provider("http://a", false);
        assert_eq!(client_rebuild_reason(Some(&old), &new, false), None);
    }

    #[test]
    fn rebuild_when_http3_toggled() {
        let old = provider("http://a", false);
        let new = provider("http://a", true);
        assert_eq!(
            client_rebuild_reason(Some(&old), &new, false),
            Some("http3 toggled")
        );
    }

    #[test]
    fn rebuild_when_url_changed() {
        let old = provider("http://a", false);
        let new = provider("http://b", false);
        assert_eq!(
            client_rebuild_reason(Some(&old), &new, false),
            Some("URL updated")
        );
    }

    #[test]
    fn rebuild_all_when_pool_size_changed() {
        // Identical provider, but a global pool-size change forces a rebuild.
        let old = provider("http://a", false);
        let new = provider("http://a", false);
        assert_eq!(
            client_rebuild_reason(Some(&old), &new, true),
            Some("pool size changed")
        );
    }

    #[test]
    fn weight_change_alone_does_not_rebuild() {
        let old = provider("http://a", false);
        let mut new = provider("http://a", false);
        new.weight = 99;
        assert_eq!(client_rebuild_reason(Some(&old), &new, false), None);
    }
}
