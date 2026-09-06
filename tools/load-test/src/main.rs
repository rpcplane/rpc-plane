/// Minimal HTTP load tester that supports TCP addresses, Unix socket paths, and
/// remote HTTPS endpoints.
///
/// The TCP and UDS paths speak plaintext HTTP/1.1 via hyper and open a **fresh
/// connection per request** — that is what the published local benchmarks in
/// `internal/performance.md` measured, and it is left unchanged so those numbers
/// stay comparable.
///
/// The `https://` path exists for one question the local paths cannot answer:
/// does HTTP/3 help? It uses reqwest built exactly as `src/proxy.rs` builds it,
/// so the transport under test is the one the proxy actually uses. Two axes
/// matter there and both are explicit flags:
///
///   --http3   QUIC instead of TCP+TLS (prior knowledge, no h2 fallback)
///   --cold    a fresh connection per request instead of a warm pooled one
///
/// QUIC's wins are a ~1-RTT handshake and loss recovery. A warm pool pays
/// neither cost, which is why `--cold` and `--http3` have to be varied
/// independently: running only the warm arms will show HTTP/3 doing nothing, and
/// running only the cold arms will overstate it.
///
/// Usage (TCP):    load-test http://127.0.0.1:9400 -c 100 -n 80000
/// Usage (UDS):    load-test unix:/tmp/proxy.sock   -c 100 -n 80000
/// Usage (remote): load-test https://mainnet.helius-rpc.com/?api-key=... \
///                   -c 8 -n 2000 --warmup 100 --rps 20 --http3
///
/// Against a real provider, keep `-n`, `-c` and `--rps` small: the defaults are
/// sized for a local dummy backend and will burn quota or trip a rate limit.
use anyhow::{bail, Result};
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, Request, Uri};
use hyper_util::rt::TokioIo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(
    name = "load-test",
    about = "HTTP load tester (TCP + Unix socket + HTTPS)"
)]
struct Cli {
    /// Target URL: http://host:port, unix:/path/to.sock, or https://host/path
    target: String,

    #[arg(short = 'c', long, default_value = "100")]
    connections: usize,

    #[arg(short = 'n', long, default_value = "80000")]
    requests: usize,

    #[arg(long, default_value = "5000")]
    warmup: usize,

    /// HTTPS only. Speak HTTP/3 (QUIC) instead of TCP+TLS. Prior knowledge, so
    /// there is no HTTP/2 fallback — if the endpoint does not serve QUIC every
    /// request fails. Check for `Alt-Svc: h3` first.
    #[arg(long)]
    http3: bool,

    /// HTTPS only. Build a fresh client per request so every request pays a full
    /// handshake. Default is a warm pooled connection, which is the shape a
    /// colocated sidecar actually runs in.
    #[arg(long)]
    cold: bool,

    /// Cap requests per second. Unset means as fast as concurrency allows. Set
    /// this against a real provider so the test does not trip a rate limit.
    #[arg(long)]
    rps: Option<u32>,

    /// JSON-RPC method to call.
    #[arg(long, default_value = "getSlot")]
    method: String,
}

#[derive(Clone, Default)]
struct Counts {
    transport: Arc<AtomicU64>,
    status: Arc<AtomicU64>,
    rate_limited: Arc<AtomicU64>,
}

impl Counts {
    fn reset(&self) {
        self.transport.store(0, Ordering::Relaxed);
        self.status.store(0, Ordering::Relaxed);
        self.rate_limited.store(0, Ordering::Relaxed);
    }
}

enum Target {
    Uds {
        path: String,
    },
    Tcp {
        host: String,
        port: u16,
    },
    Remote {
        url: String,
        client: Arc<ClientMode>,
    },
}

/// Warm holds one pooled client for the whole run; Cold rebuilds per request so
/// the handshake is inside the measured window.
struct ClientMode {
    http3: bool,
    /// `None` in cold mode: the client is rebuilt per request so the handshake
    /// lands inside the measured window.
    warm: Option<reqwest::Client>,
}

fn build_remote_client(http3: bool) -> Result<reqwest::Client> {
    // Mirrors src/proxy.rs::build_client. Kept in step with it deliberately —
    // measuring a differently-configured client would not answer the question.
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(60))
        .pool_max_idle_per_host(512)
        .pool_idle_timeout(Duration::from_secs(90));
    if http3 {
        builder = builder.http3_prior_knowledge().http3_send_grease(false);
    }
    Ok(builder.build()?)
}

impl ClientMode {
    async fn send(&self, url: &str, body: Bytes) -> Result<(), Failure> {
        let client = match &self.warm {
            Some(c) => c.clone(),
            None => build_remote_client(self.http3).map_err(|_| Failure::Transport)?,
        };
        let mut req = client
            .post(url)
            .header("content-type", "application/json")
            .header("accept", "application/json");
        // proxy.rs pins the version on every outbound request when the provider
        // has http3 set; do the same or the pooled client can pick h2.
        if self.http3 {
            req = req.version(reqwest::Version::HTTP_3);
        }
        // A transport error and an HTTP error mean completely different things
        // here: with --http3 the first usually means QUIC never established,
        // while the second is almost always the provider rate-limiting you.
        // Reporting them as one number made a rate-limited run look like a
        // failed QUIC handshake.
        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|_| Failure::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Failure::Status(status.as_u16()));
        }
        resp.bytes().await.map_err(|_| Failure::Transport)?;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// The request never completed — DNS, TCP, TLS, or a QUIC handshake that
    /// did not establish. This is what a non-h3 endpoint looks like under
    /// `--http3`, because there is no HTTP/2 fallback.
    Transport,
    /// The request completed and the server said no. 429 here means lower
    /// `--rps`, not that the transport is broken.
    Status(u16),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let body = Bytes::from(format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{}\"}}",
        cli.method
    ));

    let is_remote = cli.target.starts_with("https://");
    if (cli.http3 || cli.cold) && !is_remote {
        bail!("--http3 and --cold apply to https:// targets only");
    }

    let target = if cli.target.starts_with("unix:") {
        Target::Uds {
            path: cli.target.trim_start_matches("unix:").to_string(),
        }
    } else if is_remote {
        let mode = ClientMode {
            http3: cli.http3,
            warm: if cli.cold {
                None
            } else {
                Some(build_remote_client(cli.http3)?)
            },
        };
        Target::Remote {
            url: cli.target.clone(),
            client: Arc::new(mode),
        }
    } else {
        let uri: Uri = format!("{}/", cli.target.trim_end_matches('/')).parse()?;
        Target::Tcp {
            host: uri.host().unwrap_or("localhost").to_string(),
            port: uri.port_u16().unwrap_or(80),
        }
    };
    let target = Arc::new(target);

    let sem = Arc::new(Semaphore::new(cli.connections));
    let ok = Arc::new(AtomicU64::new(0));
    let counts = Counts::default();
    let latencies: Arc<tokio::sync::Mutex<Vec<f64>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(cli.requests)));

    run_batch(
        cli.warmup, &target, &sem, &ok, &counts, &latencies, &body, cli.rps,
    )
    .await;
    // Bail before the real run rather than printing a table of zeros — and say
    // which failure it was, because "QUIC did not establish" and "the provider
    // is throttling you" need opposite fixes.
    if cli.warmup > 0 && ok.load(Ordering::Relaxed) == 0 {
        let t = counts.transport.load(Ordering::Relaxed);
        let r = counts.rate_limited.load(Ordering::Relaxed);
        if r > 0 {
            bail!(
                "all {} warmup requests failed, {r} of them HTTP 429 — lower --rps or -c",
                cli.warmup
            );
        }
        if t > 0 && cli.http3 {
            bail!(
                "all {} warmup requests failed at the transport layer with --http3 — \
                 there is no HTTP/2 fallback, so check the endpoint advertises \
                 `Alt-Svc: h3` and that UDP/443 is not blocked on this network",
                cli.warmup
            );
        }
        bail!("all {} warmup requests failed", cli.warmup);
    }
    ok.store(0, Ordering::Relaxed);
    counts.reset();
    latencies.lock().await.clear();

    let t0 = Instant::now();
    run_batch(
        cli.requests,
        &target,
        &sem,
        &ok,
        &counts,
        &latencies,
        &body,
        cli.rps,
    )
    .await;
    let elapsed = t0.elapsed().as_secs_f64();

    let mut lats = latencies.lock().await.clone();
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = lats.len();

    let pct = |p: f64| -> f64 {
        if n == 0 {
            return 0.0;
        }
        lats[(n as f64 * p / 100.0).min((n - 1) as f64) as usize]
    };

    let avg = lats.iter().sum::<f64>() / n.max(1) as f64;
    let rps = cli.requests as f64 / elapsed;

    if is_remote {
        println!(
            "Transport:       {} / {}",
            if cli.http3 {
                "HTTP/3 (QUIC)"
            } else {
                "HTTP/2+TLS"
            },
            if cli.cold {
                "cold (handshake per request)"
            } else {
                "warm pool"
            }
        );
    }
    println!("Summary:");
    println!(
        "  Success rate:  {:.2}%",
        100.0 * ok.load(Ordering::Relaxed) as f64 / cli.requests as f64
    );
    println!("  Total:         {:.1} ms", elapsed * 1000.0);
    println!("  Requests/sec:  {rps:.1}");
    println!("  Average:       {avg:.3} ms");
    println!(
        "  Fastest:       {:.3} ms",
        lats.first().copied().unwrap_or(0.0)
    );
    println!(
        "  Slowest:       {:.3} ms",
        lats.last().copied().unwrap_or(0.0)
    );
    println!("Response time distribution:");
    for p in [50.0, 75.0, 90.0, 95.0, 99.0, 99.9_f64] {
        println!("  p{p:<5}: {:.3} ms", pct(p));
    }
    let t = counts.transport.load(Ordering::Relaxed);
    let st = counts.status.load(Ordering::Relaxed);
    let r = counts.rate_limited.load(Ordering::Relaxed);
    println!("  Errors:        {} (transport {t}, http {st})", t + st);
    if r > 0 {
        println!(
            "  Note:          {r} of the HTTP errors were 429 — the percentiles above \
             are measured over successful requests only, so a throttled run reads \
             faster than it is. Lower --rps."
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_batch(
    total: usize,
    target: &Arc<Target>,
    sem: &Arc<Semaphore>,
    ok: &Arc<AtomicU64>,
    counts: &Counts,
    latencies: &Arc<tokio::sync::Mutex<Vec<f64>>>,
    body: &Bytes,
    rps: Option<u32>,
) {
    let gap = rps.map(|r| Duration::from_secs_f64(1.0 / r.max(1) as f64));
    let mut handles = Vec::with_capacity(total);
    for _ in 0..total {
        if let Some(g) = gap {
            tokio::time::sleep(g).await;
        }
        let sem = sem.clone();
        let ok = ok.clone();
        let err_transport = counts.transport.clone();
        let err_status = counts.status.clone();
        let err_429 = counts.rate_limited.clone();
        let latencies = latencies.clone();
        let target = target.clone();
        let body = body.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let t0 = Instant::now();
            let res = match &*target {
                Target::Uds { path } => send_uds(path, body).await.map_err(|_| Failure::Transport),
                Target::Tcp { host, port } => send_tcp(host, *port, body)
                    .await
                    .map_err(|_| Failure::Transport),
                Target::Remote { url, client } => client.send(url, body).await,
            };
            let lat_ms = t0.elapsed().as_secs_f64() * 1000.0;
            match res {
                Ok(_) => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    latencies.lock().await.push(lat_ms);
                }
                Err(Failure::Transport) => {
                    err_transport.fetch_add(1, Ordering::Relaxed);
                }
                Err(Failure::Status(code)) => {
                    err_status.fetch_add(1, Ordering::Relaxed);
                    if code == 429 {
                        err_429.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn send_tcp(host: &str, port: u16, body: Bytes) -> Result<()> {
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(conn);
    send_request(&mut sender, host, body).await
}

async fn send_uds(path: &str, body: Bytes) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(path).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(conn);
    send_request(&mut sender, "localhost", body).await
}

async fn send_request(
    sender: &mut hyper::client::conn::http1::SendRequest<Full<Bytes>>,
    host: &str,
    body: Bytes,
) -> Result<()> {
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("host", host)
        .header("content-type", "application/json")
        .body(Full::new(body))?;
    let resp = sender.send_request(req).await?;
    resp.into_body().collect().await?;
    Ok(())
}
