/// Minimal HTTP load tester that supports both TCP addresses and Unix socket paths.
///
/// Each "connection" in this tool is a persistent HTTP/1.1 keep-alive connection.
/// Requests are pipelined: a connection sends the next request as soon as the
/// previous one completes, keeping the connection warm for the full test duration.
///
/// Usage (TCP):   load-test http://127.0.0.1:9400 -c 100 -n 80000
/// Usage (UDS):   load-test unix:/tmp/proxy.sock   -c 100 -n 80000
use anyhow::Result;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, Request, Uri};
use hyper_util::rt::TokioIo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(name = "load-test", about = "HTTP load tester (TCP + Unix socket)")]
struct Cli {
    /// Target URL. Use http://host:port for TCP or unix:/path/to.sock for UDS.
    target: String,

    #[arg(short = 'c', long, default_value = "100")]
    connections: usize,

    #[arg(short = 'n', long, default_value = "80000")]
    requests: usize,

    #[arg(long, default_value = "5000")]
    warmup: usize,
}

const BODY: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSlot\"}";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let is_uds = cli.target.starts_with("unix:");
    let sock_path = cli.target.strip_prefix("unix:").unwrap_or("").to_string();
    let tcp_url: Uri = if is_uds {
        "http://localhost/".parse()?
    } else {
        format!("{}/", cli.target.trim_end_matches('/')).parse()?
    };
    let tcp_host = tcp_url.host().unwrap_or("localhost").to_string();
    let tcp_port = tcp_url.port_u16().unwrap_or(80);

    // Shared counters
    let sem = Arc::new(Semaphore::new(cli.connections));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let latencies: Arc<tokio::sync::Mutex<Vec<f64>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(cli.requests)));

    // Warmup
    run_batch(
        cli.warmup, &sem, &ok, &err, &latencies, is_uds, &sock_path, &tcp_host, tcp_port,
    )
    .await;
    ok.store(0, Ordering::Relaxed);
    err.store(0, Ordering::Relaxed);
    latencies.lock().await.clear();

    // Real run
    let t0 = Instant::now();
    run_batch(
        cli.requests,
        &sem,
        &ok,
        &err,
        &latencies,
        is_uds,
        &sock_path,
        &tcp_host,
        tcp_port,
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
    println!("  Errors:        {}", err.load(Ordering::Relaxed));

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_batch(
    total: usize,
    sem: &Arc<Semaphore>,
    ok: &Arc<AtomicU64>,
    err: &Arc<AtomicU64>,
    latencies: &Arc<tokio::sync::Mutex<Vec<f64>>>,
    is_uds: bool,
    sock_path: &str,
    tcp_host: &str,
    tcp_port: u16,
) {
    let mut handles = Vec::with_capacity(total);
    for _ in 0..total {
        let sem = sem.clone();
        let ok = ok.clone();
        let err = err.clone();
        let latencies = latencies.clone();
        let sock_path = sock_path.to_string();
        let tcp_host = tcp_host.to_string();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let t0 = Instant::now();
            let res = if is_uds {
                send_uds(&sock_path).await
            } else {
                send_tcp(&tcp_host, tcp_port).await
            };
            let lat_ms = t0.elapsed().as_secs_f64() * 1000.0;
            match res {
                Ok(_) => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    latencies.lock().await.push(lat_ms);
                }
                Err(_) => {
                    err.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn send_tcp(host: &str, port: u16) -> Result<()> {
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(conn);
    send_request(&mut sender, host).await
}

async fn send_uds(path: &str) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(path).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(conn);
    send_request(&mut sender, "localhost").await
}

async fn send_request(
    sender: &mut hyper::client::conn::http1::SendRequest<Full<Bytes>>,
    host: &str,
) -> Result<()> {
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("host", host)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(BODY)))?;
    let resp = sender.send_request(req).await?;
    resp.into_body().collect().await?;
    Ok(())
}
