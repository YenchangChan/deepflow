//! Throughput baseline harness for `lumberjack`.
//!
//! Spawns one in-process server and N concurrent client tasks. Each client
//! sends batches of synthetic JSON events as fast as possible for the
//! requested duration. After the run, the harness prints aggregate
//! events/sec, MB/sec, CPU%, and peak RSS.
//!
//! Usage:
//!     cargo run --release --example baseline -- \
//!         --clients 4 --duration 10 --event-size 250 --batch 128
//!
//! All knobs default to sensible values, see [`Args::parse`].

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lumberjack::{Client, Server};
use serde_json::{json, Value};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
struct Args {
    clients: usize,
    duration: Duration,
    event_size: usize,
    batch: usize,
}

impl Args {
    fn parse() -> Self {
        let mut clients = 1usize;
        let mut duration_s = 10u64;
        let mut event_size = 250usize;
        let mut batch = 128usize;
        let mut args = std::env::args().skip(1);
        while let Some(a) = args.next() {
            match a.as_str() {
                "--clients" => clients = args.next().unwrap().parse().unwrap(),
                "--duration" => duration_s = args.next().unwrap().parse().unwrap(),
                "--event-size" => event_size = args.next().unwrap().parse().unwrap(),
                "--batch" => batch = args.next().unwrap().parse().unwrap(),
                "--help" | "-h" => {
                    eprintln!("usage: baseline [--clients N] [--duration S] [--event-size B] [--batch N]");
                    std::process::exit(0);
                }
                other => panic!("unknown arg: {other}"),
            }
        }
        Self {
            clients,
            duration: Duration::from_secs(duration_s),
            event_size,
            batch,
        }
    }
}

/// One synthetic event padded to roughly `target_size` bytes when serialized.
fn make_event(target_size: usize) -> Value {
    // The skeleton object is ~80 bytes; pad `msg` so the total reaches target.
    let skeleton =
        r#"{"timestamp":"2026-04-09T08:30:00Z","level":"info","host":"server-01","msg":""}"#;
    let pad = target_size.saturating_sub(skeleton.len());
    json!({
        "timestamp": "2026-04-09T08:30:00Z",
        "level": "info",
        "host": "server-01",
        "msg": "x".repeat(pad),
    })
}

#[cfg(unix)]
fn cpu_time() -> Duration {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        let user = Duration::new(ru.ru_utime.tv_sec as u64, (ru.ru_utime.tv_usec as u32) * 1000);
        let sys = Duration::new(ru.ru_stime.tv_sec as u64, (ru.ru_stime.tv_usec as u32) * 1000);
        user + sys
    }
}

#[cfg(not(unix))]
fn cpu_time() -> Duration {
    Duration::ZERO
}

/// Returns peak resident set size in KiB. Linux-only (reads /proc/self/status).
fn peak_rss_kib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .trim()
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    eprintln!("baseline: {args:?}");

    // Pre-build one batch and clone it per send. Both impls do this so the
    // measurement reflects protocol/codec, not JSON construction.
    let batch: Vec<Value> = (0..args.batch).map(|_| make_event(args.event_size)).collect();
    let serialized_event_bytes = serde_json::to_vec(&batch[0]).unwrap().len();
    eprintln!("event JSON size: {serialized_event_bytes} bytes");

    // ----- Server (drains and acks immediately) -----
    let mut server = Server::builder()
        .no_keepalive()
        .channel_capacity(2048)
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr();
    let server_task = tokio::spawn(async move {
        while let Some(b) = server.recv().await {
            b.ack();
        }
    });

    // ----- Spawn clients -----
    let stop = Arc::new(AtomicBool::new(false));
    let total_events = Arc::new(AtomicU64::new(0));
    let total_batches = Arc::new(AtomicU64::new(0));

    let mut clients_set: JoinSet<()> = JoinSet::new();
    for _ in 0..args.clients {
        let mut client = Client::builder()
            .compression_level(0)
            .ack_timeout(Duration::from_secs(30))
            .connect(addr)
            .await
            .unwrap();
        let stop = stop.clone();
        let total_events = total_events.clone();
        let total_batches = total_batches.clone();
        let batch = batch.clone();
        clients_set.spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Err(e) = client.send(&batch).await {
                    eprintln!("client error: {e}");
                    return;
                }
                total_events.fetch_add(batch.len() as u64, Ordering::Relaxed);
                total_batches.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // ----- Steady-state measurement window -----
    // Brief warmup so connection setup doesn't pollute the numbers.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let cpu_start = cpu_time();
    let events_start = total_events.load(Ordering::Relaxed);
    let wall_start = Instant::now();

    tokio::time::sleep(args.duration).await;

    let wall = wall_start.elapsed();
    let cpu = cpu_time() - cpu_start;
    let events = total_events.load(Ordering::Relaxed) - events_start;
    let batches = total_batches.load(Ordering::Relaxed);

    stop.store(true, Ordering::Relaxed);
    while clients_set.join_next().await.is_some() {}
    drop(server_task);

    // ----- Report -----
    let secs = wall.as_secs_f64();
    let eps = events as f64 / secs;
    let mbps = (events as f64 * serialized_event_bytes as f64) / secs / 1_048_576.0;
    let cpu_pct = cpu.as_secs_f64() / secs * 100.0;
    let rss_mib = peak_rss_kib() as f64 / 1024.0;

    println!("---");
    println!("clients         : {}", args.clients);
    println!("duration_s      : {:.3}", secs);
    println!("event_bytes     : {serialized_event_bytes}");
    println!("batch_size      : {}", args.batch);
    println!("total_events    : {events}");
    println!("total_batches   : {batches}");
    println!("events_per_sec  : {:.0}", eps);
    println!("payload_MiB_per_s: {:.2}", mbps);
    println!("cpu_total_pct   : {:.1}", cpu_pct);
    println!("peak_rss_MiB    : {:.1}", rss_mib);
}
