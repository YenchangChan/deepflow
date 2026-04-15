//! Interop tests against the real `elastic/go-lumber` reference implementation.
//!
//! These tests are gated behind the `LUMBERJACK_INTEROP` environment variable
//! so that ordinary `cargo test` runs do not require a Go toolchain.
//!
//! To run them:
//!
//!     LUMBERJACK_INTEROP=1 cargo test --test interop -- --test-threads=1
//!
//! The test build will invoke `go build` once to produce
//! `bench_harness/interop/interop`. Subsequent runs reuse the binary.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::{json, Value};

fn interop_enabled() -> bool {
    std::env::var("LUMBERJACK_INTEROP").map(|v| v == "1").unwrap_or(false)
}

fn interop_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_harness/interop")
}

/// Build the Go interop helper if it does not yet exist. Idempotent.
fn ensure_interop_binary() -> PathBuf {
    let dir = interop_dir();
    let bin = dir.join("interop");
    if !bin.exists() {
        let status = Command::new("go")
            .args(["build", "-o", "interop", "."])
            .current_dir(&dir)
            .status()
            .expect("failed to spawn `go build` — is Go installed?");
        assert!(status.success(), "go build failed");
    }
    bin
}

#[tokio::test(flavor = "multi_thread")]
async fn rust_client_to_go_server() {
    if !interop_enabled() {
        eprintln!("skipping (set LUMBERJACK_INTEROP=1 to run)");
        return;
    }
    let bin = ensure_interop_binary();

    // Spawn the Go server, expecting 3 events.
    let mut go = Command::new(&bin)
        .args(["--mode", "server", "--expect", "3"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn go server");

    // Read the announced port from stdout.
    let stdout = go.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let port: u16 = line
        .trim()
        .strip_prefix("PORT=")
        .expect("expected PORT=<n>")
        .parse()
        .unwrap();

    // Send 3 events from our Rust client.
    let mut client = Client::builder()
        .compression_level(0)
        .ack_timeout(Duration::from_secs(5))
        .connect(format!("127.0.0.1:{port}"))
        .await
        .expect("rust client connect");
    let events = vec![
        json!({"i": 0, "src": "rust"}),
        json!({"i": 1, "src": "rust"}),
        json!({"i": 2, "src": "rust"}),
    ];
    let n = client.send(&events).await.expect("send");
    assert_eq!(n, 3);
    drop(client);

    // Wait for the EVENTS=... line.
    let mut events_line = String::new();
    reader.read_line(&mut events_line).unwrap();
    let received: Vec<Value> = serde_json::from_str(
        events_line
            .trim()
            .strip_prefix("EVENTS=")
            .expect("expected EVENTS=<json>"),
    )
    .expect("parse received events");
    assert_eq!(received.len(), 3);
    for (i, ev) in received.iter().enumerate() {
        assert_eq!(ev["i"], i as i64);
        assert_eq!(ev["src"], "rust");
    }

    let _ = go.wait();
}

#[tokio::test(flavor = "multi_thread")]
async fn go_client_to_rust_server() {
    if !interop_enabled() {
        eprintln!("skipping (set LUMBERJACK_INTEROP=1 to run)");
        return;
    }
    let bin = ensure_interop_binary();

    let mut server = Server::builder()
        .no_keepalive()
        .bind("127.0.0.1:0")
        .await
        .expect("rust server bind");
    let addr = server.local_addr();

    let go = Command::new(&bin)
        .args([
            "--mode",
            "client",
            "--addr",
            &format!("127.0.0.1:{}", addr.port()),
            "--count",
            "5",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn go client");

    let mut received = Vec::new();
    while received.len() < 5 {
        let batch = server
            .recv()
            .await
            .expect("server channel closed before all events");
        for ev in batch.events() {
            received.push(ev.clone());
        }
        batch.ack();
    }

    let status = go.wait_with_output().expect("wait go client");
    assert!(status.status.success(), "go client failed");

    assert_eq!(received.len(), 5);
    for (i, ev) in received.iter().enumerate() {
        assert_eq!(ev["i"], i as i64);
        assert_eq!(ev["src"], "go-lumber");
    }
}
