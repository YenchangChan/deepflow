# lumberjack

Async Rust implementation of the [Lumberjack v2 protocol](https://github.com/elastic/go-lumber/tree/master/lj/v2) used by Elastic Beats and Logstash.

A from-scratch port of [`elastic/go-lumber`](https://github.com/elastic/go-lumber) — v2 only, fully async, with `Ack(0)` keepalive and source-port restriction.

## Features

- Tokio-based async server and client
- Lumberjack v2 only (Window / JSON / Compressed / Ack frames)
- Built-in TLS via rustls (`tls` feature, on by default)
- Built-in zlib compression via flate2 (`compression` feature, on by default)
- Server keepalive (`Ack(0)`) while user processes a batch; client tolerates it
- Optional source-port restriction on the client (`local_port_range`) so it does not collide with business ports
- Backpressure: server `mpsc` channel naturally throttles the underlying TCP

## Quick start

### Server

```rust
use lumberjack::Server;

#[tokio::main]
async fn main() -> lumberjack::Result<()> {
    let mut server = Server::bind("0.0.0.0:5044").await?;
    while let Some(batch) = server.recv().await {
        for ev in batch.events() {
            println!("{ev}");
        }
        batch.ack();
    }
    Ok(())
}
```

### Client

```rust
use lumberjack::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> lumberjack::Result<()> {
    let mut client = Client::builder()
        .compression_level(3)
        .local_port_range(60000, 65000)
        .connect("127.0.0.1:5044")
        .await?;
    client.send(&[json!({"message": "hello"})]).await?;
    Ok(())
}
```

## Error policy

- **Protocol-layer errors** (unknown frame type, oversized length, malformed zlib, non-monotonic seq) drop the connection. Lumberjack has no resync marker, so a desynchronized stream cannot be recovered safely.
- **Payload-layer errors** (a single event whose JSON body fails to deserialize) are logged via `tracing::warn!` and the offending event is dropped from the batch; the rest of the batch is delivered normally.

## Performance

A baseline harness is provided in `examples/baseline.rs` and a head-to-head comparison against `elastic/go-lumber` lives at [`docs/benchmarks/baseline.md`](docs/benchmarks/baseline.md). At 4 concurrent clients sending 250-byte JSON events:

- **~605k events/s, 144 MiB/s** payload throughput
- **~2.0× faster** than `go-lumber` on the same workload
- **~3× less memory** (peak RSS ~8 MiB vs ~12 MiB)

Run it yourself:

```bash
cargo run --release --example baseline -- --clients 4 --duration 10
```

## Interop tests

`tests/interop.rs` runs the Rust client against a real `go-lumber` server and vice versa, using a small Go bridge binary in `bench_harness/interop/`. They are gated behind an environment variable so the regular test suite does not require Go:

```bash
LUMBERJACK_INTEROP=1 cargo test --test interop -- --test-threads=1
```

The first run builds the Go helper automatically (requires `go` on `PATH`).

## License

MIT OR Apache-2.0
