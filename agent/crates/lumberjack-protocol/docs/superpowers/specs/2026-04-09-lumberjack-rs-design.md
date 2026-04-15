# lumberjack-rs Design Spec

**Date:** 2026-04-09
**Status:** Approved (pending user review of written spec)

## Goal

Reimplement [elastic/go-lumber](https://github.com/elastic/go-lumber) in Rust as a single crate `lumberjack`, providing a Tokio-based async client and server for the Lumberjack v2 protocol used by Elastic Beats / Logstash.

**Scope:**

- Lumberjack **v2 only**. v1 is out of scope.
- Both **server** (receiver) and **client** (sender).
- Fully async on **Tokio**.
- Built-in **TLS via rustls** (feature-gated).
- Built-in **zlib compression via flate2** with the pure-Rust `miniz_oxide` backend (feature-gated).
- **Unit tests** in each module plus **loopback integration tests** under `tests/`. No interop testing against the real go-lumber binary.

## Non-Goals

- v1 protocol (`D`/`E` frames).
- Sync (blocking) API surface.
- Auto-reconnect inside `Client` (callers handle reconnection).
- A buffered/batched async client analogous to go-lumber's `AsyncClient` (callers can layer their own queue if needed).
- A CLI binary.

## Project Structure

```
lumberjack-protocol/
├── Cargo.toml
├── src/
│   ├── lib.rs          # public re-exports + crate-level docs
│   ├── error.rs        # Error / Result
│   ├── frame.rs        # v2 frame encode/decode
│   ├── codec.rs        # tokio_util::codec Encoder/Decoder
│   ├── server.rs       # Server, Batch, accept/connection state machine
│   ├── client.rs       # Client + ClientBuilder
│   └── tls.rs          # rustls helpers (feature = "tls")
└── tests/              # loopback integration tests
    ├── roundtrip.rs
    └── compression.rs
```

### Cargo.toml

```toml
[dependencies]
tokio = { version = "1", features = ["net", "rt", "io-util", "macros", "sync", "time"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tracing = "0.1"
flate2 = { version = "1", optional = true }
tokio-rustls = { version = "0.26", optional = true }
rustls-pemfile = { version = "2", optional = true }
rustls-native-certs = { version = "0.8", optional = true }
rand = "0.8"

[features]
default = ["tls", "compression"]
tls = ["dep:tokio-rustls", "dep:rustls-pemfile", "dep:rustls-native-certs"]
compression = ["dep:flate2"]

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
tempfile = "3"
```

---

## 1. Protocol Layer (`frame.rs` + `codec.rs`)

### Frame Format

Every frame starts with a 2-byte header: `version(1B='2') + type(1B)`. Supported types:

| Type | Direction | Meaning | Payload |
|---|---|---|---|
| `'W'` window size | C→S | max events in this window | `uint32` |
| `'J'` json data | C→S | one JSON event | `uint32 seq + uint32 len + len bytes JSON` |
| `'C'` compressed | C→S | compressed payload containing N J-frames | `uint32 len + len bytes zlib` |
| `'A'` ack | S→C | ack up to seq (or `0` for keepalive) | `uint32 seq` |

All multi-byte integers are big-endian.

### `frame.rs`

```rust
pub enum Frame {
    Window(u32),
    Json { seq: u32, data: Bytes },   // raw JSON bytes, parsed lazily
    Compressed(Bytes),                // pre-decompressed inner payload bytes
    Ack(u32),
}

impl Frame {
    pub fn encode(&self, dst: &mut BytesMut);
    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>, Error>;
}
```

- `decode` returns `Ok(None)` when more bytes are needed (matches `tokio_util::codec::Decoder` semantics).
- `Compressed` is decompressed inline during decode; upper layers see only the inner bytes (not the wrapping `C` frame). This keeps the connection state machine ignorant of compression.
- Encoding is symmetric.

### `codec.rs`

Implements `tokio_util::codec::Decoder<Item = Frame>` and `Encoder<Frame>`. Both server and client wrap their stream in `Framed<S, LumberjackCodec>` and operate on `Stream<Frame>` / `Sink<Frame>` rather than raw bytes.

### Error Handling Policy

Two distinct categories:

- **Protocol-layer errors** — invalid version byte, unknown type byte, oversized length, malformed zlib, non-monotonic seq inside a window:
  - These desynchronize the byte stream irrecoverably (Lumberjack has no resync marker).
  - Action: `tracing::warn!` and **drop the connection**. The client must reconnect.
- **Payload-layer errors** — a single `J` frame whose JSON body fails to deserialize into `serde_json::Value`:
  - Frame boundaries are still well-defined (length is intact), so recovery is safe.
  - Action: `tracing::warn!` and **skip that single event**; surface the rest of the batch normally.

### Unit Tests

- Round-trip each `Frame` variant.
- Half-packet / partial-buffer behavior (`decode` returns `Ok(None)` until the full frame arrives).
- Invalid version → error.
- Invalid type → error.
- Length > `max_frame_size` → error.
- `Compressed` round-trip equivalence with the inline `J` sequence.
- Corrupted zlib payload → error.

---

## 2. Server API (`server.rs`)

### Public Types

```rust
pub struct Server {
    rx: mpsc::Receiver<Batch>,
    shutdown: Option<oneshot::Sender<()>>,  // signals accept loop on Drop
    local_addr: SocketAddr,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Server>;
    pub fn builder() -> ServerBuilder;
    pub fn local_addr(&self) -> SocketAddr;
    pub async fn recv(&mut self) -> Option<Batch>;
}

pub struct Batch {
    events: Vec<serde_json::Value>,
    ack: Option<oneshot::Sender<u32>>,      // None after ack()
    last_seq: u32,
}

impl Batch {
    pub fn events(&self) -> &[serde_json::Value];
    pub fn into_events(self) -> Vec<serde_json::Value>;
    pub fn len(&self) -> usize;
    pub fn ack(self);                       // explicit ack
}

impl Drop for Batch {
    // If ack is still Some, send last_seq via the oneshot.
    // Mirrors `defer batch.ACK()` in go-lumber.
}

pub struct ServerBuilder {
    keepalive: Option<Duration>,            // default Some(15s)
    channel_capacity: usize,                // default 128
    max_frame_size: usize,                  // default 16 MiB
    tls: Option<TlsAcceptor>,               // feature = "tls"
}

impl ServerBuilder {
    pub fn keepalive(mut self, interval: Duration) -> Self;
    pub fn no_keepalive(mut self) -> Self;
    pub fn channel_capacity(mut self, n: usize) -> Self;
    pub fn max_frame_size(mut self, n: usize) -> Self;
    pub fn tls(mut self, acceptor: TlsAcceptor) -> Self;
    pub async fn bind(self, addr: impl ToSocketAddrs) -> Result<Server>;
}
```

### Usage Example

```rust
let mut server = lumberjack::Server::bind("0.0.0.0:5044").await?;
while let Some(batch) = server.recv().await {
    for ev in batch.events() { /* process */ }
    batch.ack();
}
```

### Internal Architecture

```
TcpListener ──accept──> per-connection task
                            │  Framed<Stream, LumberjackCodec>
                            │  state machine: Window → N J/C frames → Batch
                            ▼
                       mpsc::Sender<Batch> ──> Server.rx
```

- One Tokio task per accepted connection. All connection tasks share a single `mpsc::Sender<Batch>` whose receiver is `Server.rx`.
- **Backpressure:** when the channel is full, the connection task's `send().await` blocks, which stops reading from the socket and naturally tightens the TCP receive window, propagating backpressure to the client.
- **ACK timing:** the connection task awaits the per-batch `oneshot::Receiver<u32>` after dispatching a batch, then writes `Frame::Ack(seq)`.
- **Drop fallback:** if the user drops a `Batch` without calling `ack()`, the `Drop` impl sends `last_seq` through the oneshot so the connection does not hang.
- **Graceful shutdown:** dropping `Server` fires the `shutdown` oneshot; the accept loop exits, the listener is closed, and existing connection tasks terminate as their reads return EOF.

### Connection State Machine

```
loop {
    expect Frame::Window(n)                      // anything else → protocol error → drop
    collect n J-frames (possibly inside C frames), validating monotonic seq
    parse each J payload into serde_json::Value (skip on JSON error)
    construct Batch { events, last_seq, ack: Some(oneshot_tx) }
    send Batch via mpsc (await — backpressure point)

    // Wait for user ack, with keepalive:
    loop {
        select {
            seq = &mut ack_rx => { send Ack(seq); break }
            _   = sleep(keepalive) if keepalive.is_some()
                                  => { send Ack(0); continue }
        }
    }
}
```

### Keepalive (`Ack(0)`)

When `keepalive` is configured, the connection task sends `Frame::Ack(0)` at the configured interval while waiting for the user to ack a batch. This is the standard Lumberjack v2 mechanism for telling a slow consumer's upstream client "I'm still alive, do not time out". Clients reset their idle timeout on **any** ACK frame received.

### Unit / Integration Tests

- **Unit (state machine):** drive a fake `Framed` over `tokio::io::duplex`; verify Window+J, Window+C, ACK ordering, half-packets, protocol-error disconnects.
- **Unit (keepalive):** with `keepalive(50ms)`, dispatch a batch and stall ack for ~250ms; assert the peer received multiple `Ack(0)` frames and finally the correct `Ack(seq)`.
- **Integration:** real `bind()`, real client → batch → ack → client observes ack; concurrent connections; backpressure (refuse to `recv` and verify the client's `send` blocks).

---

## 3. Client API (`client.rs`)

### Public Types

```rust
pub struct Client {
    framed: Framed<BoxedStream, LumberjackCodec>,
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
}

pub struct ClientBuilder {
    compression_level: u32,                 // default 3, 0 disables
    write_timeout: Option<Duration>,        // default None
    ack_timeout: Option<Duration>,          // default Some(30s)
    tls: Option<(TlsConnector, String)>,    // feature = "tls"
    local_port_range: Option<(u16, u16)>,   // optional source port window
}

impl ClientBuilder {
    pub fn compression_level(mut self, level: u32) -> Self;
    pub fn write_timeout(mut self, d: Duration) -> Self;
    pub fn ack_timeout(mut self, d: Duration) -> Self;
    pub fn tls(mut self, connector: TlsConnector, domain: impl Into<String>) -> Self;
    pub fn local_port_range(mut self, start: u16, end: u16) -> Self;
    pub async fn connect(self, addr: impl ToSocketAddrs) -> Result<Client>;
}

impl Client {
    pub fn builder() -> ClientBuilder;
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Client>;

    /// Send a batch and block until the server acks it.
    /// Returns the number of events acknowledged.
    pub async fn send<T: Serialize>(&mut self, events: &[T]) -> Result<u32>;

    pub async fn close(self) -> Result<()>;
}
```

### Send Flow

```
1. Serialize events into N J-frames with seq = 1..=N.
2. If compression_level > 0:
       concatenate the encoded J-frames into one buffer
       zlib-compress that buffer
       wrap as a single C frame
3. Write Window(N) followed by either the C frame or the N raw J frames.
4. Flush.
5. Receive frames in a loop until an Ack(seq) with seq >= N is received:
       - Ack(0) and partial Ack(<N): reset the per-frame timeout, keep waiting.
       - Any other frame:           protocol error, drop client.
       - EOF:                       ConnectionClosed.
6. Each send is self-contained: seq counter restarts at 1 for the next batch (mirrors go-lumber).
```

### Timeout Semantics

- **`write_timeout`** wraps the send/flush phase. On timeout → `Error::WriteTimeout`; the client becomes unusable (next call returns an error).
- **`ack_timeout`** is the **maximum gap between consecutive ACK frames**, not the wall-clock budget for an entire batch. Each iteration of the receive loop is wrapped in `tokio::time::timeout(ack_timeout, ...)`. As long as the server keeps sending `Ack(0)` keepalives within the interval, the client waits indefinitely. On timeout → `Error::AckTimeout`; client unusable.

### `local_port_range`

Optional source-port restriction so the client does not occupy ports overlapping a host's business services.

```rust
.local_port_range(60000, 65000)
```

Implementation:

- Resolve the target address to determine the address family.
- Construct a `tokio::net::TcpSocket` of the matching family.
- Pick a **random starting port** in `[start, end]` (uniform), then scan upward, wrapping at `end`.
- For each candidate, attempt `bind` to `0.0.0.0:port` (or `[::]:port`); on `EADDRINUSE` / `EADDRNOTAVAIL`, advance.
- After binding, call `connect(target)`.
- If every port in the range fails to bind, return `Error::NoLocalPortAvailable`.
- Validation: `start <= end`, otherwise `ClientBuilder::connect` returns `Error::InvalidConfig`.
- Random starting point avoids "thundering-herd" collisions when many client instances share the same range.
- TLS layers cleanly on top of the bound TCP stream — no interaction.

### Unit / Integration Tests

- **Unit (send):** drive `send` over `tokio::io::duplex`; on the peer side, decode with our own codec and assert Window + (C or J×N) bytes are correct; reply with `Ack(N)` and verify `send` returns `N`.
- **Unit (compression on/off):** both paths produce decodable batches.
- **Unit (keepalive tolerance):** peer sends several `Ack(0)` frames spaced just under `ack_timeout`, then a final `Ack(N)`; `send` should not time out.
- **Unit (ack_timeout):** peer sends nothing → `Error::AckTimeout`.
- **Unit (write_timeout):** peer never reads → `Error::WriteTimeout` (using `tokio::test` paused time).
- **Unit (local_port_range):**
  - Configure a 1-port range; verify the connected socket's local port matches.
  - Pre-occupy one port in a 2-port range; verify the client picks the other.
  - Pre-occupy the entire range; verify `Error::NoLocalPortAvailable`.
  - `start > end` → `Error::InvalidConfig`.
- **Integration:** real `Server` + real `Client` round-trip with and without compression.

---

## 4. Errors (`error.rs`)

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid frame: {0}")]
    InvalidFrame(&'static str),

    #[error("frame too large: {size} > {max}")]
    FrameTooLarge { size: usize, max: usize },

    #[error("compression error: {0}")]
    Compression(String),

    #[error("unexpected frame: {0}")]
    UnexpectedFrame(&'static str),

    #[error("seq out of order: got {got}, expected > {prev}")]
    SeqOutOfOrder { got: u32, prev: u32 },

    #[error("write timeout")]
    WriteTimeout,

    #[error("ack timeout")]
    AckTimeout,

    #[error("connection closed by peer")]
    ConnectionClosed,

    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("no local port available in configured range")]
    NoLocalPortAvailable,

    #[cfg(feature = "tls")]
    #[error("tls error: {0}")]
    Tls(#[from] tokio_rustls::rustls::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
```

Payload-layer JSON deserialization failures are **not** part of `Error`. They are logged via `tracing::warn!` and the offending event is dropped from the batch.

---

## 5. TLS (`tls.rs`, feature = "tls")

A thin convenience layer over rustls — no API reinvention.

```rust
#[cfg(feature = "tls")]
pub fn server_acceptor_from_pem(cert: &Path, key: &Path) -> Result<TlsAcceptor>;

#[cfg(feature = "tls")]
pub fn client_connector_with_native_roots() -> Result<TlsConnector>;
```

`Server` and `Client` store their stream as a type-erased `Pin<Box<dyn AsyncRead + AsyncWrite + Send + Unpin>>`. The TCP-vs-TLS branch happens once at accept/connect time; the rest of the codebase is stream-agnostic.

---

## 6. Implementation Order & Effort

1. `error.rs` — ~1h
2. `frame.rs` + unit tests — **core**, ~1d
3. `codec.rs` — ~2h
4. `server.rs` + unit tests (incl. keepalive) — ~1d
5. `client.rs` + `local_port_range` + unit tests — ~1d
6. `tls.rs` + TLS integration test — ~0.5d
7. `tests/` integration suite — ~0.5d
8. README + crate-level docs — ~2h

## Summary

Single crate `lumberjack`. v2-only Tokio implementation with rustls TLS, flate2 compression, channel-based server with `Batch::ack()` semantics and `Ack(0)` keepalive, and a synchronous-style async client supporting source-port restriction. Protocol-level errors disconnect; payload-level errors skip the offending event. Tested via per-module unit tests plus loopback integration tests.
