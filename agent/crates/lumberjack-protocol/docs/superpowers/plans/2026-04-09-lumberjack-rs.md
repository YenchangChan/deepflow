# lumberjack-rs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reimplement elastic/go-lumber in Rust as a single crate `lumberjack`, providing a Tokio-based async server and client for the Lumberjack v2 protocol with TLS, zlib compression, ACK keepalive, and source-port restriction.

**Architecture:** Single crate, fully async on Tokio. Frame layer (`frame.rs`) is pure functional encode/decode wrapped by a `tokio_util::codec` adapter (`codec.rs`). Server uses one task per connection feeding `Batch` values into an `mpsc` channel; the user calls `Batch::ack()` to release the connection task to write the final `Ack(seq)`. Client is a synchronous-style async API: `send(events).await` blocks until acked, with `Ack(0)` keepalive tolerance. Both ends share a type-erased boxed `AsyncRead+AsyncWrite` stream so plain TCP and TLS share one code path.

**Tech Stack:** Rust 2021, Tokio, tokio-util, bytes, serde, serde_json, thiserror, tracing, flate2 (pure-Rust backend), tokio-rustls.

**Reference spec:** `docs/superpowers/specs/2026-04-09-lumberjack-rs-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Crate metadata, dependencies, features |
| `src/lib.rs` | Module declarations + public re-exports + crate-level docs |
| `src/error.rs` | `Error` enum and `Result` alias |
| `src/frame.rs` | `Frame` enum, `encode`, `decode`, format constants |
| `src/codec.rs` | `LumberjackCodec` implementing `tokio_util::codec::{Encoder, Decoder}` |
| `src/server.rs` | `Server`, `ServerBuilder`, `Batch`, accept loop, per-connection state machine |
| `src/client.rs` | `Client`, `ClientBuilder`, send flow, ACK receive loop, source-port binding |
| `src/tls.rs` | rustls helpers (feature = "tls") |
| `tests/roundtrip.rs` | Loopback integration test: real Server + real Client |
| `tests/compression.rs` | Loopback test exercising compressed batches |

---

## Task 1: Project skeleton

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `.gitignore`

- [ ] **Step 1: Initialize git**

```bash
cd /home/chenyc/src/opensource/lumberjack-protocol
git init
```

- [ ] **Step 2: Write `.gitignore`**

```
/target
Cargo.lock
```

- [ ] **Step 3: Write `Cargo.toml`**

```toml
[package]
name = "lumberjack"
version = "0.1.0"
edition = "2021"
description = "Async Rust implementation of the Lumberjack v2 protocol (Elastic Beats / Logstash)."
license = "MIT OR Apache-2.0"
repository = "https://github.com/your-org/lumberjack-protocol"

[dependencies]
tokio = { version = "1", features = ["net", "rt", "io-util", "macros", "sync", "time"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tracing = "0.1"
futures-util = "0.3"
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
rcgen = "0.13"
```

- [ ] **Step 4: Write `src/lib.rs` (placeholder)**

```rust
//! Async Rust implementation of the Lumberjack v2 protocol.
//!
//! See the crate README for usage examples.

pub mod error;
pub mod frame;
pub mod codec;
pub mod server;
pub mod client;

#[cfg(feature = "tls")]
pub mod tls;

pub use error::{Error, Result};
pub use frame::Frame;
pub use server::{Batch, Server, ServerBuilder};
pub use client::{Client, ClientBuilder};
```

- [ ] **Step 5: Create empty module files so the crate compiles after Task 2**

```bash
touch src/error.rs src/frame.rs src/codec.rs src/server.rs src/client.rs
```

- [ ] **Step 6: Verify `cargo check` fails for the right reason (modules empty)**

Run: `cargo check`
Expected: errors about missing items in empty modules. This is fine — the next task introduces them.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml .gitignore src/
git commit -m "chore: initialize lumberjack crate skeleton"
```

---

## Task 2: Error type

**Files:**
- Modify: `src/error.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/error.rs`:

```rust
use std::error::Error as StdError;

use thiserror::Error;

#[derive(Debug, Error)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts_via_from() {
        let io = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        let err: Error = io.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.source().is_some());
    }

    #[test]
    fn display_format_includes_context() {
        let err = Error::FrameTooLarge { size: 100, max: 10 };
        assert_eq!(err.to_string(), "frame too large: 100 > 10");
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib error`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add Error enum and Result alias"
```

---

## Task 3: Frame constants and `Window` frame

**Files:**
- Modify: `src/frame.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/frame.rs`:

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

pub const PROTOCOL_VERSION: u8 = b'2';

pub const TYPE_WINDOW: u8 = b'W';
pub const TYPE_JSON: u8 = b'J';
pub const TYPE_COMPRESSED: u8 = b'C';
pub const TYPE_ACK: u8 = b'A';

/// Default ceiling for any single frame's payload, in bytes.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Window(u32),
    Json { seq: u32, data: Bytes },
    Compressed(Bytes),
    Ack(u32),
}

impl Frame {
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            Frame::Window(n) => {
                dst.reserve(2 + 4);
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_WINDOW);
                dst.put_u32(*n);
            }
            Frame::Json { .. } | Frame::Compressed(_) | Frame::Ack(_) => {
                unimplemented!("added in later tasks")
            }
        }
    }

    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>> {
        Self::decode_with_limit(src, DEFAULT_MAX_FRAME_SIZE)
    }

    pub fn decode_with_limit(src: &mut BytesMut, max_frame_size: usize) -> Result<Option<Frame>> {
        if src.len() < 2 {
            return Ok(None);
        }
        let version = src[0];
        if version != PROTOCOL_VERSION {
            return Err(Error::InvalidFrame("unsupported protocol version"));
        }
        let ty = src[1];
        match ty {
            TYPE_WINDOW => {
                if src.len() < 2 + 4 {
                    return Ok(None);
                }
                src.advance(2);
                let n = src.get_u32();
                Ok(Some(Frame::Window(n)))
            }
            _ => {
                let _ = max_frame_size;
                Err(Error::InvalidFrame("unknown frame type"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Window(42).encode(&mut buf);
        assert_eq!(&buf[..], &[b'2', b'W', 0, 0, 0, 42]);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Window(42));
        assert!(buf.is_empty());
    }

    #[test]
    fn window_partial_returns_none() {
        let mut buf = BytesMut::from(&[b'2', b'W', 0, 0][..]);
        assert!(Frame::decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 4, "decode must not consume bytes when incomplete");
    }

    #[test]
    fn invalid_version_errors() {
        let mut buf = BytesMut::from(&[b'1', b'W', 0, 0, 0, 1][..]);
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(Error::InvalidFrame("unsupported protocol version"))
        ));
    }

    #[test]
    fn unknown_type_errors() {
        let mut buf = BytesMut::from(&[b'2', b'X'][..]);
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(Error::InvalidFrame("unknown frame type"))
        ));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib frame`
Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add src/frame.rs
git commit -m "feat(frame): add Window frame encode/decode"
```

---

## Task 4: `Json` frame

**Files:**
- Modify: `src/frame.rs`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `src/frame.rs`:

```rust
    #[test]
    fn json_round_trip() {
        let payload = Bytes::from_static(b"{\"hello\":\"world\"}");
        let frame = Frame::Json { seq: 7, data: payload.clone() };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        // Header (2) + seq (4) + len (4) + payload
        assert_eq!(&buf[..2], &[b'2', b'J']);
        assert_eq!(buf.len(), 2 + 4 + 4 + payload.len());

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Json { seq: 7, data: payload });
        assert!(buf.is_empty());
    }

    #[test]
    fn json_partial_returns_none() {
        let frame = Frame::Json { seq: 1, data: Bytes::from_static(b"abc") };
        let mut full = BytesMut::new();
        frame.encode(&mut full);

        for take in 0..full.len() {
            let mut partial = BytesMut::from(&full[..take]);
            assert!(Frame::decode(&mut partial).unwrap().is_none(),
                "len={take} should be incomplete");
            assert_eq!(partial.len(), take, "must not consume on incomplete");
        }
    }

    #[test]
    fn json_oversize_errors() {
        // Header + seq + len(=10) but only declares 10 bytes; request limit < 10
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'2', b'J']);
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&10u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 10]);

        let res = Frame::decode_with_limit(&mut buf, 5);
        assert!(matches!(res, Err(Error::FrameTooLarge { size: 10, max: 5 })));
    }
```

- [ ] **Step 2: Extend `Frame::encode` and `decode_with_limit` for `Json`**

In `src/frame.rs`, replace the `Json` arm of `encode`:

```rust
            Frame::Json { seq, data } => {
                dst.reserve(2 + 4 + 4 + data.len());
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_JSON);
                dst.put_u32(*seq);
                dst.put_u32(data.len() as u32);
                dst.extend_from_slice(data);
            }
```

In `decode_with_limit`, add a `TYPE_JSON` arm before the catch-all `_`:

```rust
            TYPE_JSON => {
                if src.len() < 2 + 4 + 4 {
                    return Ok(None);
                }
                let len = u32::from_be_bytes(src[6..10].try_into().unwrap()) as usize;
                if len > max_frame_size {
                    return Err(Error::FrameTooLarge { size: len, max: max_frame_size });
                }
                if src.len() < 2 + 4 + 4 + len {
                    return Ok(None);
                }
                src.advance(2);
                let seq = src.get_u32();
                let _len = src.get_u32();
                let data = src.split_to(len).freeze();
                Ok(Some(Frame::Json { seq, data }))
            }
```

Update the `Json { .. }` arm of `encode` so it's no longer `unimplemented!()`. Remove `Frame::Json { .. } |` from the catch-all.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib frame`
Expected: 7 passed.

- [ ] **Step 4: Commit**

```bash
git add src/frame.rs
git commit -m "feat(frame): add Json frame encode/decode"
```

---

## Task 5: `Ack` frame

**Files:**
- Modify: `src/frame.rs`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module:

```rust
    #[test]
    fn ack_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Ack(99).encode(&mut buf);
        assert_eq!(&buf[..], &[b'2', b'A', 0, 0, 0, 99]);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Ack(99));
    }

    #[test]
    fn ack_zero_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Ack(0).encode(&mut buf);
        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Ack(0));
    }

    #[test]
    fn ack_partial_returns_none() {
        let mut buf = BytesMut::from(&[b'2', b'A', 0, 0][..]);
        assert!(Frame::decode(&mut buf).unwrap().is_none());
    }
```

- [ ] **Step 2: Implement `Ack`**

In `Frame::encode`, replace the `Ack(_)` arm of the catch-all:

```rust
            Frame::Ack(seq) => {
                dst.reserve(2 + 4);
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_ACK);
                dst.put_u32(*seq);
            }
```

In `decode_with_limit`, add a `TYPE_ACK` arm:

```rust
            TYPE_ACK => {
                if src.len() < 2 + 4 {
                    return Ok(None);
                }
                src.advance(2);
                let seq = src.get_u32();
                Ok(Some(Frame::Ack(seq)))
            }
```

Remove `Frame::Ack(_) |` from the encode catch-all.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib frame`
Expected: 10 passed.

- [ ] **Step 4: Commit**

```bash
git add src/frame.rs
git commit -m "feat(frame): add Ack frame encode/decode"
```

---

## Task 6: `Compressed` frame (with inline decompression)

**Files:**
- Modify: `src/frame.rs`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module:

```rust
    #[test]
    #[cfg(feature = "compression")]
    fn compressed_round_trip() {
        // Build inner payload: two Json frames concatenated.
        let inner_a = Frame::Json { seq: 1, data: Bytes::from_static(b"{\"a\":1}") };
        let inner_b = Frame::Json { seq: 2, data: Bytes::from_static(b"{\"b\":2}") };
        let mut inner = BytesMut::new();
        inner_a.encode(&mut inner);
        inner_b.encode(&mut inner);
        let inner_bytes = inner.freeze();

        let mut buf = BytesMut::new();
        Frame::Compressed(inner_bytes.clone()).encode(&mut buf);

        // Decode: yields Compressed(inner_bytes) where the inner is the *decompressed* bytes.
        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        match decoded {
            Frame::Compressed(b) => assert_eq!(b, inner_bytes),
            other => panic!("expected Compressed, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "compression")]
    fn compressed_corrupted_payload_errors() {
        // Header (C) + length 4 + 4 garbage bytes
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'2', b'C']);
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(matches!(Frame::decode(&mut buf), Err(Error::Compression(_))));
    }

    #[test]
    #[cfg(feature = "compression")]
    fn compressed_partial_returns_none() {
        let mut full = BytesMut::new();
        Frame::Compressed(Bytes::from_static(b"hello world hello world"))
            .encode(&mut full);
        for take in 0..full.len() {
            let mut partial = BytesMut::from(&full[..take]);
            assert!(Frame::decode(&mut partial).unwrap().is_none());
        }
    }
```

- [ ] **Step 2: Implement `Compressed` encode/decode**

Add `use std::io::{Read, Write};` near the top of `src/frame.rs`.

Add an internal helper at module level:

```rust
#[cfg(feature = "compression")]
fn zlib_compress(input: &[u8]) -> Vec<u8> {
    use flate2::{write::ZlibEncoder, Compression};
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(3));
    enc.write_all(input).expect("writing to Vec never fails");
    enc.finish().expect("finishing in-memory zlib never fails")
}

#[cfg(feature = "compression")]
fn zlib_decompress(input: &[u8], max: usize) -> Result<Bytes> {
    use flate2::read::ZlibDecoder;
    let mut dec = ZlibDecoder::new(input);
    let mut out = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = dec
            .read(&mut chunk)
            .map_err(|e| Error::Compression(e.to_string()))?;
        if n == 0 {
            break;
        }
        if out.len() + n > max {
            return Err(Error::FrameTooLarge { size: out.len() + n, max });
        }
        out.extend_from_slice(&chunk[..n]);
    }
    Ok(Bytes::from(out))
}
```

Add the `Compressed` arm to `encode`:

```rust
            Frame::Compressed(inner) => {
                #[cfg(feature = "compression")]
                {
                    let compressed = zlib_compress(inner);
                    dst.reserve(2 + 4 + compressed.len());
                    dst.put_u8(PROTOCOL_VERSION);
                    dst.put_u8(TYPE_COMPRESSED);
                    dst.put_u32(compressed.len() as u32);
                    dst.extend_from_slice(&compressed);
                }
                #[cfg(not(feature = "compression"))]
                {
                    let _ = inner;
                    panic!("compression feature disabled");
                }
            }
```

Add a `TYPE_COMPRESSED` arm to `decode_with_limit`:

```rust
            TYPE_COMPRESSED => {
                #[cfg(not(feature = "compression"))]
                {
                    return Err(Error::InvalidFrame("compression feature disabled"));
                }
                #[cfg(feature = "compression")]
                {
                    if src.len() < 2 + 4 {
                        return Ok(None);
                    }
                    let len = u32::from_be_bytes(src[2..6].try_into().unwrap()) as usize;
                    if len > max_frame_size {
                        return Err(Error::FrameTooLarge { size: len, max: max_frame_size });
                    }
                    if src.len() < 2 + 4 + len {
                        return Ok(None);
                    }
                    src.advance(2 + 4);
                    let payload = src.split_to(len);
                    let inflated = zlib_decompress(&payload, max_frame_size)?;
                    Ok(Some(Frame::Compressed(inflated)))
                }
            }
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib frame`
Expected: 13 passed.

- [ ] **Step 4: Commit**

```bash
git add src/frame.rs
git commit -m "feat(frame): add Compressed frame with inline zlib decompression"
```

---

## Task 7: Tokio codec adapter

**Files:**
- Modify: `src/codec.rs`

- [ ] **Step 1: Write the failing test**

Write `src/codec.rs`:

```rust
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{Error, Result};
use crate::frame::{Frame, DEFAULT_MAX_FRAME_SIZE};

#[derive(Debug, Clone)]
pub struct LumberjackCodec {
    pub max_frame_size: usize,
}

impl Default for LumberjackCodec {
    fn default() -> Self {
        Self { max_frame_size: DEFAULT_MAX_FRAME_SIZE }
    }
}

impl LumberjackCodec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for LumberjackCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        Frame::decode_with_limit(src, self.max_frame_size)
    }
}

impl Encoder<Frame> for LumberjackCodec {
    type Error = Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn encode_then_decode_window() {
        let mut codec = LumberjackCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Frame::Window(5), &mut buf).unwrap();
        let out = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(out, Frame::Window(5));
    }

    #[test]
    fn decode_streams_back_to_back_frames() {
        let mut codec = LumberjackCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Frame::Window(2), &mut buf).unwrap();
        codec
            .encode(Frame::Json { seq: 1, data: Bytes::from_static(b"{}") }, &mut buf)
            .unwrap();
        codec
            .encode(Frame::Json { seq: 2, data: Bytes::from_static(b"[]") }, &mut buf)
            .unwrap();

        let f1 = codec.decode(&mut buf).unwrap().unwrap();
        let f2 = codec.decode(&mut buf).unwrap().unwrap();
        let f3 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1, Frame::Window(2));
        assert_eq!(f2, Frame::Json { seq: 1, data: Bytes::from_static(b"{}") });
        assert_eq!(f3, Frame::Json { seq: 2, data: Bytes::from_static(b"[]") });
        assert!(buf.is_empty());
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib codec`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add src/codec.rs
git commit -m "feat(codec): add tokio_util Encoder/Decoder adapter"
```

---

## Task 8: `Batch` type and ack channel

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Write the failing test**

Write `src/server.rs`:

```rust
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};

pub struct Batch {
    events: Vec<Value>,
    last_seq: u32,
    ack: Option<oneshot::Sender<u32>>,
}

impl Batch {
    pub(crate) fn new(events: Vec<Value>, last_seq: u32, ack: oneshot::Sender<u32>) -> Self {
        Self { events, last_seq, ack: Some(ack) }
    }

    pub fn events(&self) -> &[Value] {
        &self.events
    }

    pub fn into_events(mut self) -> Vec<Value> {
        // Make sure we ack on Drop after events are taken.
        let events = std::mem::take(&mut self.events);
        events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn ack(mut self) {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(self.last_seq);
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(self.last_seq);
        }
    }
}

// Re-exported placeholder so the rest of the file can compile in later tasks.
pub(crate) type BatchSender = mpsc::Sender<Batch>;
pub(crate) type BatchReceiver = mpsc::Receiver<Batch>;

// Server / ServerBuilder added in later tasks.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn explicit_ack_sends_last_seq() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![Value::Null], 7, tx);
        batch.ack();
        assert_eq!(rx.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn drop_without_ack_still_sends_last_seq() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![Value::Null, Value::Null], 12, tx);
        drop(batch);
        assert_eq!(rx.await.unwrap(), 12);
    }

    #[tokio::test]
    async fn double_ack_is_a_noop() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![], 3, tx);
        batch.ack();
        // Drop runs after, but ack already taken — must not panic.
        assert_eq!(rx.await.unwrap(), 3);
    }
}
```

Note: also fix `src/lib.rs` re-exports if compilation complains about missing `Server`/`ServerBuilder` — comment them out for now:

```rust
pub use server::Batch;
// pub use server::{Server, ServerBuilder}; // added in Task 11
```

And remove the unresolved `Client`/`ClientBuilder` re-exports:

```rust
// pub use client::{Client, ClientBuilder}; // added in Task 13
```

Suppress the unused `Error`/`BatchSender`/`BatchReceiver` warnings for now with `#[allow(dead_code)]` on those items.

- [ ] **Step 2: Run tests**

Run: `cargo test --lib server`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add src/server.rs src/lib.rs
git commit -m "feat(server): add Batch with ack channel and Drop fallback"
```

---

## Task 9: Connection state machine — happy path

**Files:**
- Modify: `src/server.rs`

This task adds the per-connection task that reads frames, assembles a batch, dispatches it, and writes the final ACK. Keepalive is added in the next task.

- [ ] **Step 1: Write the failing test**

Append to `src/server.rs` (above `mod tests`):

```rust
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::warn;

use crate::codec::LumberjackCodec;
use crate::frame::Frame;

pub(crate) struct ConnectionConfig {
    pub max_frame_size: usize,
    pub keepalive: Option<Duration>,
}

pub(crate) async fn run_connection<S>(
    stream: S,
    cfg: ConnectionConfig,
    out: BatchSender,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let codec = LumberjackCodec::with_max_frame_size(cfg.max_frame_size);
    let mut framed = Framed::new(stream, codec);

    loop {
        // Expect a Window frame to begin a batch.
        let window = match framed.next().await {
            Some(Ok(Frame::Window(n))) => n,
            Some(Ok(_)) => {
                return Err(Error::UnexpectedFrame("expected Window at batch start"));
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()), // clean EOF
        };

        let mut events: Vec<Value> = Vec::with_capacity(window as usize);
        let mut last_seq: u32 = 0;
        let mut prev_seq: u32 = 0;

        while events.len() < window as usize {
            let frame = match framed.next().await {
                Some(Ok(f)) => f,
                Some(Err(e)) => return Err(e),
                None => return Err(Error::ConnectionClosed),
            };
            match frame {
                Frame::Json { seq, data } => {
                    if seq <= prev_seq {
                        return Err(Error::SeqOutOfOrder { got: seq, prev: prev_seq });
                    }
                    prev_seq = seq;
                    last_seq = seq;
                    push_json_event(&mut events, &data);
                }
                Frame::Compressed(inner) => {
                    // Decode the inner bytes as a stream of J frames.
                    let mut buf = bytes::BytesMut::from(&inner[..]);
                    while !buf.is_empty() && events.len() < window as usize {
                        let inner_frame = Frame::decode_with_limit(&mut buf, cfg.max_frame_size)?;
                        match inner_frame {
                            Some(Frame::Json { seq, data }) => {
                                if seq <= prev_seq {
                                    return Err(Error::SeqOutOfOrder { got: seq, prev: prev_seq });
                                }
                                prev_seq = seq;
                                last_seq = seq;
                                push_json_event(&mut events, &data);
                            }
                            Some(_) => {
                                return Err(Error::UnexpectedFrame(
                                    "compressed payload must contain only J frames",
                                ));
                            }
                            None => {
                                return Err(Error::InvalidFrame(
                                    "compressed payload truncated",
                                ));
                            }
                        }
                    }
                }
                Frame::Window(_) | Frame::Ack(_) => {
                    return Err(Error::UnexpectedFrame("expected J or C inside window"));
                }
            }
        }

        // Dispatch the batch and wait for ack (keepalive added in next task).
        let (ack_tx, ack_rx) = oneshot::channel();
        let batch = Batch::new(events, last_seq, ack_tx);
        if out.send(batch).await.is_err() {
            // Receiver dropped — server is shutting down.
            return Ok(());
        }
        let acked = match ack_rx.await {
            Ok(seq) => seq,
            Err(_) => last_seq, // sender dropped without ack — fall back
        };
        framed.send(Frame::Ack(acked)).await?;
    }
}

fn push_json_event(out: &mut Vec<Value>, data: &Bytes) {
    match serde_json::from_slice::<Value>(data) {
        Ok(v) => out.push(v),
        Err(e) => warn!("dropping event with invalid JSON: {e}"),
    }
}
```

Append a new test to `mod tests`:

```rust
    use bytes::BytesMut;
    use futures_util::SinkExt;
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use crate::codec::LumberjackCodec;
    use crate::frame::Frame;

    #[tokio::test]
    async fn server_processes_uncompressed_batch() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);

        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig { max_frame_size: 1024, keepalive: None },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json { seq: 1, data: Bytes::from_static(b"{\"a\":1}") })
            .await
            .unwrap();
        client
            .send(Frame::Json { seq: 2, data: Bytes::from_static(b"{\"b\":2}") })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.events()[0]["a"], 1);
        assert_eq!(batch.events()[1]["b"], 2);
        batch.ack();

        // Client should now read an Ack(2) frame.
        match client.next().await.unwrap().unwrap() {
            Frame::Ack(seq) => assert_eq!(seq, 2),
            other => panic!("expected Ack, got {other:?}"),
        }

        drop(client); // EOF
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_skips_invalid_json_event() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);
        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig { max_frame_size: 1024, keepalive: None },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json { seq: 1, data: Bytes::from_static(b"not json") })
            .await
            .unwrap();
        client
            .send(Frame::Json { seq: 2, data: Bytes::from_static(b"{\"ok\":true}") })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();
        assert_eq!(batch.len(), 1, "invalid JSON event should be skipped");
        assert_eq!(batch.events()[0]["ok"], true);
        batch.ack();
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_rejects_seq_out_of_order() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, _out_rx) = mpsc::channel::<Batch>(8);
        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig { max_frame_size: 1024, keepalive: None },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json { seq: 5, data: Bytes::from_static(b"{}") })
            .await
            .unwrap();
        client
            .send(Frame::Json { seq: 3, data: Bytes::from_static(b"{}") })
            .await
            .unwrap();

        let res = server.await.unwrap();
        assert!(matches!(res, Err(Error::SeqOutOfOrder { got: 3, prev: 5 })));
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib server`
Expected: 6 passed.

- [ ] **Step 3: Commit**

```bash
git add src/server.rs
git commit -m "feat(server): add per-connection state machine"
```

---

## Task 10: Server keepalive (`Ack(0)` while waiting)

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Write the failing test**

Append to `mod tests`:

```rust
    #[tokio::test]
    async fn server_sends_ack0_keepalive_while_user_holds_batch() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);

        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig {
                    max_frame_size: 1024,
                    keepalive: Some(Duration::from_millis(20)),
                },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(1)).await.unwrap();
        client
            .send(Frame::Json { seq: 1, data: Bytes::from_static(b"{}") })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();

        // Hold the batch and collect at least 2 keepalive Ack(0) frames.
        let mut zeros = 0u32;
        for _ in 0..3 {
            match client.next().await.unwrap().unwrap() {
                Frame::Ack(0) => zeros += 1,
                Frame::Ack(seq) => panic!("got real ack {seq} too early"),
                other => panic!("got {other:?}"),
            }
            if zeros >= 2 {
                break;
            }
        }
        assert!(zeros >= 2, "expected at least 2 Ack(0) keepalives, got {zeros}");

        batch.ack();
        // Eventually the real Ack(1) arrives.
        loop {
            match client.next().await.unwrap().unwrap() {
                Frame::Ack(0) => continue,
                Frame::Ack(1) => break,
                other => panic!("got {other:?}"),
            }
        }

        drop(client);
        server.await.unwrap().unwrap();
    }
```

- [ ] **Step 2: Modify the ack-wait loop in `run_connection`**

Replace the existing `let acked = match ack_rx.await { ... };` block with:

```rust
        let acked = {
            let mut ack_rx = ack_rx;
            loop {
                let recv_result = match cfg.keepalive {
                    Some(interval) => {
                        tokio::select! {
                            biased;
                            r = &mut ack_rx => Some(r),
                            _ = tokio::time::sleep(interval) => None,
                        }
                    }
                    None => Some((&mut ack_rx).await),
                };
                match recv_result {
                    Some(Ok(seq)) => break seq,
                    Some(Err(_)) => break last_seq,
                    None => {
                        framed.send(Frame::Ack(0)).await?;
                    }
                }
            }
        };
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib server`
Expected: 7 passed.

- [ ] **Step 4: Commit**

```bash
git add src/server.rs
git commit -m "feat(server): send Ack(0) keepalive while user holds batch"
```

---

## Task 11: `Server` and `ServerBuilder` (accept loop + bind)

**Files:**
- Modify: `src/server.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test**

Append to `mod tests`:

```rust
    #[tokio::test]
    async fn server_bind_accepts_real_tcp_connection() {
        let mut server = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();

        let client_task = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut client = Framed::new(stream, LumberjackCodec::new());
            client.send(Frame::Window(1)).await.unwrap();
            client
                .send(Frame::Json { seq: 1, data: Bytes::from_static(b"{\"x\":42}") })
                .await
                .unwrap();
            // Wait for ack (skip any Ack(0) keepalives).
            loop {
                match client.next().await.unwrap().unwrap() {
                    Frame::Ack(0) => continue,
                    Frame::Ack(1) => return,
                    other => panic!("unexpected: {other:?}"),
                }
            }
        });

        let batch = server.recv().await.unwrap();
        assert_eq!(batch.events()[0]["x"], 42);
        batch.ack();
        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn server_drop_stops_accept_loop() {
        let server = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        drop(server);

        // Connecting after drop should eventually fail.
        let res = tokio::time::timeout(
            Duration::from_millis(500),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .unwrap();
        assert!(res.is_err(), "expected connection refused after drop");
    }
```

- [ ] **Step 2: Implement `Server` and `ServerBuilder`**

Append to `src/server.rs` (after `run_connection`):

```rust
use std::net::SocketAddr;
use tokio::net::{TcpListener, ToSocketAddrs};

pub struct ServerBuilder {
    keepalive: Option<Duration>,
    channel_capacity: usize,
    max_frame_size: usize,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            keepalive: Some(Duration::from_secs(15)),
            channel_capacity: 128,
            max_frame_size: crate::frame::DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl ServerBuilder {
    pub fn keepalive(mut self, interval: Duration) -> Self {
        self.keepalive = Some(interval);
        self
    }

    pub fn no_keepalive(mut self) -> Self {
        self.keepalive = None;
        self
    }

    pub fn channel_capacity(mut self, n: usize) -> Self {
        self.channel_capacity = n;
        self
    }

    pub fn max_frame_size(mut self, n: usize) -> Self {
        self.max_frame_size = n;
        self
    }

    pub async fn bind<A: ToSocketAddrs>(self, addr: A) -> Result<Server> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel::<Batch>(self.channel_capacity);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let cfg = ConnectionConfig {
            max_frame_size: self.max_frame_size,
            keepalive: self.keepalive,
        };
        let cfg = std::sync::Arc::new(cfg);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _peer)) => {
                                let cfg = cfg.clone();
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = run_connection(
                                        stream,
                                        ConnectionConfig {
                                            max_frame_size: cfg.max_frame_size,
                                            keepalive: cfg.keepalive,
                                        },
                                        tx,
                                    )
                                    .await
                                    {
                                        warn!("connection terminated: {e}");
                                    }
                                });
                            }
                            Err(e) => {
                                warn!("accept failed: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Server { rx, shutdown: Some(shutdown_tx), local_addr })
    }
}

pub struct Server {
    rx: BatchReceiver,
    shutdown: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
}

impl Server {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Server> {
        ServerBuilder::default().bind(addr).await
    }

    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn recv(&mut self) -> Option<Batch> {
        self.rx.recv().await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
```

Update `src/lib.rs` to export `Server` and `ServerBuilder`:

```rust
pub use server::{Batch, Server, ServerBuilder};
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib server`
Expected: 9 passed (sometimes the second test races; if flaky, increase the timeout to 1s).

- [ ] **Step 4: Commit**

```bash
git add src/server.rs src/lib.rs
git commit -m "feat(server): add Server, ServerBuilder, accept loop, graceful shutdown"
```

---

## Task 12: `ClientBuilder` skeleton (no source-port yet)

**Files:**
- Modify: `src/client.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write `src/client.rs`**

```rust
use std::pin::Pin;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::codec::LumberjackCodec;
use crate::error::{Error, Result};
use crate::frame::Frame;

pub(crate) type BoxedStream = Pin<Box<dyn AsyncRead + AsyncWrite + Send + Unpin>>;

pub struct ClientBuilder {
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
    local_port_range: Option<(u16, u16)>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            compression_level: 3,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(30)),
            local_port_range: None,
        }
    }
}

impl ClientBuilder {
    pub fn compression_level(mut self, level: u32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn write_timeout(mut self, d: Duration) -> Self {
        self.write_timeout = Some(d);
        self
    }

    pub fn ack_timeout(mut self, d: Duration) -> Self {
        self.ack_timeout = Some(d);
        self
    }

    pub fn local_port_range(mut self, start: u16, end: u16) -> Self {
        self.local_port_range = Some((start, end));
        self
    }

    pub async fn connect<A: ToSocketAddrs>(self, addr: A) -> Result<Client> {
        if let Some((start, end)) = self.local_port_range {
            if start > end {
                return Err(Error::InvalidConfig("local_port_range start > end"));
            }
        }
        let target = tokio::net::lookup_host(addr)
            .await?
            .next()
            .ok_or_else(|| Error::InvalidConfig("no addresses resolved"))?;

        let stream: TcpStream = match self.local_port_range {
            None => TcpStream::connect(target).await?,
            Some((start, end)) => connect_with_port_range(target, start, end).await?,
        };

        let boxed: BoxedStream = Box::pin(stream);
        Ok(Client {
            framed: Framed::new(boxed, LumberjackCodec::new()),
            compression_level: self.compression_level,
            write_timeout: self.write_timeout,
            ack_timeout: self.ack_timeout,
        })
    }
}

pub struct Client {
    framed: Framed<BoxedStream, LumberjackCodec>,
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        ClientBuilder::default().connect(addr).await
    }

    pub async fn send<T: Serialize>(&mut self, _events: &[T]) -> Result<u32> {
        unimplemented!("added in Task 13")
    }

    pub async fn close(mut self) -> Result<()> {
        self.framed.close().await
    }
}

async fn connect_with_port_range(
    _target: std::net::SocketAddr,
    _start: u16,
    _end: u16,
) -> Result<TcpStream> {
    unimplemented!("added in Task 16")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_port_range_errors() {
        let res = ClientBuilder::default()
            .local_port_range(50000, 49000)
            .connect("127.0.0.1:1")
            .await;
        assert!(matches!(res, Err(Error::InvalidConfig(_))));
    }
}
```

Update `src/lib.rs`:

```rust
pub use client::{Client, ClientBuilder};
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib client`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add src/client.rs src/lib.rs
git commit -m "feat(client): add ClientBuilder skeleton and Client struct"
```

---

## Task 13: `Client::send` (uncompressed path)

**Files:**
- Modify: `src/client.rs`

- [ ] **Step 1: Write the failing test**

Append to `mod tests`:

```rust
    use crate::frame::Frame;
    use serde_json::json;
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn send_uncompressed_writes_window_then_json_then_returns_on_ack() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(5)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            let f1 = peer.next().await.unwrap().unwrap();
            assert_eq!(f1, Frame::Window(2));
            let f2 = peer.next().await.unwrap().unwrap();
            match f2 {
                Frame::Json { seq, data } => {
                    assert_eq!(seq, 1);
                    let v: serde_json::Value = serde_json::from_slice(&data).unwrap();
                    assert_eq!(v["a"], 1);
                }
                other => panic!("expected Json, got {other:?}"),
            }
            let f3 = peer.next().await.unwrap().unwrap();
            match f3 {
                Frame::Json { seq, .. } => assert_eq!(seq, 2),
                other => panic!("expected Json, got {other:?}"),
            }
            peer.send(Frame::Ack(2)).await.unwrap();
        });

        let n = client.send(&[json!({"a": 1}), json!({"b": 2})]).await.unwrap();
        assert_eq!(n, 2);
        peer.await.unwrap();
    }
```

- [ ] **Step 2: Implement uncompressed `send` and the ACK receive loop**

Replace `Client::send` with:

```rust
    pub async fn send<T: Serialize>(&mut self, events: &[T]) -> Result<u32> {
        if events.is_empty() {
            return Ok(0);
        }
        let n = events.len() as u32;

        // Encode J frames into a buffer.
        let mut payload = BytesMut::new();
        for (i, ev) in events.iter().enumerate() {
            let bytes = serde_json::to_vec(ev)
                .map_err(|e| Error::InvalidConfig_str_to_static(&e.to_string()))?;
            Frame::Json {
                seq: (i as u32) + 1,
                data: Bytes::from(bytes),
            }
            .encode(&mut payload);
        }

        // Send Window then either Compressed wrapper or raw J frames.
        self.send_with_optional_timeout(Frame::Window(n)).await?;

        if self.compression_level > 0 {
            self.send_with_optional_timeout(Frame::Compressed(payload.freeze()))
                .await?;
        } else {
            // Write the raw bytes by re-decoding into individual frames.
            let mut buf = payload;
            while let Some(frame) = Frame::decode(&mut buf)? {
                self.send_with_optional_timeout(frame).await?;
            }
        }
        self.flush_with_optional_timeout().await?;

        // Receive ACKs until seq >= n. Tolerate Ack(0) keepalives and partial acks.
        loop {
            let recv = match self.ack_timeout {
                Some(t) => tokio::time::timeout(t, self.framed.next())
                    .await
                    .map_err(|_| Error::AckTimeout)?,
                None => self.framed.next().await,
            };
            match recv {
                Some(Ok(Frame::Ack(seq))) if seq >= n => return Ok(n),
                Some(Ok(Frame::Ack(_))) => continue,
                Some(Ok(_)) => return Err(Error::UnexpectedFrame("expected Ack")),
                Some(Err(e)) => return Err(e),
                None => return Err(Error::ConnectionClosed),
            }
        }
    }

    async fn send_with_optional_timeout(&mut self, frame: Frame) -> Result<()> {
        match self.write_timeout {
            Some(t) => tokio::time::timeout(t, self.framed.feed(frame))
                .await
                .map_err(|_| Error::WriteTimeout)?,
            None => self.framed.feed(frame).await,
        }
    }

    async fn flush_with_optional_timeout(&mut self) -> Result<()> {
        match self.write_timeout {
            Some(t) => tokio::time::timeout(t, self.framed.flush())
                .await
                .map_err(|_| Error::WriteTimeout)?,
            None => self.framed.flush().await,
        }
    }
```

The temporary `Error::InvalidConfig_str_to_static` is bogus — replace it. Add a new error variant `Serialization(String)` to `src/error.rs`:

```rust
    #[error("serialization error: {0}")]
    Serialization(String),
```

Then use:

```rust
            let bytes = serde_json::to_vec(ev)
                .map_err(|e| Error::Serialization(e.to_string()))?;
```

(If you prefer not to add a variant, you can use `Error::InvalidFrame("serialization failed")`. The dedicated variant is clearer.)

- [ ] **Step 3: Run tests**

Run: `cargo test --lib client`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add src/client.rs src/error.rs
git commit -m "feat(client): implement send for uncompressed batches"
```

---

## Task 14: `Client::send` compressed path

**Files:**
- Modify: `src/client.rs`

The implementation in Task 13 already takes the compressed branch when `compression_level > 0`. This task adds a test that exercises it end-to-end.

- [ ] **Step 1: Write the failing test**

Append to `mod tests`:

```rust
    #[tokio::test]
    async fn send_compressed_emits_window_then_compressed_frame() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 3,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(5)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            assert_eq!(peer.next().await.unwrap().unwrap(), Frame::Window(2));
            // The next frame must be a Compressed frame whose inner decodes to two J frames.
            match peer.next().await.unwrap().unwrap() {
                Frame::Compressed(inner) => {
                    let mut buf = bytes::BytesMut::from(&inner[..]);
                    let f1 = Frame::decode(&mut buf).unwrap().unwrap();
                    let f2 = Frame::decode(&mut buf).unwrap().unwrap();
                    assert!(matches!(f1, Frame::Json { seq: 1, .. }));
                    assert!(matches!(f2, Frame::Json { seq: 2, .. }));
                }
                other => panic!("expected Compressed, got {other:?}"),
            }
            peer.send(Frame::Ack(2)).await.unwrap();
        });

        let n = client
            .send(&[serde_json::json!({"a": 1}), serde_json::json!({"b": 2})])
            .await
            .unwrap();
        assert_eq!(n, 2);
        peer.await.unwrap();
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib client`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add src/client.rs
git commit -m "test(client): cover compressed send path"
```

---

## Task 15: Client timeouts and keepalive tolerance

**Files:**
- Modify: `src/client.rs`

- [ ] **Step 1: Write the failing tests**

Append to `mod tests`:

```rust
    #[tokio::test]
    async fn send_returns_ack_timeout_when_peer_silent() {
        let (client_io, _peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_millis(50)),
        };
        let res = client.send(&[serde_json::json!({"x": 1})]).await;
        assert!(matches!(res, Err(Error::AckTimeout)));
    }

    #[tokio::test]
    async fn send_tolerates_ack0_keepalives() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_millis(200)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            // Drain the client's window + json frames first.
            assert!(matches!(peer.next().await.unwrap().unwrap(), Frame::Window(1)));
            assert!(matches!(peer.next().await.unwrap().unwrap(), Frame::Json { .. }));
            // Send several Ack(0)s within ack_timeout, then the real ack.
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(80)).await;
                peer.send(Frame::Ack(0)).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
            peer.send(Frame::Ack(1)).await.unwrap();
        });

        let n = client.send(&[serde_json::json!({"x": 1})]).await.unwrap();
        assert_eq!(n, 1);
        peer.await.unwrap();
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib client`
Expected: 5 passed. The implementation from Task 13 already supports this; if a test fails, fix the relevant code path before continuing.

- [ ] **Step 3: Commit**

```bash
git add src/client.rs
git commit -m "test(client): cover ack_timeout and Ack(0) keepalive tolerance"
```

---

## Task 16: `local_port_range` source-port binding

**Files:**
- Modify: `src/client.rs`

- [ ] **Step 1: Write the failing tests**

Append to `mod tests`:

```rust
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    use tokio::net::TcpListener;

    async fn listen_loopback() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    fn pick_free_port_in(range_start: u16, range_end: u16) -> u16 {
        // Bind/release to discover a usable port inside the range.
        for p in range_start..=range_end {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p);
            if std::net::TcpListener::bind(addr).is_ok() {
                return p;
            }
        }
        panic!("no free port in {range_start}..={range_end}");
    }

    #[tokio::test]
    async fn local_port_range_uses_port_within_range() {
        let (listener, target) = listen_loopback().await;
        // Pick a one-port range we know is free at this instant.
        let port = pick_free_port_in(45000, 45200);

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let client = ClientBuilder::default()
            .local_port_range(port, port)
            .connect(target)
            .await
            .unwrap();
        let (server_side, _peer_addr) = accept.await.unwrap();
        let local = server_side.peer_addr().unwrap();
        assert_eq!(local.port(), port);
        drop(client);
    }

    #[tokio::test]
    async fn local_port_range_skips_busy_port() {
        let (listener, target) = listen_loopback().await;
        // Reserve one port; verify the client picks the other one.
        let busy = pick_free_port_in(45300, 45301);
        let _hold = std::net::TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            busy,
        ))
        .unwrap();
        let other = if busy == 45300 { 45301 } else { 45300 };

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let client = ClientBuilder::default()
            .local_port_range(45300, 45301)
            .connect(target)
            .await
            .unwrap();
        let (server_side, _peer_addr) = accept.await.unwrap();
        assert_eq!(server_side.peer_addr().unwrap().port(), other);
        drop(client);
    }

    #[tokio::test]
    async fn local_port_range_exhausted_errors() {
        let (_listener, target) = listen_loopback().await;
        let port = pick_free_port_in(45400, 45400);
        let _hold = std::net::TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            port,
        ))
        .unwrap();
        let res = ClientBuilder::default()
            .local_port_range(port, port)
            .connect(target)
            .await;
        assert!(matches!(res, Err(Error::NoLocalPortAvailable)));
    }
```

- [ ] **Step 2: Implement `connect_with_port_range`**

Replace the stub:

```rust
async fn connect_with_port_range(
    target: std::net::SocketAddr,
    start: u16,
    end: u16,
) -> Result<TcpStream> {
    use rand::Rng;
    let count = (end - start) as u32 + 1;
    let offset = rand::thread_rng().gen_range(0..count);

    for i in 0..count {
        let port = start + ((offset + i) % count) as u16;
        let socket = match target {
            std::net::SocketAddr::V4(_) => TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => TcpSocket::new_v6()?,
        };
        let bind_addr: std::net::SocketAddr = match target {
            std::net::SocketAddr::V4(_) => format!("0.0.0.0:{port}").parse().unwrap(),
            std::net::SocketAddr::V6(_) => format!("[::]:{port}").parse().unwrap(),
        };
        if socket.bind(bind_addr).is_err() {
            continue;
        }
        match socket.connect(target).await {
            Ok(stream) => return Ok(stream),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse
                || e.kind() == std::io::ErrorKind::AddrNotAvailable => continue,
            Err(e) => return Err(Error::Io(e)),
        }
    }
    Err(Error::NoLocalPortAvailable)
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib client`
Expected: 8 passed.

- [ ] **Step 4: Commit**

```bash
git add src/client.rs
git commit -m "feat(client): support local_port_range for source-port restriction"
```

---

## Task 17: TLS helpers (feature = "tls")

**Files:**
- Create: `src/tls.rs`
- Modify: `src/client.rs` (add `tls` builder method + connect-with-tls path)
- Modify: `src/server.rs` (add `tls` builder method + accept-with-tls path)

- [ ] **Step 1: Write `src/tls.rs`**

```rust
//! Convenience constructors for rustls. Use these or build TlsAcceptor / TlsConnector
//! directly with rustls APIs — both work with this crate.

#![cfg(feature = "tls")]

use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::{Error, Result};

pub fn server_acceptor_from_pem(cert: &Path, key: &Path) -> Result<TlsAcceptor> {
    let cert_pem = std::fs::read(cert)?;
    let key_pem = std::fs::read(key)?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::io::Result<Vec<_>>>()?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())?
        .ok_or_else(|| Error::InvalidConfig("no private key in PEM file"))?;
    let key: PrivateKeyDer<'static> = key;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Error::Tls)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub fn client_connector_with_native_roots() -> Result<TlsConnector> {
    let mut roots = RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().unwrap_or_default() {
        let _ = roots.add(cert);
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}
```

- [ ] **Step 2: Wire TLS into `Client`**

Add to `ClientBuilder`:

```rust
    #[cfg(feature = "tls")]
    tls: Option<(tokio_rustls::TlsConnector, String)>,
```

Initialize as `None` in `Default`. Add the builder method:

```rust
    #[cfg(feature = "tls")]
    pub fn tls(mut self, connector: tokio_rustls::TlsConnector, domain: impl Into<String>) -> Self {
        self.tls = Some((connector, domain.into()));
        self
    }
```

In `connect`, after obtaining the `TcpStream`, branch on TLS:

```rust
        let boxed: BoxedStream = {
            #[cfg(feature = "tls")]
            {
                if let Some((connector, domain)) = self.tls {
                    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(
                        domain.as_str().to_string(),
                    )
                    .map_err(|_| Error::InvalidConfig("invalid TLS server name"))?;
                    let tls_stream = connector
                        .connect(server_name, stream)
                        .await
                        .map_err(Error::Io)?;
                    Box::pin(tls_stream)
                } else {
                    Box::pin(stream)
                }
            }
            #[cfg(not(feature = "tls"))]
            {
                Box::pin(stream)
            }
        };
```

- [ ] **Step 3: Wire TLS into `Server`**

Add to `ServerBuilder`:

```rust
    #[cfg(feature = "tls")]
    tls: Option<tokio_rustls::TlsAcceptor>,
```

Initialize as `None` in `Default`. Add:

```rust
    #[cfg(feature = "tls")]
    pub fn tls(mut self, acceptor: tokio_rustls::TlsAcceptor) -> Self {
        self.tls = Some(acceptor);
        self
    }
```

In `bind`, hold the optional acceptor in an `Arc<Option<...>>` captured by the accept loop. After accepting a TCP stream, conditionally upgrade to TLS before calling `run_connection`. Refactor `run_connection`'s signature constraint (`AsyncRead + AsyncWrite + Unpin`) to also require `Send` so it can be spawned.

Update the spawn site:

```rust
                            Ok((stream, _peer)) => {
                                let cfg = cfg.clone();
                                let tx = tx.clone();
                                #[cfg(feature = "tls")]
                                let tls = tls_acceptor.clone();
                                tokio::spawn(async move {
                                    let result: Result<()> = async {
                                        #[cfg(feature = "tls")]
                                        {
                                            if let Some(acceptor) = tls.as_ref() {
                                                let tls_stream = acceptor
                                                    .accept(stream)
                                                    .await
                                                    .map_err(Error::Io)?;
                                                return run_connection(
                                                    tls_stream,
                                                    ConnectionConfig {
                                                        max_frame_size: cfg.max_frame_size,
                                                        keepalive: cfg.keepalive,
                                                    },
                                                    tx,
                                                )
                                                .await;
                                            }
                                        }
                                        run_connection(
                                            stream,
                                            ConnectionConfig {
                                                max_frame_size: cfg.max_frame_size,
                                                keepalive: cfg.keepalive,
                                            },
                                            tx,
                                        )
                                        .await
                                    }
                                    .await;
                                    if let Err(e) = result {
                                        warn!("connection terminated: {e}");
                                    }
                                });
                            }
```

Where `tls_acceptor` is captured before the loop:

```rust
        #[cfg(feature = "tls")]
        let tls_acceptor: std::sync::Arc<Option<tokio_rustls::TlsAcceptor>> =
            std::sync::Arc::new(self.tls);
```

- [ ] **Step 4: Add a TLS integration test**

Create `tests/tls.rs`:

```rust
#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn make_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    (cert_der, key_der)
}

#[tokio::test]
async fn tls_round_trip() {
    let (cert, key) = make_self_signed();

    let server_cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    let mut server = Server::builder().tls(acceptor).bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .ack_timeout(Duration::from_secs(5))
            .tls(connector, "localhost")
            .connect(addr)
            .await
            .unwrap();
        let n = client.send(&[json!({"hello": "tls"})]).await.unwrap();
        assert_eq!(n, 1);
    });

    let batch = server.recv().await.unwrap();
    assert_eq!(batch.events()[0]["hello"], "tls");
    batch.ack();
    client_task.await.unwrap();
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test`
Expected: all unit tests pass plus the new `tls` integration test.

- [ ] **Step 6: Commit**

```bash
git add src/tls.rs src/client.rs src/server.rs tests/tls.rs
git commit -m "feat(tls): add rustls TLS support for client and server"
```

---

## Task 18: Loopback integration test — round trip

**Files:**
- Create: `tests/roundtrip.rs`

- [ ] **Step 1: Write the test**

```rust
use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;

#[tokio::test]
async fn end_to_end_uncompressed_round_trip() {
    let mut server = Server::builder()
        .no_keepalive()
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr();

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .compression_level(0)
            .ack_timeout(Duration::from_secs(5))
            .connect(addr)
            .await
            .unwrap();
        for i in 0..10u32 {
            let n = client.send(&[json!({"i": i})]).await.unwrap();
            assert_eq!(n, 1);
        }
    });

    let mut total = 0;
    while total < 10 {
        let batch = server.recv().await.unwrap();
        total += batch.len();
        batch.ack();
    }
    client_task.await.unwrap();
    assert_eq!(total, 10);
}

#[tokio::test]
async fn server_handles_multiple_concurrent_clients() {
    let mut server = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let mut clients = Vec::new();
    for cid in 0..5u32 {
        clients.push(tokio::spawn(async move {
            let mut client = Client::builder()
                .compression_level(0)
                .connect(addr)
                .await
                .unwrap();
            for i in 0..3u32 {
                client.send(&[json!({"cid": cid, "i": i})]).await.unwrap();
            }
        }));
    }

    let mut total = 0;
    while total < 15 {
        let batch = server.recv().await.unwrap();
        total += batch.len();
        batch.ack();
    }
    for c in clients {
        c.await.unwrap();
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test roundtrip`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/roundtrip.rs
git commit -m "test: add loopback round-trip integration tests"
```

---

## Task 19: Loopback integration test — compressed batches

**Files:**
- Create: `tests/compression.rs`

- [ ] **Step 1: Write the test**

```rust
#![cfg(feature = "compression")]

use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;

#[tokio::test]
async fn compressed_batch_round_trip() {
    let mut server = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .compression_level(6)
            .ack_timeout(Duration::from_secs(5))
            .connect(addr)
            .await
            .unwrap();
        let big: Vec<_> = (0..50).map(|i| json!({"i": i, "filler": "xxxxxxxxxxxxxxxx"})).collect();
        let n = client.send(&big).await.unwrap();
        assert_eq!(n, 50);
    });

    let batch = server.recv().await.unwrap();
    assert_eq!(batch.len(), 50);
    assert_eq!(batch.events()[0]["i"], 0);
    assert_eq!(batch.events()[49]["i"], 49);
    batch.ack();
    client_task.await.unwrap();
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test compression`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/compression.rs
git commit -m "test: add loopback compressed batch round-trip"
```

---

## Task 20: README and crate-level docs

**Files:**
- Create: `README.md`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write `README.md`**

```markdown
# lumberjack

Async Rust implementation of the [Lumberjack v2 protocol](https://github.com/elastic/go-lumber/tree/master/lj/v2) used by Elastic Beats and Logstash.

## Features

- Tokio-based async server and client
- v2 protocol only (Window / JSON / Compressed / Ack frames)
- Built-in TLS via rustls (`tls` feature, on by default)
- Built-in zlib compression via flate2 (`compression` feature, on by default)
- ACK keepalive (`Ack(0)`) on the server side and tolerance on the client side
- Optional source-port restriction on the client (`local_port_range`)

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

## License

MIT OR Apache-2.0
```

- [ ] **Step 2: Add a brief crate-level doc comment to `src/lib.rs`**

Replace the top comment with:

```rust
//! Async Rust implementation of the Lumberjack v2 protocol.
//!
//! See [`Server`] and [`Client`] for the public API. Plain TCP works out of the
//! box; rustls TLS is available with the `tls` feature, and zlib batch
//! compression with the `compression` feature.
```

- [ ] **Step 3: Final test run**

Run: `cargo test --all-features`
Expected: every unit test and integration test passes.

Run: `cargo clippy --all-features -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add README.md src/lib.rs
git commit -m "docs: add README and crate-level documentation"
```

---

## Self-Review Coverage Map (spec → tasks)

| Spec section | Implemented in |
|---|---|
| Project structure / Cargo.toml / features | Task 1 |
| `error.rs` | Task 2 (+ Serialization variant added in Task 13) |
| `frame.rs` Window | Task 3 |
| `frame.rs` Json | Task 4 |
| `frame.rs` Ack | Task 5 |
| `frame.rs` Compressed (inline decompression) | Task 6 |
| Protocol-layer error policy (drop connection) | Tasks 3-6, 9 |
| Payload-layer error policy (skip event) | Task 9 |
| `codec.rs` | Task 7 |
| `Batch` + ack channel + Drop fallback | Task 8 |
| Server connection state machine | Task 9 |
| Server `Ack(0)` keepalive | Task 10 |
| `Server` / `ServerBuilder` / accept loop / shutdown | Task 11 |
| `ClientBuilder` / `Client` skeleton | Task 12 |
| `Client::send` uncompressed | Task 13 |
| `Client::send` compressed | Task 14 |
| `ack_timeout` + Ack(0) tolerance + write_timeout | Task 15 |
| `local_port_range` source-port binding | Task 16 |
| `tls.rs` + Server/Client TLS wiring | Task 17 |
| Loopback integration tests | Tasks 18, 19 |
| README + crate docs | Task 20 |
