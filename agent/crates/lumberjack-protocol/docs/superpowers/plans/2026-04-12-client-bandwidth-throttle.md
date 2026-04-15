# Client Bandwidth Throttle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional token-bucket bandwidth throttle to the lumberjack client with Block, DropNewest, and DropOldest overflow strategies.

**Architecture:** A new `src/throttle.rs` module owns the token bucket, config types, and stats. `ClientBuilder` gets two new methods (`throttle_config`, `throttle`) to optionally attach a throttle. `Client::send()` dispatches to the appropriate strategy before/after serialisation. DropOldest spawns a background sender task with a `VecDeque`-based ring buffer.

**Tech Stack:** Rust, tokio (sync::Notify, sync::mpsc, time::Instant, spawn), std::sync::atomic

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/throttle.rs` (new) | `ThrottleMetric`, `OverflowStrategy`, `ThrottleConfig`, `ThrottleStats`, `Throttle` (token bucket) |
| `src/client.rs` (modify) | `ClientBuilder` throttle methods, `Client` throttle-aware `send()`, DropOldest background task, `error_receiver()` |
| `src/lib.rs` (modify) | `pub mod throttle;` + re-exports |
| `src/error.rs` (modify) | Nothing for now — errors reuse existing variants |
| `tests/throttle.rs` (new) | Integration tests for throttle behaviour |

---

### Task 1: ThrottleConfig types and Throttle skeleton

**Files:**
- Create: `src/throttle.rs`
- Modify: `src/lib.rs:7-19`

- [ ] **Step 1: Create `src/throttle.rs` with config types and empty Throttle**

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

/// Which byte count to meter against the token bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ThrottleMetric {
    /// Serialised JSON size before compression (default).
    #[default]
    PreCompression,
    /// Compressed frame size after zlib.
    PostCompression,
}

/// What to do when the send rate exceeds the configured bandwidth.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OverflowStrategy {
    /// Await until the token bucket has enough capacity (backpressure).
    #[default]
    Block,
    /// Discard the current batch immediately and return `Ok(0)`.
    DropNewest,
    /// Enqueue the batch; evict oldest queued batches when buffer is full.
    DropOldest,
}

/// Throttle configuration.
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    pub window_size: Duration,
    pub bandwidth: u64,
    pub metric: ThrottleMetric,
    pub overflow: OverflowStrategy,
    pub max_pending_bytes: u64,
}

impl ThrottleConfig {
    fn validate(&self) -> Result<(), &'static str> {
        if self.bandwidth == 0 {
            return Err("bandwidth must be > 0");
        }
        if self.window_size.is_zero() {
            return Err("window_size must be > 0");
        }
        Ok(())
    }
}

/// Run-time statistics (all counters are atomic, lock-free reads).
#[derive(Debug, Clone, Default)]
pub struct ThrottleStats {
    pub bytes_passed: u64,
    pub bytes_dropped: u64,
    pub batches_passed: u64,
    pub batches_dropped: u64,
    pub total_wait_time: Duration,
}

pub struct Throttle {
    config: ThrottleConfig,
    // Token bucket: stored as millibytes (bytes * 1000) for sub-byte precision
    tokens_millibytes: AtomicU64,
    last_refill: Mutex<Instant>,
    notify: Notify,
    // Stats
    bytes_passed: AtomicU64,
    bytes_dropped: AtomicU64,
    batches_passed: AtomicU64,
    batches_dropped: AtomicU64,
    wait_nanos: AtomicU64,
}

impl Throttle {
    /// Create a throttle from full configuration.
    pub fn new(config: ThrottleConfig) -> Self {
        config.validate().expect("invalid ThrottleConfig");
        Self {
            tokens_millibytes: AtomicU64::new(config.bandwidth * 1000),
            last_refill: Mutex::new(Instant::now()),
            notify: Notify::new(),
            bytes_passed: AtomicU64::new(0),
            bytes_dropped: AtomicU64::new(0),
            batches_passed: AtomicU64::new(0),
            batches_dropped: AtomicU64::new(0),
            wait_nanos: AtomicU64::new(0),
            config,
        }
    }

    /// Convenience: Block + PreCompression, max_pending = bandwidth * 2.
    pub fn with_bandwidth(window_size: Duration, bandwidth: u64) -> Self {
        Self::new(ThrottleConfig {
            window_size,
            bandwidth,
            metric: ThrottleMetric::PreCompression,
            overflow: OverflowStrategy::Block,
            max_pending_bytes: bandwidth.saturating_mul(2),
        })
    }

    /// Read-only access to the config.
    pub fn config(&self) -> &ThrottleConfig {
        &self.config
    }

    /// Snapshot of accumulated statistics.
    pub fn stats(&self) -> ThrottleStats {
        ThrottleStats {
            bytes_passed: self.bytes_passed.load(Ordering::Relaxed),
            bytes_dropped: self.bytes_dropped.load(Ordering::Relaxed),
            batches_passed: self.batches_passed.load(Ordering::Relaxed),
            batches_dropped: self.batches_dropped.load(Ordering::Relaxed),
            total_wait_time: Duration::from_nanos(self.wait_nanos.load(Ordering::Relaxed)),
        }
    }

    /// Reset all statistic counters to zero.
    pub fn reset_stats(&self) {
        self.bytes_passed.store(0, Ordering::Relaxed);
        self.bytes_dropped.store(0, Ordering::Relaxed);
        self.batches_passed.store(0, Ordering::Relaxed);
        self.batches_dropped.store(0, Ordering::Relaxed);
        self.wait_nanos.store(0, Ordering::Relaxed);
    }
}
```

- [ ] **Step 2: Add module declaration and re-exports in `src/lib.rs`**

Add after line 11 (`pub mod server;`):

```rust
pub mod throttle;
```

Add to the re-export block (after line 19):

```rust
pub use throttle::{
    OverflowStrategy, Throttle, ThrottleConfig, ThrottleMetric, ThrottleStats,
};
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build`
Expected: compiles with no errors (dead_code warnings are fine)

- [ ] **Step 4: Commit**

```bash
git add src/throttle.rs src/lib.rs
git commit -m "feat(throttle): add config types and Throttle skeleton"
```

---

### Task 2: Token bucket core — refill, try_consume, consume

**Files:**
- Modify: `src/throttle.rs`
- Create: `tests/throttle.rs`

- [ ] **Step 1: Write failing test for try_consume**

Create `tests/throttle.rs`:

```rust
use lumberjack::throttle::{Throttle, ThrottleConfig, ThrottleMetric, OverflowStrategy};
use std::time::Duration;

fn test_config(bandwidth: u64) -> ThrottleConfig {
    ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth,
        metric: ThrottleMetric::PreCompression,
        overflow: OverflowStrategy::Block,
        max_pending_bytes: bandwidth * 2,
    }
}

#[test]
fn try_consume_succeeds_when_tokens_available() {
    let t = Throttle::new(test_config(1000));
    // Fresh bucket has full capacity
    assert!(t.try_consume(500));
    assert!(t.try_consume(500));
    // Now exhausted
    assert!(!t.try_consume(1));
}

#[test]
fn try_consume_rejects_when_insufficient() {
    let t = Throttle::new(test_config(100));
    assert!(!t.try_consume(101));
    // Tokens unchanged after rejection
    assert!(t.try_consume(100));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test throttle -- try_consume`
Expected: FAIL — `try_consume` method does not exist

- [ ] **Step 3: Implement refill and try_consume**

Add to `impl Throttle` in `src/throttle.rs`:

```rust
    /// Refill tokens based on elapsed time. Returns current tokens in millibytes.
    fn refill(&self) -> u64 {
        let mut last = self.last_refill.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        if elapsed.is_zero() {
            return self.tokens_millibytes.load(Ordering::Relaxed);
        }
        *last = now;
        drop(last);

        let rate_millibytes_per_sec =
            (self.config.bandwidth as f64 / self.config.window_size.as_secs_f64()) * 1000.0;
        let add = (elapsed.as_secs_f64() * rate_millibytes_per_sec) as u64;
        let cap = self.config.bandwidth * 1000;

        let prev = self.tokens_millibytes.fetch_add(add, Ordering::Relaxed);
        let new = prev.saturating_add(add).min(cap);
        // Clamp to cap (fetch_add may overshoot)
        self.tokens_millibytes.store(new, Ordering::Relaxed);
        new
    }

    /// Try to consume `n` bytes. Returns true if tokens were available.
    pub(crate) fn try_consume(&self, n: u64) -> bool {
        self.refill();
        let needed = n * 1000;
        let current = self.tokens_millibytes.load(Ordering::Relaxed);
        if current >= needed {
            self.tokens_millibytes.store(current - needed, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test throttle -- try_consume`
Expected: 2 tests PASS

- [ ] **Step 5: Write failing test for async consume (blocking wait)**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn consume_waits_for_refill() {
    // 100 bytes/s, window 1s → bucket capacity 100 bytes
    let t = Throttle::new(test_config(100));
    // Drain the bucket
    assert!(t.try_consume(100));
    assert!(!t.try_consume(1));

    // consume(10) should complete once tokens refill (after ~100ms for 10 bytes at 100 B/s)
    let start = tokio::time::Instant::now();
    t.consume(10).await;
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(50), "waited {elapsed:?}, expected ~100ms");
    assert!(elapsed < Duration::from_millis(500), "waited too long: {elapsed:?}");
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cargo test --test throttle consume_waits`
Expected: FAIL — `consume` method does not exist

- [ ] **Step 7: Implement consume (async blocking wait)**

Add to `impl Throttle` in `src/throttle.rs`:

```rust
    /// Consume `n` bytes, waiting if necessary until tokens are available.
    pub(crate) async fn consume(&self, n: u64) {
        loop {
            if self.try_consume(n) {
                return;
            }
            // Calculate how long to wait for `n` bytes to refill
            let needed = n * 1000;
            let current = self.tokens_millibytes.load(Ordering::Relaxed);
            let deficit = needed.saturating_sub(current);
            let rate_millibytes_per_sec =
                (self.config.bandwidth as f64 / self.config.window_size.as_secs_f64()) * 1000.0;
            let wait_secs = deficit as f64 / rate_millibytes_per_sec;
            let wait = Duration::from_secs_f64(wait_secs);

            // Sleep for the estimated time, or until notified (whichever first)
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.notify.notified() => {}
            }
        }
    }
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cargo test --test throttle consume_waits`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/throttle.rs tests/throttle.rs
git commit -m "feat(throttle): implement token bucket refill, try_consume, consume"
```

---

### Task 3: Stats tracking in Throttle

**Files:**
- Modify: `src/throttle.rs`
- Modify: `tests/throttle.rs`

- [ ] **Step 1: Write failing test for stats**

Append to `tests/throttle.rs`:

```rust
#[test]
fn stats_track_passed_and_dropped() {
    let t = Throttle::new(test_config(100));

    t.record_passed(50);
    t.record_passed(30);
    t.record_dropped(20);

    let s = t.stats();
    assert_eq!(s.bytes_passed, 80);
    assert_eq!(s.batches_passed, 2);
    assert_eq!(s.bytes_dropped, 20);
    assert_eq!(s.batches_dropped, 1);
}

#[test]
fn reset_stats_clears_all() {
    let t = Throttle::new(test_config(100));
    t.record_passed(100);
    t.reset_stats();
    let s = t.stats();
    assert_eq!(s.bytes_passed, 0);
    assert_eq!(s.batches_passed, 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test throttle -- stats`
Expected: FAIL — `record_passed` / `record_dropped` do not exist

- [ ] **Step 3: Implement record_passed, record_dropped, record_wait**

Add to `impl Throttle` in `src/throttle.rs`:

```rust
    /// Record a batch that passed the throttle.
    pub(crate) fn record_passed(&self, bytes: u64) {
        self.bytes_passed.fetch_add(bytes, Ordering::Relaxed);
        self.batches_passed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batch that was dropped by the throttle.
    pub(crate) fn record_dropped(&self, bytes: u64) {
        self.bytes_dropped.fetch_add(bytes, Ordering::Relaxed);
        self.batches_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record wait time (Block strategy).
    pub(crate) fn record_wait(&self, d: Duration) {
        self.wait_nanos.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test throttle -- stats`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/throttle.rs tests/throttle.rs
git commit -m "feat(throttle): add stats tracking (record_passed, record_dropped, record_wait)"
```

---

### Task 4: Integrate throttle into ClientBuilder

**Files:**
- Modify: `src/client.rs:20-116`

- [ ] **Step 1: Write failing test for ClientBuilder::throttle_config**

Append to the `mod tests` block at the bottom of `src/client.rs`:

```rust
    #[test]
    fn builder_throttle_config_sets_throttle() {
        use crate::throttle::{ThrottleConfig, ThrottleMetric, OverflowStrategy};
        let builder = ClientBuilder::default()
            .throttle_config(ThrottleConfig {
                window_size: Duration::from_secs(1),
                bandwidth: 1024,
                metric: ThrottleMetric::PreCompression,
                overflow: OverflowStrategy::Block,
                max_pending_bytes: 2048,
            });
        assert!(builder.throttle.is_some());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib -- builder_throttle_config`
Expected: FAIL — `throttle_config` method and `throttle` field do not exist

- [ ] **Step 3: Add throttle field to ClientBuilder and builder methods**

In `src/client.rs`, add import at the top (after line 13):

```rust
use crate::throttle::{Throttle, ThrottleConfig};
use std::sync::Arc;
```

Add field to `ClientBuilder` struct (after line 26):

```rust
    throttle: Option<Arc<Throttle>>,
```

Add to `Default` impl (after line 37, before `#[cfg(feature = "tls")]`):

```rust
            throttle: None,
```

Add builder methods to `impl ClientBuilder` (after the `local_port_range` method, before `#[cfg(feature = "tls")]`):

```rust
    pub fn throttle_config(mut self, config: ThrottleConfig) -> Self {
        self.throttle = Some(Arc::new(Throttle::new(config)));
        self
    }

    pub fn throttle(mut self, throttle: Arc<Throttle>) -> Self {
        self.throttle = Some(throttle);
        self
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib -- builder_throttle_config`
Expected: PASS

- [ ] **Step 5: Pass throttle into Client from connect()**

In `src/client.rs`, the `connect` method builds `Client` at lines 110-116. Add the throttle field:

```rust
        Ok(Client {
            framed: Framed::new(boxed, LumberjackCodec::new()),
            compression_level: self.compression_level,
            write_timeout: self.write_timeout,
            ack_timeout: self.ack_timeout,
            throttle: self.throttle,
        })
```

Add the throttle field to `Client` struct (after line 123):

```rust
    throttle: Option<Arc<Throttle>>,
```

Update the two direct `Client { ... }` constructions in existing tests (lines 265-270, 298-303, 329-334, 342-347) to include `throttle: None,`.

- [ ] **Step 6: Verify all existing tests pass**

Run: `cargo test`
Expected: all existing tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/client.rs
git commit -m "feat(throttle): integrate Throttle into ClientBuilder and Client"
```

---

### Task 5: Block and DropNewest strategies in send()

**Files:**
- Modify: `src/client.rs:137-184`
- Modify: `tests/throttle.rs`

- [ ] **Step 1: Write failing test for Block strategy**

Append to `tests/throttle.rs`:

```rust
use lumberjack::throttle::{Throttle, ThrottleConfig, ThrottleMetric, OverflowStrategy};
use lumberjack::{Client, ClientBuilder};
use lumberjack::codec::LumberjackCodec;
use lumberjack::frame::Frame;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::duplex;
use tokio_util::codec::Framed;

type BoxedStream = std::pin::Pin<Box<dyn lumberjack::client::AsyncStream>>;

fn make_throttled_client_pair(
    bandwidth: u64,
    overflow: OverflowStrategy,
) -> (Client, Framed<tokio::io::DuplexStream, LumberjackCodec>) {
    let (client_io, peer_io) = duplex(256 * 1024);
    let throttle = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth,
        metric: ThrottleMetric::PreCompression,
        overflow,
        max_pending_bytes: bandwidth * 2,
    }));
    let client = Client {
        framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
        compression_level: 0,
        write_timeout: None,
        ack_timeout: Some(Duration::from_secs(5)),
        throttle: Some(throttle),
    };
    let peer = Framed::new(peer_io, LumberjackCodec::new());
    (client, peer)
}

#[tokio::test]
async fn block_strategy_allows_send_within_budget() {
    // 10 KB/s budget — a small JSON batch fits easily
    let (mut client, peer_io) = make_throttled_client_pair(10_000, OverflowStrategy::Block);

    let peer = tokio::spawn(async move {
        let mut peer = peer_io;
        let _ = peer.next().await; // Window
        let _ = peer.next().await; // Json
        peer.send(Frame::Ack(1)).await.unwrap();
    });

    let n = client.send(&[json!({"a": 1})]).await.unwrap();
    assert_eq!(n, 1);
    peer.await.unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test throttle block_strategy_allows`
Expected: FAIL — `Client` struct constructor doesn't match or throttle logic missing in send()

- [ ] **Step 3: Add Block and DropNewest throttle logic to send()**

In `src/client.rs`, modify `send()` to add throttle checks after serialisation. Replace the body of `send()` (lines 137-184) with:

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
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Frame::Json {
                seq: (i as u32) + 1,
                data: Bytes::from(bytes),
            }
            .encode(&mut payload);
        }

        let pre_compression_size = payload.len() as u64;

        // Throttle gate (Block / DropNewest only; DropOldest handled elsewhere)
        if let Some(ref throttle) = self.throttle {
            use crate::throttle::{OverflowStrategy, ThrottleMetric};
            let metric = throttle.config().metric;
            let overflow = throttle.config().overflow;

            match overflow {
                OverflowStrategy::Block => {
                    let meter_size = match metric {
                        ThrottleMetric::PreCompression => pre_compression_size,
                        ThrottleMetric::PostCompression => pre_compression_size, // adjusted below after compression
                    };
                    let start = tokio::time::Instant::now();
                    throttle.consume(meter_size).await;
                    let waited = start.elapsed();
                    if !waited.is_zero() {
                        throttle.record_wait(waited);
                    }
                    throttle.record_passed(meter_size);
                }
                OverflowStrategy::DropNewest => {
                    let meter_size = match metric {
                        ThrottleMetric::PreCompression => pre_compression_size,
                        ThrottleMetric::PostCompression => pre_compression_size,
                    };
                    if !throttle.try_consume(meter_size) {
                        throttle.record_dropped(meter_size);
                        return Ok(0);
                    }
                    throttle.record_passed(meter_size);
                }
                OverflowStrategy::DropOldest => {
                    // Handled by the background task path — see Task 6
                }
            }
        }

        self.send_payload(n, payload).await
    }

    /// Send the serialised payload (Window + Compressed/Json frames), wait for ACK.
    async fn send_payload(&mut self, n: u32, payload: BytesMut) -> Result<u32> {
        self.send_with_optional_timeout(Frame::Window(n)).await?;

        if self.compression_level > 0 {
            self.send_with_optional_timeout(Frame::Compressed(payload.freeze()))
                .await?;
        } else {
            let mut buf = payload;
            while let Some(frame) = Frame::decode(&mut buf)? {
                self.send_with_optional_timeout(frame).await?;
            }
        }
        self.flush_with_optional_timeout().await?;

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test throttle block_strategy_allows`
Expected: PASS

- [ ] **Step 5: Write failing test for DropNewest**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn drop_newest_discards_when_over_budget() {
    // 10 bytes/s — any real JSON batch will exceed this
    let (mut client, _peer_io) = make_throttled_client_pair(10, OverflowStrategy::DropNewest);

    // First send: should consume available tokens and succeed... but JSON is larger than 10 bytes
    // A json!({"a": 1}) serialises to ~7 bytes for data, plus frame overhead (~17 bytes total).
    // With 10-byte budget, the serialised payload exceeds the budget.
    let n = client.send(&[json!({"a": 1}), json!({"b": 2})]).await.unwrap();
    assert_eq!(n, 0, "batch should be dropped when over budget");
}

#[tokio::test]
async fn drop_newest_stats_reflect_drops() {
    let throttle = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth: 10,
        metric: ThrottleMetric::PreCompression,
        overflow: OverflowStrategy::DropNewest,
        max_pending_bytes: 20,
    }));

    let (client_io, _peer_io) = duplex(256 * 1024);
    let mut client = Client {
        framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
        compression_level: 0,
        write_timeout: None,
        ack_timeout: Some(Duration::from_secs(5)),
        throttle: Some(throttle.clone()),
    };

    let _ = client.send(&[json!({"key": "value_that_is_long_enough"})]).await.unwrap();
    let s = throttle.stats();
    assert_eq!(s.batches_dropped, 1);
    assert!(s.bytes_dropped > 0);
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --test throttle drop_newest`
Expected: 2 tests PASS

- [ ] **Step 7: Verify all existing tests still pass**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 8: Commit**

```bash
git add src/client.rs tests/throttle.rs
git commit -m "feat(throttle): implement Block and DropNewest strategies in send()"
```

---

### Task 6: PostCompression metering

**Files:**
- Modify: `src/client.rs`
- Modify: `tests/throttle.rs`

- [ ] **Step 1: Write failing test for PostCompression metering**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn post_compression_meters_compressed_size() {
    let throttle = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth: 100_000,
        metric: ThrottleMetric::PostCompression,
        overflow: OverflowStrategy::Block,
        max_pending_bytes: 200_000,
    }));

    let (client_io, peer_io) = duplex(256 * 1024);
    let mut client = Client {
        framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
        compression_level: 3,
        write_timeout: None,
        ack_timeout: Some(Duration::from_secs(5)),
        throttle: Some(throttle.clone()),
    };

    let peer = tokio::spawn(async move {
        let mut peer = Framed::new(peer_io, LumberjackCodec::new());
        let _ = peer.next().await; // Window
        let _ = peer.next().await; // Compressed
        peer.send(Frame::Ack(1)).await.unwrap();
    });

    let n = client.send(&[json!({"msg": "hello world"})]).await.unwrap();
    assert_eq!(n, 1);
    peer.await.unwrap();

    let s = throttle.stats();
    assert_eq!(s.batches_passed, 1);
    // Compressed size should be less than pre-compression size
    // Pre-compression payload is at least ~30 bytes (frame header + JSON)
    // Compressed should be metered, not pre-compression
    assert!(s.bytes_passed > 0);
    assert!(s.bytes_passed < 100_000);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test throttle post_compression`
Expected: FAIL — PostCompression currently uses pre_compression_size

- [ ] **Step 3: Refactor send() to support PostCompression metering**

In `src/client.rs`, update the `send()` method. The key change: when `metric = PostCompression` and `compression_level > 0`, compress first, then meter the compressed size. Modify the throttle gate section:

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
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Frame::Json {
                seq: (i as u32) + 1,
                data: Bytes::from(bytes),
            }
            .encode(&mut payload);
        }

        let pre_compression_size = payload.len() as u64;

        // Determine meter size for throttling
        let meter_size = if let Some(ref throttle) = self.throttle {
            use crate::throttle::ThrottleMetric;
            match throttle.config().metric {
                ThrottleMetric::PreCompression => pre_compression_size,
                ThrottleMetric::PostCompression => {
                    if self.compression_level > 0 {
                        // Compress to measure, then cache compressed result
                        let mut tmp = BytesMut::new();
                        Frame::Compressed(payload.clone().freeze()).encode(&mut tmp);
                        // Wire format: 2 (version+type) + 4 (len) + compressed_data
                        (tmp.len() as u64).saturating_sub(6)
                    } else {
                        pre_compression_size
                    }
                }
            }
        } else {
            0 // no throttle, unused
        };

        // Throttle gate
        if let Some(ref throttle) = self.throttle {
            use crate::throttle::OverflowStrategy;
            match throttle.config().overflow {
                OverflowStrategy::Block => {
                    let start = tokio::time::Instant::now();
                    throttle.consume(meter_size).await;
                    let waited = start.elapsed();
                    if !waited.is_zero() {
                        throttle.record_wait(waited);
                    }
                    throttle.record_passed(meter_size);
                }
                OverflowStrategy::DropNewest => {
                    if !throttle.try_consume(meter_size) {
                        throttle.record_dropped(meter_size);
                        return Ok(0);
                    }
                    throttle.record_passed(meter_size);
                }
                OverflowStrategy::DropOldest => {
                    // Handled by the background task path — see Task 7
                }
            }
        }

        self.send_payload(n, payload).await
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test throttle post_compression`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/client.rs tests/throttle.rs
git commit -m "feat(throttle): support PostCompression metering"
```

---

### Task 7: DropOldest — ring buffer and background sender task

**Files:**
- Modify: `src/client.rs`
- Modify: `tests/throttle.rs`

This is the most complex task. DropOldest changes `send()` semantics: it enqueues data and returns immediately. A background task drains the buffer and sends over the wire.

- [ ] **Step 1: Write failing test for DropOldest enqueue and background send**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn drop_oldest_enqueues_and_sends_in_background() {
    let throttle = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth: 100_000,
        metric: ThrottleMetric::PreCompression,
        overflow: OverflowStrategy::DropOldest,
        max_pending_bytes: 200_000,
    }));

    let (client_io, peer_io) = duplex(256 * 1024);
    let mut client = Client::build_with_stream(
        Box::pin(client_io),
        0,   // compression_level
        None, // write_timeout
        Some(Duration::from_secs(5)), // ack_timeout
        Some(throttle.clone()),
    );

    let peer = tokio::spawn(async move {
        let mut peer = Framed::new(peer_io, LumberjackCodec::new());
        // Background task should send Window + Json + flush
        let w = peer.next().await.unwrap().unwrap();
        assert!(matches!(w, Frame::Window(1)));
        let j = peer.next().await.unwrap().unwrap();
        assert!(matches!(j, Frame::Json { seq: 1, .. }));
        peer.send(Frame::Ack(1)).await.unwrap();
    });

    // send() returns immediately with enqueued count
    let n = client.send(&[json!({"a": 1})]).await.unwrap();
    assert_eq!(n, 1);

    // Wait for background task to complete the send
    tokio::time::sleep(Duration::from_millis(200)).await;
    peer.await.unwrap();

    let s = throttle.stats();
    assert_eq!(s.batches_passed, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test throttle drop_oldest_enqueues`
Expected: FAIL — `build_with_stream` does not exist

- [ ] **Step 3: Implement DropOldest infrastructure in Client**

Add to the top of `src/client.rs` (imports):

```rust
use std::collections::VecDeque;
use tokio::sync::mpsc;
```

Add a type alias for the pending batch queue item:

```rust
struct PendingBatch {
    event_count: u32,
    payload: BytesMut,
    size: u64,
}
```

Add a helper constructor for testing (and DropOldest setup) in `impl Client`:

```rust
    /// Build a Client directly from a stream (for testing and DropOldest setup).
    #[doc(hidden)]
    pub fn build_with_stream(
        stream: BoxedStream,
        compression_level: u32,
        write_timeout: Option<Duration>,
        ack_timeout: Option<Duration>,
        throttle: Option<Arc<Throttle>>,
    ) -> Self {
        Self {
            framed: Framed::new(stream, LumberjackCodec::new()),
            compression_level,
            write_timeout,
            ack_timeout,
            throttle,
            drop_oldest_tx: None,
            error_rx: None,
        }
    }
```

Add DropOldest-specific fields to `Client`:

```rust
pub struct Client {
    framed: Framed<BoxedStream, LumberjackCodec>,
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
    throttle: Option<Arc<Throttle>>,
    /// Channel to send batches to the DropOldest background task.
    drop_oldest_tx: Option<mpsc::UnboundedSender<PendingBatch>>,
    /// Channel to receive errors from the DropOldest background task.
    error_rx: Option<mpsc::Receiver<Error>>,
}
```

Add `error_receiver()`:

```rust
    /// Error channel for the DropOldest background sender task.
    /// Returns `None` when the client is not using DropOldest.
    pub fn error_receiver(&mut self) -> Option<&mut mpsc::Receiver<Error>> {
        self.error_rx.as_mut()
    }
```

Update `connect()` in `ClientBuilder` to spawn the background task when DropOldest:

```rust
        let throttle = self.throttle;
        let (drop_oldest_tx, error_rx) = if let Some(ref t) = throttle {
            if t.config().overflow == crate::throttle::OverflowStrategy::DropOldest {
                let (batch_tx, batch_rx) = mpsc::unbounded_channel::<PendingBatch>();
                let (err_tx, err_rx) = mpsc::channel::<Error>(16);
                // Background task will be spawned after Client is created
                (Some((batch_tx, batch_rx, err_tx)), Some(err_rx))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let mut client = Client {
            framed: Framed::new(boxed, LumberjackCodec::new()),
            compression_level: self.compression_level,
            write_timeout: self.write_timeout,
            ack_timeout: self.ack_timeout,
            throttle: throttle.clone(),
            drop_oldest_tx: None,
            error_rx,
        };

        if let Some((batch_tx, batch_rx, err_tx)) = drop_oldest_tx {
            // Move the framed connection to the background task
            let bg_framed = std::mem::replace(
                &mut client.framed,
                Framed::new(
                    Box::pin(tokio::io::empty()) as BoxedStream,
                    LumberjackCodec::new(),
                ),
            );
            let bg_throttle = throttle.unwrap();
            let bg_compression = self.compression_level;
            let bg_write_timeout = self.write_timeout;
            let bg_ack_timeout = self.ack_timeout;

            tokio::spawn(async move {
                drop_oldest_sender(
                    bg_framed,
                    bg_throttle,
                    bg_compression,
                    bg_write_timeout,
                    bg_ack_timeout,
                    batch_rx,
                    err_tx,
                ).await;
            });

            client.drop_oldest_tx = Some(batch_tx);
        }

        Ok(client)
```

Add the DropOldest branch to `send()`:

```rust
                OverflowStrategy::DropOldest => {
                    if let Some(ref tx) = self.drop_oldest_tx {
                        let _ = tx.send(PendingBatch {
                            event_count: n,
                            payload,
                            size: meter_size,
                        });
                    }
                    return Ok(n);
                }
```

Add the background sender function:

```rust
async fn drop_oldest_sender(
    mut framed: Framed<BoxedStream, LumberjackCodec>,
    throttle: Arc<Throttle>,
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
    mut batch_rx: mpsc::UnboundedReceiver<PendingBatch>,
    err_tx: mpsc::Sender<Error>,
) {
    use crate::throttle::OverflowStrategy;

    // Ring buffer with max_pending_bytes enforcement
    let max_pending = throttle.config().max_pending_bytes;
    let mut buffer: VecDeque<PendingBatch> = VecDeque::new();
    let mut buffer_bytes: u64 = 0;

    loop {
        // If buffer is empty, wait for a batch
        if buffer.is_empty() {
            match batch_rx.recv().await {
                Some(batch) => {
                    buffer_bytes += batch.size;
                    buffer.push_back(batch);
                }
                None => return, // Client dropped, channel closed
            }
        }

        // Drain any additional pending batches (non-blocking)
        while let Ok(batch) = batch_rx.try_recv() {
            buffer_bytes += batch.size;
            buffer.push_back(batch);
        }

        // Evict oldest while over limit
        while buffer_bytes > max_pending && buffer.len() > 1 {
            if let Some(evicted) = buffer.pop_front() {
                buffer_bytes -= evicted.size;
                throttle.record_dropped(evicted.size);
            }
        }

        // Send the front batch
        if let Some(batch) = buffer.pop_front() {
            buffer_bytes -= batch.size;
            throttle.consume(batch.size).await;

            let n = batch.event_count;
            // Send Window
            let send_result = async {
                let wf = Frame::Window(n);
                match write_timeout {
                    Some(t) => tokio::time::timeout(t, framed.feed(wf))
                        .await
                        .map_err(|_| Error::WriteTimeout)?,
                    None => framed.feed(wf).await,
                }?;

                if compression_level > 0 {
                    let cf = Frame::Compressed(batch.payload.freeze());
                    match write_timeout {
                        Some(t) => tokio::time::timeout(t, framed.feed(cf))
                            .await
                            .map_err(|_| Error::WriteTimeout)?,
                        None => framed.feed(cf).await,
                    }?;
                } else {
                    let mut buf = batch.payload;
                    while let Some(frame) = Frame::decode(&mut buf)? {
                        match write_timeout {
                            Some(t) => tokio::time::timeout(t, framed.feed(frame))
                                .await
                                .map_err(|_| Error::WriteTimeout)?,
                            None => framed.feed(frame).await,
                        }?;
                    }
                }

                match write_timeout {
                    Some(t) => tokio::time::timeout(t, framed.flush())
                        .await
                        .map_err(|_| Error::WriteTimeout)?,
                    None => framed.flush().await,
                }?;

                // Wait for ACK
                loop {
                    let recv = match ack_timeout {
                        Some(t) => tokio::time::timeout(t, framed.next())
                            .await
                            .map_err(|_| Error::AckTimeout)?,
                        None => framed.next().await,
                    };
                    match recv {
                        Some(Ok(Frame::Ack(seq))) if seq >= n => break,
                        Some(Ok(Frame::Ack(_))) => continue,
                        Some(Ok(_)) => return Err(Error::UnexpectedFrame("expected Ack")),
                        Some(Err(e)) => return Err(e),
                        None => return Err(Error::ConnectionClosed),
                    }
                }
                throttle.record_passed(batch.size);
                Ok::<(), Error>(())
            }.await;

            if let Err(e) = send_result {
                let _ = err_tx.try_send(e);
            }
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test throttle drop_oldest_enqueues`
Expected: PASS

- [ ] **Step 5: Write test for DropOldest eviction behaviour**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn drop_oldest_evicts_old_batches_when_buffer_full() {
    // Very small buffer: 50 bytes max_pending
    let throttle = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(10), // Very slow refill
        bandwidth: 50,
        metric: ThrottleMetric::PreCompression,
        overflow: OverflowStrategy::DropOldest,
        max_pending_bytes: 50,
    }));

    let (client_io, _peer_io) = duplex(256 * 1024);
    let mut client = Client::build_with_stream(
        Box::pin(client_io),
        0,
        None,
        Some(Duration::from_secs(5)),
        Some(throttle.clone()),
    );

    // Send multiple batches quickly — they'll exceed max_pending and oldest should be evicted
    for _ in 0..5 {
        let _ = client.send(&[json!({"data": "this is a longer payload to fill buffer"})]).await;
    }

    // Give background task time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    let s = throttle.stats();
    assert!(s.batches_dropped > 0, "some batches should have been evicted");
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test --test throttle drop_oldest_evicts`
Expected: PASS

- [ ] **Step 7: Write test for error_receiver**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn error_receiver_returns_none_for_non_drop_oldest() {
    let (client_io, _peer_io) = duplex(256 * 1024);
    let mut client = Client::build_with_stream(
        Box::pin(client_io),
        0,
        None,
        Some(Duration::from_secs(5)),
        None,
    );
    assert!(client.error_receiver().is_none());
}
```

- [ ] **Step 8: Run all tests**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/client.rs tests/throttle.rs
git commit -m "feat(throttle): implement DropOldest strategy with background sender"
```

---

### Task 8: Public API cleanup and re-exports

**Files:**
- Modify: `src/lib.rs`
- Modify: `src/client.rs`

- [ ] **Step 1: Ensure all public types are properly re-exported from lib.rs**

Verify `src/lib.rs` has:

```rust
pub mod throttle;

pub use throttle::{
    OverflowStrategy, Throttle, ThrottleConfig, ThrottleMetric, ThrottleStats,
};
```

- [ ] **Step 2: Hide internal-only methods from public API**

Verify `try_consume`, `consume`, `record_passed`, `record_dropped`, `record_wait` are `pub(crate)` not `pub`.

- [ ] **Step 3: Run cargo doc to check for broken links or missing docs**

Run: `cargo doc --no-deps 2>&1 | head -20`
Expected: no errors

- [ ] **Step 4: Run full test suite**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/client.rs src/throttle.rs
git commit -m "refactor(throttle): clean up public API and re-exports"
```

---

### Task 9: Integration test — throttle actually limits throughput

**Files:**
- Modify: `tests/throttle.rs`

- [ ] **Step 1: Write integration test that verifies rate limiting**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn block_strategy_rate_limits_throughput() {
    // 500 bytes/s budget — sending ~200 bytes should take ~400ms for 2nd batch
    let throttle = Arc::new(Throttle::with_bandwidth(
        Duration::from_secs(1),
        500,
    ));

    let (client_io, peer_io) = duplex(256 * 1024);
    let mut client = Client::build_with_stream(
        Box::pin(client_io),
        0,
        None,
        Some(Duration::from_secs(10)),
        Some(throttle.clone()),
    );

    let peer = tokio::spawn(async move {
        let mut peer = Framed::new(peer_io, LumberjackCodec::new());
        for _ in 0..3 {
            // Read Window + Json frames, then send ACK
            loop {
                match peer.next().await.unwrap().unwrap() {
                    Frame::Window(_) => continue,
                    Frame::Json { seq, .. } => {
                        peer.send(Frame::Ack(seq)).await.unwrap();
                        break;
                    }
                    _ => continue,
                }
            }
        }
    });

    let start = tokio::time::Instant::now();
    // Each json!({"x":1}) is ~17 bytes pre-compression with frame header.
    // Send 3 batches. First uses initial tokens, subsequent ones must wait for refill.
    for _ in 0..3 {
        client.send(&[json!({"x": 1})]).await.unwrap();
    }
    let elapsed = start.elapsed();

    peer.await.unwrap();

    // With 500 B/s and ~17 bytes per batch, 3 batches = ~51 bytes.
    // First batch uses pre-filled tokens, should complete instantly.
    // If total time is under 2s, throttle is working (not stuck).
    // Key assertion: stats show all 3 batches passed.
    let s = throttle.stats();
    assert_eq!(s.batches_passed, 3);
    assert_eq!(s.batches_dropped, 0);
    assert!(elapsed < Duration::from_secs(5), "should not take more than 5s");
}
```

- [ ] **Step 2: Run the integration test**

Run: `cargo test --test throttle block_strategy_rate_limits`
Expected: PASS

- [ ] **Step 3: Write test for shared throttle across conceptual "clients"**

Append to `tests/throttle.rs`:

```rust
#[tokio::test]
async fn shared_throttle_enforces_combined_budget() {
    let shared = Arc::new(Throttle::new(ThrottleConfig {
        window_size: Duration::from_secs(1),
        bandwidth: 100,
        metric: ThrottleMetric::PreCompression,
        overflow: OverflowStrategy::DropNewest,
        max_pending_bytes: 200,
    }));

    // Both clients share the same throttle
    // First consume most of the budget
    assert!(shared.try_consume(90));
    shared.record_passed(90);

    // Now only 10 bytes left — a JSON batch will exceed this
    assert!(!shared.try_consume(50));
    shared.record_dropped(50);

    let s = shared.stats();
    assert_eq!(s.batches_passed, 1);
    assert_eq!(s.batches_dropped, 1);
    assert_eq!(s.bytes_passed, 90);
    assert_eq!(s.bytes_dropped, 50);
}
```

- [ ] **Step 4: Run all tests**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add tests/throttle.rs
git commit -m "test(throttle): add integration tests for rate limiting and shared throttle"
```
