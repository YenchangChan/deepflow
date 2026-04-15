# Client Bandwidth Throttle Design

## Overview

Add bandwidth throttling to the lumberjack client (sender side), allowing users to
limit the data rate within a configurable time window. The throttle is optional — when
not configured, existing behaviour is unchanged with zero overhead.

## Motivation

Upstream log producers can burst at rates that overwhelm the network link or the
downstream receiver. A client-side throttle lets operators cap the send rate and choose
how to handle excess data (block, drop newest, or drop oldest).

---

## Public API

### New types

```rust
/// Which byte count to meter against the token bucket.
#[derive(Debug, Clone, Copy, Default)]
pub enum ThrottleMetric {
    /// Serialised JSON size before compression (default).
    #[default]
    PreCompression,
    /// Compressed frame size after zlib.
    PostCompression,
}

/// What to do when the send rate exceeds the configured bandwidth.
#[derive(Debug, Clone, Copy, Default)]
pub enum OverflowStrategy {
    /// Await until the token bucket has enough capacity (backpressure).
    #[default]
    Block,
    /// Discard the current batch immediately and return `Ok(0)`.
    DropNewest,
    /// Enqueue the batch; if the internal buffer exceeds `max_pending_bytes`,
    /// evict the oldest queued batch(es) to make room.
    DropOldest,
}

/// Throttle configuration.
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Time window over which `bandwidth` bytes are allowed.
    pub window_size: Duration,
    /// Maximum bytes permitted per `window_size`.
    pub bandwidth: u64,
    /// Metering point (default: `PreCompression`).
    pub metric: ThrottleMetric,
    /// Overflow behaviour (default: `Block`).
    pub overflow: OverflowStrategy,
    /// Maximum bytes buffered internally for `DropOldest`.
    /// Ignored by other strategies. Default: `bandwidth * 2`.
    pub max_pending_bytes: u64,
}

/// Run-time statistics (all counters are atomic, lock-free reads).
#[derive(Debug, Clone, Default)]
pub struct ThrottleStats {
    /// Total bytes that passed through the throttle.
    pub bytes_passed: u64,
    /// Total bytes dropped by the throttle.
    pub bytes_dropped: u64,
    /// Number of batches that passed through.
    pub batches_passed: u64,
    /// Number of batches dropped.
    pub batches_dropped: u64,
    /// Cumulative time `send()` spent waiting for tokens (Block strategy).
    pub total_wait_time: Duration,
}
```

### `Throttle`

```rust
/// Bandwidth throttle backed by a token-bucket algorithm.
/// `Send + Sync` — wrap in `Arc` to share across clients.
pub struct Throttle { /* ... */ }

impl Throttle {
    /// Create a throttle from full configuration.
    pub fn new(config: ThrottleConfig) -> Self;

    /// Convenience: `Block` + `PreCompression`, `max_pending = bandwidth * 2`.
    pub fn with_bandwidth(window_size: Duration, bandwidth: u64) -> Self;

    /// Snapshot of accumulated statistics (lock-free).
    pub fn stats(&self) -> ThrottleStats;

    /// Reset all statistic counters to zero.
    pub fn reset_stats(&self);
}
```

### `ClientBuilder` additions

```rust
impl ClientBuilder {
    /// Attach an independently-owned throttle built from `config`.
    pub fn throttle_config(self, config: ThrottleConfig) -> Self;

    /// Attach a shared (externally-owned) throttle.
    pub fn throttle(self, throttle: Arc<Throttle>) -> Self;
}
```

Both methods are mutually exclusive; the last call wins.
Calling neither preserves the current unthrottled behaviour.

### `Client` additions

```rust
impl Client {
    /// Error channel for the `DropOldest` background sender task.
    /// Returns `None` when the client is not using `DropOldest`.
    pub fn error_receiver(&mut self) -> Option<&mut mpsc::Receiver<Error>>;
}
```

---

## Token-Bucket Algorithm

Linear (continuous) refill — avoids bursty traffic at window boundaries.

```
refill_rate  = bandwidth / window_size          (bytes per second)
elapsed      = now - last_refill
refill       = elapsed * refill_rate
tokens       = min(tokens + refill, bandwidth)  // bucket capacity = bandwidth
last_refill  = now
```

Key properties:

* Bucket capacity equals `bandwidth`, so a single batch can use the full
  window allowance if it arrives after an idle period.
* Refill is computed on every `try_consume` / `consume` call — no background
  timer needed.

### Internal methods (not public)

```rust
impl Throttle {
    /// Try to consume `n` bytes. Returns `true` if tokens were available.
    fn try_consume(&self, n: u64) -> bool;

    /// Async: consume `n` bytes, waiting if necessary.
    async fn consume(&self, n: u64);

    /// Async: wait until at least `n` tokens are available (does not consume).
    async fn wait_for_tokens(&self, n: u64);
}
```

Wakeup uses `tokio::sync::Notify`; `consume` calls `notify_one` after refill
so that other waiters can re-check.

---

## `send()` Behaviour Per Strategy

### Block

```
serialize events → compute batch_size
loop {
    if throttle.try_consume(batch_size) → break
    throttle.wait_for_tokens(batch_size).await
}
compress → send frames → flush → wait ACK
return Ok(acked_count)
```

No internal buffer. Backpressure propagates directly to the caller.

### DropNewest

```
serialize events → compute batch_size
if !throttle.try_consume(batch_size) {
    // update stats: bytes_dropped, batches_dropped
    return Ok(0)
}
compress → send frames → flush → wait ACK
return Ok(acked_count)
```

No internal buffer. Caller sees `Ok(0)` when a batch is discarded.

### DropOldest

```
serialize events → push serialised batch into ring buffer
while buffer.total_bytes > max_pending_bytes {
    evict oldest batch   // update stats: bytes_dropped, batches_dropped
}
return Ok(events.len() as u32)   // enqueued, not yet sent
```

A background task (spawned at `Client` creation) drains the buffer:

```
loop {
    batch = buffer.pop_front().await       // suspend when empty
    throttle.consume(batch.size).await     // wait for tokens
    send Window + Compressed/Json frames
    wait for ACK   // errors → error channel
}
```

Return value semantics change: `Ok(n)` means *enqueued*, not *ACKed*.

### PostCompression metric

When `metric = PostCompression`, the throttle cannot meter until after
compression. The flow becomes:

```
serialize → compress → measure compressed size → consume tokens → send
```

If the batch is then dropped (DropNewest), the compression work is wasted.
This is an inherent trade-off of metering post-compression and is acceptable.

---

## Error Handling

### Block / DropNewest

Errors (network, ACK timeout) propagate through `send()`'s return value, same
as today.

### DropOldest background task

* Errors are sent to a bounded `tokio::sync::mpsc` channel (capacity 16).
* If the channel is full, new errors are silently dropped to avoid blocking
  the sender task.
* The caller may poll `client.error_receiver()` or ignore it entirely.

---

## Observability

`ThrottleStats` provides atomic counters:

| Counter | Meaning |
|---------|---------|
| `bytes_passed` | Total bytes that cleared the throttle |
| `bytes_dropped` | Total bytes discarded (DropNewest / DropOldest eviction) |
| `batches_passed` | Batch count that cleared the throttle |
| `batches_dropped` | Batch count discarded |
| `total_wait_time` | Cumulative wall-clock time blocked waiting for tokens |

All fields use `AtomicU64` (nanoseconds for the duration). No external metrics
crate dependency — users bridge `stats()` into their own monitoring stack.

---

## Client Lifecycle

| Event | Block / DropNewest | DropOldest |
|-------|-------------------|------------|
| `connect()` | No extra work | Spawns background sender task |
| `send()` | Synchronous throttle check | Enqueue into ring buffer |
| `drop(Client)` | TCP close | Abort background task; buffered data is lost |

Graceful-flush (`flush()`) is intentionally deferred to a future iteration.

---

## File Changes

| File | Change |
|------|--------|
| `src/throttle.rs` (new) | `Throttle`, `ThrottleConfig`, `ThrottleStats`, token bucket, ring buffer |
| `src/client.rs` | `ClientBuilder::throttle` / `throttle_config`; `send()` strategy dispatch; `DropOldest` background task; `error_receiver()` |
| `src/lib.rs` | `pub mod throttle;` re-exports |
| `src/error.rs` | (if needed) new error variants for throttle-specific failures |

---

## Explicitly Out of Scope (v1)

* **Graceful flush on drop** — `flush()` method deferred.
* **Delivery callback for DropOldest** — no per-batch ACK notification.
* **Dynamic bandwidth adjustment** — config is immutable after construction.
* **External metrics integration** — users bridge `ThrottleStats` themselves.
