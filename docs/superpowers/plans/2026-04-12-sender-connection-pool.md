# Sender Connection Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable DeepFlow Agent to send data to multiple Ingester endpoints with batch-level failover, eliminating single-IP dependency.

**Architecture:** Add `ingester_ips: Vec<String>` config field alongside existing `ingester_ip`. Introduce a `ConnectionPool` that wraps multiple `Connection` instances and provides round-robin selection with batch-level retry. `UniformSender` delegates TCP writes to the pool instead of managing a single connection directly. The existing `ConnectionType` (Global/PrivateShared/Private) system is preserved - each type simply holds a pool instead of a single connection.

**Tech Stack:** Rust (stable), std::net::TcpStream, existing arc-swap config pattern

**Design doc:** `@docs/design/agent/sender-connection-pool-proposal.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `agent/src/sender/connection_pool.rs` | `ConnectionPool` struct: manages multiple `Connection`s, round-robin selection, batch-level retry |
| Modify | `agent/src/sender/mod.rs` | Export `connection_pool` module |
| Modify | `agent/src/config/config.rs:2653-2687` | Add `ingester_ips: Vec<String>` to `Communication` struct |
| Modify | `agent/src/config/handler.rs:248-271` | Add `dest_ips: Vec<String>` to `SenderConfig` |
| Modify | `agent/src/config/handler.rs:1978-1985` | Build `dest_ips` from `ingester_ips` or fallback to `ingester_ip` |
| Modify | `agent/src/config/handler.rs:4342-4355` | Add hot-update handler for `ingester_ips` |
| Modify | `agent/src/sender/uniform_sender.rs:362-364` | Replace `GLOBAL_CONNECTION` with `GLOBAL_CONNECTION_POOL` |
| Modify | `agent/src/sender/uniform_sender.rs:373-396` | Make `Connection` public for use by `connection_pool.rs` |
| Modify | `agent/src/sender/uniform_sender.rs:398-430` | Replace connection fields with pool fields in `UniformSender` |
| Modify | `agent/src/sender/uniform_sender.rs:486-537` | Update `update_connection()` to configure pools |
| Modify | `agent/src/sender/uniform_sender.rs:554-666` | Rewrite `send_buffer()` to use pool with retry |
| Modify | `server/agent_config/template.yaml:~786` | Add `ingester_ips` config definition |

---

## Task 1: Add `ingester_ips` config field

**Files:**
- Modify: `agent/src/config/config.rs:2653-2687`

- [ ] **Step 1: Add `ingester_ips` field to `Communication` struct**

In `agent/src/config/config.rs`, add `ingester_ips` field to the `Communication` struct (after `ingester_ip`):

```rust
pub struct Communication {
    #[serde(with = "humantime_serde")]
    pub proactive_request_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub max_escape_duration: Duration,
    pub ingester_ip: String,
    pub ingester_ips: Vec<String>,  // NEW: multi-endpoint support
    pub ingester_port: u16,
    #[serde(skip)]
    pub grpc_buffer_size: usize,
    pub max_throughput_to_ingester: u64,
    #[serde(deserialize_with = "to_traffic_overflow_action")]
    pub ingester_traffic_overflow_action: TrafficOverflowAction,
    pub request_via_nat_ip: bool,
    pub proxy_controller_ip: String,
    pub proxy_controller_port: u16,
}
```

And in `Default for Communication`, add:

```rust
ingester_ips: vec![],
```

- [ ] **Step 2: Verify compilation**

Run: `cd agent && cargo check 2>&1 | head -20`
Expected: compiles without errors (serde derives `Deserialize` for `Vec<String>` automatically)

- [ ] **Step 3: Commit**

```bash
git add agent/src/config/config.rs
git commit -m "feat(sender): add ingester_ips field to Communication config"
```

---

## Task 2: Add `dest_ips` to `SenderConfig` and wire config flow

**Files:**
- Modify: `agent/src/config/handler.rs:248-271` (SenderConfig)
- Modify: `agent/src/config/handler.rs:1978-1985` (dest_ip construction)
- Modify: `agent/src/config/handler.rs:2125-2126` (SenderConfig assignment)
- Modify: `agent/src/config/handler.rs:4342-4355` (hot-update handler)

- [ ] **Step 1: Add `dest_ips` field to `SenderConfig`**

In `agent/src/config/handler.rs`, add `dest_ips` after `dest_ip`:

```rust
pub struct SenderConfig {
    pub dest_ip: String,
    pub dest_ips: Vec<String>,  // NEW
    // ... rest unchanged
}
```

- [ ] **Step 2: Build `dest_ips` from config**

In `agent/src/config/handler.rs`, near lines 1978-1985, replace the `dest_ip` construction block with:

```rust
let dest_ips: Vec<String> = if !conf.global.communication.ingester_ips.is_empty() {
    conf.global.communication.ingester_ips.clone()
} else if !conf.global.communication.ingester_ip.is_empty() {
    vec![conf.global.communication.ingester_ip.clone()]
} else {
    vec![match controller_ip {
        IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.to_string(),
        IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.to_string(),
    }]
};
let dest_ip = dest_ips[0].clone();
```

- [ ] **Step 3: Pass `dest_ips` in SenderConfig assignment**

In the `SenderConfig { ... }` block (around line 2125), add:

```rust
sender: SenderConfig {
    dest_ip: dest_ip.clone(),
    dest_ips: dest_ips.clone(),  // NEW
    // ... rest unchanged
}
```

- [ ] **Step 4: Add hot-update handler for `ingester_ips`**

Near lines 4342-4355, after the existing `ingester_ip` update handler, add:

```rust
if communication.ingester_ips != new_communication.ingester_ips {
    info!(
        "Update global.communication.ingester_ips from {:?} to {:?}.",
        communication.ingester_ips, new_communication.ingester_ips
    );
    communication.ingester_ips = new_communication.ingester_ips.clone();
}
```

- [ ] **Step 5: Verify compilation**

Run: `cd agent && cargo check 2>&1 | head -20`
Expected: compiles. There will be a warning about `dest_ips` being unused - that's fine for now.

- [ ] **Step 6: Commit**

```bash
git add agent/src/config/handler.rs
git commit -m "feat(sender): add dest_ips to SenderConfig with backward compatibility"
```

---

## Task 3: Create `ConnectionPool` module

**Files:**
- Create: `agent/src/sender/connection_pool.rs`
- Modify: `agent/src/sender/mod.rs`
- Modify: `agent/src/sender/uniform_sender.rs:373-396` (make `Connection` public)

- [ ] **Step 1: Make `Connection` and its fields visible to the new module**

In `agent/src/sender/uniform_sender.rs`, change `Connection` fields from private to `pub(super)`:

```rust
pub(super) struct Connection {
    pub(super) tcp_stream: Option<TcpStream>,
    pub(super) reconnect_interval: u8,
    pub(super) dest_ip: String,
    pub(super) dest_port: u16,
    pub(super) reconnect: bool,
    pub(super) last_reconnect: Duration,
}
```

- [ ] **Step 2: Create `connection_pool.rs` with core struct and round-robin selection**

Create `agent/src/sender/connection_pool.rs`:

```rust
/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io::{ErrorKind, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use log::{debug, error, info};
use rand::{thread_rng, RngCore};

use super::uniform_sender::Connection;

const TCP_WRITE_TIMEOUT: u64 = 3; // s
const DEFAULT_RECONNECT_INTERVAL: u8 = 10; // s

pub(crate) struct ConnectionPool {
    connections: Vec<Connection>,
    next_index: usize,
    port: u16,
    pub(crate) counter: PoolCounter,
}

#[derive(Debug, Default)]
pub(crate) struct PoolCounter {
    pub(crate) retry_successes: AtomicU64,
    pub(crate) all_endpoints_failed: AtomicU64,
}

impl ConnectionPool {
    pub(crate) fn new(ips: &[String], port: u16) -> Self {
        let connections = ips
            .iter()
            .map(|ip| Connection {
                tcp_stream: None,
                reconnect_interval: DEFAULT_RECONNECT_INTERVAL,
                dest_ip: ip.clone(),
                dest_port: port,
                reconnect: false,
                last_reconnect: Duration::ZERO,
            })
            .collect();
        Self {
            connections,
            next_index: 0,
            port,
            counter: PoolCounter::default(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Update endpoints list and port. Connections to removed endpoints are
    /// closed; new endpoints get fresh connections; existing endpoints keep
    /// their current connection state.
    pub(crate) fn update_endpoints(&mut self, ips: &[String], port: u16) {
        let port_changed = self.port != port;
        self.port = port;

        // Close connections whose IP is no longer in the list or whose port changed
        self.connections.retain_mut(|conn| {
            let keep = ips.contains(&conn.dest_ip) && !port_changed;
            if !keep {
                if let Some(stream) = conn.tcp_stream.take() {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            }
            keep
        });

        // Add new endpoints
        for ip in ips {
            if !self.connections.iter().any(|c| c.dest_ip == *ip) || port_changed {
                if port_changed && self.connections.iter().any(|c| c.dest_ip == *ip) {
                    continue; // already retained with port update pending via reconnect
                }
                self.connections.push(Connection {
                    tcp_stream: None,
                    reconnect_interval: DEFAULT_RECONNECT_INTERVAL,
                    dest_ip: ip.clone(),
                    dest_port: port,
                    reconnect: false,
                    last_reconnect: Duration::ZERO,
                });
            }
        }

        // Update port on retained connections
        if port_changed {
            for conn in &mut self.connections {
                conn.dest_port = port;
                conn.reconnect = true;
                conn.last_reconnect = Duration::ZERO;
            }
        }

        if self.next_index >= self.connections.len() {
            self.next_index = 0;
        }
    }

    /// Pick next available connection index using round-robin, skipping
    /// those in reconnect cooldown.
    fn pick_connection(&mut self) -> Option<usize> {
        let total = self.connections.len();
        if total == 0 {
            return None;
        }
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        for _ in 0..total {
            let index = self.next_index % total;
            self.next_index = self.next_index.wrapping_add(1);
            if self.can_try_now(&self.connections[index], now) {
                return Some(index);
            }
        }
        None
    }

    fn can_try_now(&self, conn: &Connection, now: Duration) -> bool {
        if conn.tcp_stream.is_some() && !conn.reconnect {
            return true;
        }
        // Check reconnect cooldown
        let last = conn.last_reconnect;
        if last > now {
            return true; // clock went backward, allow retry
        }
        last + Duration::from_secs(conn.reconnect_interval as u64) <= now
    }

    /// Ensure the connection at `index` has a live TcpStream. Returns true
    /// if the stream is ready.
    fn ensure_connected(&mut self, index: usize, sender_name: &str) -> bool {
        let conn = &mut self.connections[index];

        if conn.tcp_stream.is_some() && !conn.reconnect {
            return true;
        }

        // Shut down old stream
        if let Some(stream) = conn.tcp_stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        if conn.last_reconnect > now {
            conn.last_reconnect = now;
        }
        if conn.last_reconnect + Duration::from_secs(conn.reconnect_interval as u64) > now {
            return false;
        }

        conn.last_reconnect = now;
        conn.tcp_stream = TcpStream::connect((conn.dest_ip.as_str(), conn.dest_port)).ok();
        if let Some(stream) = conn.tcp_stream.as_mut() {
            if let Err(e) =
                stream.set_write_timeout(Some(Duration::from_secs(TCP_WRITE_TIMEOUT)))
            {
                debug!(
                    "{sender_name} sender tcp stream set write timeout failed {e}"
                );
                conn.tcp_stream.take();
                return false;
            }
            info!(
                "{sender_name} sender tcp connection to {}:{} succeed.",
                conn.dest_ip, conn.dest_port
            );
            conn.reconnect = false;
            conn.reconnect_interval = 0;
            true
        } else {
            error!(
                "{sender_name} sender tcp connection to {}:{} failed",
                conn.dest_ip, conn.dest_port
            );
            conn.reconnect_interval =
                DEFAULT_RECONNECT_INTERVAL + (thread_rng().next_u64() % 5) as u8;
            false
        }
    }

    /// Write buffer to a connection. On failure, marks the connection for
    /// reconnect and returns Err.
    fn write_to(
        &mut self,
        index: usize,
        buffer: &[u8],
        sender_name: &str,
        running: &std::sync::atomic::AtomicBool,
    ) -> std::io::Result<()> {
        let conn = &mut self.connections[index];
        let stream = conn.tcp_stream.as_mut().unwrap();
        let mut offset = 0usize;
        while running.load(Ordering::Relaxed) {
            match stream.write(&buffer[offset..]) {
                Ok(size) => {
                    offset += size;
                    if offset == buffer.len() {
                        return Ok(());
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    debug!("{sender_name} sender tcp stream write data block {e}");
                    continue;
                }
                Err(e) => {
                    error!(
                        "{sender_name} sender tcp stream write data to {}:{} failed: {e}",
                        conn.dest_ip, conn.dest_port
                    );
                    conn.tcp_stream.take();
                    conn.reconnect_interval =
                        DEFAULT_RECONNECT_INTERVAL + (thread_rng().next_u64() % 5) as u8;
                    return Err(e);
                }
            }
        }
        Err(std::io::Error::new(ErrorKind::Interrupted, "sender stopped"))
    }

    /// Send buffer with up to `max_retries` failover attempts to other
    /// endpoints. Returns Ok(()) if any endpoint accepted the write.
    pub(crate) fn send_with_retry(
        &mut self,
        buffer: &[u8],
        max_retries: usize,
        sender_name: &str,
        running: &std::sync::atomic::AtomicBool,
    ) -> std::io::Result<()> {
        let mut last_err = None;
        for attempt in 0..=max_retries {
            let index = match self.pick_connection() {
                Some(i) => i,
                None => {
                    self.counter
                        .all_endpoints_failed
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(last_err.unwrap_or_else(|| {
                        std::io::Error::new(
                            ErrorKind::NotConnected,
                            "all endpoints in reconnect cooldown",
                        )
                    }));
                }
            };

            if !self.ensure_connected(index, sender_name) {
                continue;
            }

            match self.write_to(index, buffer, sender_name, running) {
                Ok(()) => {
                    if attempt > 0 {
                        self.counter
                            .retry_successes
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }

        self.counter
            .all_endpoints_failed
            .fetch_add(1, Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| {
            std::io::Error::new(ErrorKind::NotConnected, "all retry attempts failed")
        }))
    }

    /// Close all TCP streams in the pool.
    pub(crate) fn close_all(&mut self) {
        for conn in &mut self.connections {
            if let Some(stream) = conn.tcp_stream.take() {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
    }

    /// Get current endpoint addresses for logging.
    pub(crate) fn endpoint_summary(&self) -> String {
        self.connections
            .iter()
            .map(|c| format!("{}:{}", c.dest_ip, c.dest_port))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_connections_for_each_ip() {
        let pool = ConnectionPool::new(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        assert_eq!(pool.connections.len(), 2);
        assert_eq!(pool.connections[0].dest_ip, "10.0.0.1");
        assert_eq!(pool.connections[1].dest_ip, "10.0.0.2");
        assert_eq!(pool.connections[0].dest_port, 30033);
    }

    #[test]
    fn test_new_single_ip_backward_compat() {
        let pool = ConnectionPool::new(&["192.168.1.1".to_string()], 30033);
        assert_eq!(pool.connections.len(), 1);
    }

    #[test]
    fn test_new_empty_ips() {
        let pool = ConnectionPool::new(&[], 30033);
        assert!(pool.is_empty());
        assert_eq!(pool.connections.len(), 0);
    }

    #[test]
    fn test_update_endpoints_adds_new() {
        let mut pool = ConnectionPool::new(&["10.0.0.1".to_string()], 30033);
        pool.update_endpoints(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        assert_eq!(pool.connections.len(), 2);
    }

    #[test]
    fn test_update_endpoints_removes_old() {
        let mut pool = ConnectionPool::new(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        pool.update_endpoints(&["10.0.0.2".to_string()], 30033);
        assert_eq!(pool.connections.len(), 1);
        assert_eq!(pool.connections[0].dest_ip, "10.0.0.2");
    }

    #[test]
    fn test_update_endpoints_port_change_triggers_reconnect() {
        let mut pool = ConnectionPool::new(&["10.0.0.1".to_string()], 30033);
        pool.connections[0].reconnect = false;
        pool.update_endpoints(&["10.0.0.1".to_string()], 30034);
        assert_eq!(pool.connections[0].dest_port, 30034);
        assert!(pool.connections[0].reconnect);
    }

    #[test]
    fn test_pick_connection_round_robin() {
        let mut pool = ConnectionPool::new(
            &[
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
                "10.0.0.3".to_string(),
            ],
            30033,
        );
        // Simulate all connections having streams so they are "available"
        // (without real TCP, just test index rotation)
        // All are in initial state: no stream, reconnect_interval=10, last_reconnect=ZERO
        // can_try_now returns true when last_reconnect + interval <= now,
        // and last_reconnect is ZERO so this should pass for all.
        let idx0 = pool.pick_connection();
        let idx1 = pool.pick_connection();
        let idx2 = pool.pick_connection();
        let idx3 = pool.pick_connection(); // wraps around
        assert_eq!(idx0, Some(0));
        assert_eq!(idx1, Some(1));
        assert_eq!(idx2, Some(2));
        assert_eq!(idx3, Some(0));
    }

    #[test]
    fn test_pick_connection_skips_cooldown() {
        let mut pool = ConnectionPool::new(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        // Put first connection in cooldown
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        pool.connections[0].last_reconnect = now;
        pool.connections[0].reconnect_interval = 60; // long cooldown
        pool.connections[0].reconnect = true;
        // Second connection is available (last_reconnect = ZERO, interval = 10)
        let idx = pool.pick_connection();
        assert_eq!(idx, Some(1));
    }

    #[test]
    fn test_pick_connection_all_in_cooldown_returns_none() {
        let mut pool = ConnectionPool::new(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        for conn in &mut pool.connections {
            conn.last_reconnect = now;
            conn.reconnect_interval = 60;
            conn.reconnect = true;
        }
        assert_eq!(pool.pick_connection(), None);
    }

    #[test]
    fn test_endpoint_summary() {
        let pool = ConnectionPool::new(
            &["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            30033,
        );
        let summary = pool.endpoint_summary();
        assert!(summary.contains("10.0.0.1:30033"));
        assert!(summary.contains("10.0.0.2:30033"));
    }
}
```

- [ ] **Step 3: Export module in `mod.rs`**

In `agent/src/sender/mod.rs`, add after `mod tcp_packet;`:

```rust
pub(crate) mod connection_pool;
```

- [ ] **Step 4: Run tests**

Run: `cd agent && cargo test -p deepflow-agent --lib sender::connection_pool::tests -- --nocapture 2>&1 | tail -20`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add agent/src/sender/connection_pool.rs agent/src/sender/mod.rs agent/src/sender/uniform_sender.rs
git commit -m "feat(sender): add ConnectionPool with round-robin selection and retry"
```

---

## Task 4: Integrate ConnectionPool into UniformSender

**Files:**
- Modify: `agent/src/sender/uniform_sender.rs`

This is the core integration task. It replaces the three connection holders (global, private_shared, private) with three corresponding pool holders, and rewrites `send_buffer()` and `update_connection()`.

- [ ] **Step 1: Update imports and global static**

In `agent/src/sender/uniform_sender.rs`, add import:

```rust
use super::connection_pool::ConnectionPool;
```

Replace the `GLOBAL_CONNECTION` lazy_static:

```rust
lazy_static! {
    static ref GLOBAL_CONNECTION_POOL: Arc<Mutex<ConnectionPool>> =
        Arc::new(Mutex::new(ConnectionPool::new(&[], 30033)));
}
```

- [ ] **Step 2: Update `UniformSender` struct fields**

Replace the connection-related fields in `UniformSender`:

```rust
pub struct UniformSender<T> {
    id: usize,
    name: &'static str,

    input: Arc<Receiver<T>>,
    counter: Arc<SenderCounter>,
    overwritten_count: u64,

    encoder: Encoder<T>,
    private_pool: Mutex<ConnectionPool>,                    // was: private_conn
    private_shared_pool: Option<Arc<Mutex<ConnectionPool>>>, // was: private_shared_conn
    global_shared_pool: Arc<Mutex<ConnectionPool>>,          // was: global_shared_conn
    connection_type: ConnectionType,
    multiple_sockets_to_ingester: bool,
    dest_ip: String,
    dest_ips: Vec<String>,  // NEW
    dest_port: u16,
    max_throughput_mbps: u64,
    leaky_bucket: Arc<LeakyBucket>,
    last_traffic_overflow: Duration,

    config: SenderAccess,

    running: Arc<AtomicBool>,
    stats: Arc<Collector>,
    stats_registered: bool,
    exception_handler: ExceptionHandler,
    buf_writer: Option<BufWriter<File>>,
    file_path: String,
    pre_file_path: String,
    written_size: u64,

    cached: bool,
}
```

- [ ] **Step 3: Update `UniformSender::new()`**

Update the constructor to initialize pool fields:

```rust
pub fn new(
    id: usize,
    name: &'static str,
    input: Arc<Receiver<T>>,
    config: SenderAccess,
    running: Arc<AtomicBool>,
    stats: Arc<Collector>,
    exception_handler: ExceptionHandler,
    private_shared_pool: Option<Arc<Mutex<ConnectionPool>>>,
    sender_encoder: SenderEncoder,
    leaky_bucket: Arc<LeakyBucket>,
) -> Self {
    let cfg = config.load();
    Self {
        id,
        name,
        input,
        counter: Arc::new(SenderCounter::default()),
        overwritten_count: 0,
        encoder: Encoder::new(
            0,
            SendMessageType::TaggedFlow,
            cfg.agent_id,
            u8::from(sender_encoder),
        ),
        config,
        private_pool: Mutex::new(ConnectionPool::new(&[], cfg.dest_port)),
        private_shared_pool,
        global_shared_pool: GLOBAL_CONNECTION_POOL.clone(),
        connection_type: ConnectionType::Global,
        multiple_sockets_to_ingester: false,
        dest_ip: "127.0.0.1".to_string(),
        dest_ips: vec![],
        dest_port: cfg.dest_port,
        max_throughput_mbps: 0,
        leaky_bucket,
        last_traffic_overflow: Duration::ZERO,

        running,
        stats,
        stats_registered: false,
        exception_handler,
        buf_writer: None,
        file_path: String::new(),
        pre_file_path: String::new(),
        written_size: 0,
        cached: true,
    }
}
```

- [ ] **Step 4: Update `UniformSenderThread` to pass pool instead of connection**

In `UniformSenderThread`, change `private_shared_conn` to `private_shared_pool`:

```rust
pub struct UniformSenderThread<T> {
    id: usize,
    name: &'static str,
    input: Arc<Receiver<T>>,
    config: SenderAccess,

    thread_handle: Option<JoinHandle<()>>,

    running: Arc<AtomicBool>,
    stats: Arc<Collector>,
    exception_handler: ExceptionHandler,

    private_shared_pool: Option<Arc<Mutex<ConnectionPool>>>,
    sender_encoder: SenderEncoder,
    leaky_bucket: Arc<LeakyBucket>,
}
```

Update `UniformSenderThread::new()` to accept `Option<Arc<Mutex<ConnectionPool>>>` and pass it through to `UniformSender::new()` in `start()`.

- [ ] **Step 5: Rewrite `update_connection()`**

Replace the existing `update_connection()` method:

```rust
fn update_connection(&mut self, cfg: &SenderConfig) {
    if self.multiple_sockets_to_ingester != cfg.multiple_sockets_to_ingester
        || self.dest_ips != cfg.dest_ips
        || self.dest_ip != cfg.dest_ip
        || self.dest_port != cfg.dest_port
    {
        self.multiple_sockets_to_ingester = cfg.multiple_sockets_to_ingester;
        self.dest_ip = cfg.dest_ip.clone();
        self.dest_ips = cfg.dest_ips.clone();
        self.dest_port = cfg.dest_port;

        let old_connection_type = self.connection_type;
        if self.multiple_sockets_to_ingester {
            if self.private_shared_pool.is_some() {
                self.connection_type = ConnectionType::PrivateShared;
            } else {
                self.connection_type = ConnectionType::Private;
            }
            self.global_shared_pool.lock().unwrap().close_all();
        } else {
            self.connection_type = ConnectionType::Global;
            self.private_pool.lock().unwrap().close_all();
            if let Some(pool) = self.private_shared_pool.as_ref() {
                pool.lock().unwrap().close_all();
            }
        }
        if old_connection_type != self.connection_type {
            info!(
                "{} sender update connection type from {old_connection_type:?} to {:?}",
                self.name, self.connection_type
            );
        }

        let mut pool = match self.connection_type {
            ConnectionType::Global => self.global_shared_pool.lock().unwrap(),
            ConnectionType::PrivateShared => {
                self.private_shared_pool.as_ref().unwrap().lock().unwrap()
            }
            ConnectionType::Private => self.private_pool.lock().unwrap(),
        };
        pool.update_endpoints(&self.dest_ips, self.dest_port);
        info!(
            "{} sender updated endpoints: [{}]",
            self.name,
            pool.endpoint_summary()
        );
    }
}
```

- [ ] **Step 6: Rewrite `send_buffer()`**

Replace the existing `send_buffer()` method:

```rust
fn send_buffer(&mut self, config: &SenderConfig) {
    if self.is_traffic_overflow(config) {
        return;
    }
    if !self.running.load(Ordering::Relaxed) {
        return;
    }

    let buffer = self.encoder.get_buffer().to_vec();

    let mut pool = match self.connection_type {
        ConnectionType::Global => self.global_shared_pool.lock().unwrap(),
        ConnectionType::PrivateShared => {
            self.private_shared_pool.as_ref().unwrap().lock().unwrap()
        }
        ConnectionType::Private => self.private_pool.lock().unwrap(),
    };

    if pool.is_empty() {
        self.counter.dropped.fetch_add(1, Ordering::Relaxed);
        return;
    }

    match pool.send_with_retry(&buffer, 1, self.name, &self.running) {
        Ok(()) => {
            self.counter.tx.fetch_add(1, Ordering::Relaxed);
            self.counter
                .tx_bytes
                .fetch_add(buffer.len() as u64, Ordering::Relaxed);
        }
        Err(e) => {
            if self.counter.dropped.load(Ordering::Relaxed) == 0 {
                self.exception_handler.set(
                    Exception::AnalyzerSocketError,
                    Some(format!(
                        "{} sender all endpoints failed: {e}",
                        self.name
                    )),
                );
            }
            self.counter.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

- [ ] **Step 7: Verify compilation**

Run: `cd agent && cargo check 2>&1 | head -30`
Expected: compiles without errors

- [ ] **Step 8: Run existing tests (if any) and ConnectionPool unit tests**

Run: `cd agent && cargo test -p deepflow-agent --lib sender:: 2>&1 | tail -20`
Expected: ConnectionPool tests pass

- [ ] **Step 9: Commit**

```bash
git add agent/src/sender/uniform_sender.rs agent/src/sender/connection_pool.rs
git commit -m "feat(sender): integrate ConnectionPool into UniformSender with batch-level retry"
```

---

## Task 5: Update all `UniformSenderThread` creation sites in `trident.rs`

**Files:**
- Modify: `agent/src/trident.rs`

The `UniformSenderThread::new()` signature now takes `Option<Arc<Mutex<ConnectionPool>>>` instead of `Option<Arc<Mutex<Connection>>>`. All creation sites need to be updated.

- [ ] **Step 1: Update imports in `trident.rs`**

Add or update the import:

```rust
use crate::sender::connection_pool::ConnectionPool;
```

Remove the import of `Connection` from `uniform_sender` if it exists.

- [ ] **Step 2: Update all `Connection::new()` calls to `ConnectionPool::new()`**

Find all lines like:

```rust
let log_stats_shared_connection = Arc::new(Mutex::new(Connection::new()));
```

And replace with:

```rust
let log_stats_shared_pool = Arc::new(Mutex::new(ConnectionPool::new(&[], 30033)));
```

Update corresponding `Some(log_stats_shared_connection.clone())` arguments to `Some(log_stats_shared_pool.clone())`.

- [ ] **Step 3: Update all `UniformSenderThread::new()` calls**

Change the `private_shared_conn` argument from `Some(xxx_connection.clone())` / `None` to `Some(xxx_pool.clone())` / `None` for every sender creation site.

- [ ] **Step 4: Verify compilation**

Run: `cd agent && cargo check 2>&1 | head -30`
Expected: compiles without errors

- [ ] **Step 5: Commit**

```bash
git add agent/src/trident.rs
git commit -m "refactor(sender): update sender creation sites to use ConnectionPool"
```

---

## Task 6: Add `ingester_ips` to server config template

**Files:**
- Modify: `server/agent_config/template.yaml`
- Regenerate: `server/agent_config/README.md` and `server/agent_config/README-CH.md`

- [ ] **Step 1: Add `ingester_ips` config definition**

In `server/agent_config/template.yaml`, after the `ingester_ip` block (around line 786), add:

```yaml
# type: strings
# name:
#   en: Ingester IP Addresses
#   ch: Ingester IP 地址列表
# modification: hot_update
# ee_feature: false
# description:
#   en: |-
#     When this value is set, deepflow-agent will use these IPs to send data
#     to multiple deepflow-server ingesters with client-side failover.
#     Takes precedence over ingester_ip when non-empty.
#   ch: |-
#     当设置此值时，deepflow-agent 将使用这些 IP 向多个 deepflow-server
#     ingester 发送数据，并支持客户端侧故障切换。
#     当非空时优先于 ingester_ip。
ingester_ips: []
```

- [ ] **Step 2: Regenerate docs**

Run: `cd server/agent_config && python3 gendoc.py`

- [ ] **Step 3: Verify generated docs**

Run: `grep -n "ingester_ips" server/agent_config/README.md server/agent_config/README-CH.md`
Expected: the new field appears in both generated docs

- [ ] **Step 4: Commit**

```bash
git add server/agent_config/template.yaml server/agent_config/README.md server/agent_config/README-CH.md
git commit -m "feat(config): add ingester_ips config for multi-endpoint sender"
```

---

## Task 7: Add pool-level counters to sender stats

**Files:**
- Modify: `agent/src/sender/uniform_sender.rs` (SenderCounter)

- [ ] **Step 1: Add pool stats to `SenderCounter`**

Add new fields to `SenderCounter`:

```rust
#[derive(Debug, Default)]
pub struct SenderCounter {
    raw_bytes: AtomicU64,
    pub rx: AtomicU64,
    pub tx: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub dropped: AtomicU64,
    pub waited: AtomicU64,
    pub retry_successes: AtomicU64,       // NEW
    pub all_endpoints_failed: AtomicU64,  // NEW
}
```

- [ ] **Step 2: Export new counters in `get_counters()`**

Add to the `RefCountable` impl's `get_counters()` method:

```rust
(
    "retry-successes",
    CounterType::Counted,
    CounterValue::Unsigned(self.retry_successes.swap(0, Ordering::Relaxed)),
),
(
    "all-endpoints-failed",
    CounterType::Counted,
    CounterValue::Unsigned(self.all_endpoints_failed.swap(0, Ordering::Relaxed)),
),
```

- [ ] **Step 3: Collect pool counters in the processing loop**

In `UniformSender::process()`, after each `flush_encoder()` call, or periodically in the timeout branch, transfer counters from `ConnectionPool::counter` to `SenderCounter`. Add a helper method:

```rust
fn collect_pool_stats(&self) {
    let pool = match self.connection_type {
        ConnectionType::Global => self.global_shared_pool.lock().unwrap(),
        ConnectionType::PrivateShared => {
            self.private_shared_pool.as_ref().unwrap().lock().unwrap()
        }
        ConnectionType::Private => self.private_pool.lock().unwrap(),
    };
    let retries = pool.counter.retry_successes.swap(0, Ordering::Relaxed);
    let failed = pool.counter.all_endpoints_failed.swap(0, Ordering::Relaxed);
    if retries > 0 {
        self.counter
            .retry_successes
            .fetch_add(retries, Ordering::Relaxed);
    }
    if failed > 0 {
        self.counter
            .all_endpoints_failed
            .fetch_add(failed, Ordering::Relaxed);
    }
}
```

Call `self.collect_pool_stats()` in the `Err(Error::Timeout)` branch of `process()`.

- [ ] **Step 4: Verify compilation and tests**

Run: `cd agent && cargo check && cargo test -p deepflow-agent --lib sender::connection_pool::tests 2>&1 | tail -20`
Expected: compiles and all tests pass

- [ ] **Step 5: Commit**

```bash
git add agent/src/sender/uniform_sender.rs
git commit -m "feat(sender): add retry-successes and all-endpoints-failed counters"
```

---

## Task 8: Full build verification

- [ ] **Step 1: Format code**

Run: `cd agent && cargo fmt`

- [ ] **Step 2: Full build**

Run: `cd agent && cargo build 2>&1 | tail -20`
Expected: builds successfully

- [ ] **Step 3: Run all sender tests**

Run: `cd agent && cargo test -p deepflow-agent --lib sender:: 2>&1 | tail -30`
Expected: all tests pass

- [ ] **Step 4: Final commit (if fmt changed anything)**

```bash
git add -u agent/
git commit -m "style(sender): format code"
```
