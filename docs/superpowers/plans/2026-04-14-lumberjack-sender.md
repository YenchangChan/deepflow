# Lumberjack Sender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Lumberjack v2 Sender to DeepFlow Agent as an alternative to the existing TCP/Protobuf channel, with built-in connection pool HA and multi-endpoint failover. Add a Lumberjack Receiver to Server Ingester using `elastic/go-lumber`.

**Architecture:** Agent-side: new `LumberjackSender<T>` with `LumberjackConnectionPool`, runs tokio `current_thread` runtime per sender thread. Config parsed from `outputs.lumberjack` YAML section, flattened into existing `SenderConfig`. Server-side: new `lumberjack` handler module wrapping `elastic/go-lumber` server, routing JSON events by `_msg_type` to existing handlers. The two channels are mutually exclusive.

**Tech Stack:** Rust (agent), `lumberjack-protocol` crate (external, at `../lumberjack-protocol`), tokio 1.x, serde_json; Go (server), `elastic/go-lumber`, ClickHouse

**Design doc:** `@docs/lumberjack-sender-design.md`

---

## Scope

This plan covers **Agent-side only** (Tasks 1-8). Server-side Lumberjack Receiver is a separate plan — it depends on a different codebase (Go), different module (`server/ingester/`), and can be developed in parallel.

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `@agent/Cargo.toml` | Add `lumberjack-protocol` dependency |
| Modify | `@agent/crates/public/src/sender.rs` | Add `to_json_value()` to Sendable trait |
| Modify | `@agent/src/config/config.rs` | Add `Lumberjack` config struct + `Outputs.lumberjack` field |
| Modify | `@agent/src/config/handler.rs` | Add `lumberjack_*` fields to `SenderConfig`, populate from config |
| Create | `@agent/src/sender/lumberjack_sender.rs` | `LumberjackConnectionPool`, `LumberjackSender<T>`, `LumberjackSenderThread<T>` |
| Modify | `@agent/src/sender/mod.rs` | Add `pub(crate) mod lumberjack_sender;` |
| Modify | `@agent/src/trident.rs` | `if/else` branch to create LumberjackSenderThread when enabled |
| Modify | `@server/agent_config/template.yaml` | Add `outputs.lumberjack` section |

---

## Task 1: Add `lumberjack-protocol` dependency to Agent workspace

**Files:**
- Modify: `@agent/Cargo.toml:108` (dependencies section)

- [ ] **Step 1: Add dependency**

Add after line 108 (`serde_json = "1.0.72"`), keeping alphabetical order:

```toml
lumberjack-protocol = { path = "../lumberjack-protocol", default-features = false, features = ["compression"] }
```

Note: TLS feature omitted for now — will be enabled in a later task.

- [ ] **Step 2: Verify it compiles**

Run from `@agent`:
```bash
cargo check 2>&1 | head -20
```
Expected: compiles without errors (warnings OK).

- [ ] **Step 3: Commit**

```bash
git add agent/Cargo.toml agent/Cargo.lock
git commit -m "chore: add lumberjack-protocol dependency to agent workspace"
```

---

## Task 2: Extend `Sendable` trait with `to_json_value()`

**Files:**
- Modify: `@agent/crates/public/src/sender.rs:22-33`
- Modify: `@agent/crates/public/Cargo.toml:28` (verify serde_json already present)

- [ ] **Step 1: Add `to_json_value()` method to Sendable trait**

In `@agent/crates/public/src/sender.rs`, add a new default method to the `Sendable` trait after line 32 (`to_kv_string`):

```rust
    /// Serialize to JSON for Lumberjack output.
    /// Returns None if this type does not support Lumberjack output.
    fn to_json_value(&self) -> Option<serde_json::Value> {
        None
    }
```

Also add the import at the top of the file (after line 19):

```rust
use serde_json;
```

- [ ] **Step 2: Verify it compiles**

Run from `@agent`:
```bash
cargo check 2>&1 | head -20
```
Expected: compiles. All existing `Sendable` implementations use the default `None` return.

- [ ] **Step 3: Commit**

```bash
git add agent/crates/public/src/sender.rs
git commit -m "feat: add to_json_value() to Sendable trait for Lumberjack output"
```

---

## Task 3: Add `Lumberjack` config struct and YAML parsing

**Files:**
- Modify: `@agent/src/config/config.rs:2860-2867` (near `Socket` struct)
- Modify: `@agent/src/config/config.rs:3077-3083` (`Outputs` struct)
- Modify: `@server/agent_config/template.yaml:7536` (after `multiple_sockets_to_ingester`)

- [ ] **Step 1: Add `LumberjackTls` and `Lumberjack` config structs**

In `@agent/src/config/config.rs`, add after the `Socket` impl block (after line 2878):

```rust
#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LumberjackTls {
    pub enabled: bool,
    pub ca_file: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Lumberjack {
    pub enabled: bool,
    pub endpoints: Vec<String>,
    pub compression_level: u32,
    pub batch_size: usize,
    #[serde(with = "humantime_serde")]
    pub ack_timeout: Duration,
    pub local_port_range: String,
    pub tls: LumberjackTls,
}

impl Default for Lumberjack {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: vec![],
            compression_level: 3,
            batch_size: 100,
            ack_timeout: Duration::from_secs(30),
            local_port_range: String::new(),
            tls: LumberjackTls::default(),
        }
    }
}
```

- [ ] **Step 2: Add `lumberjack` field to `Outputs` struct**

In `@agent/src/config/config.rs`, modify the `Outputs` struct (line 3077) to add the field:

```rust
pub struct Outputs {
    pub socket: Socket,
    pub flow_log: OutputsFlowLog,
    pub flow_metrics: FlowMetrics,
    pub npb: Npb,
    pub compression: OutputCompression,
    pub lumberjack: Lumberjack,
}
```

- [ ] **Step 3: Add `outputs.lumberjack` section to template.yaml**

In `@server/agent_config/template.yaml`, add after line 7536 (`multiple_sockets_to_ingester: false`) and before the `flow_log:` section (line 7537):

```yaml
  # type: section
  # name:
  #   en: Lumberjack
  #   ch: Lumberjack 输出
  # description:
  #   en: |-
  #     When enabled, deepflow-agent sends data via Lumberjack v2 protocol
  #     instead of the default TCP/Protobuf channel. The two channels are
  #     mutually exclusive. Rate limiting is controlled by
  #     global.communication.max_throughput_to_ingester (shared).
  #   ch: |-
  #     启用后，deepflow-agent 通过 Lumberjack v2 协议发送数据，替代默认的
  #     TCP/Protobuf 通道。两个通道互斥，不会同时工作。限流由
  #     global.communication.max_throughput_to_ingester 统一控制。
  lumberjack:
    # type: bool
    # modification: agent_restart
    # description:
    #   en: Enable Lumberjack output channel (disables default TCP/Protobuf channel).
    #   ch: 启用 Lumberjack 输出通道（同时禁用默认的 TCP/Protobuf 通道）。
    enabled: false
    # type: list
    # modification: hot_update
    # description:
    #   en: |-
    #     List of Ingester endpoints for Lumberjack. Each entry is "ip:port".
    #     Port can be omitted and defaults to 7070. Supports multiple endpoints
    #     for HA with round-robin and automatic failover.
    #   ch: |-
    #     Lumberjack Ingester 地址列表，格式为 "ip:port"。端口可省略，默认 7070。
    #     支持多个 endpoint 实现 HA（round-robin + 自动故障切换）。
    endpoints: []
    # type: int
    # range: [0, 9]
    # modification: hot_update
    # description:
    #   en: "zlib compression level. 0 = disabled, 3 = default."
    #   ch: "zlib 压缩级别。0 = 关闭，3 = 默认。"
    compression_level: 3
    # type: int
    # range: [0, 1000]
    # modification: hot_update
    # description:
    #   en: "Maximum number of events per Lumberjack batch. 0 = no batching."
    #   ch: "每个 Lumberjack 批次的最大事件数。0 = 逐条发送。"
    batch_size: 100
    # type: duration
    # range: [1s, 120s]
    # modification: hot_update
    # description:
    #   en: "Maximum time to wait for server ACK before treating as failure."
    #   ch: "等待 Server ACK 的最大超时时间，超时视为发送失败。"
    ack_timeout: 30s
    # type: string
    # modification: hot_update
    # description:
    #   en: |-
    #     Restrict the local (source) TCP port range. Format: "start,end".
    #     Empty string means the OS picks an ephemeral port.
    #   ch: |-
    #     限制本地 TCP 端口范围，格式 "起始,结束"。为空表示 OS 自动选择。
    local_port_range: ""
    # type: section
    # modification: agent_restart
    tls:
      enabled: false
      ca_file: ""
```

- [ ] **Step 4: Regenerate config docs**

Run from `@server/agent_config`:
```bash
python3 gendoc.py
```

- [ ] **Step 5: Verify config parsing**

Run from `@agent`:
```bash
cargo check 2>&1 | head -20
```
Expected: compiles. The `Lumberjack` struct is deserialized from YAML via serde with defaults.

- [ ] **Step 6: Commit**

```bash
git add agent/src/config/config.rs server/agent_config/template.yaml server/agent_config/README.md server/agent_config/README-CH.md
git commit -m "feat: add outputs.lumberjack config section for Lumberjack sender"
```

---

## Task 4: Flatten Lumberjack config into SenderConfig

**Files:**
- Modify: `@agent/src/config/handler.rs:247-271` (SenderConfig struct)
- Modify: `@agent/src/config/handler.rs:2125-2159` (SenderConfig population)

- [ ] **Step 1: Add `lumberjack_*` fields to SenderConfig**

In `@agent/src/config/handler.rs`, add after line 270 (`pub enabled: bool,`) before the closing brace:

```rust
    // Lumberjack output (from outputs.lumberjack, flattened here)
    pub lumberjack_enabled: bool,
    pub lumberjack_endpoints: Vec<(String, u16)>,
    pub lumberjack_compression_level: u32,
    pub lumberjack_batch_size: usize,
    pub lumberjack_ack_timeout: Duration,
    pub lumberjack_local_port_range: Option<(u16, u16)>,
    pub lumberjack_tls_enabled: bool,
    pub lumberjack_tls_ca_path: Option<String>,
```

- [ ] **Step 2: Add helper functions**

Add these free functions near the top of `handler.rs` (or in a helper section):

```rust
fn parse_lumberjack_endpoint(s: &str) -> (String, u16) {
    const DEFAULT_PORT: u16 = 7070;
    if let Some(bracket_end) = s.rfind(']') {
        let host = &s[..=bracket_end];
        let port = s[bracket_end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_PORT);
        return (host.to_string(), port);
    }
    match s.rsplit_once(':') {
        Some((host, port_str)) => match port_str.parse::<u16>() {
            Ok(port) => (host.to_string(), port),
            Err(_) => (s.to_string(), DEFAULT_PORT),
        },
        None => (s.to_string(), DEFAULT_PORT),
    }
}

fn parse_port_range(s: &str) -> Option<(u16, u16)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (start_str, end_str) = s.split_once(',')?;
    let start: u16 = start_str.trim().parse().ok()?;
    let end: u16 = end_str.trim().parse().ok()?;
    if start > 0 && end >= start {
        Some((start, end))
    } else {
        None
    }
}
```

- [ ] **Step 3: Add `is_lumberjack_enabled()` and `validate_lumberjack()` to SenderConfig**

```rust
impl SenderConfig {
    pub fn is_lumberjack_enabled(&self) -> bool {
        self.lumberjack_enabled
    }

    pub fn validate_lumberjack(&self) -> std::result::Result<(), String> {
        if self.lumberjack_enabled && self.lumberjack_endpoints.is_empty() {
            return Err(
                "outputs.lumberjack.enabled is true but endpoints is empty. \
                 Configure at least one endpoint."
                    .to_string(),
            );
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Populate lumberjack fields in SenderConfig initialization**

In `@agent/src/config/handler.rs`, in the `sender: SenderConfig {` block (around line 2125), add after `enabled: conf.outputs.flow_metrics.enabled,` (line 2158):

```rust
                lumberjack_enabled: conf.outputs.lumberjack.enabled,
                lumberjack_endpoints: conf
                    .outputs
                    .lumberjack
                    .endpoints
                    .iter()
                    .map(|s| parse_lumberjack_endpoint(s))
                    .collect(),
                lumberjack_compression_level: conf.outputs.lumberjack.compression_level,
                lumberjack_batch_size: conf.outputs.lumberjack.batch_size,
                lumberjack_ack_timeout: conf.outputs.lumberjack.ack_timeout,
                lumberjack_local_port_range: parse_port_range(
                    &conf.outputs.lumberjack.local_port_range,
                ),
                lumberjack_tls_enabled: conf.outputs.lumberjack.tls.enabled,
                lumberjack_tls_ca_path: if conf.outputs.lumberjack.tls.ca_file.is_empty() {
                    None
                } else {
                    Some(conf.outputs.lumberjack.tls.ca_file.clone())
                },
```

- [ ] **Step 5: Update SenderConfig Default impl**

In the `Default` impl for `SenderConfig` (line 273), it delegates to `ModuleConfig::default()`. Since `Lumberjack` has its own `Default`, this should work automatically. Verify by running:

```bash
cargo check 2>&1 | head -20
```

- [ ] **Step 6: Commit**

```bash
git add agent/src/config/handler.rs
git commit -m "feat: flatten Lumberjack config into SenderConfig with endpoint parsing"
```

---

## Task 5: Implement `LumberjackConnectionPool`

**Files:**
- Create: `@agent/src/sender/lumberjack_sender.rs`

This task creates the connection pool only. The full `LumberjackSender<T>` is in the next task.

- [ ] **Step 1: Create `lumberjack_sender.rs` with pool data structures**

Create `@agent/src/sender/lumberjack_sender.rs`:

```rust
/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...standard header...
 */

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use log::{info, warn};
use lumberjack::{Client, ClientBuilder};
use serde::Serialize;

pub struct PoolStats {
    pub total_endpoints: AtomicU64,
    pub available_endpoints: AtomicU64,
    pub connect_failures: AtomicU64,
    pub write_failures: AtomicU64,
    pub retry_successes: AtomicU64,
    pub all_endpoints_failed: AtomicU64,
}

impl PoolStats {
    pub fn new() -> Self {
        Self {
            total_endpoints: AtomicU64::new(0),
            available_endpoints: AtomicU64::new(0),
            connect_failures: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            retry_successes: AtomicU64::new(0),
            all_endpoints_failed: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
enum EndpointState {
    Available,
    Slow { keepalive_count: u8 },
    Cooldown { until: Instant },
}

struct LumberjackEndpoint {
    ip: String,
    port: u16,
    client: Option<Client>,
    state: EndpointState,
    consecutive_failures: u8,
}

impl LumberjackEndpoint {
    fn new(ip: String, port: u16) -> Self {
        Self {
            ip,
            port,
            client: None,
            state: EndpointState::Available,
            consecutive_failures: 0,
        }
    }

    fn addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    fn is_available(&self) -> bool {
        matches!(self.state, EndpointState::Available)
    }

    fn is_slow(&self) -> bool {
        matches!(self.state, EndpointState::Slow { .. })
    }

    fn can_try_now(&self) -> bool {
        match &self.state {
            EndpointState::Available | EndpointState::Slow { .. } => true,
            EndpointState::Cooldown { until } => Instant::now() >= *until,
        }
    }
}

pub(crate) struct PoolConfig {
    pub compression_level: u32,
    pub ack_timeout: Duration,
    pub local_port_range: Option<(u16, u16)>,
}

pub(crate) struct LumberjackConnectionPool {
    endpoints: Vec<LumberjackEndpoint>,
    next_index: usize,
    config: PoolConfig,
}

impl LumberjackConnectionPool {
    pub fn new(endpoints: Vec<(String, u16)>, config: PoolConfig) -> Self {
        Self {
            endpoints: endpoints
                .into_iter()
                .map(|(ip, port)| LumberjackEndpoint::new(ip, port))
                .collect(),
            next_index: 0,
            config,
        }
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    fn pick_endpoint_by<F>(&mut self, predicate: F) -> Option<usize>
    where
        F: Fn(&LumberjackEndpoint) -> bool,
    {
        let total = self.endpoints.len();
        for _ in 0..total {
            let index = self.next_index % total;
            self.next_index = self.next_index.wrapping_add(1);
            if predicate(&self.endpoints[index]) {
                return Some(index);
            }
        }
        None
    }

    fn pick_endpoint(&mut self) -> Option<usize> {
        // Priority: Available > Slow > Cooldown (expired)
        self.pick_endpoint_by(|ep| ep.is_available())
            .or_else(|| self.pick_endpoint_by(|ep| ep.is_slow()))
            .or_else(|| self.pick_endpoint_by(|ep| ep.can_try_now()))
    }

    pub async fn send_with_retry<T: Serialize>(
        &mut self,
        events: &[T],
        stats: &PoolStats,
    ) -> Result<u32, String> {
        let first_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                stats.all_endpoints_failed.fetch_add(1, Relaxed);
                return Err("all endpoints unavailable".to_string());
            }
        };

        match self.try_send(first_idx, events).await {
            Ok(acked) => {
                self.mark_success(first_idx, 0);
                return Ok(acked);
            }
            Err(e) => {
                let addr = self.endpoints[first_idx].addr();
                warn!("Lumberjack send to {addr} failed: {e}, trying next endpoint");
                self.mark_failure(first_idx);
                stats.write_failures.fetch_add(1, Relaxed);
            }
        }

        // Retry once on a different endpoint
        let retry_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                stats.all_endpoints_failed.fetch_add(1, Relaxed);
                return Err("all endpoints unavailable after retry".to_string());
            }
        };

        match self.try_send(retry_idx, events).await {
            Ok(acked) => {
                self.mark_success(retry_idx, 0);
                stats.retry_successes.fetch_add(1, Relaxed);
                Ok(acked)
            }
            Err(e) => {
                self.mark_failure(retry_idx);
                stats.all_endpoints_failed.fetch_add(1, Relaxed);
                Err(format!("retry also failed: {e}"))
            }
        }
    }

    async fn try_send<T: Serialize>(
        &mut self,
        idx: usize,
        events: &[T],
    ) -> Result<u32, String> {
        let ep = &mut self.endpoints[idx];

        // Lazy connect / reconnect
        if ep.client.is_none() {
            let addr = ep.addr();
            let mut builder = Client::builder()
                .compression_level(self.config.compression_level)
                .ack_timeout(self.config.ack_timeout);
            if let Some((start, end)) = self.config.local_port_range {
                builder = builder.local_port_range(start, end);
            }
            match builder.connect(&addr).await {
                Ok(client) => {
                    info!("Lumberjack connected to {addr}");
                    ep.client = Some(client);
                }
                Err(e) => {
                    return Err(format!("connect to {addr}: {e}"));
                }
            }
        }

        match ep.client.as_mut().unwrap().send(events).await {
            Ok(acked) => Ok(acked),
            Err(e) => {
                ep.client = None; // drop broken connection, will reconnect next time
                Err(format!("send: {e}"))
            }
        }
    }

    fn mark_success(&mut self, idx: usize, keepalives: u32) {
        let ep = &mut self.endpoints[idx];
        ep.consecutive_failures = 0;
        if keepalives > 0 {
            ep.state = EndpointState::Slow {
                keepalive_count: keepalives.min(255) as u8,
            };
        } else {
            ep.state = EndpointState::Available;
        }
    }

    fn mark_failure(&mut self, idx: usize) {
        let ep = &mut self.endpoints[idx];
        ep.consecutive_failures = ep.consecutive_failures.saturating_add(1);
        let exp = ep.consecutive_failures.min(3).saturating_sub(1) as u32;
        let base = Duration::from_secs(10) * 2u32.pow(exp);
        let jitter = Duration::from_millis(rand::random::<u64>() % 5000);
        ep.state = EndpointState::Cooldown {
            until: Instant::now() + base.min(Duration::from_secs(60)) + jitter,
        };
        ep.client = None;
    }
}
```

- [ ] **Step 2: Register module**

In `@agent/src/sender/mod.rs`, add after line 22 (`pub(crate) mod uniform_sender;`):

```rust
pub(crate) mod lumberjack_sender;
```

- [ ] **Step 3: Verify it compiles**

```bash
cd agent && cargo check 2>&1 | head -20
```

- [ ] **Step 4: Commit**

```bash
git add agent/src/sender/lumberjack_sender.rs agent/src/sender/mod.rs
git commit -m "feat: implement LumberjackConnectionPool with round-robin and batch-level failover"
```

---

## Task 6: Implement `LumberjackSender<T>` and `LumberjackSenderThread<T>`

**Files:**
- Modify: `@agent/src/sender/lumberjack_sender.rs`

- [ ] **Step 1: Add sender structs and main loop**

Append to `@agent/src/sender/lumberjack_sender.rs`:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use public::leaky_bucket::LeakyBucket;
use public::queue::Receiver;
use public::sender::{SendMessageType, Sendable};

use crate::config::handler::SenderAccess;
use crate::sender::get_sender_id;

use super::QUEUE_BATCH_SIZE;

pub struct SenderCounter {
    pub rx: AtomicU64,
    pub tx: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub raw_bytes: AtomicU64,
    pub dropped: AtomicU64,
    pub waited: AtomicU64,
}

impl SenderCounter {
    pub fn new() -> Self {
        Self {
            rx: AtomicU64::new(0),
            tx: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
            raw_bytes: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            waited: AtomicU64::new(0),
        }
    }
}

pub struct LumberjackSenderThread<T: Sendable> {
    name: String,
    input: Arc<Receiver<T>>,
    config: SenderAccess,
    leaky_bucket: Arc<LeakyBucket>,
    running: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
    counter: Arc<SenderCounter>,
    pool_stats: Arc<PoolStats>,
    _id: u8,
}

impl<T: Sendable> LumberjackSenderThread<T> {
    pub fn new(
        name: &str,
        input: Arc<Receiver<T>>,
        config: SenderAccess,
        leaky_bucket: Arc<LeakyBucket>,
    ) -> Self {
        Self {
            name: name.to_string(),
            input,
            config,
            leaky_bucket,
            running: Arc::new(AtomicBool::new(false)),
            thread: Mutex::new(None),
            counter: Arc::new(SenderCounter::new()),
            pool_stats: Arc::new(PoolStats::new()),
            _id: get_sender_id(),
        }
    }

    pub fn start(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        let running = self.running.clone();
        let input = self.input.clone();
        let config = self.config.clone();
        let leaky_bucket = self.leaky_bucket.clone();
        let counter = self.counter.clone();
        let pool_stats = self.pool_stats.clone();
        let name = self.name.clone();

        let handle = thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let mut sender = LumberjackSender::new(
                    input, config, leaky_bucket, counter, pool_stats, running,
                );
                rt.block_on(sender.run());
            })
            .unwrap();

        *self.thread.lock().unwrap() = Some(handle);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

struct LumberjackSender<T: Sendable> {
    input: Arc<Receiver<T>>,
    config: SenderAccess,
    leaky_bucket: Arc<LeakyBucket>,
    counter: Arc<SenderCounter>,
    pool: Option<LumberjackConnectionPool>,
    pool_stats: Arc<PoolStats>,
    running: Arc<AtomicBool>,
}

impl<T: Sendable> LumberjackSender<T> {
    fn new(
        input: Arc<Receiver<T>>,
        config: SenderAccess,
        leaky_bucket: Arc<LeakyBucket>,
        counter: Arc<SenderCounter>,
        pool_stats: Arc<PoolStats>,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            input,
            config,
            leaky_bucket,
            counter,
            pool: None,
            pool_stats,
            running,
        }
    }

    fn ensure_pool(&mut self) {
        if self.pool.is_some() {
            return;
        }
        let cfg = self.config.load();
        let endpoints = cfg.lumberjack_endpoints.clone();
        if endpoints.is_empty() {
            return;
        }
        self.pool = Some(LumberjackConnectionPool::new(
            endpoints,
            PoolConfig {
                compression_level: cfg.lumberjack_compression_level,
                ack_timeout: cfg.lumberjack_ack_timeout,
                local_port_range: cfg.lumberjack_local_port_range,
            },
        ));
        self.pool_stats
            .total_endpoints
            .store(self.pool.as_ref().unwrap().endpoint_count() as u64, Relaxed);
    }

    async fn run(&mut self) {
        let mut batch = Vec::with_capacity(QUEUE_BATCH_SIZE);

        while self.running.load(Ordering::Relaxed) {
            self.ensure_pool();

            // Recv batch from queue
            batch.clear();
            let cfg = self.config.load();
            let batch_size = if cfg.lumberjack_batch_size == 0 {
                1
            } else {
                cfg.lumberjack_batch_size.min(QUEUE_BATCH_SIZE)
            };

            match self.input.recv(Some(Duration::from_secs(3))) {
                Ok(msg) => batch.push(msg),
                Err(_) => continue,
            }
            // Drain up to batch_size
            while batch.len() < batch_size {
                match self.input.recv(Some(Duration::ZERO)) {
                    Ok(msg) => batch.push(msg),
                    Err(_) => break,
                }
            }

            self.counter.rx.fetch_add(batch.len() as u64, Relaxed);

            // JSON serialize
            let mut events: Vec<serde_json::Value> = Vec::with_capacity(batch.len());
            let mut raw_bytes: u64 = 0;
            for msg in batch.drain(..) {
                if let Some(json) = msg.to_json_value() {
                    let size = serde_json::to_string(&json).map(|s| s.len() as u64).unwrap_or(0);
                    raw_bytes += size;
                    events.push(json);
                } else {
                    self.counter.dropped.fetch_add(1, Relaxed);
                }
            }

            if events.is_empty() {
                continue;
            }
            self.counter.raw_bytes.fetch_add(raw_bytes, Relaxed);

            // Rate limiting
            if !self.leaky_bucket.acquire(raw_bytes) {
                self.counter.dropped.fetch_add(events.len() as u64, Relaxed);
                continue;
            }

            // Send via connection pool
            let pool = match self.pool.as_mut() {
                Some(p) => p,
                None => {
                    self.counter.dropped.fetch_add(events.len() as u64, Relaxed);
                    continue;
                }
            };

            match pool.send_with_retry(&events, &self.pool_stats).await {
                Ok(acked) => {
                    self.counter.tx.fetch_add(acked as u64, Relaxed);
                    self.counter.tx_bytes.fetch_add(raw_bytes, Relaxed);
                }
                Err(e) => {
                    warn!("Lumberjack send failed: {e}");
                    self.counter.dropped.fetch_add(events.len() as u64, Relaxed);
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd agent && cargo check 2>&1 | head -20
```

- [ ] **Step 3: Commit**

```bash
git add agent/src/sender/lumberjack_sender.rs
git commit -m "feat: implement LumberjackSender with JSON serialization and rate limiting"
```

---

## Task 7: Integrate into trident.rs

**Files:**
- Modify: `@agent/src/trident.rs:2438` (where UniformSenderThread instances are created)

- [ ] **Step 1: Add import**

At the top of `@agent/src/trident.rs`, add:

```rust
use crate::sender::lumberjack_sender::LumberjackSenderThread;
```

- [ ] **Step 2: Add startup validation**

In the `AgentComponents::new()` method, before the sender creation section (around line 2420), add validation:

```rust
        if let Err(e) = candidate_config.sender.validate_lumberjack() {
            warn!("Lumberjack config error: {e}, falling back to TCP/Protobuf");
        }
```

- [ ] **Step 3: Wrap each UniformSenderThread creation in an if/else**

This is the most involved change. For each `UniformSenderThread::new(...)` call (there are ~15), wrap it in a conditional. Example for l4_flow (line 2438):

```rust
        let l4_flow_uniform_sender = if candidate_config.sender.is_lumberjack_enabled() {
            // Lumberjack path — type-erased to avoid changing the field type
            // TODO: This requires a shared trait or enum wrapper for UniformSenderThread
            // and LumberjackSenderThread. For now, use a feature flag approach.
            todo!("Wire LumberjackSenderThread into agent component lifecycle")
        } else {
            UniformSenderThread::new(
                l4_flow_aggr_queue_name,
                Arc::new(l4_flow_aggr_receiver),
                config_handler.sender(),
                stats_collector.clone(),
                exception_handler.clone(),
                None,
                if candidate_config.metric_server.l4_flow_log_compressed {
                    SenderEncoder::Zstd
                } else {
                    SenderEncoder::Raw
                },
                sender_leaky_bucket.clone(),
            )
        };
```

**Note:** The exact integration strategy depends on whether `LumberjackSenderThread` and `UniformSenderThread` share a common trait. In `trident.rs`, the senders are stored as named struct fields. The cleanest approach is to store them as `Box<dyn SenderThread>` or use an enum wrapper. This step will require careful review of the surrounding code — adapt to the actual pattern.

- [ ] **Step 4: Verify it compiles**

```bash
cd agent && cargo check 2>&1 | head -20
```

- [ ] **Step 5: Commit**

```bash
git add agent/src/trident.rs
git commit -m "feat: integrate LumberjackSender into agent startup with config-based channel selection"
```

---

## Task 8: Implement `to_json_value()` for Phase 1 data types

**Files:**
- Modify: impl files for `ApplicationLog`, `BoxAppProtoLogsData`, `BoxedTaggedFlow`
- These are spread across multiple files; locate with:
  ```bash
  cd agent && grep -rn "impl Sendable for" --include="*.rs" | head -20
  ```

- [ ] **Step 1: Find all Sendable implementations**

```bash
cd agent && grep -rn "impl Sendable for" --include="*.rs"
```

This will show all types that implement `Sendable`. For Phase 1, implement `to_json_value()` for:
- `ApplicationLog`
- `BoxAppProtoLogsData` (L7 protocol logs)
- `BoxedTaggedFlow` (L4 flow logs)

- [ ] **Step 2: Implement `to_json_value()` for each type**

For each type, override the default method. Example pattern:

```rust
fn to_json_value(&self) -> Option<serde_json::Value> {
    // Serialize the inner data struct which already derives Serialize
    // Add _msg_type metadata field
    let mut map = serde_json::to_value(self).ok()?;
    if let Some(obj) = map.as_object_mut() {
        obj.insert("_msg_type".to_string(), serde_json::Value::String("application_log".to_string()));
    }
    Some(map)
}
```

The exact implementation depends on each type's internal structure. Some types already implement `serde::Serialize`; others may need it added. Inspect each type and implement accordingly.

- [ ] **Step 3: Verify it compiles**

```bash
cd agent && cargo check 2>&1 | head -20
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: implement to_json_value() for ApplicationLog, ProtocolLog, and TaggedFlow"
```

---

## Dependency Graph

```
Task 1 (dependency) ─────────────────────────────────┐
Task 2 (Sendable trait) ──────────────────────┐      │
Task 3 (config structs + YAML) ──┐            │      │
Task 4 (SenderConfig flatten) ───┤            │      │
                                 ▼            ▼      ▼
                          Task 5 (ConnectionPool) ───►Task 6 (Sender) ──► Task 7 (trident.rs)
                                                                          Task 8 (JSON impls)
```

Tasks 1, 2, 3 can be done in parallel. Task 4 depends on 3. Tasks 5-6 depend on 1+2+4. Task 7 depends on 5+6. Task 8 depends on 2 and can be done in parallel with 5-7.
