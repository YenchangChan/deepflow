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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::access::Access;
use log::{info, warn};
use lumberjack::{Client, ClientBuilder};
use public::leaky_bucket::LeakyBucket;
use public::queue::Receiver;
use public::sender::Sendable;
use serde::Serialize;

use crate::config::handler::SenderAccess;

use super::get_sender_id;
use super::uniform_sender::SenderCounter;
use super::QUEUE_BATCH_SIZE;

// ============================================================
// PoolStats — connection pool level metrics
// ============================================================

#[derive(Debug, Default)]
pub struct PoolStats {
    pub write_failures: AtomicU64,
    pub retry_successes: AtomicU64,
    pub all_endpoints_failed: AtomicU64,
}

// ============================================================
// EndpointState — per-endpoint health state machine
// ============================================================

enum EndpointState {
    Available,
    Slow { _keepalive_count: u8 },
    Cooldown { until: Instant },
}

// ============================================================
// LumberjackEndpoint — one endpoint with its Client connection
// ============================================================

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

// ============================================================
// PoolConfig — shared connection parameters
// ============================================================

struct PoolConfig {
    compression_level: u32,
    ack_timeout: Duration,
    local_port_range: Option<(u16, u16)>,
}

// ============================================================
// LumberjackConnectionPool — multi-endpoint HA connection pool
// ============================================================

pub(crate) struct LumberjackConnectionPool {
    endpoints: Vec<LumberjackEndpoint>,
    next_index: usize,
    config: PoolConfig,
}

impl LumberjackConnectionPool {
    fn new(endpoints: Vec<(String, u16)>, config: PoolConfig) -> Self {
        Self {
            endpoints: endpoints
                .into_iter()
                .map(|(ip, port)| LumberjackEndpoint::new(ip, port))
                .collect(),
            next_index: 0,
            config,
        }
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

    async fn send_with_retry<T: Serialize>(
        &mut self,
        events: &[T],
        stats: &PoolStats,
    ) -> Result<u32, String> {
        let first_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                stats.all_endpoints_failed.fetch_add(1, Ordering::Relaxed);
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
                stats.write_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Retry once on a different endpoint
        let retry_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                stats.all_endpoints_failed.fetch_add(1, Ordering::Relaxed);
                return Err("all endpoints unavailable after retry".to_string());
            }
        };

        match self.try_send(retry_idx, events).await {
            Ok(acked) => {
                self.mark_success(retry_idx, 0);
                stats.retry_successes.fetch_add(1, Ordering::Relaxed);
                Ok(acked)
            }
            Err(e) => {
                self.mark_failure(retry_idx);
                stats.all_endpoints_failed.fetch_add(1, Ordering::Relaxed);
                Err(format!("retry also failed: {e}"))
            }
        }
    }

    async fn try_send<T: Serialize>(&mut self, idx: usize, events: &[T]) -> Result<u32, String> {
        let ep = &mut self.endpoints[idx];

        // Lazy connect / reconnect
        if ep.client.is_none() {
            let addr = ep.addr();
            let mut builder = ClientBuilder::default()
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
                ep.client = None; // drop broken connection
                Err(format!("send: {e}"))
            }
        }
    }

    fn mark_success(&mut self, idx: usize, keepalives: u32) {
        let ep = &mut self.endpoints[idx];
        ep.consecutive_failures = 0;
        if keepalives > 0 {
            ep.state = EndpointState::Slow {
                _keepalive_count: keepalives.min(255) as u8,
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

// ============================================================
// LumberjackSenderThread<T> — thread lifecycle wrapper
// ============================================================

pub struct LumberjackSenderThread<T: Sendable> {
    id: usize,
    name: &'static str,
    input: Arc<Receiver<T>>,
    config: SenderAccess,
    leaky_bucket: Arc<LeakyBucket>,
    running: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>,
    counter: Arc<SenderCounter>,
    pool_stats: Arc<PoolStats>,
}

impl<T: Sendable> LumberjackSenderThread<T> {
    pub fn new(
        name: &'static str,
        input: Arc<Receiver<T>>,
        config: SenderAccess,
        leaky_bucket: Arc<LeakyBucket>,
    ) -> Self {
        Self {
            id: get_sender_id() as usize,
            name,
            input,
            config,
            leaky_bucket,
            running: Arc::new(AtomicBool::new(false)),
            thread_handle: None,
            counter: Arc::new(SenderCounter::default()),
            pool_stats: Arc::new(PoolStats::default()),
        }
    }

    pub fn start(&mut self) {
        if self.running.swap(true, Ordering::Relaxed) {
            warn!(
                "{} lumberjack sender id: {} already started",
                self.name, self.id
            );
            return;
        }
        let running = self.running.clone();
        let input = self.input.clone();
        let config = self.config.clone();
        let leaky_bucket = self.leaky_bucket.clone();
        let counter = self.counter.clone();
        let pool_stats = self.pool_stats.clone();

        self.thread_handle = Some(
            thread::Builder::new()
                .name("lumberjack-sender".to_owned())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let mut sender = LumberjackSender::new(
                        input,
                        config,
                        leaky_bucket,
                        counter,
                        pool_stats,
                        running,
                    );
                    rt.block_on(sender.run());
                })
                .unwrap(),
        );
        info!("{} lumberjack sender id: {} started", self.name, self.id);
    }

    pub fn notify_stop(&mut self) -> Option<JoinHandle<()>> {
        if !self.running.swap(false, Ordering::Relaxed) {
            warn!("lumberjack sender id: {} already stopped", self.id);
            return None;
        }
        info!("notified stopping lumberjack sender id: {}", self.id);
        self.thread_handle.take()
    }

    pub fn stop(&mut self) {
        if !self.running.swap(false, Ordering::Relaxed) {
            warn!("lumberjack sender id: {} already stopped", self.id);
            return;
        }
        info!("stopping lumberjack sender id: {}", self.id);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
        info!("stopped lumberjack sender id: {}", self.id);
    }
}

// ============================================================
// LumberjackSender<T> — async send loop
// ============================================================

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
    }

    async fn run(&mut self) {
        let mut batch = Vec::with_capacity(QUEUE_BATCH_SIZE);

        while self.running.load(Ordering::Relaxed) {
            self.ensure_pool();

            batch.clear();
            let cfg = self.config.load();
            let batch_size = if cfg.lumberjack_batch_size == 0 {
                1
            } else {
                cfg.lumberjack_batch_size.min(QUEUE_BATCH_SIZE)
            };

            // Block on first message, then drain up to batch_size
            match self.input.recv(Some(Duration::from_secs(3))) {
                Ok(msg) => batch.push(msg),
                Err(_) => continue,
            }
            while batch.len() < batch_size {
                match self.input.recv(Some(Duration::ZERO)) {
                    Ok(msg) => batch.push(msg),
                    Err(_) => break,
                }
            }

            self.counter
                .rx
                .fetch_add(batch.len() as u64, Ordering::Relaxed);

            // JSON serialize and inject metadata
            let mut events: Vec<serde_json::Value> = Vec::with_capacity(batch.len());
            let mut raw_bytes: u64 = 0;
            let agent_id = cfg.agent_id;
            let team_id = cfg.team_id;
            let org_id = cfg.organize_id;
            for msg in batch.drain(..) {
                if let Some(mut json) = msg.to_json_value() {
                    if let Some(obj) = json.as_object_mut() {
                        if let Some(topic) = cfg.topics.get(&msg.message_type()) {
                            obj.insert("@topic".into(), serde_json::json!(topic));
                        }
                        obj.insert("_agent_id".into(), serde_json::json!(agent_id));
                        obj.insert("_team_id".into(), serde_json::json!(team_id));
                        obj.insert("_org_id".into(), serde_json::json!(org_id));
                        if let Some(tags) = cfg.labels.get(&msg.message_type()) {
                            let tag_obj: serde_json::Map<String, serde_json::Value> = tags
                                .iter()
                                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                                .collect();
                            obj.insert("labels".into(), serde_json::Value::Object(tag_obj));
                        }
                    }
                    let size = serde_json::to_vec(&json)
                        .map(|v| v.len() as u64)
                        .unwrap_or(0);
                    raw_bytes += size;
                    events.push(json);
                } else {
                    self.counter.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }

            if events.is_empty() {
                continue;
            }

            // Rate limiting
            if !self.leaky_bucket.acquire(raw_bytes) {
                self.counter
                    .dropped
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                continue;
            }

            // Send via connection pool
            let pool = match self.pool.as_mut() {
                Some(p) => p,
                None => {
                    self.counter
                        .dropped
                        .fetch_add(events.len() as u64, Ordering::Relaxed);
                    continue;
                }
            };

            match pool.send_with_retry(&events, &self.pool_stats).await {
                Ok(acked) => {
                    self.counter.tx.fetch_add(acked as u64, Ordering::Relaxed);
                    self.counter
                        .tx_bytes
                        .fetch_add(raw_bytes, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!("Lumberjack send failed: {e}");
                    self.counter
                        .dropped
                        .fetch_add(events.len() as u64, Ordering::Relaxed);
                }
            }
        }
    }
}
