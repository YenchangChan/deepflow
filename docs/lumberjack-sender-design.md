# Lumberjack Sender 集成设计文档

> **状态**：设计评审中
> **创建日期**：2026-04-14
> **关联文档**：[sender-connection-pool-proposal.md](design/agent/sender-connection-pool-proposal.md)

## 1. 背景与目标

### 1.1 需求概述

在 DeepFlow Agent 中新增基于 Lumberjack v2 协议的 Sender，作为现有 TCP/Protobuf 传输通道的**替代方案**（二选一）。同时在 Server Ingester 中新增 Lumberjack Receiver。

### 1.2 核心约束

1. **Lumberjack 与现有 Ingester 通道互斥**：根据配置 `collector_socket_type`，Agent 选择 TCP/Protobuf（现有）或 Lumberjack（新增），不做双写
2. **默认端口 7070**
3. **从第一版起内建连接池 HA**：支持多 endpoint、round-robin、batch 级 failover，遵循 [sender-connection-pool-proposal.md](design/agent/sender-connection-pool-proposal.md) 的设计思路

### 1.3 目标

1. **Agent 侧**：新增 `LumberjackSender`，内建连接池，复用 LeakyBucket 限流，将 `Sendable` 数据 JSON 序列化后通过 Lumberjack v2 协议发送
2. **Server 侧**：在 Ingester 中新增 Lumberjack Receiver（端口 7070），接收并解码 Lumberjack 数据，写入存储
3. **HA**：支持多 Ingester endpoint 配置，batch 级故障切换

### 1.4 非目标

- 不改动现有 `UniformSender` 的行为
- 不做 Lumberjack 与 Protobuf 通道的双写
- 不在第一版实现持久化 buffer 或 exactly-once 语义

---

## 2. Lumberjack v2 协议概述

### 2.1 协议特征

| 特征 | 说明 |
|------|------|
| 传输层 | TCP（可选 TLS） |
| 数据格式 | JSON（每个事件一个 JSON 对象） |
| 帧类型 | Window / Json / Compressed / Ack |
| 压缩 | zlib（可选，帧级别） |
| 流控 | Window 帧声明批次大小，Ack 帧确认接收 |
| 可靠性 | 同步 Ack 模型，发送方等待确认后才继续 |

### 2.2 交互时序

```
Client (Agent)                        Server (Ingester)
    │                                      │
    │  ── Window(N) ──────────────────►    │  声明本批次有 N 个事件
    │                                      │
    │  ── Compressed(N个Json帧) ─────►    │  发送压缩后的 N 个事件
    │                                      │
    │                                      │  解压、解析、入库
    │                                      │
    │  ◄────────────────── Ack(N) ──      │  确认收到 N 个事件
    │                                      │
    │  （下一批次...）                      │
```

### 2.3 与现有协议的关键差异

| 维度 | 现有 TCP/Protobuf | Lumberjack v2 |
|------|-------------------|---------------|
| 序列化 | Protobuf（二进制） | JSON（文本） |
| 帧头 | 19 字节自定义 Header | 6 字节 Window/Ack + 变长 Json/Compressed |
| 压缩 | Zstd（可选） | zlib（可选） |
| 确认机制 | 无（fire-and-forget） | 有（同步 Ack） |
| 默认端口 | 20033 | 7070 |

### 2.4 Ack 机制带来的 HA 优势

Lumberjack 的同步 Ack 使得故障检测更精确：

```
UniformSender（fire-and-forget）:
  write() 成功 ≠ Ingester 收到数据
  只有 TCP RST/超时才能发现故障（延迟数秒）

LumberjackSender（同步 Ack）:
  send() 返回 Ok(acked_seq) = Ingester 确认收到
  Ack 超时 = 明确的故障信号，可立即切换 endpoint
```

---

## 3. 整体架构

### 3.1 通道选择（互斥模型）

通过 `outputs.lumberjack.enabled` 开关控制，两个通道独立配置、互斥运行：

```
                        agent_config
                            │
              ┌─────────────┴──────────────┐
              │                            │
  outputs.lumberjack.enabled         outputs.lumberjack.enabled
         = false (默认)                    = true
              │                            │
              ▼                            ▼
  ┌─────────────────────┐    ┌──────────────────────────┐
  │   UniformSender     │    │   LumberjackSender       │
  │   (现有，不改动)     │    │   (新增，内建连接池)      │
  │                     │    │                          │
  │ 配置来源:            │    │ 配置来源:                 │
  │  global.communication│    │  outputs.lumberjack      │
  │  outputs.socket     │    │  (独立 section)           │
  │                     │    │                          │
  │ Protobuf 编码       │    │ JSON 序列化              │
  │ 单连接/全局连接      │    │ Lumberjack v2 帧编码     │
  │ fire-and-forget     │    │ 同步 Ack + batch 重试    │
  │ TCP:20033           │    │ TCP:7070                 │
  └─────────────────────┘    └──────────────────────────┘
```

### 3.2 Lumberjack Sender 连接池架构

```
                     DeepFlow Agent
┌───────────────────────────────────────────────────────────┐
│                                                           │
│  Collectors ──► Queue ──► LumberjackSender<T>             │
│                               │                           │
│                      ┌────────▼─────────┐                 │
│                      │  LeakyBucket     │                 │
│                      │  (限流)          │                 │
│                      └────────┬─────────┘                 │
│                               │                           │
│                      ┌────────▼─────────┐                 │
│                      │  JSON 序列化      │                 │
│                      │  + 批次聚合       │                 │
│                      └────────┬─────────┘                 │
│                               │                           │
│                      ┌────────▼──────────────────┐        │
│                      │  LumberjackConnectionPool │        │
│                      │                           │        │
│                      │  ┌─────────┐ ┌─────────┐ │        │
│                      │  │Endpoint1│ │Endpoint2│ │        │
│                      │  │Client   │ │Client   │ │        │
│                      │  │10.0.0.1 │ │10.0.0.2 │ │        │
│                      │  └────┬────┘ └────┬────┘ │        │
│                      │       │           │      │        │
│                      │  round-robin + 故障切换   │        │
│                      └───────┼───────────┼──────┘        │
│                              │           │                │
└──────────────────────────────┼───────────┼────────────────┘
                               │           │
                               ▼           ▼
                         Ingester 1   Ingester 2
                         :7070        :7070
```

### 3.3 Server 侧接收架构

```
                     DeepFlow Server Ingester
┌───────────────────────────────────────────────────────────┐
│                                                           │
│  ┌─────────────────────────────────────────────────┐      │
│  │  TCP:20033 (现有 Receiver)                       │      │
│  │  BaseHeader/FlowHeader → Protobuf → Handler     │      │
│  └─────────────────────────────────────────────────┘      │
│                                                           │
│  ┌─────────────────────────────────────────────────┐      │
│  │  TCP:7070 (Lumberjack Receiver)      【新增】    │      │
│  │                                                 │      │
│  │  Lumberjack v2 帧解码                            │      │
│  │       │                                         │      │
│  │       ▼                                         │      │
│  │  JSON 解析 → 按 _msg_type 路由                   │      │
│  │       │                                         │      │
│  │       ├──► flow_log Handler ──► ClickHouse       │      │
│  │       ├──► metrics Handler ──► ClickHouse        │      │
│  │       ├──► app_log Handler ──► ClickHouse        │      │
│  │       └──► ...                                  │      │
│  └─────────────────────────────────────────────────┘      │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

---

## 4. Agent 侧设计

### 4.1 模块结构

```
agent/src/sender/
├── mod.rs                        # 增加 lumberjack_sender 模块声明
├── uniform_sender.rs             # 现有（不改动）
├── npb_sender.rs                 # 现有（不改动）
├── tcp_packet.rs                 # 现有（不改动）
└── lumberjack_sender.rs          # 【新增】Lumberjack Sender + 连接池
```

### 4.2 配置

#### 4.2.1 现有配置结构分析

项目本身的 `template.yaml` 按 OTel Collector 风格组织为六大顶层块：

```yaml
global:              # 全局（资源限制、通信、调优）
  communication:     #   ← ingester_ip, ingester_port, max_throughput 在这里
inputs:              # 采集输入（proc, cbpf, ebpf）
processors:          # 数据处理（过滤、聚合）
outputs:             # 输出
  socket:            #   ← data_socket_type: TCP|UDP|FILE 在这里
  flow_log:          #   ← 流日志输出配置
  flow_metrics:      #   ← 指标输出配置
  npb:               #   ← NPB 输出配置
  compression:       #   ← 压缩配置
plugins:             # 插件
dev:                 # 开发调试
```

**问题**：现有 ingester 的配置散落在 `global.communication`（地址端口）和 `outputs.socket`（socket 类型）两处。如果把 Lumberjack 也塞进去、靠 `data_socket_type` 一个字段区分，会导致两套完全不同的传输协议的配置混在一起，既不直观也容易出错。

#### 4.2.2 设计原则：配置面独立 section，代码面展平到 SenderConfig

两个层面各取其优：

- **配置模板**：Lumberjack 在 `outputs` 下拥有独立 section，与 `socket`/`npb` 平级，用户看到的配置清晰隔离
- **Agent 代码**：解析时将 Lumberjack 字段展平到现有 `SenderConfig`，共享同一个 `SenderAccess` 和 watcher 链路，最小化代码侵入

```
template.yaml (用户视角)              Agent 代码 (内部视角)
┌─────────────────────────┐          ┌─────────────────────────┐
│ outputs:                │          │ SenderConfig {           │
│   socket:               │   解析   │   dest_ip,              │
│     data_socket_type: TCP│ ──────► │   dest_port,            │
│   lumberjack:           │          │   ...                   │
│     enabled: false      │          │   lumberjack_enabled,   │
│     endpoints: [...]    │          │   lumberjack_endpoints, │
│     port: 7070          │          │   lumberjack_port,      │
│     ...                 │          │   ...                   │
└─────────────────────────┘          └─────────────────────────┘
  独立 section，清晰隔离               同一个 struct，一套 Access
```

**为什么这样做**：

| 维度 | 独立 LumberjackConfig + LumberjackAccess | 展平到 SenderConfig（采用方案） |
|------|----------------------------------------|-------------------------------|
| 配置模板可读性 | 好 | 好（模板层面仍是独立 section） |
| protobuf 改动 | 新增 message | 现有 message 加字段 |
| config 解析 | 新增解析路径 + watcher | 现有路径扩展 |
| handler.rs | 新增 LumberjackAccess | 现有 SenderAccess 加字段 |
| trident.rs | 持有两个 Access，两套生命周期 | 一个 Access，`if/else` 分叉 |
| Sender 拿配置 | 另一个 Access | 同一个 Access |
| 侵入文件数 | ~6 个 | ~3 个 |

#### 4.2.3 限流配置复用

限流由 `global.communication` 下的两个现有字段控制，Lumberjack 和 Ingester 通道共享（互斥模型下只有一个在跑）：

```yaml
global:
  communication:
    max_throughput_to_ingester: 100     # 单位 Mbps，0 = 不限制
    ingester_traffic_overflow_action: WAIT  # WAIT | DROP
```

不在 `outputs.lumberjack` 中重复限流配置——它们是全局的发送带宽约束，与传输协议无关。

#### 4.2.4 配置模板（server/agent_config/template.yaml）

```yaml
outputs:
  socket:
    # 现有字段（保持不变）
    data_socket_type: TCP
    multiple_sockets_to_ingester: false

  # 【新增】Lumberjack 输出通道
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
    #     for HA with round-robin and automatic failover. When empty, falls back
    #     to global.communication.ingester_ip:7070.
    #   ch: |-
    #     Lumberjack Ingester 地址列表，格式为 "ip:port"。端口可省略，
    #     默认 7070。支持多个 endpoint 实现 HA（round-robin + 自动故障切换）。
    #     当 enabled=true 但 endpoints 为空时，Agent 启动报错并拒绝启用 Lumberjack。
    # examples:
    #   - "ingester-1.example.com:7070"
    #   - "ingester-2.example.com:7071"    # 同一机器不同端口
    #   - "10.0.0.3"                       # 省略端口，默认 7070
    endpoints: []

    # type: int
    # range: [0, 9]
    # modification: hot_update
    # description:
    #   en: "zlib compression level. 0 = disabled, 3 = default."
    #   ch: "zlib 压缩级别。0 = 关闭压缩，3 = 默认。"
    compression_level: 3

    # type: int
    # range: [0, 1000]
    # modification: hot_update
    # description:
    #   en: |-
    #     Maximum number of events per Lumberjack batch.
    #     0 means send each event individually (no batching).
    #   ch: |-
    #     每个 Lumberjack 批次的最大事件数。
    #     0 表示逐条发送（不做批次聚合）。
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
    #     Restrict the local (source) TCP port range used by the Lumberjack client.
    #     Format: "start,end". Useful in environments with strict firewall or NAT
    #     rules. Empty string means the OS picks an ephemeral port (default).
    #   ch: |-
    #     限制 Lumberjack 客户端使用的本地（源）TCP 端口范围，格式为 "起始,结束"。
    #     适用于有严格防火墙或 NAT 规则的环境。为空表示由操作系统自动选择临时端口。
    # examples: "60000,65000"
    local_port_range: ""

    # type: section
    # modification: agent_restart
    tls:
      enabled: false
      ca_file: ""
```

#### 4.2.5 Agent 侧 Rust 配置结构（展平到 SenderConfig）

```rust
// agent/src/config/handler.rs

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SenderConfig {
    // === 现有字段（不改动） ===
    pub dest_ip: String,
    pub dest_port: u16,
    pub agent_id: u16,
    pub team_id: u32,
    pub organize_id: u32,
    pub multiple_sockets_to_ingester: bool,
    pub max_throughput_to_ingester: u64,
    pub ingester_traffic_overflow_action: TrafficOverflowAction,
    pub collector_socket_type: SocketType,
    pub enabled: bool,
    // ...

    // === 新增：从 outputs.lumberjack section 展平而来 ===
    pub lumberjack_enabled: bool,
    pub lumberjack_endpoints: Vec<(String, u16)>,  // (ip, port) 对，端口省略时默认 7070
    pub lumberjack_compression_level: u32,         // 默认 3
    pub lumberjack_batch_size: usize,              // 默认 100, 范围 [0, 1000]
    pub lumberjack_ack_timeout: Duration,          // 默认 30s
    pub lumberjack_local_port_range: Option<(u16, u16)>,  // 解析 "60000,65000" → Some((60000, 65000))
    pub lumberjack_tls_enabled: bool,
    pub lumberjack_tls_ca_path: Option<String>,
}

// 类型别名不变，无需新增 Access
pub type SenderAccess = Access<SenderConfig>;
```

#### 4.2.6 endpoint 解析

配置中的 endpoint 格式为 `"ip:port"` 或 `"ip"`（省略端口默认 7070）：

```rust
/// 解析 "host:port" 或 "host"（默认 7070）
fn parse_lumberjack_endpoint(s: &str) -> (String, u16) {
    // 处理 IPv6 地址 [::1]:7070
    if let Some(bracket_end) = s.rfind(']') {
        let host = &s[..=bracket_end];
        let port = s[bracket_end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(7070);
        return (host.to_string(), port);
    }
    match s.rsplit_once(':') {
        Some((host, port_str)) => match port_str.parse::<u16>() {
            Ok(port) => (host.to_string(), port),
            Err(_) => (s.to_string(), 7070),
        },
        None => (s.to_string(), 7070),
    }
}
```

#### 4.2.7 互斥判断与启动校验

```rust
impl SenderConfig {
    /// 是否使用 Lumberjack 通道
    pub fn is_lumberjack_enabled(&self) -> bool {
        self.lumberjack_enabled
    }

    /// 启动时校验：enabled=true 但 endpoints 为空则报错
    pub fn validate_lumberjack(&self) -> Result<(), String> {
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

不做 `ingester_ip` 回退——`ingester_ip` 是 Protobuf 通道的地址（不带端口语义不匹配），硬拼 7070 既不直观也容易误导。`enabled=true` + `endpoints` 为空直接报错，让用户显式配置。

### 4.3 核心数据结构

```rust
// agent/src/sender/lumberjack_sender.rs

use lumberjack_protocol::Client;

/// 连接池：管理多个 Lumberjack Client 连接
pub struct LumberjackConnectionPool {
    endpoints: Vec<LumberjackEndpoint>,
    next_index: usize,                    // round-robin 指针
}

pub struct LumberjackEndpoint {
    ip: String,
    port: u16,                           // 每个 endpoint 独立端口
    client: Option<Client>,              // 异步 Lumberjack Client
    state: EndpointState,
    consecutive_failures: u8,
    last_attempt: Instant,
}

/// 连接池级参数（从 SenderConfig.lumberjack_* 读取）
struct PoolConfig {
    compression_level: u32,
    ack_timeout: Duration,
    local_port_range: Option<(u16, u16)>,  // 解析自 "60000,65000"，None = OS 选择
    tls_enabled: bool,
    tls_ca_path: Option<String>,
}

enum EndpointState {
    Available,                            // 上次发送成功
    Cooldown {                            // 连接/发送失败，冷却中
        until: Instant,
    },
}

/// Sender 线程封装（使用与 UniformSender 相同的 SenderAccess）
pub struct LumberjackSenderThread<T: Sendable> {
    name: String,
    input: Arc<Receiver<T>>,
    config: SenderAccess,                 // 与 UniformSender 共享同一类型
    leaky_bucket: Arc<LeakyBucket>,
    running: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
    counter: Arc<SenderCounter>,
    pool_stats: Arc<PoolStats>,
}

/// 实际的发送逻辑
struct LumberjackSender<T: Sendable> {
    input: Arc<Receiver<T>>,
    config: SenderAccess,                 // 通过 config.load().lumberjack_* 读取配置
    leaky_bucket: Arc<LeakyBucket>,
    counter: Arc<SenderCounter>,
    pool: LumberjackConnectionPool,
    pool_stats: Arc<PoolStats>,
    runtime: tokio::runtime::Runtime,     // current_thread Runtime
}
```

### 4.4 连接池设计（遵循 connection-pool-proposal）

#### 4.4.1 连接选择：等权 Round-Robin

```rust
impl LumberjackConnectionPool {
    /// 选择一个可用 endpoint。跳过处于冷却期的 endpoint。
    fn pick_endpoint(&mut self) -> Option<usize> {
        let total = self.endpoints.len();
        for _ in 0..total {
            let index = self.next_index % total;
            self.next_index = self.next_index.wrapping_add(1);
            if self.endpoints[index].can_try_now() {
                return Some(index);
            }
        }
        None  // 所有 endpoint 都在冷却期
    }
}

impl LumberjackEndpoint {
    fn can_try_now(&self) -> bool {
        match &self.state {
            EndpointState::Available => true,
            EndpointState::Cooldown { until } => Instant::now() >= *until,
        }
    }
}
```

#### 4.4.2 Batch 级故障切换

```rust
impl LumberjackConnectionPool {
    /// 发送一批事件，失败时切换到下一个 endpoint 重试一次
    async fn send_with_retry<T: Serialize>(
        &mut self,
        events: &[T],
        pool_stats: &PoolStats,
    ) -> Result<u32, Error> {
        // 第一次尝试
        let first_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                pool_stats.all_endpoints_failed.fetch_add(1, Relaxed);
                return Err(Error::AllEndpointsUnavailable);
            }
        };

        match self.try_send(first_idx, events).await {
            Ok(result) => {
                self.mark_success(first_idx, result.keepalives);
                return Ok(result.acked);
            }
            Err(e) => {
                warn!("Lumberjack send to {} failed: {e}, trying next endpoint",
                      self.endpoints[first_idx].ip);
                self.mark_failure(first_idx);
                pool_stats.write_failures.fetch_add(1, Relaxed);
            }
        }

        // 重试一次：选择不同的 endpoint
        let retry_idx = match self.pick_endpoint() {
            Some(idx) => idx,
            None => {
                pool_stats.all_endpoints_failed.fetch_add(1, Relaxed);
                return Err(Error::AllEndpointsUnavailable);
            }
        };

        match self.try_send(retry_idx, events).await {
            Ok(result) => {
                self.mark_success(retry_idx, result.keepalives);
                pool_stats.retry_successes.fetch_add(1, Relaxed);
                Ok(result.acked)
            }
            Err(e) => {
                self.mark_failure(retry_idx);
                pool_stats.all_endpoints_failed.fetch_add(1, Relaxed);
                Err(e)
            }
        }
    }

    async fn try_send<T: Serialize>(
        &mut self,
        idx: usize,
        events: &[T],
    ) -> Result<u32, Error> {
        let ep = &mut self.endpoints[idx];

        // Lazy connect
        if ep.client.is_none() {
            let addr = format!("{}:{}", ep.ip, ep.port);
            let mut builder = Client::builder()
                .compression_level(self.pool_config.compression_level)
                .ack_timeout(self.pool_config.ack_timeout);
            if let Some((start, end)) = self.pool_config.local_port_range {
                builder = builder.local_port_range(start, end);
            }
            // TLS 配置（略）
            ep.client = Some(builder.connect(&addr).await?);
        }

        ep.client.as_mut().unwrap().send(events).await
            .map_err(|e| {
                ep.client = None;  // 连接异常，下次重建
                e.into()
            })
    }

    fn mark_success(&mut self, idx: usize, keepalives: u32) {
        let ep = &mut self.endpoints[idx];
        ep.consecutive_failures = 0;
        if keepalives > 0 {
            // 收到过 Ack(0)：server 处理慢，降低优先级
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
        // 冷却时间：10s * 2^(failures-1)，上限 60s，加随机抖动
        let base = Duration::from_secs(10) * 2u32.pow(ep.consecutive_failures.min(3) as u32 - 1);
        let jitter = Duration::from_millis(rand::random::<u64>() % 5000);
        ep.state = EndpointState::Cooldown {
            until: Instant::now() + base.min(Duration::from_secs(60)) + jitter,
        };
        ep.client = None;  // 丢弃旧连接，下次 try_send 时 lazy reconnect
    }
}
```

#### 4.4.4 断连重连机制

连接池不维护后台 reconnect 线程，全部通过 **lazy reconnect** 实现——`try_send()` 发现 `client == None` 时自动重建连接：

```
Endpoint 生命周期:

  初始 ─► client=None, state=Available
            │
            │  首次 try_send()
            ▼
  client=None → lazy connect → 成功 → client=Some, state=Available
                                │
                         连接建立失败
                                │
                                ▼
                        client=None, state=Cooldown
                                │
                         冷却期过后，下次被 pick_endpoint() 选中
                                │
                                ▼
                        try_send() → lazy connect → 重试

  ────── 正常发送中 ──────

  client=Some, state=Available
            │
            │  send() 返回 Err（Ack 超时 / 连接断开 / IO 错误）
            ▼
  mark_failure():
    client = None              ← 丢弃旧连接
    state = Cooldown(10s~60s)  ← 指数退避
    consecutive_failures++
            │
            │  冷却期过后被选中
            ▼
  try_send() → client=None → lazy connect → 新连接
```

**关键行为**：
- `ep.client = None` 同时完成"断连"和"标记需要重连"两件事
- `try_send()` 开头的 `if ep.client.is_none()` 就是 reconnect 入口
- 不需要单独的 reconnect 逻辑——connect 和 reconnect 是同一条代码路径
- 冷却期（Cooldown）防止对不可用 endpoint 频繁重连
- `consecutive_failures` 驱动指数退避：10s → 20s → 40s → 60s（上限）
- 重连成功后 `consecutive_failures` 清零，冷却时间恢复到 10s
```

#### 4.4.3 Ack 信号驱动的连接池决策

Lumberjack 的 Ack 机制为连接池提供了三个层次的健康信号，每个都应该驱动不同的连接池行为：

```
信号层次：

  Ack(N)        正常确认     endpoint 健康，清零 slow 计数
      │
  Ack(0)        keepalive    server 在指定时间内没处理完，正在挣扎
      │                      ──► 标记 endpoint 为 slow，降低优先级
      │
  Ack 超时       硬故障       server 不可达或完全无响应
      │                      ──► 断连，Cooldown，立即切换 endpoint
      │
  连接断开       硬故障       TCP RST / EOF
                             ──► 断连，Cooldown，立即切换 endpoint
```

**关键洞察：Ack(0) 不是"一切正常请继续等"，而是"我还活着但处理不过来了"。** 连续收到 Ack(0) 说明该 endpoint 过载，即使最终返回了 Ack(N)，连接池也应该将后续 batch 优先发到其他 endpoint。

##### Endpoint 状态机扩展

```rust
enum EndpointState {
    Available,                   // 上次 Ack(N) 成功，无 Ack(0)
    Slow {                       // 收到过 Ack(0)，但最终 Ack(N) 成功
        keepalive_count: u8,     // 本次 batch 收到的 Ack(0) 次数
    },
    Cooldown {                   // 硬故障（Ack 超时/断连），冷却中
        until: Instant,
    },
}
```

##### Client::send() 需要暴露 Ack(0) 信息

当前 `Client::send()` 内部静默吞掉 Ack(0)（`continue`），连接池无法感知。需要修改返回值：

```rust
/// send() 的丰富返回值
pub struct SendResult {
    pub acked: u32,            // 确认的事件数
    pub keepalives: u32,       // 收到的 Ack(0) 次数
}

// Client::send() 签名变更
pub async fn send<T: Serialize>(&mut self, events: &[T]) -> Result<SendResult>
```

或者更轻量的方式——不改 `Client` 的公开 API，在连接池层通过 `send()` 的**耗时**间接判断：

```rust
let start = Instant::now();
let acked = client.send(&events).await?;
let rtt = start.elapsed();

// 如果 RTT 远超预期（比如 > 2 * keepalive_interval），
// 说明中间收到了多次 Ack(0)，标记为 slow
if rtt > Duration::from_secs(30) {  // keepalive 默认 15s，2 轮以上
    endpoint.state = EndpointState::Slow { keepalive_count: (rtt.as_secs() / 15) as u8 };
}
```

**推荐方案**：修改 `Client::send()` 返回 `SendResult`，这是最准确的。RTT 间接判断有误差（压缩大 batch 本身也可能耗时长）。

##### 连接池选择时的 Slow 降权

```rust
fn pick_endpoint(&mut self) -> Option<usize> {
    let total = self.endpoints.len();

    // 第一轮：优先选 Available 状态的 endpoint
    for _ in 0..total {
        let index = self.next_index % total;
        self.next_index = self.next_index.wrapping_add(1);
        if matches!(self.endpoints[index].state, EndpointState::Available) {
            return Some(index);
        }
    }

    // 第二轮：退而求其次，选 Slow 的（至少还能用）
    for _ in 0..total {
        let index = self.next_index % total;
        self.next_index = self.next_index.wrapping_add(1);
        if matches!(self.endpoints[index].state, EndpointState::Slow { .. }) {
            return Some(index);
        }
    }

    // 第三轮：尝试冷却期已过的 Cooldown endpoint
    for _ in 0..total {
        let index = self.next_index % total;
        self.next_index = self.next_index.wrapping_add(1);
        if self.endpoints[index].can_try_now() {
            return Some(index);
        }
    }

    None  // 所有 endpoint 都在冷却期
}
```

##### 状态转换规则

```
                    Ack(N) 成功，无 Ack(0)
              ┌──────────────────────────────┐
              │                              │
              ▼                              │
         Available ──── Ack(N) 成功，有 Ack(0) ────► Slow
              │                                       │
              │         Ack 超时 / 断连                │  Ack 超时 / 断连
              ▼                                       ▼
         Cooldown ◄───────────────────────────── Cooldown
              │
              │  冷却期过后尝试发送
              │
              ├── Ack(N) 成功，无 Ack(0) ──► Available
              ├── Ack(N) 成功，有 Ack(0) ──► Slow
              └── 失败 ──────────────────── Cooldown（延长冷却）
```

##### 与 connection-pool-proposal 的对齐

connection-pool-proposal 第一版只做被动健康判断，后续增强才考虑失败率降权。Lumberjack 的 Ack(0) 机制让我们在**第一版就能获得 Slow 降权能力**，且不需要主动健康检测线程——这是 Lumberjack 协议相比 fire-and-forget 的天然优势。

| 能力 | connection-pool-proposal Phase | Lumberjack 第一版即可实现 |
|------|-------------------------------|-------------------------|
| 硬故障检测 | Phase 1（write 失败） | Ack 超时 / 断连 |
| 软故障（慢节点）检测 | Phase 4（失败率统计） | Ack(0) keepalive 信号 |
| 主动健康检测 | Phase 4 | 不需要（Ack 机制已足够） |

### 4.5 JSON 序列化策略

#### 4.5.1 Sendable Trait 扩展

```rust
// agent/crates/public/src/sender.rs
pub trait Sendable: Debug + Send + 'static {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, prost::EncodeError>;
    fn message_type(&self) -> SendMessageType;
    fn file_name(&self) -> &str { "" }
    fn to_kv_string(&self, _: &mut String) {}

    /// 【新增】JSON 序列化，用于 Lumberjack 输出。
    /// 默认返回 None，表示该类型不支持 Lumberjack 输出。
    fn to_json_value(&self) -> Option<serde_json::Value> { None }
}
```

#### 4.5.2 JSON 格式规范

每个事件序列化为 JSON 对象，包含公共元数据字段和类型特定字段：

```json
{
    "_msg_type": "tagged_flow",
    "_agent_id": 1234,
    "_team_id": 5678,
    "_org_id": 1,
    "_timestamp": 1718000000,

    "src_ip": "10.0.0.1",
    "dst_ip": "10.0.0.2",
    "src_port": 45678,
    "dst_port": 8080,
    "protocol": "TCP",
    "bytes_tx": 1024,
    "bytes_rx": 2048
}
```

`_msg_type` 映射表：

| SendMessageType | _msg_type 值 | 实现优先级 |
|-----------------|-------------|-----------|
| TaggedFlow | `"tagged_flow"` | Phase 1 |
| ProtocolLog | `"protocol_log"` | Phase 1 |
| ApplicationLog | `"application_log"` | Phase 1 |
| Metrics | `"metrics"` | Phase 2 |
| Profile | `"profile"` | Phase 2 |
| OpenTelemetry | `"opentelemetry"` | Phase 2 |
| Prometheus | `"prometheus"` | Phase 2 |
| 其余类型 | ... | Phase 3 |

### 4.6 发送主循环

```rust
impl<T: Sendable> LumberjackSender<T> {
    fn run(mut self) {
        self.runtime.block_on(async {
            loop {
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }

                // 1. 检查配置更新（dest_ips 变化时重建连接池）
                self.check_config_update();

                // 2. 批量接收消息（最多 batch_size 条，超时 batch_timeout）
                let messages = self.recv_batch();
                if messages.is_empty() {
                    continue;
                }

                self.counter.rx.fetch_add(messages.len() as u64, Relaxed);

                // 3. JSON 序列化
                let mut events: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
                let mut raw_bytes: u64 = 0;
                for msg in messages {
                    if let Some(json) = msg.to_json_value() {
                        raw_bytes += estimate_json_size(&json) as u64;
                        events.push(json);
                    } else {
                        self.counter.dropped.fetch_add(1, Relaxed);
                    }
                }

                if events.is_empty() {
                    continue;
                }

                self.counter.raw_bytes.fetch_add(raw_bytes, Relaxed);

                // 4. 限流
                if !self.try_acquire(raw_bytes) {
                    self.counter.dropped.fetch_add(events.len() as u64, Relaxed);
                    continue;
                }

                // 5. 通过连接池发送（含 batch 级重试）
                match self.pool.send_with_retry(&events, &self.pool_stats).await {
                    Ok(acked) => {
                        self.counter.tx.fetch_add(acked as u64, Relaxed);
                        self.counter.tx_bytes.fetch_add(raw_bytes, Relaxed);
                    }
                    Err(e) => {
                        warn!("Lumberjack send failed after retry: {e}");
                        self.counter.dropped.fetch_add(events.len() as u64, Relaxed);
                    }
                }
            }
        });
    }

    fn try_acquire(&self, bytes: u64) -> bool {
        if self.leaky_bucket.acquire(bytes) {
            return true;
        }
        match self.overflow_action {
            TrafficOverflowAction::Waiting => {
                // 等待重试，最多 2s
                for _ in 0..100 {
                    std::thread::sleep(Duration::from_millis(20));
                    if self.leaky_bucket.acquire(bytes) {
                        self.counter.waited.fetch_add(1, Relaxed);
                        return true;
                    }
                }
                false
            }
            TrafficOverflowAction::Dropping => false,
        }
    }
}
```

### 4.7 异步运行时管理

```rust
impl<T: Sendable> LumberjackSenderThread<T> {
    pub fn start(&self) {
        let sender = LumberjackSender::new(/* ... */);
        let handle = thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || {
                // 在 Sender 线程内创建独立的 tokio current_thread Runtime
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                sender.runtime = rt;
                sender.run();
            })
            .unwrap();
        *self.thread.lock().unwrap() = Some(handle);
    }
}
```

选择 `current_thread` Runtime 的理由：
- 每个 LumberjackSender 对应一个消息类型，是单连接池模型
- 不需要多线程并发，single-threaded executor 足够
- 资源占用最小

### 4.8 trident.rs 集成

同一个 `sender_access`，`if/else` 分叉创建不同 Sender：

```rust
// agent/src/trident.rs — AgentComponents::new()

let config = sender_access.load();

if config.is_lumberjack_enabled() {
    // Lumberjack 路径
    let l4_flow_sender = LumberjackSenderThread::new(
        "l4-flow-lumberjack", l4_flow_receiver,
        sender_access.clone(), leaky_bucket.clone(),  // 同一个 SenderAccess
    );
    let l7_flow_sender = LumberjackSenderThread::new(
        "l7-flow-lumberjack", l7_flow_receiver,
        sender_access.clone(), leaky_bucket.clone(),
    );
    // ...
} else {
    // 现有 Ingester 路径（不改动）
    let l4_flow_sender = UniformSenderThread::new(
        "l4-flow-uniform", l4_flow_receiver,
        sender_access.clone(), ...
    );
    let l7_flow_sender = UniformSenderThread::new(
        "l7-flow-uniform", l7_flow_receiver,
        sender_access.clone(), ...
    );
    // ...
}
```

### 4.9 限流方案

**完全复用现有 LeakyBucket**，与 UniformSender 使用相同的配置参数：

```
配额来源: max_throughput_to_ingester (Mbps)
计量单位: 序列化后的 JSON 字节数（raw_bytes）
溢出行为: ingester_traffic_overflow_action (Waiting | Dropping)
```

由于是互斥模型（Lumberjack 或 Protobuf 二选一），不存在两个 Sender 争抢带宽的问题。整个令牌桶的配额全部归 Lumberjack Sender 使用。

---

## 5. Server 侧设计

### 5.1 模块结构

```
server/ingester/
├── ingester/
│   └── ingester.go             # 增加 Lumberjack Receiver 启动逻辑
├── lumberjack/                 # 【新增】
│   ├── lumberjack.go           # Handler 入口：接收 → 解码 → 路由
│   ├── config/
│   │   └── config.go           # 配置结构
│   ├── receiver/
│   │   └── receiver.go         # Lumberjack v2 帧解码 + TCP Server
│   └── decoder/
│       └── decoder.go          # JSON → Go 结构体映射
└── config/
    └── config.go               # 增加 Lumberjack 相关全局配置项
```

### 5.2 Lumberjack Receiver

由于 Lumberjack v2 帧格式与现有 BaseHeader/FlowHeader 完全不兼容，使用**独立监听端口**。

Server 端直接使用 [`elastic/go-lumber`](https://github.com/elastic/go-lumber) 库（`lumberjack-protocol` Rust crate 本身就是从这个 Go 库移植的），不重复造轮子：

```go
// server/ingester/lumberjack/receiver/receiver.go

import (
    "github.com/elastic/go-lumber/server"
)

type Receiver struct {
    ljServer  *server.Server         // elastic/go-lumber Server
    batchChan chan *Batch
}

type Batch struct {
    Events     []map[string]interface{}
    AckFunc    func()                    // go-lumber 的 ACK 回调
    RemoteAddr string
    RecvTime   time.Time
}

func NewReceiver(config Config) (*Receiver, error) {
    opts := []server.Option{
        server.Timeout(config.Timeout),
    }
    if config.TLSEnabled {
        opts = append(opts, server.TLS(config.TLSCertFile, config.TLSKeyFile))
    }

    ljServer, err := server.ListenAndServe(
        fmt.Sprintf(":%d", config.ListenPort),
        opts...,
    )
    if err != nil {
        return nil, err
    }

    return &Receiver{
        ljServer:  ljServer,
        batchChan: make(chan *Batch, 128),
    }, nil
}

func (r *Receiver) Start() {
    go r.receiveLoop()
}

func (r *Receiver) receiveLoop() {
    for batch := range r.ljServer.ReceiveChan() {
        // go-lumber 返回的 batch 包含 Events 和 ACK 回调
        events := make([]map[string]interface{}, 0, len(batch.Events))
        for _, ev := range batch.Events {
            events = append(events, ev.(map[string]interface{}))
        }
        r.batchChan <- &Batch{
            Events:  events,
            AckFunc: func() { batch.ACK() },
        }
    }
}
```

**使用 `elastic/go-lumber` 的好处**：
- 与 Rust 侧 `lumberjack-protocol` crate 同源（Rust 版从 Go 版移植），协议行为完全一致
- Lumberjack v2 帧解码、zlib 解压、Ack(0) keepalive、连接管理全部内置
- 久经 Elastic/Filebeat 生态验证，无需自己处理协议边界情况
- 减少约 200 行手写帧解码代码

### 5.3 消息路由

```go
// server/ingester/lumberjack/decoder/decoder.go

func (d *Decoder) Decode(batch *Batch) {
    for _, event := range batch.Events {
        msgType, ok := event["_msg_type"].(string)
        if !ok {
            d.stats.decodeErrors.Add(1)
            continue
        }

        agentID := getUint16(event, "_agent_id")
        teamID  := getUint32(event, "_team_id")
        orgID   := getUint16(event, "_org_id")

        switch msgType {
        case "tagged_flow":
            d.routeToFlowLog(event, agentID, teamID, orgID)
        case "protocol_log":
            d.routeToProtocolLog(event, agentID, teamID, orgID)
        case "application_log":
            d.routeToAppLog(event, agentID, teamID, orgID)
        case "metrics":
            d.routeToMetrics(event, agentID, teamID, orgID)
        case "profile":
            d.routeToProfile(event, agentID, teamID, orgID)
        default:
            log.Warningf("Unknown lumberjack msg_type: %s", msgType)
            d.stats.unknownTypes.Add(1)
        }
    }

    // 所有事件路由完成后，发送 Ack
    batch.AckFunc()
}
```

每个 `routeTo*` 方法将 JSON 字段映射为对应的 Go 结构体，然后放入现有 Handler 的 `dropletqueue`，复用现有的 Decoder → DBWriter → ClickHouse 管线。

### 5.4 配置

```go
// server/ingester/lumberjack/config/config.go

type Config struct {
    Enabled      bool   `yaml:"enabled"`         // 默认 false
    ListenPort   int    `yaml:"listen-port"`     // 默认 7070
    TLSEnabled   bool   `yaml:"tls-enabled"`     // 默认 false
    TLSCertFile  string `yaml:"tls-cert-file"`
    TLSKeyFile   string `yaml:"tls-key-file"`
    MaxFrameSize int    `yaml:"max-frame-size"`  // 默认 16MB
    WorkerCount  int    `yaml:"worker-count"`    // 默认 CPU 核心数
}
```

### 5.5 Ingester 启动集成

```go
// server/ingester/ingester/ingester.go

func NewIngester(...) {
    // ... 现有初始化 ...

    // 【新增】Lumberjack Receiver（条件启动）
    if config.LumberjackConfig.Enabled {
        ljReceiver, err := lumberjack.NewReceiver(config.LumberjackConfig)
        if err != nil {
            log.Errorf("Failed to start lumberjack receiver: %v", err)
        } else {
            ljHandler := lumberjack.NewHandler(
                ljReceiver,
                platformDataManager,
                existingHandlers,  // 复用 flow_log, metrics 等现有 handler
            )
            ljHandler.Start()
            closers = append(closers, ljHandler)
            log.Infof("Lumberjack receiver started on port %d", config.LumberjackConfig.ListenPort)
        }
    }
}
```

---

## 6. 监控指标

### 6.1 Agent 侧

复用现有 `SenderCounter` + 新增连接池指标：

```rust
// 现有 SenderCounter（标签 type="lumberjack"）
sender_rx, sender_tx, sender_tx_bytes, sender_dropped, sender_waited

// 新增连接池指标（对齐 connection-pool-proposal）
pub struct PoolStats {
    pub total_endpoints: AtomicU64,
    pub available_endpoints: AtomicU64,
    pub connect_failures: AtomicU64,
    pub write_failures: AtomicU64,
    pub reconnects_total: AtomicU64,
    pub retry_successes: AtomicU64,        // batch 切换 endpoint 后成功
    pub all_endpoints_failed: AtomicU64,   // batch 重试后仍失败
}
```

### 6.2 Server 侧

```go
// Lumberjack Receiver 指标
ingester_lumberjack_connections        // 当前连接数
ingester_lumberjack_batches_total      // 接收批次总数
ingester_lumberjack_events_total       // 接收事件总数
ingester_lumberjack_decode_errors      // JSON 解码错误
ingester_lumberjack_unknown_types      // 未知 _msg_type 数
ingester_lumberjack_ack_sent           // 发送 Ack 数
```

### 6.3 告警建议

| 指标 | 阈值 | 含义 |
|------|------|------|
| `pool_available_endpoints == 0` | 持续 1 分钟 | 所有 Ingester 不可用 |
| `pool_all_endpoints_failed > 0` | 任意非零 | batch 重试仍失败，数据丢失 |
| `pool_retry_successes > 0` | 持续出现 | 正在发生 endpoint 故障切换 |
| `decode_errors / events_total > 1%` | 持续 5 分钟 | JSON 格式异常 |

---

## 7. 错误处理

| 场景 | Agent 行为 | Server 行为 |
|------|-----------|-------------|
| 单 endpoint 连接失败 | 标记 Cooldown，切换到下一个 endpoint | N/A |
| 所有 endpoint 连接失败 | `all_endpoints_failed++`，丢弃 batch | N/A |
| 发送成功但 Ack 超时 | 断连，标记 Cooldown，切换 endpoint 重试 | 清理连接资源 |
| JSON 序列化失败 | 跳过该消息，`dropped++` | N/A |
| Server JSON 解码失败 | N/A | `decode_errors++`，Ack 整个 batch（避免 Client 阻塞） |
| 限流触发 | 等待/丢弃（按配置） | N/A |
| TLS 握手失败 | 断连，标记 Cooldown | 记录错误，关闭连接 |

---

## 8. 实施计划

### Phase 1: 基础框架（约 2 周）

| PR | 内容 |
|----|------|
| **PR #1** | `SocketType::Lumberjack` 枚举值 + `SenderConfig` 扩展 + `template.yaml` 配置项 + 文档生成 |
| **PR #2** | `LumberjackConnectionPool` 基础结构：多 endpoint、round-robin、Cooldown 状态机、batch 级重试 |
| **PR #3** | `LumberjackSender<T>` + `LumberjackSenderThread<T>` 主循环：tokio Runtime 管理、recv_batch、限流 |

### Phase 2: JSON 序列化 + Agent 集成（约 2 周）

| PR | 内容 |
|----|------|
| **PR #4** | `Sendable::to_json_value()` trait 扩展 + Phase 1 数据类型实现（ApplicationLog, ProtocolLog, TaggedFlow） |
| **PR #5** | `trident.rs` 集成：根据 `collector_socket_type` 条件创建 Sender、启停管理 |
| **PR #6** | 连接池指标 `PoolStats` + SenderCounter 注册 |

### Phase 3: Server 侧 Receiver（约 2 周）

| PR | 内容 |
|----|------|
| **PR #7** | Lumberjack v2 帧解码器（Go 实现） + `LumberjackReceiver` TCP 监听 |
| **PR #8** | JSON → Go 结构体 Decoder + 路由到现有 Handler |
| **PR #9** | Ingester 集成：配置、条件启动、ClickHouse 写入验证 |

### Phase 4: 完善（约 1 周）

| PR | 内容 |
|----|------|
| **PR #10** | TLS 端到端测试 |
| **PR #11** | 补齐 Phase 2/3 数据类型的 JSON 序列化 |
| **PR #12** | 性能基准测试 + 文档 |

---

## 9. 风险与注意事项

### 9.1 性能

- **JSON vs Protobuf**：JSON 序列化/反序列化约慢 3-5x，体积约大 2-3x。Lumberjack 通道在超高吞吐场景（> 100k events/s）需要关注 CPU 开销
- **zlib vs Zstd**：zlib 压缩率和速度均不如 Zstd，同等数据量带宽消耗更高
- **Ack 延迟**：同步 Ack 限制了单连接吞吐上限。增大 `batch_size` 可摊薄开销，但增加了单批失败的数据损失量

### 9.2 连接池

- **Ack 超时 vs 冷却时间**：Ack 超时（默认 30s）决定故障检测延迟，冷却时间（10s-60s）决定恢复探测频率。两者需合理配比
- **连接数**：N 个 Agent × M 个 Ingester，每个 Agent 对每个 Ingester 保持 1 个连接。3000 Agent × 3 Ingester = 9000 连接，每个 Ingester 承载 3000 连接，对 Linux 不是问题
- **batch 级乱序**：不同 batch 可能发到不同 Ingester。DeepFlow 数据不依赖到达顺序（ClickHouse MergeTree 按 ORDER BY 组织），但需验证

### 9.3 兼容性

- `lumberjack-protocol` crate 依赖 tokio 1.x，需确认与 Agent 现有 tokio 版本无冲突
- Server 侧 Lumberjack Receiver 是独立端口（7070），不影响现有 20033 端口

### 9.4 互斥模型的运维考量

- 切换 `collector_socket_type` 需要 Agent 重启（或等待下次配置下发生效）
- 切换期间存在短暂的数据间隙
- 建议提供 `deepflow-ctl` 命令验证 Agent 当前使用的通道类型

---

## 10. 后续增强

| 方向 | 说明 |
|------|------|
| 主动健康检测 | 后台线程定期检测 endpoint 可用性（TCP keepalive 或 idle reconnect） |
| Slow Start | endpoint 恢复后逐步增加流量，避免雪崩 |
| per-endpoint 权重 | 支持 `ingesters: [{ip, port, weight}]` 配置 |
| 持久化 buffer | 所有 endpoint 不可用时，将 encoded batch 写入磁盘，恢复后 replay |
| 应用层 ACK | Ingester 写入 ClickHouse 成功后才 Ack，提供更强的可靠性保证 |
