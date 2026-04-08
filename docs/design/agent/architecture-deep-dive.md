# DeepFlow Agent 架构深度分析

本文档对 DeepFlow Agent（Rust 实现）的整体设计架构进行深度分析，作为理解 Agent 代码和贡献新功能的基础。

## 1. Agent 的位置和角色

**DeepFlow Agent**（代称 `deepflow-agent` 或 `trident`）是部署在业务主机、容器或虚拟机上的流量采集和分析代理，负责：

- **采集网络流量**：网络包、应用层协议日志（HTTP、MySQL、Redis 等 20+ 种协议）
- **采集系统观测数据**：性能指标、事件日志、CPU/内存 profiling
- **集成外部数据源**：OTLP、Prometheus、Telegraf、Datadog、Pyroscope 等
- **实时上报数据**：给 DeepFlow 的 Ingester 组件，通过 TCP:20033 推送

支持两种运行模式：
- **Managed**（推荐）：受 Controller 管理（配置下发、状态同步）
- **Standalone**：独立运行，不依赖 Controller

---

## 1.1 三大功能块架构

Agent 的所有模块可以从职责维度分为**三个独立的功能块**，每块有明确的责任边界：

### 块 1：管理面（与 Server 交互）

**核心模块**：Synchronizer、ConfigHandler、Policy

**职责**：
- 通过 gRPC:20035 与 Controller 双向通信
- **上行**：汇报 Agent 状态、本机 IP/MAC、容器元数据
- **下行**：接收配置、策略、黑名单、ingester 地址
- 检测配置变更，触发块 2 和块 3 的热更新

**关键特性**：
- 60s 周期的 Sync 机制
- 无需重启的热配置更新（arc-swap）
- 可被完全禁用（Standalone 模式）

### 块 2：采集面（核心数据采集）

**核心模块**：Dispatcher、EbpfCollector、IntegrationCollector、FlowGenerator、Collector、UniformSender

**职责**：
- 从多源采集观测数据（网包、eBPF、OTLP、Prometheus 等）
- 数据分类、聚合、处理（管道架构）
- 生成 Metrics、L4/L7FlowLog、Profile 等数据类型
- 压缩后通过 TCP:20033 上报给 Ingester

**关键特性**：
- 8+ 种采集来源支持
- 9 个分类队列，各自独立处理
- 5 阶段管道，端到端延迟 <1ms
- **最独立的块**：仅需本地配置即可工作

### 块 3：平台发现（K8s/Docker 自动感知）

**核心模块**：PlatformSynchronizer、ApiWatcher、Platform 平台适配层

**职责**：
- 自动检测运行平台（Host/Docker/K8s/Libvirt）
- 探测本机网卡、IP、hostname（所有平台）
- **K8s 专有**：Optional 启动 ApiWatcher，watch Pod/Node/Namespace 变化（仅一个 Agent 被指派）
- 实时上报元数据给 Controller.genesis_sync

**关键特性**：
- 平台自适应（同一二进制适配各类部署）
- K8s 监听任务由 Controller 动态指派
- 可禁用但仍可工作（简化版）

### 三块的依赖关系

```
块 1（管理面）
    │ 下发决策
    ├──────────────────────────────► 块 2（采集面）
    │  "采集哪些协议、采样率"         └─ 无条件工作
    │
    └──────────────────────────────► 块 3（平台发现）
       决定 kubernetes_api_enabled   ├─ true：启动 ApiWatcher
                                      └─ false：只做本机探测

块 2 和块 3 都依赖块 1 的配置指挥，但：
- 块 2 可独立工作（仅需本地配置）✓ 最独立
- 块 3 可独立工作（功能受限）△ 中等独立
- 块 1 无法独立工作（无配置 Agent 无法启动）✗ 最依赖
```

---

## 2. 核心模块清单

Agent 采用**分层模块化设计**。核心 12 个模块如下：

| 模块 | 语言/库 | 核心职责 |
|------|--------|--------|
| **Trident** | Rust | Agent 主入口和生命周期管理；启动各子模块 (@agent/src/main.rs:119-130) |
| **Dispatcher** | Rust | 网络包接收和分发；支持 6 种采集模式：local/mirror/analyzer/plus/multins/netflow (@agent/src/dispatcher/mod.rs) |
| **EbpfCollector** | Rust + eBPF | 内核 syscall 和应用层堆栈追踪；20+ 协议解析 (@agent/src/ebpf_dispatcher.rs) |
| **EbpfDispatcher** | Rust | 管理 eBPF 程序的加载、运行和性能监控 |
| **IntegrationCollector** | Rust | 集成 OTLP、Prometheus、Telegraf、Datadog、Pyroscope、SkyWalking；HTTP/gRPC 接收器 (@agent/src/integration_collector.rs) |
| **FlowGenerator** | Rust | 聚合网络包为流；生成 L4/L7 性能指标和协议日志 (@agent/src/flow_generator/) |
| **Collector** | Rust | 流聚合器（1s/1m 时间窗）；生成 Metrics Document (@agent/src/collector/collector.rs) |
| **Synchronizer** | Rust + gRPC | 与 Controller 通信；同步配置、策略、黑名单 (@agent/src/rpc/synchronizer.rs) |
| **UniformSender** | Rust | 数据上报；TCP 连接池、压缩（zstd/gzip）、重传 (@agent/src/sender/uniform_sender.rs) |
| **ConfigHandler** | Rust | 配置解析、验证、热更新；本地 YAML + 远程动态配置 (@agent/src/config/handler.rs) |
| **Policy** | Rust | ACL 策略执行、端点表、动态标签管理 |
| **Platform** | Rust | K8s/Docker/Libvirt 平台感知；网络接口和容器元数据同步 |

---

## 3. 采集来源与采集器

Agent 支持**8+ 种采集来源**，分为两大类：

### 3.1 网络包采集

| 采集来源 | 采集器 | 特点 | 数据类型 |
|---------|--------|------|--------|
| **BPF/cBPF** | Dispatcher | AF_PACKET、DPDK、Libpcap；支持 6 种采集模式 | MetaPacket（原始网包） |
| **eBPF Syscall** | EbpfCollector | uprobe/kprobe 拦截 socket syscall；零网包丢失 | MetaPacket + AppProtocol |
| **eBPF Stack Trace** | EbpfCollector | 函数调用栈追踪；应用层协议识别 | AppProtoLogsData（协议日志） |

### 3.2 集成数据采集

| 采集来源 | 采集器 | 接收方式 | 数据类型 |
|---------|--------|--------|--------|
| **OpenTelemetry (OTLP)** | IntegrationCollector | gRPC 接收 (4317) 或 HTTP 接收 | Trace + Metrics |
| **Prometheus** | IntegrationCollector | HTTP pull 或 remote-write push | Metrics（prom-pb 格式） |
| **Telegraf** | IntegrationCollector | InfluxDB 兼容协议 | Metrics |
| **Datadog** | IntegrationCollector | HTTP 接收 | Metrics + Events |
| **Pyroscope** | IntegrationCollector | HTTP remote-write | Profile（CPU/Memory） |
| **SkyWalking** | IntegrationCollector | gRPC/HTTP（企业版） | Trace |

---

## 4. 数据处理管道

Agent 的数据流采用**管道架构**，从采集到上报分为 5 个阶段。

### 4.1 整体架构

```
┌─────────────────────────────────────────────────────────┐
│ Stage 1: 采集接收                                        │
│ Dispatcher / EbpfCollector / IntegrationCollector       │
│ ↓↓↓ 多源数据汇聚                                         │
└──────────────────┬──────────────────────────────────────┘
                   │ MetaPacket / AppProtoLogsData
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Stage 2: 数据分类（9 个分类队列）                        │
│ Queue 1: Metrics (1s/1m 聚合)                           │
│ Queue 2-3: L4FlowLog / L7FlowLog                        │
│ Queue 4: ExtMetrics (Prometheus/Telegraf)               │
│ Queue 5: Event (ProcEvent/Syslog)                       │
│ Queue 6: Profile (CPU/Memory)                           │
│ Queue 7: Telemetry (Agent 自身状态)                     │
│ Queue 8-9: 其他                                          │
└──────────────────┬──────────────────────────────────────┘
                   │ 背压管理：超限阻塞、限流
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Stage 3: 流处理和聚合                                    │
│ FlowGenerator → FlowMap (per-worker AHashMap)           │
│ QuadrupleGenerator → SubQuadGen (1s/1m 聚合)            │
│ Collector → Metrics 文档生成                            │
│ AppProtoLogsParser → SessionAggregator (L7 聚合)        │
└──────────────────┬──────────────────────────────────────┘
                   │ 生成 Document/L4FlowLog/L7FlowLog/Profile
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Stage 4: 速率控制                                        │
│ Throttler (LeakyBucket)                                 │
│ Queue depth 监控（背压）                                 │
│ 超时弃包                                                 │
└──────────────────┬──────────────────────────────────────┘
                   │ 数据再入队
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Stage 5: 上报                                            │
│ UniformSender: TCP 连接池                               │
│ Compression: zstd / gzip                                │
│ Retry: 指数退避                                         │
└──────────────────┬──────────────────────────────────────┘
                   │ TCP:20033 (protobuf 编码)
                   ▼
              DeepFlow Ingester
```

### 4.2 关键处理路径

**Metrics 生成路径（data-flow.md 1.2）：**

```
Dispatcher → 网包分类
  → FlowGenerator (FlowMap 查表、建表)
  → TaggedFlow (1s) @ queue.1
  → QuadrupleGenerator
  → SubQuadGen (1s/1m)
  → Collector (聚合)
  → Document(Metrics) @ 输出队列
  → UniformSender
  → Ingester.flow_metrics
```

**L4 FlowLog 生成路径：**

```
Dispatcher → FlowMap
  → TaggedFlow (1m 聚合)
  → FlowAggr (1 分钟聚合)
  → Throttler (限流)
  → L4FlowLog(flow_log type)
  → UniformSender
  → Ingester.flow_log
```

**L7 FlowLog 生成路径（两个入口）：**

```
①网包路径：
Dispatcher → FlowGenerator → MetaAppProto
  → AppProtoLogsParser (协议解析)
  → SessionAggregator (会话聚合)
  → Throttler
  → L7FlowLog

②eBPF路径：
EbpfCollector → MetaPacket
  → EbpfRunner (eBPF 事件处理)
  → SessionAggregator
  → Throttler
  → L7FlowLog
```

### 4.3 性能关键指标

| 指标 | 目标值 | 机制 |
|-----|------|------|
| 单包处理延迟 (p99) | <100 μs | Share-nothing AHashMap、异步队列 |
| 吞吐量 | >10Gbps @ 单核 | 批处理、向量化 |
| 内存占用 | <500MB | 流老化、队列深度限制 |
| 丢包率 | <0.1% | 背压、限流、Throttler |

---

## 5. 与 Controller 的交互（管理面）

Agent 通过 **Synchronizer** 与 Controller 进行双向通信，实现配置下发、状态同步和动态更新。

### 5.1 交互流程（状态机）

```
┌──────────────┐
│ Agent Start  │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ Synchronizer::start()                   │
│ 启动 sync 循环 (周期 = 60s)              │
└──────┬────────────────────────────────┘
       │
       ▼ (首次和周期触发)
┌─────────────────────────────────────────┐
│ Synchronizer::generate_sync_request()    │
│ 生成 SyncRequest：                        │
│  - agent_id / vtap_id                    │
│  - platform info (K8s / Docker / Host)   │
│  - version / 功能支持                     │
│  - 上报 IP/MAC/网卡信息                  │
└──────┬────────────────────────────────┘
       │
       ▼ gRPC:20035
┌─────────────────────────────────────────┐
│ Controller.Sync() / Push()               │
│ @agent/src/rpc/synchronizer.rs:1319     │
└──────┬────────────────────────────────┘
       │
       ▼ 解析 SyncResponse
┌─────────────────────────────────────────┐
│ 热更新应用：                             │
│  1. ConfigHandler::apply_config()        │
│     - protocol whitelist                 │
│     - collection threshold               │
│     - sampling_rate                      │
│  2. Policy::update()                     │
│     - ACL rules                          │
│     - endpoint mapping                   │
│  3. Blacklist::update()                  │
│     - IPs / subnets 加入黑名单          │
│  4. Platform::update()                   │
│     - K8s namespace / node / pod 元数据  │
└──────┬────────────────────────────────┘
       │
       ▼ (无需重启)
┌─────────────────────────────────────────┐
│ 新采集流程采用新配置                     │
│ 存量流保持原有配置直到老化                │
└──────┬────────────────────────────────┘
       │
       └───────(60s 后)──→ 回到 generate_sync_request
```

### 5.2 Synchronizer 关键函数

| 函数 | 位置 | 作用 |
|-----|------|------|
| `Synchronizer::sync_loop()` | @agent/src/rpc/synchronizer.rs | 主循环，周期性 sync |
| `generate_sync_request()` | @agent/src/rpc/synchronizer.rs:772 | 构造 Sync 请求 |
| `grpc_push_with_statsd()` | @agent/src/rpc/synchronizer.rs:1319 | 发送 gRPC 请求，获取应答 |
| `get_config()` | @agent/src/rpc/synchronizer.rs:401 | 从 SyncResponse 提取动态配置 |
| `get_blacklist()` | @agent/src/rpc/synchronizer.rs:510 | 提取黑名单 |
| `parse_containers()` | @agent/src/rpc/synchronizer.rs:857 | 解析 K8s/Docker 元数据 |

### 5.3 运行模式差异

| 模式 | Synchronizer | ConfigHandler | 数据上报 | 典型场景 |
|------|--------------|---------------|--------|--------|
| **Managed** | 必需，周期 Sync | 远程 + 本地配置 | 上报到 Ingester | 生产环境，集中管理 |
| **Standalone** | 禁用 | 仅本地 YAML | 可选或关闭 | 开发测试、隔离网络 |

---

## 6. 配置体系

### 6.1 配置来源（分层优先级）

```
┌──────────────────────────────────────────────────────────┐
│ 优先级 1 (最高)：远程动态配置                             │
│ 来源：Controller 通过 Sync/Push gRPC 下发                │
│ 生效：即时（无需重启）                                    │
│ 例：protocol whitelist、sampling rate                    │
└──────────────────────────────────────────────────────────┘
              ↓ (本地无配置时)
┌──────────────────────────────────────────────────────────┐
│ 优先级 2：本地配置文件                                     │
│ 位置：/etc/deepflow-agent.yaml 或 -f 指定               │
│ 格式：YAML                                               │
│ 生效：启动时加载                                          │
└──────────────────────────────────────────────────────────┘
              ↓ (缺失字段)
┌──────────────────────────────────────────────────────────┐
│ 优先级 3：内置默认值                                       │
│ 来源：@agent/src/config/default.yaml (或代码内嵌)       │
└──────────────────────────────────────────────────────────┘
```

### 6.2 配置热更新支持矩阵

| 配置项 | 远程下发支持 | 热更新 | 生效机制 | 备注 |
|-------|------------|------|--------|------|
| **采集配置** | ✓ | ✓ | 新包立即应用 | BPF filter、DPDK 参数 |
| **协议解析** | ✓ | ✓ | 新流应用 | LogParser whitelist、custom endpoints |
| **流聚合参数** | ✓ | ✓ | 新流应用 | QueueSize、burst size |
| **采样率** | ✓ | ✓ | 新包应用 | sampling_rate (packet level) |
| **性能参数** | ✓ | ✓ | 新流应用 | buffer size、worker threads |
| **Tap 类型** | ✗ | ✗ | 需重启 | 网络采集点切换（local→mirror） |
| **网卡绑定** | ✗ | ✗ | 需重启 | Interface binding |
| **K8s/Docker 集成** | ✓ | ✓ | 实时感知 | PlatformSynchronizer 动态更新元数据 |

### 6.3 ConfigHandler 的职责

**位置**：@agent/src/config/handler.rs（252KB，核心配置模块）

```rust
pub struct Config {
    // 采集相关
    pub dispatcher_config: DispatcherConfig,
    pub ebpf_config: EbpfConfig,
    pub flow_generator_config: FlowGeneratorConfig,

    // 输出相关
    pub sender_config: SenderConfig,
    pub collector_config: CollectorConfig,

    // 管理相关
    pub synchronizer_config: SynchronizerConfig,
    pub platform_config: PlatformConfig,
    pub policy: Arc<Policy>,
}

impl Config {
    pub fn apply_config(&mut self, remote_config: DynamicConfig) -> Result<()>;
    pub fn validate(&self) -> Result<()>;
}
```

---

## 7. 多线程/异步架构

### 7.1 并发模型

Agent 采用 **Tokio 异步运行时** 驱动，所有 I/O 和网络操作无阻塞。

```
Main Thread
  │
  └─► Tokio Runtime (@agent/src/trident.rs:41)
      │
      ├─ 工作线程池
      │  ├─ 核心数 = CPU 核数（可配置）
      │  └─ 可配置 max_blocking_threads
      │
      └─ 任务调度队列 (FIFO)
```

### 7.2 关键线程/任务类型

| 类型 | 数量 | 职责 | 同步机制 |
|------|------|------|--------|
| **Dispatcher** | 1+ per interface | 网包接收、分发到分类队列 | 无锁环形缓冲 + 背压 |
| **FlowGenerator** | 1 per Dispatcher | 流表管理 (per-worker AHashMap)、TaggedFlow 生成 | 无锁，share-nothing |
| **QuadrupleGenerator** | N (pooled) | 二级聚合（1s/1m）、Metrics 文档生成 | bounded queue + blocking |
| **Collector** | N (pooled) | 聚合计算、Document 序列化 | channel send |
| **AppProtoLogsParser** | N | L7 协议解析和日志生成 | channel rx |
| **SessionAggregator** | 1 | 会话级聚合 (L4/L7) | internal state |
| **Throttler** | 1 | 限流器 (LeakyBucket) | atomic counter |
| **UniformSender** | M | 数据压缩、TCP 发送、重试 | buffered channel |
| **Synchronizer** | 1 | gRPC 同步（60s 周期） | tokio::time::interval |
| **Monitor** | 1 | 健康检查、资源监控、告警 | metrics collection |
| **PlatformSynchronizer** | 1 | K8s/Docker 资源实时同步 | watch + event queue |

### 7.3 线程间通信

```
Message Queue Pattern
├─ 使用 public::queue (有界队列)
│  ├─ 队列深度：1K ~ 64K（可配置）
│  └─ 超满时：背压阻塞发送方
│
├─ DebugSender / Receiver<T>（Rust channel）
│  ├─ 用于 Throttler、Monitor、Synchronizer 等
│  └─ 无锁，基于 mpmc + RwLock
│
├─ Broadcast 通知
│  ├─ Synchronizer 配置变更时 broadcast
│  └─ 让所有处理线程感知（快速反应）
│
└─ 共享只读数据（通过 Arc）
   ├─ FlowMap（**非共享**，每个 Dispatcher 独占一份 AHashMap）
   ├─ Config (arc-swap 无锁更新)
   └─ Policy (共享 ACL 和端点表)
```

### 7.4 性能关键指标

| 指标 | 目标值 | 达成机制 |
|-----|------|--------|
| **单包处理 p99 延迟** | <100 µs | Share-nothing AHashMap、async I/O |
| **吞吐量** | >10 Gbps (单 CPU) | 批处理 (ring buffer)、SIMD |
| **端到端延迟** | <1 ms | 流缓冲、定期 flush、async send |
| **内存占用** | <500 MB (单 Gbps) | 流老化 (5 min timeout)、队列深度限制 |
| **CPU 利用率** | <1 core per Gbps | 向量化、无锁设计、task pinning |

---

## 8. 关键依赖分析

### 8.1 核心运行时和协议库

| 库 | 版本 | 用途 | 关键原因 |
|----|------|------|--------|
| **tokio** | 1.20+ | 异步运行时 | 高吞吐、低延迟 I/O |
| **tonic** | 0.10 | gRPC 框架 | 与 Controller 通信；需特定版本保证 TLS/buffer 行为 |
| **prost** | 0.12 | Protobuf 编解码 | 与 tonic 配套 |
| **hyper** | 0.14 | HTTP 客户端 | OTLP、Prometheus 接收端 |
| **pcap / special_recv_engine** | — | 网包捕获 | AF_PACKET/DPDK 驱动 |
| **zstd** | 0.13+ | 压缩算法 | 数据上报压缩（默认） |
| **ahash** | — | 快速非加密哈希 | FlowMap 的 AHashMap 实现，per-worker 独立 |
| **arc-swap** | 1.5+ | 无锁配置更新 | ConfigHandler 热更新（无停机） |
| **kube** | 0.98 | K8s API 客户端 | K8s 资源同步 (PlatformSynchronizer) |

### 8.2 协议解析库

```
@agent/crates/l7/
├─ l7_protocol/         → 基础协议类型定义
├─ plugins/l7/          → 20+ 应用层协议解析器
│  ├─ http.rs           → HTTP/1.0/1.1
│  ├─ http2.rs          → HTTP/2
│  ├─ mysql.rs          → MySQL
│  ├─ redis.rs          → Redis
│  ├─ dns.rs            → DNS
│  └─ ... (20+ 种)
└─ AppProtoLogsParser   → 流式解析引擎

@agent/crates/l4_protocol/
└─ TCP/UDP 4 层协议处理

@agent/src/ebpf/plugins/
├─ http2/               → HTTP/2 eBPF 助手
├─ tunnel/              → VXLAN/GRE 解封装
├─ npb_handler/         → NPB (网络包裸流转)
└─ ...
```

---

## 9. 关键代码路径导航

| 功能模块 | 文件路径 | 关键函数 | 行数 |
|---------|--------|--------|------|
| **Agent 启动** | @agent/src/main.rs | `main()` → `Trident::start()` | 50 |
| **数据流入口** | @agent/src/dispatcher/mod.rs | `Dispatcher::run()` | 500+ |
| **网包处理** | @agent/src/flow_generator/flow_map.rs | `FlowMap::insert()` / `lookup()` | 154K |
| **指标聚合** | @agent/src/collector/collector.rs | `Collector::collect()` | 10K |
| **配置同步** | @agent/src/rpc/synchronizer.rs | `Synchronizer::sync_loop()` / `apply_config()` | 83K |
| **数据发送** | @agent/src/sender/uniform_sender.rs | `UniformSenderThread::send()` | 30K |
| **eBPF 加载** | @agent/src/ebpf/mod.rs | `EbpfCollector::load_ebpf()` | 41K |
| **K8s 同步** | @agent/src/platform/kubernetes/ | `PlatformSynchronizer::run()` | 200+ |
| **配置管理** | @agent/src/config/handler.rs | `ConfigHandler::apply_config()` | 252K |
| **OTLP 接收** | @agent/src/integration_collector.rs | `IntegrationCollector::start()` | 10K |
| **L7 解析** | @agent/crates/l7/ | `AppProtoLogsParser::parse()` | 1000+ |

---

## 10. 部署模式与三块的协作

Agent 支持两种部署模式，三个功能块在不同模式下的行为显著不同。

### 10.1 Managed 模式 vs Standalone 模式对比

| 方面 | Managed（默认） | Standalone |
|-----|------------------|----------|
| **块 1（管理面）** | ✓ 必需工作 | ✗ 完全禁用 |
| **块 2（采集面）** | ✓ 工作 | ✓ 工作 |
| **块 3（平台发现）** | ✓ 工作 | △ 简化版工作 |
| **Synchronizer** | 60s 周期 Sync | 禁用（enabled=false） |
| **ConfigHandler** | 远程配置 + 本地配置 | 仅本地配置 |
| **ApiWatcher** | Controller 指派启动 | 不启动 |
| **数据上报** | 自动发现 ingester | 手动指定 ingester |
| **配置修改** | 热更新（无需重启） | 需要重启 |
| **K8s 支持** | ✓ 完整（Pod 标签） | △ 受限（只有 Node 级别） |
| **适用场景** | 生产环境，集中管理 | 测试开发，隔离网络 |

### 10.2 Standalone 模式启用方法

**方法 1：启动参数**

```bash
# 完全禁用 Synchronizer
deepflow-agent --synchronizer enabled=false

# 或者指定 standalone 模式
deepflow-agent --mode standalone
```

**方法 2：配置文件（推荐）**

创建 `/etc/deepflow-agent.yaml`：

```yaml
# 禁用远程同步（块 1）
synchronizer:
  enabled: false

# 启用采集（块 2）
dispatcher:
  tap_type: local                     # 采集类型：local/mirror/analyzer
  interface: eth0                     # 指定网卡（可选）

collector:
  enabled: true

flow_generator:
  enabled: true

# 数据上报配置（块 2）
sender:
  enabled: true
  server_ip: 10.0.0.1                # 手动指定 Ingester IP
  server_port: 20033                 # Ingester 数据端口

# 协议采集白名单（本地定义）
protocol:
  enabled_tags:
    - http
    - mysql
    - redis
    - dns
    - kafka
    - grpc
  sampling_rate: 1000                # 每 1000 个包采 1 个

# 禁用 K8s API 监听（块 3）
platform:
  kubernetes_api_enabled: false
```

### 10.3 Standalone 模式下三块的工作情况

**启动流程：**

```
deepflow-agent -c /etc/deepflow-agent.yaml
  │
  ├─ 块 1（管理面）✗ 跳过
  │  └─ Synchronizer.start() 检查 enabled=false，直接返回
  │
  ├─ 块 2（采集面）✓ 启动
  │  ├─ ConfigHandler 加载本地 YAML 配置
  │  ├─ Dispatcher 启动网包采集（tap_type=local）
  │  ├─ EbpfCollector 启动（如配置）
  │  ├─ FlowGenerator、Collector 启动
  │  └─ UniformSender 启动
  │     └─ 直接连接配置文件里指定的 Ingester
  │
  └─ 块 3（平台发现）△ 简化版启动
     ├─ PlatformSynchronizer 启动（本机探测）
     │  └─ 探测本机网卡、IP、hostname
     ├─ ApiWatcher ✗ 不启动
     │  └─ kubernetes_api_enabled=false 禁用
     └─ 不向任何 Controller 汇报
        _（数据到 Ingester，由 Ingester 调 Controller enrichment）_
```

### 10.4 Standalone 模式的功能限制

#### ✓ 可以做的事

| 功能 | 是否支持 | 说明 |
|-----|--------|------|
| 网包采集 | ✓ | 本地 Dispatcher 完全支持 |
| eBPF syscall 采集 | ✓ | EbpfCollector 无需 Controller 控制 |
| 协议解析 | ✓ | protocol_whitelist 本地配置即可 |
| 流聚合（Metrics） | ✓ | FlowGenerator 和 Collector 独立完成 |
| 数据上报 | ✓ | 手动指定 Ingester 地址 |
| Docker 容器识别 | ✓ | PlatformSynchronizer 支持 |
| 性能指标采集 | ✓ | 独立工作 |

#### ✗ 不能做的事

| 功能 | 影响 | 原因 |
|-----|------|------|
| **K8s Pod 元数据** | 看不到 Pod 标签、命名空间标签 | 没人指派 ApiWatcher；无 K8s API 凭证 |
| **动态配置更新** | 修改采集策略需要重启 Agent | 不连接 Controller |
| **黑名单管理** | 黑名单固化在配置文件里 | 无法动态更新 |
| **版本升级** | 无法自动升级 | 没有 Controller 管理 |
| **Agent 健康状态监控** | 无法在 UI 看到 Agent 状态 | 无法向 Controller 上报 Agent 状态 |

### 10.5 典型场景选择

| 场景 | 推荐模式 | 原因 |
|-----|--------|------|
| 生产环境，Controller 正常运行 | **Managed** | 完整功能、集中管理 |
| 开发测试，快速验证采集 | **Standalone** | 快速启动，无需 Controller |
| K8s 集群，需要 Pod 标签 | **Managed** | Pod 元数据需要 Controller |
| 隔离网络，无法连接 Controller | **Standalone** | 独立工作 |
| 宿主机独立部署，简化运维 | **Standalone** | 配置简单 |

### 10.6 多部署场景 Agent 的协作

**场景：K8s DaemonSet 部署一个集群**

```
一个集群中的多个 Agent（分布在不同 Node 上）
  │
  ├─ 「被指派」的 Agent（通常是第一个启动的）
  │  ├─ kubernetes_api_enabled = TRUE
  │  ├─ ApiWatcher 启动，watch 整个集群的 Pod/Node/NS
  │  └─ 上报完整 K8s 元数据给 Controller
  │
  └─ 其他 Agent（其他 Node）
     ├─ kubernetes_api_enabled = FALSE
     ├─ ApiWatcher 不启动（已有其他 Agent 负责）
     └─ 只做本 Node 的网卡同步

Controller 的指派逻辑：
  ├─ 收到第 1 个 Agent Sync：kubernetes_api_enabled = TRUE
  ├─ 收到第 2~N 个 Agent Sync：kubernetes_api_enabled = FALSE
  └─ Agent 数量变化时，自动重新指派

这样设计的好处：
  ✓ 避免多个 Agent 重复监听 K8s API（浪费资源）
  ✓ 单点故障转移：如果被指派的 Agent 故障，其他 Agent 可接管
  ✓ 自动做生负载均衡
```

---

## 11. 架构特色与设计权衡

### 11.1 设计亮点

| 特色 | 实现 | 收益 |
|-----|------|------|
| **Share-nothing 流表** | per-worker AHashMap + arc-swap | 支持 >1M 并发流；零跨核同步 |
| **热配置更新** | Controller Push + arc-swap | 无需重启，秒级生效 |
| **背压管理** | 有界队列 + blocking 检测 | 防止内存溢出；公平调度 |
| **多源采集** | 8+ 持集成 (BPF/eBPF/OTLP/Prom/…) | 一个 Agent 统一所有观测数据 |
| **适应网络拓扑** | 6 种 Dispatcher 模式 (local/mirror/…) | 支持多种网络部署 |
| **应用层感知** | eBPF uprobe + 协议解析 | 超越网络层，获得应用语义 |

### 11.2 性能权衡

| 权衡项 | 选择 | 原因 |
|-------|------|------|
| **采集精度 vs 本地处理** | 本地流聚合 (5s~1m) | 减少上报量 10-100x；够用于时序分析 |
| **实时性 vs CPU** | 可配置聚合窗口 | 允许用户在延迟和 CPU 间权衡 |
| **压缩率 vs 速度** | zstd (默认)，可选 gzip | zstd 兼顾性能和压缩率 |
| **内存占用 vs 流 timeout** | 5~10 min timeout | 平衡内存（<500MB）和流识别能力 |
| **网包接收模式** | AF_PACKET (通用)、DPDK (高性能) | 按场景选择；AF_PACKET 无需驱动改造 |

### 11.3 已验证 vs 未验证

#### ✓ 已验证

- Agent 的 6 种 Dispatcher 采集模式架构
- 9 个分类队列的数据流向设计
- Synchronizer 的 60s 周期 Sync 机制和热更新逻辑
- ConfigHandler 的动态配置应用框架
- Tokio 异步运行时使用和 per-worker 独立 AHashMap 的 share-nothing 架构
- eBPF + 应用层双层采集的协作模式
- 三块模块的独立性和 Controller 指派机制

#### ? 未验证（建议后续深入）

- 单个网包处理的具体延迟分布（需 benchmark 和 profiling）
- eBPF 程序的内存占用和各类 hook 的 CPU 开销
- 大流量 (>50Gbps) 场景下队列深度的自适应调整逻辑
- NTP 同步对时序准确性的实际影响 (@agent/src/rpc/ntp.rs)
- Enterprise 版本中 analyzer_mode 和 vector_component 的具体功能和架构

---

## 12. 参考文档

本文档与以下文件配套：

- **@docs/design/data-flow.md** — 数据采集、meta 同步、Agent 注册流程的详细 Mermaid 流程图
- **@docs/design/microservices-architecture.md** — Agent 与 Server（Controller/Ingester/Querier）的整体链路
- **@agent/README.md** — Agent 编译、部署、配置快速入门
- **@agent/build.md** — Agent 开发环境和构建依赖

## 13. 后续深入方向

## 11. Agent 核心采集流程详解

本章从**数据包进入 Agent 的那一刻**开始，逐阶段追踪数据的转换、处理、聚合和上报过程，完整展现块 2（采集面）的内部工作原理。

### 11.1 采集流程的 5 个阶段

```
┌─────────────┐     ┌──────────────┐     ┌────────────────┐     ┌───────────────┐     ┌──────────┐
│   Stage 1   │     │    Stage 2   │     │     Stage 3    │     │     Stage 4   │     │ Stage 5 │
│   Capture   │────▶│    Parse     │────▶│   Flow Stat    │────▶│  Aggregation  │────▶│  Send   │
│  (Packet)   │     │  (Protocol)  │     │  (FlowGen)     │     │ (Collector)   │     │ (RPC)   │
└─────────────┘     └──────────────┘     └────────────────┘     └───────────────┘     └──────────┘
  Dispatcher        PacketHandler       FlowGenerator/Map      QuadrupleGen/Agg    Sender Thread
  (原始包/1Gbps)    (协议栈解析)        (活流维护/聚合)      (1s/1m 窗口)        (编码/压缩)
```

#### Stage 1: 数据包捕获（Dispatcher）

**输入**：网络接口上的原始数据包
**输出**：Packet 结构体（含时间戳、接口索引、长度）

Dispatcher 负责从网络接口接收数据包。代码位置：`@agent/src/dispatcher/`

```rust
pub struct Packet {
    pub timestamp: Duration,           // 包的时间戳
    pub raw: BatchedBuffer<u8>,        // 原始包数据
    pub original_length: u32,          // 抓包时的原始长度
    pub raw_length: u32,               // 实际捕获的长度
    pub if_index: isize,               // 接口索引
    pub ns_ino: u32,                   // network namespace inode
}
```

**支持的 Capture Mode**（通过 dispatcher_config.yaml 配置）：

| Mode | 说明 | 应用场景 |
|------|------|---------|
| `local` | 直接从物理接口抓包 | 宿主机、虚拟机直通网卡 |
| `mirror` | 从 TAP 端口接收镜像流量 | 交换机镜像、vSwitch 镜像 |
| `analyzer` | 主动分析模式（企业版） | 网络设备直接将流量转发 |
| `local-plus` | local 模式加增强（af_packet v3） | 高流量场景（>10Gbps） |
| `mirror-plus` | mirror 模式加增强 | 高流量镜像场景 |

**核心处理**：
- 使用 `libpcap` 或 `af_packet` 接收数据包
- 批量处理包以提高吞吐（BatchedBuffer）
- 支持 DPDK、vhost-user 等高性能源（企业版）
- 按 network namespace 隔离采集（支持容器环境）

#### Stage 2: 协议解析（PacketHandler → MetaPacket）

**输入**：Packet（原始数据）
**输出**：MetaPacket（协议信息已提取）

PacketHandler 对每个包进行协议栈解析：

```rust
pub struct MetaPacket<'a> {
    pub raw_packet: &'a [u8],
    pub timestamp: Duration,

    // L2 层
    pub mac_src: MacAddr,              // 源 MAC
    pub mac_dst: MacAddr,              // 目的 MAC
    pub vlan_id: u32,                  // VLAN ID

    // L3 层
    pub ip_src: IpAddr,                // 源 IP
    pub ip_dst: IpAddr,                // 目的 IP
    pub proto: IpProtocol,             // 协议号（TCP/UDP/...)

    // L4 层
    pub port_src: u16,                 // 源端口（TCP/UDP）
    pub port_dst: u16,                 // 目的端口

    // L7 层（应用层）
    pub app_proto: L7Protocol,         // HTTP、MySQL、Kafka...
    pub app_layer: &'a [u8],           // 应用层负载

    // 隧道信息
    pub tunnel_type: TunnelType,       // VxLAN、MPLS...
    pub tunnel_outer_ip_src: IpAddr,   // 隧道外层 IP

    // 其他元数据
    pub packet_len: u32,               // 包的总长度
    pub flow_id: u64,                  // 流 ID（由 FlowGenerator 赋值）
    pub is_socket_closed: bool,        // 是否收到 FIN/RST（TCP 关闭）
}
```

**解析步骤**：
1. 提取 L2 头（MAC、VLAN）
2. 解析 L3 头（IPv4/IPv6、TTL、fragmentation）
3. 解析 L4 头（TCP/UDP、端口）
4. 去隧道化（VxLAN、MPLS、GRE 等）
5. 识别 L7 协议（DPI - Deep Packet Inspection）
6. 检查数据包方向（入/出）

#### Stage 3: 流状态维护（FlowGenerator → FlowMap）

**输入**：MetaPacket 流
**输出**：TaggedFlow / FlowLog / AppProto（L7 日志）

FlowGenerator 是 Agent 最核心的模块，维护所有活跃的网络流。**完整的深度分析见 11.12 节**；本节给出概览。

**核心架构**：per-Dispatcher 独立的 AHashMap（不是 DashMap！），share-nothing 设计，零跨核同步。

```rust
// @agent/src/flow_generator/flow_map.rs:186
pub struct FlowMap {
    node_map: Option<(
        AHashMap<FlowMapKey, Vec<Box<FlowNode>>>,     // 每个 Dispatcher 独占一份
        Vec<HashSet<FlowMapKey>>,                      // 时间轮，slot-based
    )>,
    id: u32,
    state_machine_master: StateMachine,                // TCP 正向状态机
    state_machine_slave: StateMachine,                 // TCP 反向状态机
    // ...
}
```

**关键流程** - `inject_meta_packet()`：

```
1. inject_flush_ticker() 推进时间窗口，GC 过期流
2. 计算 FlowMapKey（双向流自动归一）
3. 在 AHashMap 中查找 + Vec 里精确匹配
   ├─ 找到 → update_tcp_node() / update_udp_node()
   │        ├─ 查 TCP 状态机 (O(1))
   │        ├─ 更新统计、TCP flags
   │        └─ 调用 L7 协议解析器
   │
   └─ 未找到 → new_flow_node() → 插入 AHashMap + time_set
4. 流结束 → node_removed_aftercare() 输出 FlowLog
```

**完整细节**（双向流哈希技巧、TCP 状态机查表、时间轮 GC、eBPF 流特殊处理等）参见 **11.12 Stage 3 深度剖析**。

#### Stage 4: 指标聚合（Collector）

**输入**：FlowLog 流（每个 Flow 的统计数据）
**输出**：聚合的 Metrics（每秒、每分钟）

Collector 模块完成指标的时间维度和维度组合的聚合：

```rust
pub struct CollectorThread {
    pub quadruple_generator: QuadrupleGeneratorThread,  // 提取四元组
    l4_flow_aggr: Option<FlowAggrThread>,              // L4 层聚合
    second_collector: Option<Collector>,               // 秒级采样
    minute_collector: Option<Collector>,               // 分钟级采样
}
```

**处理链路**：

```
FlowLog (单个流的统计)
    ↓
QuadrupleGenerator
    ├─ 提取 src_ip, dst_ip, src_port, dst_port, proto
    └─ 生成多个维度的 MiniFlow
    ↓
FlowAggrThread (L4 层聚合，可选)
    ├─ 按 IP+Proto+Port 聚合多个 Flow
    ├─ 合并统计数据（packet_count, byte_count）
    └─ 输出聚合后的 MiniFlow
    ↓
Collector (时间窗口聚合)
    ├─ 秒级窗口（1s）
    ├─ 分钟级窗口（60s）
    └─ 生成 Document
    ↓
Sender
    └─ 编码并通过 TCP:20033 上报
```

**StashKey** - 聚合的维度组合：

```rust
pub struct StashKey {
    pub ip_src: IpAddr,
    pub ip_dst: IpAddr,
    pub src_gpid: u32,              // Group ID（来自 CIDR）
    pub dst_gpid: u32,
    pub endpoint_hash: u32,
    pub time_span: u32,             // 请求-响应时间跨度（L7）
    pub biz_type: u8,               // 业务分类
}
```

**聚合算法**（以秒级为例）：

```
时间窗口: [00:00, 00:01)

1. 收集所有在此窗口内到达的 FlowLog
2. 按 StashKey 分桶，相同 key 的指标合并
   ├─ packet_count 相加
   ├─ byte_count 相加
   ├─ min/max/stddev 等统计量
   └─ TCP flags、L7 协议统计汇总
3. 填充 Document（MetricID、时间戳、维度、指标值）
4. 在窗口结束时 flush（生成一条 Document 记录）
```

**Document 结果示例**：

```
时间: 2024-04-08 10:00:00
source_ip: 10.1.2.3
dest_ip: 10.4.5.6
protocol: TCP
source_port: 8080
dest_port: 3306

指标:
  packet_count: 1250
  byte_count: 512000
  duration: 5.2 秒
  tcp_rtt: 12ms
  l7_protocol: MySQL
  l7_request_count: 45
  l7_response_count: 45
  l7_error_count: 0
  tcp_establish_rtt: 1.2ms
```

#### Stage 5: 数据上报（Sender）

**输入**：Document 流（聚合的 Metrics）
**输出**：gRPC 或 TCP 报文，发往 Server:20033

Sender 对聚合的数据进行编码和压缩，实时上报给 DeepFlow Server：

```rust
pub struct UniformSender {
    receiver: Receiver<BoxedDocument>,  // 接收聚合数据
    rpc_client: RpcClient,              // RPC 连接
    encoder: SenderEncoder,             // 编码器
    stats_collector: Arc<Collector>,    // 统计
}
```

**编码流程**：

```
Document
    ↓
Encoder (选择编码器)
    ├─ Raw: 原始 Protobuf 编码
    ├─ Gzip: 压缩率 ~80%
    └─ Custom: 自定义压缩
    ↓
RPC 发送
    ├─ gRPC Call (阻塞式)
    ├─ TCP batched (异步)
    └─ 支持失败重试
    ↓
Server Ingester:20033
    └─ 接收、反序列化、写入 ClickHouse
```

**发送统计**：

```rust
pub struct UniformSenderCounter {
    out: AtomicU64,              // 发送的 Document 数
    window_delay: AtomicI64,     // 窗口到达延迟
    flow_delay: AtomicI64,       // 单个流延迟
    compress_ratio: AtomicU64,   // 压缩率
}
```

### 11.2 数据流转的关键性能特性

| 特性 | 实现 | 性能影响 |
|------|------|---------|
| **无锁并发** | Share-nothing per-worker AHashMap + Arc<Swap<>> | 多核通过分 worker 扩展，零跨核同步 |
| **批处理** | BatchedBuffer（将多个小数据聚合成大包）| 减少内存分配次数 |
| **异步 I/O** | tokio runtime | 高吞吐 RPC、不阻塞主线程 |
| **内存复用** | RwLock<Option> 的流 FlowMap | 避免频繁的堆分配 |
| **时间窗口** | HashSet<FlowMapKey> 按秒索引 | O(1) 查找过期流 |
| **限流** | LeakyBucket（漏桶算法）| 防突增数据压垮 Server |

### 11.3 采集流程中的关键决策点

**决策 1：何时 Flush 一条 Flow？**

```
时机：
  1. 流结束（TCP FIN/RST、UDP 超时）
  2. 时间窗口滑动（60s 边界）
  3. 内存压力（AHashMap 大小超过阈值）
  4. L7 协议完成事件（HTTP response 完整）

操作：
  ├─ 生成 FlowLog 发往 Collector
  ├─ 更新最终统计数据
  └─ 从 DashMap 中删除 FlowNode
```

**决策 2：采集哪些维度？**

```
配置：collector_config.yaml
  ├─ l3_epc_id: 是否采集 EPC（虚拟网络标识）
  ├─ l7_protocol: 是否采集 L7（应用层）
  ├─ flow_aggr.enabled: 是否聚合相同端点的多个 Flow
  └─ metrics.window: 1s 还是 60s 窗口

维度权衡：
  └─ 维度越多 → 数据越细致，但存储空间/查询时间增加
```

**决策 3：什么情况下重新捕获？**

```
触发 Dispatcher 热重启：
  1. TAP 接口正则表达式变更 → 扫描新接口
  2. Capture Mode 变更 → 切换采集方式
  3. Filter BPF 表达式变更 → 重新加载 BPF
  4. Network Namespace 变更 → 重新初始化隔离

无需重启的热更新：
  ├─ 数据采样率
  ├─ 流超时时间
  ├─ L7 协议识别规则
  └─ 上报目标地址
```

### 11.4 Dispatcher 与 eBPF：两条独立的采集路径

Agent 的采集面有**两条相互独立又互补**的路径，对应两套截然不同的技术栈。

#### 路径对比

```
路径 A：Dispatcher（用户态抓包）
   网卡 → libpcap/af_packet/DPDK → Dispatcher → FlowGenerator
                                    (用户态)

路径 B：EbpfCollector（内核态探针）
   内核 syscall/uprobe → eBPF → EbpfCollector → FlowGenerator
                              (内核态钩子)
```

| 维度 | Dispatcher（抓包）| EbpfCollector（eBPF）|
|------|------------------|---------------------|
| **数据源** | 网卡上的原始字节流 | 内核 socket 事件、函数调用 |
| **运行位置** | 用户态收包 | 内核态钩子 |
| **可观测内容** | 完整网包、L7 协议解析 | 加密流量（uprobe 解密前）、syscall |
| **性能开销** | 高（需要拷贝包）| 低（内核态聚合）|
| **加密流量** | 看不到明文（除非 SSL 终止）| 可以（uprobe OpenSSL）|
| **代码位置** | `@agent/src/dispatcher/` | `@agent/src/ebpf_dispatcher.rs`、`@agent/src/ebpf/` |

#### Dispatcher 支持的接收引擎

代码位置：`@agent/src/dispatcher/recv_engine/`

| 引擎 | 技术 | 场景 |
|------|------|------|
| **af_packet** | Linux PACKET_MMAP | 默认，宿主机/容器 |
| **libpcap** | 跨平台抓包库 | 兼容性场景 |
| **DPDK** | 内核旁路高性能框架（企业版）| 10Gbps+ 高速链路 |
| **vhost-user** | 虚拟交换机直连（企业版）| OVS-DPDK、虚拟化场景 |
| **DpdkFromEbpf** | eBPF + DPDK 混合（企业版）| 特殊高性能场景 |

> 注意：**Dispatcher 主流路径不依赖 eBPF**。`DpdkFromEbpf` 是企业版的一个特例，仅作为 DPDK 的辅助。

#### EbpfCollector 的能力维度

eBPF 路径采集的是**完全不同的数据**，主要包括：

- **socket 事件**：通过 kprobe 钩 `tcp_sendmsg`、`tcp_recvmsg` 等内核函数
- **uprobe**：钩 OpenSSL、Go runtime 等用户态函数（解密 HTTPS、追踪应用层）
- **profile**：CPU on-cpu/off-cpu 性能采样、内存 profiling
- **syscall tracing**：跟踪系统调用

#### 互补关系

两者**不是替代关系，而是协同工作**，最终汇聚到同一个 FlowGenerator：

```
Dispatcher    ──┐
EbpfCollector ──┼──→ FlowGenerator → Collector → Sender
IntegrationCol──┘
```

- Dispatcher 提供**完整的网络包视图**（适合非加密流量、网络层分析）
- EbpfCollector 提供**应用视角**（适合加密流量、跨进程追踪、低开销）

### 11.5 默认启用情况与配置开关

#### 总体默认状态

| 路径 | 默认状态 | 是否需要显式开启 |
|------|---------|----------------|
| **Dispatcher** | 默认启用 | 需要配置采集网卡，通过 `inputs.cbpf.common.capture_mode` 选择模式 |
| **EbpfCollector 主开关** | 默认启用 (`inputs.ebpf.disabled: false`) | 主开关默认开 |
| **EbpfCollector 子功能** | 部分默认关 | uprobe、profile 等需手动开 |

源码确认：`@server/agent_config/template.yaml:2669` 是 `disabled: false`

#### eBPF 子开关（粒度细）

eBPF 主开关只是"是否加载 eBPF 框架"，能采集什么由子开关决定：

```yaml
inputs:
  ebpf:
    disabled: false                # 主开关（默认 false 即启用）

    socket:
      uprobe:
        golang:
          enabled: false           # Go 应用 uprobe（默认关）
        tls:
          enabled: false           # OpenSSL/HTTPS 解密（默认关）

      sock_ops:
        tcp_option_trace:
          enabled: false           # TCP 选项追踪（默认关）

    profile:
      on_cpu:
        disabled: false            # CPU profiling（默认开）
      off_cpu:
        disabled: true             # off-CPU profiling（默认关）
      memory:
        disabled: true             # 内存 profiling（默认关）
```

**总结**：eBPF 框架默认会 attach 内核 socket 跟踪，但 **uprobe（应用层追踪）和大部分 profile（性能采样）默认关闭**，按需打开即可。

### 11.6 权限要求与部署前置条件

**结论：Agent 必须以 root 运行，或为非 root 二进制授予等价的 capabilities。** 普通用户无法直接运行。

#### 必需的 Linux capabilities

| 权限 | 用途 | 代码位置 |
|------|------|---------|
| **CAP_SYS_ADMIN** | `setns()` 进入 network namespace；加载 eBPF 程序 | `@agent/src/trident.rs:697` |
| **CAP_NET_ADMIN** | 配置网卡（promisc 模式、修改 BPF filter）| `@agent/src/main.rs:64` |
| **CAP_NET_RAW** | 开 raw socket 抓包（af_packet）| 同上 |
| **CAP_NET_BIND_SERVICE** | 绑定特权端口 | 同上 |
| **CAP_IPC_LOCK** | 锁定内存（eBPF 加载所需）| K8s securityContext |

源码佐证：

```rust
// @agent/src/main.rs:64
/// Grant capabilities including cap_net_admin, cap_net_raw, cap_net_bind_service
#[clap(long)]
add_cap: bool,
```

```rust
// @agent/src/trident.rs:697
return Err(anyhow!(
    "agent must have CAP_SYS_ADMIN to run without 'hostNetwork: true'. setns error: {}", e
));
```

#### 三种部署方式的权限处理

**方式 1：root 用户直接运行（最简单）**

```bash
sudo deepflow-agent -f /etc/deepflow-agent.yaml
```

**方式 2：非 root + setcap 授权 capabilities**

```bash
sudo setcap cap_net_admin,cap_net_raw,cap_net_bind_service,cap_sys_admin+ep deepflow-agent
./deepflow-agent
```

或使用 Agent 自带的 `--add-cap` 命令行参数自动授予。

**方式 3：K8s DaemonSet 部署**

```yaml
spec:
  hostNetwork: true              # 必需，否则需要 setns 进入宿主机 namespace
  containers:
  - name: deepflow-agent
    securityContext:
      privileged: true           # 最简单的方式
      # 或细粒度授权:
      capabilities:
        add:
        - SYS_ADMIN
        - NET_ADMIN
        - NET_RAW
        - IPC_LOCK              # eBPF 加载内存锁定
```

#### eBPF 的内核版本要求

源码 `@agent/src/ebpf_dispatcher.rs:1346` 给出权威建议：

> "如果当前内核版本低 (<5.2)，升级到 5.2+ (启用 CONFIG_DEBUG_INFO_BTF=y) 可解决问题"

实际兼容性矩阵：

| 内核版本 | 支持情况 |
|---------|---------|
| **< 4.14** | eBPF 基本不可用 |
| **4.14 - 5.1** | 基础 eBPF 可用，需要 kernel-devel 适配偏移量 |
| **5.2+ 且开启 BTF** | ✅ 推荐，CO-RE 自动适配，开箱即用 |
| **< 5.2 无 BTF** | 需要手动 BPF CO-RE 适配，可能失败 |

**BTF 检查命令**：

```bash
# 检查 BTF 是否启用
ls /sys/kernel/btf/vmlinux 2>/dev/null && echo "BTF enabled" || echo "BTF NOT available"

# 检查内核配置（如果有 /proc/config.gz）
zcat /proc/config.gz | grep CONFIG_DEBUG_INFO_BTF
```

#### 部署前置检查清单

| 检查项 | 命令 | 期望结果 |
|--------|------|---------|
| 是 root 或有 cap | `id` / `getcap deepflow-agent` | uid=0 或 cap_sys_admin,cap_net_admin... |
| 内核版本 | `uname -r` | ≥ 5.2 推荐 |
| BTF 支持 | `ls /sys/kernel/btf/vmlinux` | 文件存在 |
| 网卡 promisc 权限 | `ip link set <iface> promisc on` | 不报错 |
| K8s hostNetwork | `kubectl get pod -o yaml \| grep hostNetwork` | true |

### 11.7 AutoTracing：无侵入分布式追踪

AutoTracing 是 DeepFlow 区别于传统 APM 工具的核心卖点之一——**无需修改应用代码、无需注入 SDK、无需 sidecar**，Agent 就能自动识别跨进程的调用链路并生成 trace_id。它完全由 **Agent 内置的 EbpfCollector 模块**实现，不依赖任何外部服务。

#### 工作原理

```
┌─────────────────────────────────────────────────┐
│              deepflow-agent 进程                 │
│                                                  │
│  ┌─────────────────────┐                        │
│  │  EbpfCollector      │                        │
│  │                     │                        │
│  │  ┌───────────────┐  │                        │
│  │  │ 内核态 eBPF   │  │  ← syscall_trace_id    │
│  │  │ - kprobe       │  │    在这里计算          │
│  │  │ - hash 表关联  │  │                        │
│  │  └───────┬───────┘  │                        │
│  │          ▼          │                        │
│  │  ┌───────────────┐  │                        │
│  │  │ 用户态收数据  │  │                        │
│  │  └───────┬───────┘  │                        │
│  └──────────┼──────────┘                        │
│             ▼                                    │
│  ┌─────────────────────┐                        │
│  │  FlowGenerator      │  ← trace_id 注入到     │
│  │  (L7 协议日志)      │    HttpLog/MysqlLog    │
│  └──────────┬──────────┘                        │
│             ▼                                    │
│  ┌─────────────────────┐                        │
│  │  Sender → 上报 Server │                       │
│  └─────────────────────┘                        │
└─────────────────────────────────────────────────┘

Server 端做的事：
  - 接收带 trace_id 的 L7 log
  - 写入 ClickHouse
  - 在查询时按 trace_id 把跨进程的多条 log 串成一条调用链
```

核心机制是 **syscall 关联**：

```
应用 A 线程 T1：
   recvfrom(fd=5) ── 收到上游请求        @ T0
   sendto(fd=8)   ── 发给下游 B          @ T0+5ms
                                           ↑
                    同一线程 + 时间窗口内的两个 syscall
                    内核态 eBPF 可以推断出"这是 A 收到请求后转发给了 B"
                    生成同一个 syscall_trace_id
```

源码佐证：`@agent/src/ebpf_dispatcher.rs:1084` 的注释：

> "设置用于线程追踪会话的 hash 表项最大值，**SK_BPF_DATA 结构的 syscall_trace_id_session 关联这个哈希表**"

这个哈希表就在 eBPF 程序内部维护，用来关联同一线程的 syscall。

#### 在三块架构中的位置

```
块 1（管理面）        块 2（采集面）              块 3（平台发现）
                          │
                          ├─ Dispatcher
                          ├─ EbpfCollector ◄── AutoTracing 在这里
                          ├─ FlowGenerator
                          ├─ Collector
                          └─ Sender
```

AutoTracing 完全属于**块 2 的 EbpfCollector**，是 Agent 进程内的一个能力，不是独立服务。

#### 配置开关（默认启用）

源码：`@server/agent_config/template.yaml:3166`

```yaml
inputs:
  ebpf:
    socket:
      tunning:
        # 默认 false，即默认开启 AutoTracing
        syscall_trace_id_disabled: false

    # 读取应用自带的 trace_id（混合模式）
    l7_log_collect:
      apm_trace_id: [traceparent, sw8]   # OpenTelemetry + SkyWalking
```

模板注释中的关键说明：

> "当 trace_id 注入所有请求时（指应用已自带 OpenTelemetry 等埋点），所有请求的 syscall_trace_id 计算逻辑可以关闭。这将大大减少 eBPF hook 进程的 CPU 消耗。"

#### 三种使用场景

| 场景 | 推荐配置 | 说明 |
|------|---------|------|
| 应用**没有**任何埋点 | `syscall_trace_id_disabled: false`（默认）| 让 Agent 自动追踪 |
| 应用**已接入** OpenTelemetry/SkyWalking | 可设为 `true` 省 CPU | 让应用自己的 trace_id 接管 |
| 想要**混合追踪**（双保险）| 保持默认 false | Agent 同时识别两种 trace_id |

#### 部署要求

| 步骤 | 命令/配置 |
|------|---------|
| 1. 部署 Agent | 正常部署 deepflow-agent 即可 |
| 2. 满足 eBPF 条件 | 内核 ≥ 5.2、BTF 启用、有 CAP_SYS_ADMIN |
| 3. 确认开关 | `inputs.ebpf.disabled: false`（默认）+ `syscall_trace_id_disabled: false`（默认）|

**不需要做的事**：
- ❌ 不需要改应用代码
- ❌ 不需要注入 SDK
- ❌ 不需要部署额外的 collector/agent
- ❌ 不需要 sidecar

部署完直接就能在 DeepFlow 的 UI 上看到无侵入的调用链。

#### 局限性

AutoTracing 不是万能的，它依赖**线程模型 + 时间窗口**进行因果推断，所以不同场景效果不同：

| 场景 | 是否能追踪 | 原因 |
|------|---------|------|
| **同步阻塞调用**（同一线程内串行）| ✅ 完美 | 同线程 + 连续 syscall |
| **跨主机的多次 RPC** | ✅ | 通过 TCP seq + 时间戳关联 |
| **线程池转发**（请求被另一个线程处理）| ⚠️ 可能断 | 线程 ID 不同，时间窗口可能超限 |
| **协程 / async**（Go goroutine、Rust async）| ⚠️ 取决于 runtime | runtime 是否切线程决定成败 |
| **消息队列异步处理**（Kafka 消费）| ❌ | 跨进程时间间隔太大 |
| **批处理 / 延迟任务** | ❌ | 时间窗口无法关联 |

对于断链的场景，DeepFlow 支持读取应用自带的 trace_id（配置项 `apm_trace_id: [traceparent, sw8]`）做**混合关联**。

#### 实际调用链视图示例

部署 DeepFlow 后，查询某个 HTTP 请求，能看到这样的数据：

```
[Trace ID: auto-generated-xyz]
├─ web-frontend (Pod=web-1, PID=100)
│  └─ HTTP GET /api/users  (T0, RTT=120ms)
│     ↓ eBPF AutoTracing 关联
├─ api-service (Pod=api-1, PID=200)
│  └─ HTTP GET /db/query   (T0+5ms, RTT=80ms)
│     ↓
├─ mysql-proxy (Pod=mysql-1, PID=300)
│  └─ MySQL: SELECT * FROM users WHERE id=42  (T0+20ms, 50ms)
│     ↓
└─ mysql-server (PID=400)
   └─ Query 执行成功，返回 1 行
```

每一行的信息来源：

| 字段 | 来源 |
|------|------|
| Pod / PID / 进程名 | **eBPF**（kprobe + cgroup） |
| HTTP 方法、URL、状态码 | **Dispatcher 抓包 + DPI 解析**（或 uprobe 解 HTTPS） |
| MySQL SQL 语句 | **Dispatcher 抓包 DPI**（MySQL 协议解析）|
| RTT、时延 | **eBPF**（syscall enter/exit 时间戳） |
| 跳与跳之间的因果关系 | **eBPF AutoTracing**（线程 + 时间关联） |

#### 一句话总结

> **eBPF 告诉你"谁调用了谁"和"调用是同一个请求触发的"，Dispatcher 抓包告诉你"调用里具体说了什么"——两者结合才是完整的调用链路视图。而 AutoTracing 正是把这两者粘合在一起的那把胶水。**

### 11.8 跨主机调用链：Agent 打标签，Server 拼链路

上一节讲了单机内的 AutoTracing 如何靠线程 ID 和时间窗口关联 syscall。但**分布式系统最关键的问题是跨主机**：Host A 上的 nginx 发出一个 HTTP 请求，到达 Host B 上的 api-service，这两条 log 是在两台机器上由两个独立的 Agent 采集的，它们怎么拼到一起？

#### 核心认知：Agent 互不通信，Server 查询时拼接

**这是最容易误解的一点**：

```
❌ 错误理解：
   Agent A 发现了一个请求 → 通知 Agent B → Agent B 关联上 → 合并成一条 trace

✅ 实际架构：
   Agent A 独立采集 → 打上多个关联键 → 上报 Server
   Agent B 独立采集 → 打上多个关联键 → 上报 Server
                                      ↓
                              Server ClickHouse
                                      ↓
                     用户查询时，Server 用关联键做 JOIN
                     迭代扩散，拼出完整调用链
```

**Agent 之间永远不直接通信**。每台机器的 Agent 只做一件事：**把自己看到的每条 L7 调用打上尽量多的"关联键"**，剩下的拼接工作交给 Server。

#### Agent 在每条 L7 log 上打的关联键

源码：`@agent/src/flow_generator/protocol_logs/parser.rs:121-134`

```rust
req_tcp_seq: 0,                      // ← 跨主机关联核心
resp_tcp_seq: 0,                     // ← 跨主机关联核心
syscall_trace_id_request: 0,         // 单机 eBPF 关联
syscall_trace_id_response: 0,
syscall_trace_id_thread_0: 0,        // 内核线程 ID
syscall_trace_id_thread_1: 0,
syscall_cap_seq_0: 0,                // eBPF 捕获序列号
syscall_cap_seq_1: 0,
x_request_id_0,                      // 应用中间件注入的 request id
x_request_id_1,
trace_id,                            // 应用 OpenTelemetry/SkyWalking 埋点
```

#### 跨主机关联的物理基础：TCP seq 号

**这是最关键的一环**。TCP seq 号的妙处在于——**同一个 TCP 数据包在发送端和接收端看到的 seq 号完全相同**（这是 TCP 协议的强保证）。

```
Host A (web-frontend)                    Host B (api-service)
─────────────────────                    ─────────────────────
进程 X 调用 sendto()                       进程 Y 被 recvfrom() 唤醒
   │                                         │
   ▼                                         ▼
Agent A 抓到/eBPF 捕获                     Agent B 抓到/eBPF 捕获
   │                                         │
   ▼                                         ▼
记录 L7 log:                              记录 L7 log:
  req_tcp_seq = 12345                      req_tcp_seq = 12345  ← 完全相同!
  ip_src = 10.1.1.1                        ip_src = 10.1.1.1
  ip_dst = 10.2.2.2                        ip_dst = 10.2.2.2
  process_0 = nginx                        process_1 = api-server
  pod_0 = web-frontend                     pod_1 = api-service
  syscall_trace_id = 888 (本地)            syscall_trace_id = 999 (本地)
   │                                         │
   │                                         │
   └──────────► Server Ingester ◄────────────┘
                     │
                     ▼
               ClickHouse
               l7_flow_log 表
```

两条完全独立采集的 log，因为 **req_tcp_seq 相同**，Server 查询时就能把它们 JOIN 起来——这就是 DeepFlow 能做到跨主机调用链追踪的底层物理基础。

#### 关联键的"作用半径"

| 关联键 | 作用范围 | 来源 |
|--------|---------|------|
| **`trace_id`** | 全局（跨主机、跨语言、跨云）| 应用自带的 OpenTelemetry/SkyWalking 埋点 |
| **`x_request_id`** | 全局，但依赖中间件注入 | nginx/envoy/istio 等注入的 HTTP header |
| **`req_tcp_seq`/`resp_tcp_seq`** | **跨主机**（核心） | TCP 协议强保证，Agent 从抓包/eBPF 中提取 |
| **`syscall_trace_id`** | 仅限单机 | Agent 内 eBPF 线程关联计算 |
| **`syscall_thread`** | 仅限单机 | 内核 tid |
| **`syscall_cap_seq`** | 仅限单机 | eBPF 事件序列号 |

**设计哲学**：

- **单机内** 靠 eBPF 的 syscall 关联（`syscall_trace_id` + `thread_id`）
- **跨主机** 靠 TCP seq 号（物理定律保证）+ 应用自带 trace_id（如果有）
- **Server 查询时** 用 OR 条件把这些键全部拼起来，实现"任意关联、自动追踪"

#### Server 查询端的 6 个关联维度

源码：`@server/querier/engine/clickhouse/tag/translation.go:1398-1467`

```go
tagResourceMap["trace_id"]         → "trace_id %s %s OR trace_id_2 %s %s"
tagResourceMap["x_request_id"]     → "x_request_id_0 %s %s OR x_request_id_1 %s %s"
tagResourceMap["syscall_thread"]   → "syscall_thread_0 %s %s OR syscall_thread_1 %s %s"
tagResourceMap["syscall_coroutine"]→ "syscall_coroutine_0 %s %s OR syscall_coroutine_1 %s %s"
tagResourceMap["syscall_cap_seq"]  → "syscall_cap_seq_0 %s %s OR syscall_cap_seq_1 %s %s"
tagResourceMap["syscall_trace_id"] → "syscall_trace_id_request %s %s OR syscall_trace_id_response %s %s"
tagResourceMap["tcp_seq"]          → "req_tcp_seq %s %s OR resp_tcp_seq %s %s"
```

每个关联键都会在 ClickHouse 两个方向的字段上 OR 查询（`_0`/`_1`、`_request`/`_response`），因为同一条 log 既可能是"我发的"也可能是"我收的"。

#### 跨主机调用链拼接的 5 步算法

Server 的 distributed_tracing 模块实现了一个**迭代扩散 BFS 算法**（`@server/querier/app/distributed_tracing/service/tracemap/`）：

```
Step 1: 用户在 UI 点击某条 HTTP 请求（入口）
   └─ 查询参数: trace_id = "xxx" 或 (ip, time, tcp_seq)

Step 2: Server (querier) 查 ClickHouse l7_flow_log 表
   └─ 找到这条 log 作为种子 (seed)
   └─ 提取它的所有关联键:
      - trace_id = "abc-123" (如果应用有埋点)
      - x_request_id_0 = "req-456"
      - syscall_trace_id_request = 888
      - syscall_trace_id_thread_0 = 100
      - req_tcp_seq = 12345
      - resp_tcp_seq = 12346

Step 3: 迭代扩散查询 (BFS)
   ├─ SQL 1: WHERE trace_id = 'abc-123'
   │   → 拉到所有带同一 trace_id 的 log (覆盖全主机)
   │
   ├─ SQL 2: WHERE x_request_id_0 = 'req-456' OR x_request_id_1 = 'req-456'
   │   → 拉到 nginx/envoy 等中间件注入的请求 ID 关联 log
   │
   ├─ SQL 3: WHERE req_tcp_seq = 12345 OR resp_tcp_seq = 12345
   │   → 拉到跨主机的"下一跳"(B 机器上的 log)
   │
   ├─ SQL 4: WHERE syscall_trace_id_request = 888 OR syscall_trace_id_response = 888
   │   → 拉到同一主机上 eBPF 关联的 log
   │
   └─ SQL 5: WHERE syscall_cap_seq_0 = N (同机 eBPF 细粒度关联)

Step 4: 把新找到的 log 当作新种子，继续扩散
   └─ 直到没有新的 log 被发现 (收敛)

Step 5: 按时间排序 + 按 TCP 连接组合，生成 TraceMap
   └─ 返回给 UI 渲染成调用链瀑布图
```

这是一个**BFS 图搜索**：每条 log 是一个节点，关联键是边。种子节点作为起点，通过反复 JOIN 扩散，直到 trace 图收敛。

#### 完整跨主机例子

```
用户 → Nginx (Host A) → API (Host B) → MySQL (Host C)

Host A (nginx)                    Host B (api)                    Host C (mysql)
──────────────                    ────────────                    ──────────────
Agent A 采集到:                   Agent B 采集到:                 Agent C 采集到:
  log1: HTTP /login                 log3: HTTP /db/query            log5: MySQL SELECT
    resp_tcp_seq=100                  req_tcp_seq=300                 req_tcp_seq=500
    x_request_id_0=req1               x_request_id_0=req1             pod_1=mysql
    pod_0=nginx                       pod_0=nginx (对端)              
                                      pod_1=api                       
                                                                      
  log2: HTTP /login (out)           log4: HTTP /db/query (out)       log6: MySQL SELECT (out)
    req_tcp_seq=200                    resp_tcp_seq=400                resp_tcp_seq=600
    x_request_id_0=req1

查询 SQL 迭代:
  Round 1: WHERE x_request_id_0/1 = 'req1'
    → 命中: log1, log2, log3, log4   (nginx 和 api 都有这个 id)
  
  Round 2: 用新 log 的 tcp_seq 扩散
    log4 的 resp_tcp_seq=400 → 搜 WHERE req_tcp_seq=400
    → 命中 log5 (mysql 入口)
    
  Round 3: 用 syscall_trace_id 扩散 (同机内关联)
    → 命中 log6 (mysql 出响应)
    
最终 trace:
   log1 → log2 → log3 → log4 → log5 → log6
   (nginx 收) (nginx 发) (api 收) (api 发) (mysql 收) (mysql 发)
```

#### 各层级的分工总结

| 层级 | 职责 | 不做什么 |
|------|------|---------|
| **Agent** | 独立采集，打上尽量多的关联键（TCP seq、syscall ID、x_request_id 等）| ❌ 不与其他 Agent 通信<br>❌ 不做 trace 拼接 |
| **Ingester (Server)** | 接收 log，写入 ClickHouse | ❌ 不做 trace 计算 |
| **Querier (Server)** | 查询时 BFS 扩散，拼接跨主机调用链 | 仅在用户查询时才计算 |
| **ClickHouse** | 存储带关联键的 l7_flow_log，支持高效 WHERE 查询 | — |

#### 这种架构的优势和局限

**优势**：

1. **完全无侵入**：Agent 不依赖应用修改，只靠 TCP seq 和 syscall 关联
2. **Agent 轻量**：不做全局状态维护，单机内就能完成所有工作
3. **水平扩展**：每台主机独立，Server 只在查询时拼接
4. **支持混合模式**：有 trace_id 就用 trace_id，没有就靠 TCP seq + syscall

**局限**：

1. **依赖时间窗口**：TCP seq 在一个 TCP 连接的生命周期内唯一，长时间跨度可能碰撞
2. **查询开销**：拼接发生在查询时，复杂 trace 可能触发多轮 SQL 扩散
3. **异步/消息队列断链**：Kafka 等场景跨进程时间间隔大，tcp_seq 也无法关联
4. **NAT/代理场景**：TCP seq 在 NAT 转换时会被重写，需要依赖 `x_request_id` 补充

这也解释了为什么 DeepFlow 文档里反复强调**推荐让应用同时注入 trace_id**——作为 TCP seq 关联的"兜底方案"，让跨异步边界、跨 NAT 的场景也能追踪。

### 11.9 OpenTelemetry / SkyWalking trace_id 的利用

上一节解释了跨主机调用链的关联机制，其中最重要的"全局关联键"就是应用自带的 `trace_id`。DeepFlow 对 OpenTelemetry 和 SkyWalking 等 APM 系统的 trace_id 采集有**两条完全不同的路径**——一条是"被动偷"，一条是"主动收"，两者可以同时使用互为补充。

#### 路径 1：被动解析（从 HTTP/RPC header 里提取）

这是 DeepFlow 最有特色的用法——**应用不需要做任何配置**，只要应用本身已经注入了 trace_id 到 HTTP header 或 RPC 消息里（比如集成了 OpenTelemetry SDK、SkyWalking agent 等），DeepFlow Agent 就能从网络流量中自动把它提取出来。

##### 支持的 trace 格式（12 种）

源码 `@agent/src/config/handler.rs:1405-1419` 定义了 `TraceType` 枚举：

```rust
pub enum TraceType {
    Disabled,
    XB3,                   // Zipkin B3 旧格式:  x-b3-traceid
    XB3Span,               // Zipkin B3 旧格式:  x-b3-spanid
    Uber,                  // Jaeger:            uber-trace-id
    Sw3,                   // SkyWalking v3
    Sw6,                   // SkyWalking v6
    Sw8,                   // SkyWalking v8     (当前主流)
    TraceParent,           // W3C/OpenTelemetry: traceparent
    NewRpcTraceContext,    // SOFA RPC
    XTingyun(String),      // 听云 APM
    CloudWise,             // 云智慧 APM
    Customize(String),     // 自定义 header 名
    B3,                    // Zipkin B3 新格式:  b3
}
```

**默认配置**（`@server/agent_config/template.yaml:6594`）：

```yaml
processors:
  request_log:
    tag_extraction:
      tracing_tag:
        apm_trace_id: [traceparent, sw8]   # 默认支持 OpenTelemetry + SkyWalking
        apm_span_id:  [traceparent, sw8]
```

Agent 按配置顺序在每个 L7 请求里扫描，命中哪个 header 就用哪个。可以同时配置多个（`[traceparent, sw8, b3, uber]`）实现混合兼容。

##### 每种格式的专用 decoder

DeepFlow 不是简单地把 header 值当 trace_id，而是为每种格式实现了专门的解析逻辑。源码 `@agent/src/config/handler.rs:1509-1608`：

**OpenTelemetry W3C traceparent**

```rust
// traceparent: 00-TRACEID-SPANID-01
fn decode_traceparent(value: &str, id_type: u8) -> Option<&str> {
    let mut segs = value.split("-");
    if id_type == Self::TRACE_ID {
        segs.nth(1)        // 第 2 段是 trace_id
    } else {
        segs.nth(2)        // 第 3 段是 span_id
    }
}
```

**SkyWalking sw8**（需要 Base64 解码）

```rust
// sw8: 1-TRACEID-SEGMENTID-3-PARENT_SERVICE-PARENT_INSTANCE-PARENT_ENDPOINT-IPPORT
// 注意：trace_id 和 segment_id 都是 Base64 编码的
fn decode_skywalking_id(value: &str, id_type: u8) -> Option<Cow<'_, str>> {
    let mut segs = value.split("-");
    if id_type == Self::TRACE_ID {
        let id = segs.nth(1)?;
        Some(BASE64_STANDARD.decode(id).ok()...)    // 需要 Base64 解码
    } else {
        // span_id = "SEGMENTID-SPANID" 拼接
    }
}
```

**SkyWalking sw3**（旧版，格式不同）

```rust
// sw3: SEGMENTID|SPANID|100|100|#IPPORT|#PARENT_ENDPOINT|#ENDPOINT|TRACEID|SAMPLING
// 用 | 分隔，trace_id 在第 8 段
```

**Zipkin B3 新格式**

```rust
// b3: TRACEID-SPANID-1
// 例: 4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1
fn decode_b3(value: &str, id_type: u8) -> Option<&str> {
    let mut segs = value.split("-");
    if id_type == Self::TRACE_ID { segs.nth(0) } else { segs.nth(1) }
}
```

**Jaeger uber-trace-id**

```rust
// uber-trace-id: TRACEID:SPANID:PARENTSPANID:FLAGS
// 用 : 分隔
fn decode_uber_id(value: &str, id_type: u8) -> Option<&str> {
    let mut segs = value.split(":");
    if id_type == Self::TRACE_ID { segs.nth(0) } else { segs.nth(2) }
}
```

##### 支持的协议覆盖范围

不只是 HTTP——几乎所有 DeepFlow 支持的 L7 协议解析器都能提取 trace_id：

| 协议 | 代码位置 | trace_id 来源 |
|------|---------|--------------|
| **HTTP/HTTPS** | `@agent/src/flow_generator/protocol_logs/http.rs` | HTTP header |
| **Dubbo** | `@agent/src/flow_generator/protocol_logs/rpc/dubbo.rs:1288-1294` | Dubbo attachment（支持 EagleEye、x-g-rid、sw8 等）|
| **SOFA RPC** | `rpc/sofa_rpc.rs:645-648` | SOFA 协议扩展字段 |
| **bRPC (Baidu)** | `rpc/brpc.rs:411-412` | bRPC meta + sw8/traceparent |
| **Tars** | `rpc/tars.rs:634-635` | sw8 + traceparent |
| **SOME/IP** | `rpc/some_ip.rs:345-346` | 汽车协议 + sw8/traceparent |
| **MySQL** | `sql/mysql.rs:189` (`copy_apm_trace_id`) | **SQL 注释**（sqlcommenter 规范）|
| **PostgreSQL** | `sql/postgresql.rs` | **SQL 注释** |
| **Kafka** | `mq/kafka.rs` | Kafka message header |

**亮点：MySQL/PostgreSQL 的 SQL 注释追踪**

数据库协议本身没有 header 机制，但很多 ORM（如 sqlcommenter、Hibernate）会把 trace_id 作为 SQL 注释发出：

```sql
/* trace_id='abc123',span_id='def456' */ SELECT * FROM users WHERE id=42
```

DeepFlow 的 MySQL DPI 解析器会**从注释里提取 trace_id**，这解决了"数据库调用无法在 header 里带 trace_id"的难题。

##### 解析后的数据流

```
网络包（HTTP 请求）
  ↓
Dispatcher / eBPF 抓到
  ↓
L7 协议解析 (http.rs / dubbo.rs / mysql.rs ...)
  ↓
检测到 header "traceparent: 00-abc123-def456-01"
  ↓
TraceType::TraceParent::decode_traceparent()
  → trace_id = "abc123"
  → span_id  = "def456"
  ↓
写入 L7 log 的 trace_id / span_id 字段
  ↓
Sender → Server → ClickHouse (l7_flow_log.trace_id 列)
  ↓
查询时 Querier 用 trace_id 做 BFS 扩散（见 11.8 节）
```

#### 路径 2：主动接收（IntegrationCollector OTLP Receiver）

对于已经部署了 OpenTelemetry SDK 的应用，DeepFlow Agent **内置了一个 OTLP Receiver**，让应用可以直接把 trace 数据 push 给 Agent——**Agent 扮演了 OTel Collector 的角色**。

##### 监听的端点

源码 `@agent/src/integration_collector.rs:643`：

```rust
// Agent 内置的 HTTP 服务器监听的端点
(&Method::POST, "/api/v1/otel/trace") => ...      // OTLP trace
(&Method::POST, "/api/v1/otel/metrics") => ...    // OTLP metrics
(&Method::POST, "/api/v1/prometheus") => ...      // Prometheus remote write
(&Method::POST, "/api/v1/profile/...") => ...     // pprof / Pyroscope
(&Method::POST, "/api/v1/telegraf") => ...        // Telegraf
```

Agent 默认监听 `0.0.0.0:38086`，应用只需要配置 OTLP Exporter 指向这里：

```yaml
# 应用端的 OpenTelemetry 配置
exporters:
  otlp:
    endpoint: http://<agent-host>:38086/api/v1/otel/trace
    protocol: http/protobuf
```

##### 接收后的处理流程

```
┌──────────────────┐
│   应用进程        │
│  OpenTelemetry   │
│     SDK          │
└────────┬─────────┘
         │ HTTP POST /api/v1/otel/trace
         │ (OTLP protobuf)
         ▼
┌──────────────────────────────────┐
│   deepflow-agent                  │
│   ┌────────────────────────────┐  │
│   │ IntegrationCollector       │  │
│   │  (HTTP server 38086)       │  │
│   │                            │  │
│   │  decode_otel_trace_data()  │  │
│   │    1. 解析 OTLP protobuf    │  │
│   │    2. 提取 service.name    │  │
│   │    3. 提取 trace_id/span_id│  │
│   │    4. 转成 MetaAppProto    │  │
│   └────────────┬───────────────┘  │
│                ▼                   │
│   ┌────────────────────────────┐  │
│   │  lookup_from_otel()        │  │
│   │  ↑ 补充网络层元数据         │  │
│   │    (Pod ID, EPC, namespace)│  │
│   └────────────┬───────────────┘  │
│                ▼                   │
│   ┌────────────────────────────┐  │
│   │  Sender（和其他 log 同管道）│  │
│   └────────────┬───────────────┘  │
└────────────────┼──────────────────┘
                 ▼
        Server → ClickHouse
```

关键源码：

```rust
// @agent/src/integration_collector.rs:260
fn decode_otel_trace_data(...) {
    // 解析 OTLP ResourceSpans → ScopeSpans → Span
    // 提取 service_name, service_instance 等
    // 为每个 Span 构造一条 DeepFlow L7 log
}

// @agent/src/integration_collector.rs:552
flow.lookup_from_otel(&mut lookup_key, local_epc_id as i32);
//  ↑ 关键：把 OTLP Span 的 IP 信息查本地拓扑表
//    补上 Pod ID、L3 EPC、K8s namespace 等元数据
//    让应用视角的 trace 和网络视角的 flow 在同一张表里可关联
```

**核心设计**：OTLP 数据经过转换后，走的是和 eBPF/抓包数据**完全相同的上报链路**。最终都存在同一张 ClickHouse `l7_flow_log` 表里，查询时**不区分数据来源**，统一按 trace_id 关联。

#### 两条路径的对比

| 维度 | 路径 1：被动解析 | 路径 2：主动接收（OTLP）|
|------|-----------------|----------------------|
| **应用侵入度** | 零侵入（应用本身已有 SDK 即可）| 需要配置 OTLP Exporter 指向 Agent |
| **数据丰富度** | 仅 trace_id / span_id | 完整 OTLP Span（所有 attributes、events、status）|
| **支持格式** | 12 种（traceparent/sw8/b3/uber/...）| OTLP 标准协议 |
| **HTTPS 加密流量** | ❌ 看不到 header（除非 uprobe 解密）| ✅ 应用端已是明文 |
| **消息队列异步调用** | ❌ tcp_seq 无法关联 | ✅ OTLP 自带因果链 |
| **性能开销** | 几乎零（抓包/eBPF 本来就在做）| 每条 Span 一次 HTTP POST |
| **延迟** | 随网络包实时到达 | 取决于 SDK batch export 周期 |
| **补网络元数据** | ✅ 本来就来自网络 | ✅ 通过 `lookup_from_otel` 补 |

#### 推荐的混合部署模式

DeepFlow 官方推荐**同时启用两条路径，互为兜底**：

```
业务场景：电商系统

  前端 ──HTTP──► API Gateway ──HTTP──► 订单服务 ──MySQL──► DB
                                          │
                                          └──Kafka──► 物流服务 (异步)

推荐配置：

  1. API Gateway、订单服务、物流服务 集成 OpenTelemetry SDK 注入 traceparent
     │
     ├─ Agent 在每台主机抓包/eBPF (零配置)
     │   → 路径 1：从 HTTP header 提取 traceparent，得到 trace_id
     │
     └─ 可选：物流服务（Kafka 消费者）额外配置 OTLP Exporter → Agent:38086
         → 路径 2：因为 Kafka 异步 tcp_seq 关联不了，必须靠 OTLP 主动上报

  2. MySQL 调用：ORM 使用 sqlcommenter，让 Agent 从 SQL 注释里提取 trace_id

  3. 查询时 Server 用 trace_id 做 JOIN，整条链（含异步段）完整拼出
```

#### 混合追踪的实际例子

```
调用链: 用户 → Nginx → Java(SkyWalking) → Go(OTel SDK) → MySQL

Nginx:
  HTTP header out: x-request-id: req-xyz
  Agent 抓包 → x_request_id_0 = "req-xyz"

Java 服务:
  HTTP header in:  sw8: 1-VHJhY2VJZA==-...  x-request-id: req-xyz
  HTTP header out: sw8: 1-VHJhY2VJZA==-...  traceparent: 00-4bf92f35...
  Agent 同时提取 sw8 和 traceparent（配置了两种）

Go 服务:
  用 OTel SDK → POST /api/v1/otel/trace 到 Agent:38086
  Agent 收到 OTLP Span，trace_id = "4bf92f35..."

  同时 Go 服务发出 MySQL 查询:
    SQL: SELECT * FROM users /* traceparent='00-4bf92f35-...' */
  Agent 的 MySQL 解析器从 SQL 注释里提取 trace_id

MySQL:
  看不到 trace_id（MySQL 原生协议不传）
  但通过 tcp_seq 跟 Go 服务的 MySQL 请求关联（11.8 节机制）

Server 查询时:
  起点: Nginx 的 log (x_request_id_0 = "req-xyz")
  Round 1: 按 x_request_id 扩散 → 拉到 Java 的入站 log
  Round 2: 按 sw8 解析的 trace_id 扩散 → 仍是 Java
  Round 3: 按 traceparent 解析的 trace_id 扩散 → 拉到 Go 服务（OTLP 上报的）
  Round 4: 按 Go 服务的 tcp_seq 扩散 → 拉到 MySQL

最终 trace 穿越了 4 种不同的追踪格式，
打通了 OTLP 主动上报路径和 header 被动解析路径！
```

#### 统一存储：trace_id 落到哪里

无论走哪条路径，解析出来的 trace_id 最终都会落到 ClickHouse 的 `l7_flow_log` 表里：

```
l7_flow_log 表的 trace 相关字段:
├─ trace_id              ← 主要字段（从 traceparent/sw8/OTLP 来）
├─ trace_id_2            ← 同一条 log 可带第二个 trace_id（混合场景）
├─ span_id
├─ parent_span_id
├─ x_request_id_0
├─ x_request_id_1
├─ req_tcp_seq / resp_tcp_seq      ← Agent 自动生成的物理关联键
├─ syscall_trace_id_request         ← eBPF 单机关联
├─ syscall_trace_id_response
└─ ... (其他关联键)
```

查询时 Server 的 BFS 扩散会**同时利用所有这些键做 OR JOIN**，实现跨路径、跨格式、跨主机的统一追踪。

#### 一句话总结

> **DeepFlow 对 OpenTelemetry 和 SkyWalking 的支持是"双通道"：一个通道是从网络流量里自动解析 12 种 trace_id 格式（零侵入首选），另一个通道是内置 OTLP Receiver 直接接收 SDK 上报（有侵入但数据丰富）。两条通道最终都把 trace_id 填到统一的 l7_flow_log 表里，由 Server 查询时做跨通道、跨主机的关联。**

### 11.10 L7 协议解析支持全景

回顾 Stage 2（协议解析），本节完整梳理 DeepFlow Agent 当前支持的 L7 协议清单、DPI 识别机制、以及企业版协议的特殊处理方式。

#### 协议总览

根据 `@agent/crates/public/src/l7_protocol.rs:47-96` 中 `L7Protocol` 枚举和 `@agent/src/common/l7_protocol_log.rs:157-219` 中 `impl_protocol_parser!` 宏的展开，DeepFlow 支持的协议可分为 8 大族：

| 族 | 社区版 | 企业版额外 | 代码目录 |
|----|-------|-----------|---------|
| **HTTP 族** | HTTP/1、HTTP/2、gRPC、Triple | — | `protocol_logs/http.rs` |
| **RPC 族** | Dubbo、SOFA-RPC、bRPC、Tars、FastCGI | SOME/IP、ISO-8583 | `protocol_logs/rpc/` |
| **SQL 数据库** | MySQL、PostgreSQL | Oracle | `protocol_logs/sql/` |
| **NoSQL** | Redis、MongoDB、Memcached | — | `protocol_logs/sql/` |
| **消息队列** | Kafka、MQTT、AMQP、OpenWire、NATS、Pulsar、ZMTP、RocketMQ | WebSphereMQ | `protocol_logs/mq/` |
| **基础设施** | DNS、Ping | TLS | `protocol_logs/` |
| **可扩展** | Custom（WASM/SO/规则）| — | `protocol_logs/plugin/` |

**合计**：社区版内置 23 种，企业版额外 5 种，共 28 种。

#### 详细清单

##### HTTP 族（3 种协议，1 个解析器）

DeepFlow 用**同一个 HttpLog 结构处理 HTTP 所有变体**，通过 flag 区分：

| 协议 | 枚举值 | 构造方式 |
|------|-------|---------|
| HTTP/1.x | `Http1=20` | `HttpLog::new_v1()` |
| HTTP/2 | `Http2=21` | `HttpLog::new_v2(false)` |
| gRPC | （复用 Http2）| `HttpLog::new_v2(true)` |
| Triple | `Triple=49` | `HttpLog::new_triple()`（Dubbo 3 基于 HTTP/2）|

##### RPC 族（社区 5 + 企业 2）

| 协议 | 枚举值 | 代码位置 | 备注 |
|------|-------|---------|------|
| Dubbo | `Dubbo=40` | `rpc/dubbo.rs` | 支持 sw8、EagleEye、x-g-rid 等多种 trace 格式 |
| SOFA-RPC | `SofaRPC=43` | `rpc/sofa_rpc.rs` | 蚂蚁金服 |
| FastCGI | `FastCGI=44` | `fastcgi.rs` | — |
| bRPC | `Brpc=45` | `rpc/brpc.rs` | 百度 |
| Tars | `Tars=46` | `rpc/tars.rs` | 腾讯 |
| **SOME/IP** ⭐ | `SomeIp=47` | `rpc/some_ip.rs` | 汽车电子通信协议（企业版）|
| **ISO-8583** ⭐ | `Iso8583=48` | `rpc/iso8583.rs` | 金融支付协议（企业版）|

##### SQL 数据库（社区 2 + 企业 1）

| 协议 | 枚举值 | 代码位置 | 亮点 |
|------|-------|---------|------|
| MySQL | `MySQL=60` | `sql/mysql.rs` | 支持 SQL 脱敏（`sql_obfuscate.rs`）+ 从 SQL 注释提取 trace_id |
| PostgreSQL | `PostgreSQL=61` | `sql/postgresql.rs` | 同上 |
| **Oracle** ⭐ | `Oracle=62` | `sql/oracle.rs` | 企业版（Oracle TNS 协议逆向）|

##### NoSQL（3 种）

| 协议 | 枚举值 | 代码位置 |
|------|-------|---------|
| Redis | `Redis=80` | `sql/redis.rs` |
| MongoDB | `MongoDB=81` | `sql/mongo.rs` |
| Memcached | `Memcached=82` | `sql/memcached.rs` |

##### 消息队列（社区 8 + 企业 1）

| 协议 | 枚举值 | 代码位置 | 备注 |
|------|-------|---------|------|
| Kafka | `Kafka=100` | `mq/kafka.rs` | 有 `kafka-apis.py` 脚本生成 API 定义 |
| MQTT | `MQTT=101` | `mq/mqtt.rs` | IoT 主流协议 |
| AMQP | `AMQP=102` | `mq/amqp.rs` | RabbitMQ 核心协议 |
| OpenWire | `OpenWire=103` | `mq/openwire.rs` | ActiveMQ |
| NATS | `NATS=104` | `mq/nats.rs` | 云原生 MQ |
| Pulsar | `Pulsar=105` | `mq/pulsar.rs` | 有 `PulsarApi.proto` |
| ZMTP | `ZMTP=106` | `mq/zmtp.rs` | ZeroMQ |
| RocketMQ | `RocketMQ=107` | `mq/rocketmq.rs` | 阿里开源 |
| **WebSphereMQ** ⭐ | `WebSphereMq=108` | `mq/web_sphere_mq.rs` | IBM 企业消息队列（企业版）|

##### 基础设施协议（社区 2 + 企业 1）

| 协议 | 枚举值 | 代码位置 | 用途 |
|------|-------|---------|------|
| DNS | `DNS=120` | `dns.rs` | 域名解析 |
| Ping | `Ping=122` | `ping.rs` | ICMP 响应时延 |
| **TLS** ⭐ | `TLS=121` | `tls.rs` | TLS 握手、证书、SNI、ALPN（企业版）|

##### 可扩展机制（Custom）

源码：`@agent/src/flow_generator/protocol_logs/plugin/`

| 方式 | 代码 | 说明 |
|------|------|------|
| **WASM 插件** | `plugin/wasm.rs` | 用 Rust/Go/C 编译成 WASM 模块，动态加载 |
| **Shared Object** | `plugin/shared_obj.rs` | 直接加载原生动态库（`.so`）|
| **CustomPolicy** | `plugin/custom_protocol_policy.rs` | 基于规则的字段提取（无需写代码）|

这套机制让用户可以**在不修改 Agent 源码的前提下**添加私有协议解析。

#### 协议识别机制（DPI）

Agent 通过 `L7ProtocolParserInterface` trait 实现 DPI，源码 `@agent/src/common/l7_protocol_log.rs:259-261`：

```rust
pub trait L7ProtocolParserInterface {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) 
        -> Option<LogMessageType>;
    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) 
        -> Result<L7ParseResult>;
}
```

**识别流程**：

```
新流的第一个数据包到达
   ↓
FlowGenerator 调用 get_all_protocol() 拿到所有启用的解析器
   ↓
逐个调用 check_payload()，每个解析器用自己的特征匹配
   ├─ HTTP:    看 "GET "、"POST "、"HTTP/"
   ├─ MySQL:   看握手协议的 protocol version 字节
   ├─ Redis:   看 "*N\r\n$M\r\n"
   ├─ Kafka:   看 ApiKey + ApiVersion + CorrelationId 结构
   ├─ Dubbo:   看 magic bytes 0xDABB
   ├─ DNS:     看 question count 和 flags
   └─ ...
   ↓
第一个匹配成功的协议"绑定"到这条流
   └─ 后续包直接走 parse_payload()，不再重试识别
   ↓
所有协议都不匹配 → L7Protocol::Unknown
   └─ 仍记录 L4 统计，但没有 L7 log
```

**多路复用支持**：某些协议在同一个 TCP 连接上可以有并发会话，源码 `@agent/crates/public/src/l7_protocol.rs:99-115`：

```rust
impl L7Protocol {
    pub fn has_session_id(&self) -> bool {
        match self {
            DNS | FastCGI | Http2 | TLS | Kafka | Dubbo | SofaRPC 
            | SomeIp | Ping | Triple | Custom => true,
            _ => false,
        }
    }
}
```

这些协议不能按"一对一请求响应"处理，需要特殊的 session 跟踪和 stream ID 关联。

#### 企业版协议：Stub Crate 模式

**企业版协议的真实解析逻辑并没有开源**。DeepFlow 使用了一种巧妙的 **"Stub Crate 替换"模式** 在单一代码库里同时支持开源版和企业版。

##### 三层结构

```
┌──────────────────────────────────────────────────────────┐
│  开源仓库                                                  │
│                                                           │
│  Layer 1: 适配层（胶水代码）                               │
│  ─────────────────────────────                           │
│  @agent/src/flow_generator/protocol_logs/rpc/iso8583.rs  │
│  @agent/src/flow_generator/protocol_logs/sql/oracle.rs   │
│  @agent/src/flow_generator/protocol_logs/tls.rs          │
│  @agent/src/flow_generator/protocol_logs/mq/web_sphere_mq.rs │
│  @agent/src/flow_generator/protocol_logs/rpc/some_ip.rs  │
│                                                           │
│   └─ 实现 L7ProtocolParserInterface trait                 │
│   └─ 把 DeepFlow 的数据结构转换成 enterprise_utils 的入参 │
│   └─ 调用 enterprise_utils::xxx::Parser::parse_payload()  │
│                           │                               │
│                           ▼                               │
│  Layer 2: 存根 crate（类型签名 + unimplemented!）        │
│  ──────────────────────────────────────────             │
│  @agent/crates/enterprise-utils/src/lib.rs                │
│                                                           │
│  impl OracleParser {                                      │
│      pub fn check_payload(...) -> ... {                   │
│          unimplemented!()         ← 空实现                │
│      }                                                    │
│  }                                                        │
└─────────────────────────┬────────────────────────────────┘
                          │
                          │ 企业版发布时替换整个 crate
                          ▼
┌─────────────────────────────────────────────────────────┐
│  闭源仓库（云杉网络内部）                                  │
│                                                          │
│  Layer 3: 真实实现                                        │
│  ─────────────────                                      │
│  enterprise-utils (真实版本)                              │
│                                                          │
│  impl OracleParser {                                     │
│      pub fn check_payload(data: &[u8], ...) -> ... {    │
│          // 几百上千行真实的 Oracle TNS 协议解析         │
│          // 这是云杉网络的商业 IP，不开源                 │
│      }                                                   │
│  }                                                       │
└─────────────────────────────────────────────────────────┘
```

##### Cargo.toml 配置

`@agent/Cargo.toml`：

```toml
# Line 58: 可选依赖
enterprise-utils = { path = "crates/enterprise-utils", optional = true }

# Line 183: enterprise feature
enterprise = ["dep:enterprise-utils", "dep:pcap-parser"]
```

##### 三种构建情形

| 构建配置 | 命令 | 结果 |
|---------|------|------|
| **默认社区版** | `cargo build` | `enterprise` feature 关闭，5 个企业协议文件都不编译 |
| **开源 + enterprise feature** | `cargo build --features enterprise` | 代码编译通过，但运行到企业协议解析时 **panic**（`unimplemented!()`）|
| **真正的企业版** | 替换 `enterprise-utils` crate 后编译 | 完整功能 |

##### 这种设计的巧妙之处

| 好处 | 说明 |
|------|------|
| **单一代码库** | 开源版和企业版共用主工程，不用维护两个 fork |
| **接口约束** | Stub 里的类型签名强制企业版遵循固定接口 |
| **开源透明** | 用户能看到"企业版有哪些协议、接口长什么样"，只是看不到实现 |
| **编译保证** | 开源版本身 `cargo check` 能通过，CI 可运行 |
| **IP 隔离** | Oracle TNS 逆向、ISO-8583 字段字典等商业知识不泄露 |

##### 从 Stub 能推测的企业版能力

虽然实现闭源，但 stub 的类型签名透露了企业版做了什么：

**Oracle**（`@agent/crates/enterprise-utils/src/lib.rs:284-289`）

```rust
pub struct OracleParseConfig {
    pub is_be: bool,                  // 大小端识别
    pub int_compress: bool,            // 整型压缩支持
    pub resp_0x04_extra_byte: bool,    // 0x04 响应的特殊字节处理
}
```

暗示企业版完成了 Oracle **TNS 协议的完整逆向**（Oracle 没有公开协议规范）。

**ISO-8583**（`lib.rs:365-377`）

```rust
pub struct Iso8583ParseConfig {
    pub extract_fields: Bitmap,        // 要提取哪些字段
    pub translation_enabled: bool,     // 业务代码翻译
    pub pan_obfuscate: bool,           // 信用卡号脱敏
}

pub struct FieldValue {
    pub id: u32,
    pub description: String,            // 字段业务描述
    pub value: String,
    pub translated: Option<String>,     // 翻译后的值
}
```

暗示企业版内置了 **ISO-8583 完整的 128 个位图字段字典**（字段名、业务含义、枚举值翻译），这是金融领域的 know-how。

**WebSphereMQ**（`lib.rs:334-338`）

```rust
pub struct WebSphereMqParser {
    pub base: L7LogBase,
    pub orig_send_time: String,
    pub skip_frame: bool,
}
```

IBM WebSphere MQ 是私有协议，企业版的逆向实现属于商业 IP。

##### 其他藏在 stub 里的企业功能

除了协议解析，`enterprise_utils` 里还隐藏了几个企业版能力：

```rust
// 1. 自定义字段策略（复杂的 DPI 规则引擎）
pub mod custom_policy { ... }          // 全部 unimplemented!()

// 2. 内核版本断路器
pub mod kernel_version {
    pub fn kernel_version_check() -> ActionFlags { unimplemented!() }
}

// 3. 远程执行额外命令
pub mod rpc::remote_exec {
    pub fn extra_commands() -> Vec<...> { unimplemented!() }
}
```

#### 一些设计洞察

| 观察 | 说明 |
|------|------|
| **HTTP 是"超级协议"** | HTTP/1、HTTP/2、gRPC、Triple 共用同一个 `HttpLog`，通过 flag 区分 |
| **SQL 协议集中处理** | MySQL/PostgreSQL/Oracle/Redis/MongoDB/Memcached 都在 `sql/` 目录，共享 SQL 脱敏和关键词检查 |
| **RPC 族平行实现** | 每种 RPC 协议有独立解析器，因协议细节差异大（magic bytes、编码方式等）|
| **消息队列覆盖最广** | 9 种 MQ 协议，从传统 AMQP/WebSphereMQ 到云原生 NATS/Pulsar 全覆盖 |
| **TLS 是企业版** | 开源版看 HTTPS 只能拿 L4 统计，想看握手元数据要么用企业版，要么靠 eBPF uprobe 解密 |
| **可扩展机制完整** | WASM + SO + CustomPolicy 三种方式覆盖"写代码"到"写配置"的全谱系 |

#### 一句话总结

> **DeepFlow 的 L7 协议解析覆盖 28 种主流协议，社区版 23 种（HTTP/SQL/NoSQL/MQ/RPC 主力），企业版额外 5 种（Oracle、TLS、SOME/IP、ISO-8583、WebSphereMQ）。企业版协议的胶水代码开源但解析逻辑闭源——通过 `enterprise-utils` stub crate 模式在单一代码库里同时支持两种构建。想要自定义协议？用 WASM/SO/CustomPolicy 三种插件机制，无需修改 Agent 源码。**

### 11.11 L7 协议扩展：4 种方式与插件资源约束

上一节介绍了 DeepFlow 原生支持的 28 种协议。当用户需要扩展一个**新协议**（比如公司内部的私有 RPC）时，有 **4 条路径**可选，每条路径的侵入性、性能、灵活度和运维特性都不同。本节给出完整对比，并深入讨论插件的资源约束问题。

#### 4 种扩展方式总览

```
┌───────────────────────────────────────────────────────────────┐
│                 扩展 L7 协议的 4 条路径                         │
├────────────────┬──────────┬──────────┬─────────────────────┤
│ 方式           │ 侵入性   │ 性能     │ 适用场景             │
├────────────────┼──────────┼──────────┼─────────────────────┤
│ 1. 源码修改     │ 🔴 最高  │ 🟢 最高  │ 贡献上游/极致性能     │
│ 2. WASM 插件    │ 🟡 中    │ 🟡 -30%  │ 私有协议/跨平台       │
│ 3. SO 插件      │ 🟡 中    │ 🟢 接近  │ 极致性能+C 代码复用   │
│ 4. CustomPolicy │ 🟢 最低  │ 🟡 较高  │ 已有协议加业务标签   │
└────────────────┴──────────┴──────────┴─────────────────────┘
```

#### 方式 1：源码修改（原生协议）

侵入最强但性能最高的方式，所有内置的 28 种协议都采用此方式。

**需要修改的 5 个地方**：

```
1. 新建解析器文件
   @agent/src/flow_generator/protocol_logs/rpc/myproto.rs
   - 实现 L7ProtocolParserInterface trait
   - 实现 check_payload() 和 parse_payload()

2. 添加枚举值
   @agent/crates/public/src/l7_protocol.rs
   pub enum L7Protocol {
       ...
       MyProto = 109,
   }

3. 注册到宏
   @agent/src/common/l7_protocol_log.rs
   impl_protocol_parser! {
       pub enum L7ProtocolParser {
           ...
           MyProto(MyProtoLog),
       }
   }

4. 添加字符串转换
   impl From<String> for L7Protocol {
       "myproto" => Self::MyProto,
   }

5. 重新编译
   cargo build --release
```

| 优点 | 缺点 |
|------|------|
| ✅ 性能最高（零插件框架开销）| ❌ 需要维护私有分支 |
| ✅ 完整访问所有 Agent 内部结构 | ❌ 跟上游同步困难 |
| ✅ 可以做任何优化（零拷贝等）| ❌ 要懂 Rust |
| ✅ 适合开源贡献 | ❌ 用户需要重编译二进制 |

#### 方式 2：WASM 插件

源码：`@agent/src/flow_generator/protocol_logs/plugin/wasm.rs`

**工作原理**：Agent 内嵌 wasmtime 运行时，加载用户编写的 `.wasm` 文件作为虚拟协议解析器。

```rust
// @agent/src/flow_generator/protocol_logs/plugin/wasm.rs:35
impl L7ProtocolParserInterface for WasmLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> Option<LogMessageType> {
        let mut vm_ref = param.wasm_vm.borrow_mut();
        let vm = vm_ref.as_mut()?;
        // ↓ 把 payload 转发给 WASM 虚拟机执行
        let (proto_num, proto_str, message_type) = vm.on_check_payload(&payload, &param)?;
        Some(message_type)
    }

    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        let mut vm_ref = param.wasm_vm.borrow_mut();
        let vm = vm_ref.as_mut().ok_or(Error::WasmParseFail)?;
        // ↓ WASM 返回 Vec<CustomInfo>
        let infos = vm.on_parse_payload(payload, param, self.proto_num.unwrap())?;
        // 转换成 DeepFlow 内部格式
        ...
    }
}
```

**用户只需**：用 Rust/Go/C/AssemblyScript 编写插件代码，编译成 `.wasm` 文件，由 Server 下发配置加载。

##### WASM 跨平台原理

WASM 的跨平台能力来自**中间字节码**设计——和 Java 字节码、.NET IL 是同一类东西。

```
传统编译（不跨平台）                   WASM 编译（跨平台）
───────────────────                   ──────────────────

my_proto.rs                           my_proto.rs
    │                                     │
    │ cargo build                         │ cargo build --target wasm32-unknown-unknown
    │ --target x86_64-linux               │                    ↑
    ▼                                     │              关键在这里
┌─────────────┐                           ▼
│ x86 ELF     │  ← 只能 x86 Linux      ┌─────────────┐
│ mov rax,…   │                        │ .wasm 字节码 │ ← 任何平台
│ call …      │                        │ local.get 0 │
└─────────────┘                        │ i32.load    │
                                       └─────┬───────┘
                                             │ 运行时加载
                                             ▼
                                    ┌────────────────────┐
                                    │ wasmtime Runtime   │
                                    │ (JIT 编译成当前     │
                                    │  CPU 的原生指令)    │
                                    │  ┌────┬────┬────┐  │
                                    │  │x86 │arm │... │  │
                                    │  └────┴────┴────┘  │
                                    └────────────────────┘
```

**关键认知**：源码编译时指向的不是任何真实 CPU，而是 WASM 虚拟指令集。wasmtime 运行时在加载时 JIT 翻译成目标 CPU 的原生指令——**编译一次，到处运行**。

##### WASM vs 原生性能差距的来源

WASM 比原生代码慢 2-3 倍，但**主要原因不是 JIT 翻译**（那只在启动时发生一次）。真正的损耗来自运行时开销：

| 损耗来源 | 占比 | 说明 |
|---------|------|------|
| **Host↔Guest 数据拷贝** | ~40% | 每个数据包都要把 payload 拷贝到 WASM 线性内存 |
| **内存边界检查** | ~25% | 每次内存访问强制检查越界（沙箱保证）|
| **JIT 优化不如 LLVM** | ~20% | Cranelift 追求"快速编译"，不如 LLVM 激进 |
| **线性内存间接寻址** | ~10% | 指针实际是偏移量，要加 memory base |
| **栈式虚拟机语义** | ~5% | 不如寄存器式高效 |

对于 DeepFlow 协议解析这种"频繁读字节 + 字段提取"的场景，数据拷贝是最大瓶颈。

##### WASM 性能优化建议

| 建议 | 理由 |
|------|------|
| 只做必要的字节读取 | 减少边界检查次数 |
| 尽量在一次调用里完成所有解析 | 减少 host↔guest 切换 |
| 用 i32 偏移传递"字段在 payload 里的位置"而非分配字符串 | 避免跨边界数据拷贝 |
| 返回值精简 | 只返回必要元数据 |
| 启用 wasmtime SIMD | 对向量化友好的代码有效 |

| 优点 | 缺点 |
|------|------|
| ✅ 不用改 Agent 源码 | ❌ 有虚拟机开销（约比原生慢 2-3 倍）|
| ✅ 支持多语言（Rust/Go/C/AS）| ❌ 只能访问 Agent 暴露的 API |
| ✅ **沙箱安全**（插件崩溃不影响 Agent）| ❌ 字符串和复杂类型传递麻烦 |
| ✅ **跨平台**（编译一次到处跑）| ❌ WASM 体积比原生大 |
| ✅ 热加载（无需重启 Agent）| |

#### 方式 3：Shared Object (.so) 插件

源码：`@agent/src/flow_generator/protocol_logs/plugin/shared_obj.rs`

**工作原理**：通过 FFI（C ABI）直接加载 `.so` 动态库，调用里面的 C 函数。这是性能最高的插件方式。

```rust
// @agent/src/flow_generator/protocol_logs/plugin/shared_obj.rs:47
impl L7ProtocolParserInterface for SoLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> Option<LogMessageType> {
        let so_func_ref = param.so_func.borrow();
        let c_funcs = so_func_ref.as_ref()?;
        let ctx = &ParseCtx::from((param, payload));

        for c in c_funcs.iter() {
            // ↓ 直接通过函数指针调用 C 函数（无沙箱）
            let res = unsafe { (c.check_payload)(ctx as *const ParseCtx) };
            ...
        }
    }
}
```

**重要警告**（源码注释 `shared_obj.rs:60-69`）：

> "call the func from so, correctness depends on plugin implementation. there is impossible to verify the plugin implementation correctness, so plugin maybe do some UB, for example, modify the payload ... the plugin correctness depend on the implementation of the developer"

SO 插件**没有任何沙箱保护**——一个有 bug 的插件可以直接搞崩 Agent 进程或篡改数据。

Agent 会为每次 SO 调用**测量耗时**并记录统计（`shared_obj.rs:72-84`），运维可以通过这个计数器发现性能问题，但无法强制终止慢插件。

| 优点 | 缺点 |
|------|------|
| ✅ 性能接近原生（仅 FFI 开销）| ❌ **没有沙箱**，崩溃会搞死 Agent |
| ✅ 可用 C/C++/Rust | ❌ 需要针对目标平台编译 |
| ✅ 可以复用现有 C 协议库 | ❌ 内存安全全靠插件作者 |
| | ❌ 热加载需要重启 Agent |

#### 方式 4：CustomPolicy（基于规则）

源码：`@agent/src/flow_generator/protocol_logs/plugin/custom_protocol_policy.rs`

**特别提示**：CustomPolicy 不是"解析新协议"，而是**"给已知协议打业务标签"**。它基于已有协议（HTTP、Dubbo 等）的特定字段，识别出"这是业务 XX 服务"。

**应用场景**：

```
场景：你有一个内部 RPC 服务，用 HTTP POST + JSON，URL 固定是 /api/order/*
需求：想把这类请求标记为"订单服务"，而不是笼统的 "HTTP"
```

**代码逻辑**（`custom_protocol_policy.rs:82-100`）：

```rust
for policy in policies {
    // 1. 先让底层协议（HTTP/Dubbo 等）识别和解析
    let mut parser = if policy.l7_protocol() != L7Protocol::Custom {
        crate::common::l7_protocol_log::get_parser(policy.l7_protocol().into())
    } else { None };

    if let Some(p) = parser.as_mut() {
        if direction == TrafficDirection::RESPONSE
            || p.check_payload(payload, param).is_none()
        { continue; }
    }

    // 2. 在底层协议识别成功后，用规则匹配额外字段
    match policy.check_payload(payload, direction) {
        None => continue,
        Some(msg_type) => {
            // 识别出是某个"业务协议"，打上标签
            self.policy = Some(policy.clone());
            ...
        }
    }
}
```

**用户只需**：写 YAML 规则配置，零代码。

```yaml
custom_protocol_policies:
  - name: order-service
    base_protocol: HTTP
    port_range: [8080]
    match:
      - source: url
        pattern: "^/api/order/.*"
    extract:
      - name: order_id
        source: url
        regex: "/order/([0-9]+)"
```

**重要限制**：CustomPolicy 依赖 `enterprise_utils` crate（`custom_protocol_policy.rs:21`），**属于企业版功能**。在开源版里调用会触发 `unimplemented!()` panic。

| 优点 | 缺点 |
|------|------|
| ✅ 零代码，写 YAML 即可 | ❌ **企业版专属** |
| ✅ 热更新 | ❌ 只能基于已有协议 |
| ✅ 适合快速业务上线 | ❌ 复杂二进制解析表达不了 |

#### 4 种方式横向对比

| 维度 | 源码修改 | WASM 插件 | SO 插件 | CustomPolicy |
|------|---------|----------|--------|-------------|
| **本质** | 新增解析器 | 新增解析器 | 新增解析器 | 已有协议加标签 |
| **开发语言** | Rust | Rust/Go/C/AS | C/C++/Rust | YAML |
| **性能** | 🟢 最高 | 🟡 较高（-30%）| 🟢 接近原生 | 🟡 较高 |
| **沙箱** | — | ✅ | ❌ | ✅（纯匹配）|
| **热加载** | ❌ 重编译 | ✅ | ❌ 重启 | ✅ |
| **跨平台** | ❌ 交叉编译 | ✅ | ❌ 交叉编译 | ✅ |
| **社区版可用** | ✅ | ✅ | ✅ | ❌ 企业版 |

#### 选择决策树

```
想扩展协议？
├─ 是全新的二进制协议吗？
│  ├─ 是
│  │  ├─ 会长期维护且想开源？
│  │  │  └─ 是 → 方式 1：改源码，贡献上游
│  │  │
│  │  ├─ 需要极致性能（10Gbps+）？
│  │  │  └─ 是 → 方式 3：SO 插件（注意沙箱风险）
│  │  │
│  │  └─ 一般性能需求？
│  │     └─ 是 → 方式 2：WASM 插件 ★推荐
│  │
│  └─ 否（基于 HTTP/Dubbo 加业务识别）
│     └─ 用了企业版？
│        ├─ 是 → 方式 4：CustomPolicy（写 YAML）
│        └─ 否 → 方式 2：WASM 插件（自己写 HTTP 二次解析）
```

#### 插件的资源约束：cgroup 对 WASM/SO 的作用

一个常见问题：**如果对 Agent 做了 cgroup 资源限额，WASM 和 SO 插件是否也受约束？**

**答案：是，但这种"同样生效"是双刃剑。**

##### 核心机制：整个进程加入 cgroup

源码 `@agent/src/utils/cgroups/linux.rs:98-101`：

```rust
if let Err(e) = cpus.add_task_by_tgid(&CgroupPid::from(pid)) {
    return Err(Error::CpuControllerSetFailed(e.to_string()));
}
if let Err(e) = mem.add_task_by_tgid(&CgroupPid::from(pid)) {
    return Err(Error::MemControllerSetFailed(e.to_string()));
}
```

**关键点**：`add_task_by_tgid` 写入的是 **TGID（Thread Group ID = 进程 PID）**。这意味着 Agent 进程的所有线程——包括 WASM 虚拟机、SO 插件调用——**全部被纳入同一个 cgroup**。

##### 为什么 WASM/SO 必然受约束

```
Agent 进程 (PID=12345)
├─ 主线程
├─ Dispatcher 线程
│   ├─ 调用 WASM VM: vm.on_check_payload()
│   │     ↓ wasmtime JIT 产生的 x86 代码
│   │     ↓ 在 Dispatcher 线程里执行
│   │     ↑ 消耗 CPU 计入 Agent
│   │     ↑ WASM 线性内存 malloc 自 Agent 堆
│   │
│   └─ 调用 SO: (c.check_payload)(ctx)
│         ↓ dlopen 加载的 .so 里的函数
│         ↓ 在 Dispatcher 线程里执行
│         ↑ CPU/内存全算 Agent 的
│
└─ 其他线程...

整个进程被 cgroup 管着
```

##### CPU 限制：精确生效

cgroup CPU 限额（`cpu.cfs_quota_us`）通过内核调度器在线程调度时生效。WASM 死循环或 SO 重计算都会被 throttle 整个 Agent 进程。

**观察命令**：

```bash
cat /sys/fs/cgroup/cpu/deepflow-agent/cpu.stat
# nr_throttled 678       ← 被 throttle 次数
# throttled_time 4567890 ← 总 throttle 时间（ns）
```

**盲区**：cgroup 无法区分"Agent 核心用了多少 CPU" vs "某个插件用了多少 CPU"。

##### 内存限制：生效但有坑

**WASM 内存来源**：wasmtime 通过 mmap/malloc 从 Agent 进程堆分配线性内存，**计入 Agent RSS**。

**SO 内存来源**：SO 插件调用 `malloc()` 直接走 Agent 的 libc，也**计入 Agent RSS**。

**坑点 1：OOM 杀掉整个 Agent**

cgroup memory 是硬限制，超限后内核触发 OOM killer 杀**整个进程**：

```
场景：WASM 插件内存泄漏
  WASM 插件累积 500MB
  Agent 核心 600MB
  总计 1100MB > cgroup 1GB 限额
      ↓
  OOM killer → 杀死 Agent 💥
      ↓
  所有观测功能中断
```

**坑点 2：cgroup 看不到"谁用了多少"**

cgroup 只知道整个进程的 RSS：

```bash
cat /sys/fs/cgroup/memory/deepflow-agent/memory.usage_in_bytes
# 950000000  ← 只有一个数字，区分不出 Agent 核心还是插件
```

要区分只能靠 wasmtime 自己的统计、jemalloc 分析器、pmap 手动分析。

**坑点 3：WASM 线性内存的虚拟预留**

wasmtime 默认预留 **4GB 虚拟地址空间**给 WASM 内存（为了优化边界检查），但实际 RSS 只有用到的部分。cgroup memory 计的是 RSS 不是 virtual size，一般不会误触发 OOM。

##### cgroup 对 4 种扩展方式的作用

| 扩展方式 | CPU 限制 | 内存限制 | 精细监控 | OOM 后果 |
|---------|---------|---------|---------|---------|
| **源码修改** | ✅ 生效 | ✅ 生效 | ✅ Agent 知道 | 整个 Agent 挂 |
| **WASM 插件** | ✅ 生效 | ✅ 生效 | 🟡 wasmtime 能统计 | 整个 Agent 挂 |
| **SO 插件** | ✅ 生效 | ✅ 生效 | 🔴 只有耗时统计 | 整个 Agent 挂 |
| **CustomPolicy** | ✅ 生效 | ✅ 生效 | ✅ 纯规则无独立内存 | 整个 Agent 挂 |

##### 插件级资源隔离的补充手段

cgroup 只能做进程级隔离，做不到"WASM 插件独占 20% CPU 不影响 Agent 主流程"。需要额外机制：

**方案 1：wasmtime Fuel 机制（推荐）**

给每次 WASM 调用分配"燃料"（每条 WASM 指令消耗 1 单位），耗尽后自动中断：

```rust
let mut config = Config::new();
config.consume_fuel(true);
let engine = Engine::new(&config)?;

store.set_fuel(1_000_000)?;  // 100 万条指令预算
match check_payload.call(&mut store, (ptr, len)) {
    Ok(_) => { /* 正常完成 */ }
    Err(TrapReason::OutOfFuel) => {
        // 插件超时被强制终止，不影响其他线程
    }
}
```

这是 **WASM 相对 SO 的巨大优势**——可以做单次调用的计算预算，SO 无法实现。

**方案 2：wasmtime Epoch 机制**

更轻量的做法，定时器周期性检查：

```rust
config.epoch_interruption(true);
store.set_epoch_deadline(5);

// 另一线程定时触发
thread::spawn(|| loop {
    thread::sleep(Duration::from_millis(100));
    engine.increment_epoch();
});
```

**方案 3：独立进程 + IPC（DeepFlow 未采用）**

把插件放到独立 sidecar 进程里，通过 Unix socket 通信：

- ✅ 真正的资源隔离
- ❌ IPC 开销（μs 级，对高流量太慢）
- ❌ 运维复杂度增加

#### 生产环境插件使用建议

| 建议 | 理由 |
|------|------|
| **cgroup 留足余量** | Agent 核心 + 插件总共不超过 80%，防突发 OOM |
| **首选 WASM，避免 SO** | WASM 有 fuel 机制，SO 完全无保护 |
| **启用 wasmtime fuel** | 每次调用预算，防死循环拖垮 Agent |
| **监控 Agent RSS 和插件 exe_duration** | 泄漏先在 RSS 曲线显现 |
| **灰度发布** | 新插件先 1 台跑一周再全量 |
| **SO 插件审查严格** | 内存安全全靠作者，生产慎用 |

#### 一句话总结

> **DeepFlow 扩展 L7 协议有 4 条路：改源码（最强最侵入）、WASM 插件（推荐，跨平台有沙箱）、SO 插件（最快但无沙箱）、CustomPolicy（企业版，YAML 规则打业务标签）。cgroup 对 4 种方式的 CPU/内存限制都生效——因为插件都在 Agent 进程内运行。但这种"共享"是双刃剑：插件 OOM 会搞死整个 Agent。真正的插件级隔离要靠 wasmtime 的 fuel/epoch 机制，SO 则只能靠严格审查。**

### 11.12 Stage 3 深度剖析：FlowGenerator / FlowMap

11.1 节给出了 Stage 3 的概览，本节深入剖析 Agent 中**最核心、最复杂**的模块——FlowGenerator/FlowMap 的内部设计。

**重要修正**：早期文档版本中曾把 FlowMap 描述为"使用 DashMap 无锁并发"，**这是错的**。本节给出基于代码证据的正确描述。

#### 11.12.1 核心架构：Share-Nothing + AHashMap

源码 `@agent/src/flow_generator/flow_map.rs:186-244`：

```rust
pub struct FlowMap {
    // The original std HashMap uses SipHash-1-3 and is slow.
    // Use ahash for better performance.
    //
    // Strangely, using AES reduces performance.
    node_map: Option<(
        AHashMap<FlowMapKey, Vec<Box<FlowNode>>>,    // ★ AHashMap, 不是 DashMap
        Vec<HashSet<FlowMapKey>>,                      // 时间轮
    )>,
    id: u32,
    state_machine_master: StateMachine,                // TCP 正向状态机
    state_machine_slave: StateMachine,                 // TCP 反向状态机
    service_table: ServiceTable,
    app_table: AppTable,
    // ...

    tcp_perf_pool: MemoryPool<TcpPerf>,
    flow_node_pool: MemoryPool<FlowNode>,
    tagged_flow_allocator: Allocator<TaggedFlow>,
    l7_stats_allocator: Allocator<L7Stats>,
    // ...
}
```

**关键认知**：每个 Dispatcher worker 拥有**独立的 FlowMap 实例**（注意 `id: u32` 字段），它们之间完全**不共享数据**——这就是 share-nothing 架构。

```
Dispatcher 1 (CPU 0)         Dispatcher 2 (CPU 1)         Dispatcher 3 (CPU 2)
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│ FlowMap (id=0)    │         │ FlowMap (id=1)    │         │ FlowMap (id=2)    │
│  AHashMap         │         │  AHashMap         │         │  AHashMap         │
│  时间轮           │         │  时间轮           │         │  时间轮           │
│  状态机           │         │  状态机           │         │  状态机           │
│  内存池           │         │  内存池           │         │  内存池           │
└──────────────────┘         └──────────────────┘         └──────────────────┘
       ↑                            ↑                            ↑
   独立 + 无锁                  独立 + 无锁                  独立 + 无锁
```

**为什么 share-nothing 比 fine-grained locking（如 DashMap）更快**：

| 优势 | 说明 |
|------|------|
| **无原子操作** | 直接 `&mut`，不需要 CAS |
| **CPU cache 友好** | 每个核的 hash 表只在自己的 L1/L2 cache |
| **无 false sharing** | 不会因不同 key 落在同一 cache line 而跨核 ping-pong |
| **延迟可预测** | 没有锁竞争导致的尖刺 |

**唯一代价**：同一条流的所有包必须落到同一个 Dispatcher——这在 Dispatcher 阶段通过网卡 RSS 或 BPF 哈希保证。

#### 11.12.2 FlowMapKey：双向流的统一标识

源码 `@agent/src/flow_generator/flow_node.rs:50-126`：

```rust
pub(super) struct FlowMapKey {
    lhs: u64,    // l3 哈希（IP 对）
    rhs: u64,    // l4 哈希 + tap 信息
}

impl FlowMapKey {
    fn l3_hash(lookup_key: &LookupKey) -> u64 {
        let (src, dst) = ...;  // 提取 IPv4/IPv6
        // ★ 关键技巧：把大者放高位
        if src >= dst {
            (src as u64) << 32 | dst as u64
        } else {
            (dst as u64) << 32 | src as u64
        }
    }

    fn l4_hash(lookup_key: &LookupKey) -> u64 {
        // ★ 同样：大端口放高位
        if lookup_key.src_port >= lookup_key.dst_port {
            (lookup_key.src_port as u64) << 16 | lookup_key.dst_port as u64
        } else {
            (lookup_key.dst_port as u64) << 16 | lookup_key.src_port as u64
        }
    }
}
```

**双向流自动归一**：

```
client → server 的包：
  src=10.1.1.1:12345, dst=10.2.2.2:80
  l3_hash: max(10.2.2.2)<<32 | 10.1.1.1
  l4_hash: max(12345)<<16 | 80

server → client 的响应包：
  src=10.2.2.2:80, dst=10.1.1.1:12345
  l3_hash: max(10.2.2.2)<<32 | 10.1.1.1   ← 完全相同！
  l4_hash: max(12345)<<16 | 80              ← 完全相同！
```

**双向流的两个方向自动哈希到同一个 key**，无需任何方向判断代码。

#### 11.12.3 哈希冲突的处理：每个桶是 Vec

```rust
AHashMap<FlowMapKey, Vec<Box<FlowNode>>>
                    ^^^^^^^^^^^^^^^^^^^^
                    冲突时同一个 key 对应多个 FlowNode
```

冲突可能来自：
- VLAN/隧道维度的差异（同四元组在不同 VLAN）
- TAP 来源不同
- 哈希算法本身的极小概率冲突

源码 `flow_map.rs:725` 用 `match_node()` 在 Vec 里精确匹配：

```rust
let index = nodes.iter().position(|node| {
    node.match_node(meta_packet, ignore_l2_end, ignore_tor_mac, ignore_idc_vlan, agent_type)
});
```

**`Box<FlowNode>` 的作用**：FlowNode > 200 字节，用 Box 让 Vec 只存指针，避免 move 时的大块内存拷贝。

**`slot_max_depth` 计数器**追踪 Vec 最大长度——正常情况下应该是 1-2，如果持续 > 5 说明哈希冲突严重，需要调优。

#### 11.12.4 FlowNode：流的状态容器

源码 `@agent/src/flow_generator/flow_node.rs:128-159`：

```rust
pub struct FlowNode {
    pub tagged_flow: TaggedFlow,            // 流的元数据 + 双向统计

    // ── 时间相关 ──
    pub min_arrived_time: Timestamp,        // 第一个包的时间
    pub recent_time: Timestamp,             // 最近一个包的时间
    pub timeout: Timestamp,                 // 当前的相对超时（动态调整）
    pub timestamp_key: u64,                 // 在 time_set 里的 slot key

    // ── L7 解析状态 ──
    pub meta_flow_log: Option<Box<FlowLog>>,
    pub policy_data_cache: [Option<Arc<PolicyData>>; 2],   // 双向策略缓存
    pub endpoint_data_cache: Option<EndpointDataPov>,

    // ── eBPF 流专用 ──
    pub residual_request: i32,              // 未配对的请求数
    pub next_tcp_seq0: u32,                 // 双向 TCP seq 期望值
    pub next_tcp_seq1: u32,
    pub last_cap_seq: u32,

    // ── 当前秒级周期标记 ──
    pub policy_in_tick: [bool; 2],
    pub packet_in_tick: bool,
    pub flow_state: FlowState,              // TCP 状态机的当前状态

    // ── 企业版功能 ──
    pub packet_sequence_block: Option<Box<PacketSequenceBlock>>,
    pub tcp_segments: Option<PacketSegmentationReassembly>,   // TCP 段重组
}
```

#### 11.12.5 TCP 状态机：18 状态 + 二维查表

源码 `@agent/src/flow_generator/flow_state.rs:25-48`：

```rust
pub enum FlowState {
    Raw,                          // 初始
    Opening1, Opening2,           // 三次握手中
    Established,                  // 已建立
    ClosingTx1, ClosingTx2,       // 主动关闭
    ClosingRx1, ClosingRx2,       // 被动关闭
    Closed, Reset,                // 终态
    Exception,                    // 异常

    // 异常子状态
    ServerReset,                  // 服务端 RST
    ServerCandidateQueueLack,     // 服务端 backlog 满
    ClientL4PortReuse,            // 客户端端口复用
    Syn1, SynAck1,                // 单次 SYN/SYN-ACK 后断
    EstablishReset,               // 建立后立即 RST
    OpeningRst,                   // 握手中 RST

    Max,
}
```

**对比 Linux 内核 TCP 状态**：DeepFlow 的状态机比 Linux 内核更细——专门追踪异常状态（`ServerReset` / `ClientL4PortReuse` / `OpeningRst` 等），因为这些异常正是网络观测的核心价值。

##### 状态机的二维数组实现

源码 `flow_state.rs:81`：

```rust
const N_FLAGS: usize = TcpFlags::MASK.bits() as usize + 1;  // 64
const N_STATES: usize = FlowState::Max as usize;            // 18

pub struct StateMachine([[StateEntry; N_FLAGS]; N_STATES]);
//                       └─ 18 行 ─┘ └─ 64 列 ─┘
//                       行 = 当前状态
//                       列 = 收到的 TCP flag 组合
```

**18 × 64 = 1152 个预计算条目**，每个 cell 存：

```rust
struct StateValue {
    pub timeout: Timestamp,    // 这个状态的超时
    pub state: FlowState,      // 下一个状态
    pub closed: bool,          // 是否终态
}
```

##### 状态转换示例

源码 `flow_state.rs:111-132` 的 Raw 状态：

```rust
// FlowState::Raw 收到 SYN → 进入 Opening1
let s = Rc::new(StateValue::new(t.opening, FlowState::Opening1, false));
m[FlowState::Raw as usize][TcpFlags::SYN.bits() as usize] = Some(s);

// FlowState::Raw 收到 FIN → 进入 ClosingTx1
let s = Rc::new(StateValue::new(t.closing, FlowState::ClosingTx1, false));
m[FlowState::Raw as usize][TcpFlags::FIN.bits() as usize] = Some(s.clone());

// FlowState::Raw 收到 RST → 进入 Reset
let s = Rc::new(StateValue::new(t.closing, FlowState::Reset, false));
m[FlowState::Raw as usize][TcpFlags::RST.bits() as usize] = Some(s.clone());

// FlowState::Raw 收到 ACK → 进入 Established（握手在抓包前完成）
let s = Rc::new(StateValue::new(t.established, FlowState::Established, false));
m[FlowState::Raw as usize][TcpFlags::ACK.bits() as usize] = Some(s.clone());
```

**为什么用 `Rc<StateValue>`**：很多 (state, flag) 组合的转换结果相同（比如所有 PSH_ACK 都是 Established），用 Rc 让相同的 StateValue 只存一份。

##### 状态机查询性能

```rust
// 伪代码：O(1) 查表
let next = state_machine.0[current as usize][flags.bits() as usize];
```

**完全无分支，CPU 流水线友好**。这是 DeepFlow 能处理百万级 PPS TCP 流的关键。

##### 双向状态机：master + slave

```rust
state_machine_master: StateMachine,    // 主方向（client→server）
state_machine_slave: StateMachine,     // 从方向（server→client）
```

为什么需要两套？因为同一个 TCP flag 在两个方向上语义不同——客户端的 FIN 触发 ClosingTx，服务端的 FIN 触发 ClosingRx。两个状态机分别记录两个方向的状态。

#### 11.12.6 流超时：状态感知的智能策略

源码 `@agent/src/flow_generator/flow_config.rs:25-28`：

```rust
pub const TIMEOUT_OTHERS: Timestamp      = Timestamp::from_secs(5);    // opening/closing/exception
pub const TIMEOUT_ESTABLISHED: Timestamp = Timestamp::from_secs(300);  // 已建立 5 分钟
pub const TIMEOUT_CLOSING: Timestamp     = Timestamp::from_secs(35);   // 已收 RST
pub const TIMEOUT_OPENING_RST: Timestamp = Timestamp::from_secs(1);    // 握手 RST 1 秒
```

**关键洞察：超时时间随 TCP 状态动态变化**

| TCP 状态 | 超时 | 设计理由 |
|---------|------|---------|
| **Opening / Exception** | 5s | 半建立连接快速回收（防 SYN flood 内存爆炸）|
| **Established** | 300s | 长连接 keepalive，避免误判活跃连接为死亡 |
| **ClosingTx/Rx** | 5s | TCP 协议正常关闭，预期很快完成 |
| **EstablishReset** | 35s | 已建立后 RST，给重连一些时间 |
| **OpeningRst** | 1s | 端口探测/拒绝连接，立即清理 |
| **ClosedFin** | 2s | 双向 FIN 完成，快速 GC |

##### 实际案例

```
场景 1：正常 HTTP 短连接
  T=0    SYN          → Raw → Opening1, timeout=5s
  T=10ms SYN+ACK      → Opening1 → Opening2, timeout=5s
  T=20ms ACK          → Opening2 → Established, timeout=300s ← 重置成 300s
  T=30ms HTTP request
  T=80ms HTTP response
  T=100ms FIN         → Established → ClosingTx1, timeout=5s ← 缩回 5s
  T=120ms FIN+ACK     → ClosingTx1 → ClosingTx2, timeout=2s
  T=130ms 流被 GC，输出 FlowLog

场景 2：SYN flood 攻击
  T=0    SYN → Opening1, timeout=5s
  (没有后续)
  T=5s   流被 GC（不会一直占内存）

场景 3：长连接 idle
  T=0    SYN... 完成握手, timeout=300s
  T=10s  有数据，timeout 重置 300s
  ...
  T=310s 最后一次有数据
  T=610s 超时，GC（5 分钟空闲后清理）
```

#### 11.12.7 时间轮 GC：高效的过期清理

DeepFlow 没用 LRU（开销大），而是用 **slot-based 时间轮**：

```rust
time_set: Vec<HashSet<FlowMapKey>>,
time_window_size: usize,    // 必须是 2 的幂
```

##### 数据结构

```
time_set 数组（环形）：
   ┌────┬────┬────┬────┬────┬────┬────┬────┐
   │ s0 │ s1 │ s2 │ s3 │ s4 │ s5 │ s6 │ s7 │ ...
   └────┴────┴────┴────┴────┴────┴────┴────┘
     │
     ▼
  HashSet { flow_key_a, flow_key_b, ... }
  「将在某秒过期的流的 key 集合」

访问方式: time_set[time_in_unit & (time_window_size - 1)]
                                    ↑
                               位运算取模（必须 2^n）
```

##### Flush 主流程：`inject_flush_ticker()`

源码 `@agent/src/flow_generator/flow_map.rs:543-680`：

```rust
pub fn inject_flush_ticker(&mut self, config: &Config, mut timestamp: Duration) -> bool {
    // 1. 计算下一个起始时间
    let next_start_time_in_unit = ...;

    // 2. 扫描即将过期的 slot
    for time_in_unit in self.start_time_in_unit..next_start_time_in_unit {
        let time_hashset = &mut time_set[time_in_unit & (self.time_window_size - 1)];

        // 3. 遍历这个 slot 里的所有流
        for flow_key in time_hashset.drain() {
            let nodes = node_map.get_mut(&flow_key)?;

            // 4. 把节点按"已超时" vs "未超时"分区
            let index = Self::sort_nodes_by_timeout(nodes, timestamp, time_in_unit);

            // 5. 处理已超时的流：输出 FlowLog 并删除
            for node in nodes.drain(index..) {
                if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
                    self.send_socket_close_event(&node);
                }
                self.node_removed_aftercare(config, node, ...);
            }

            // 6. 未超时的：更新统计 + 重新插入到新 slot
            for node in nodes.iter_mut() {
                self.node_updated_aftercare(config, node, ...);
                let timeout = node.recent_time + node.timeout;
                if node.timestamp_key != timeout.as_secs() {
                    node.timestamp_key = timeout.as_secs();
                    moved_key.push((timeout.as_secs(), flow_key));
                }
            }

            // 7. 收缩过大的 HashSet（防内存泄漏）
            if time_hashset.capacity() > 2 * self.time_set_slot_size {
                time_hashset.shrink_to(self.time_set_slot_size);
            }
        }
    }

    // 8. 重新插入移动过的流
    for key in moved_key.drain(..) {
        time_set[key.0 & (window_size - 1)].insert(key.1);
    }
}
```

**关键优化**：

| 优化 | 说明 |
|------|------|
| **位运算取模** | `time_in_unit & (size - 1)`，要求 size 是 2 的幂 |
| **分区排序** | `sort_nodes_by_timeout` 把超时和未超时的流分两段，避免双重遍历 |
| **就地重插** | 未超时的流计算新过期时间后才移动 slot |
| **HashSet shrink** | 防止集合容量永久增大 |

##### 时间轮 vs LRU

| 维度 | LRU 链表 | 时间轮 (slot-based) |
|------|---------|-------------------|
| **插入** | O(1) | O(1) |
| **更新** | O(1) 但有指针操作 | O(1) 仅哈希 |
| **找出过期** | O(n) 遍历链表 | O(slot 大小) 只扫一个 bucket |
| **批量过期** | 慢，逐个处理 | 快，整个 slot 一次处理 |
| **缓存友好** | 差（链表节点分散）| 好（HashSet 内存连续）|

DeepFlow 每秒可能有数十万流，时间轮的"批量过期"特性对性能至关重要。

#### 11.12.8 eBPF 流的特殊处理

eBPF 抓到的数据和抓包不一样——它直接 hook syscall（`tcp_sendmsg`/`tcp_recvmsg`），看到的是 socket 数据而非完整的 TCP 包。

##### 区别 1：FlowMapKey 用 ebpf_flow_id

源码 `@agent/src/flow_generator/flow_node.rs:91-97`：

```rust
pub(super) fn new(packet: &MetaPacket) -> Self {
    if packet.tap_port.is_from(TapPort::FROM_EBPF) {
        return Self {
            lhs: 0,
            rhs: packet.generate_ebpf_flow_id(),  // ← 内核 socket 的 ID
        };
    }
    // 否则按 IP+port 哈希
    ...
}
```

eBPF 流不用 IP+port 计算 key，直接用内核 socket 的 ID（pid+fd 等组成）——同一个 socket 上的所有数据自动归类。

##### 区别 2：靠 residual_request 判断流是否结束

源码 `@agent/src/flow_generator/flow_map.rs:921-934`：

```rust
if node.tagged_flow.flow.signal_source == SignalSource::EBPF && count > 0 {
    if meta_packet.lookup_key.direction == PacketDirection::ClientToServer {
        node.residual_request += count;   // 客户端请求 +1
    } else {
        node.residual_request -= count;   // 服务端响应 -1
    }
    // For eBPF data, timeout as soon as possible when there are no unaggregated requests.
    if node.residual_request == 0 {
        node.timeout = flow_config.flow_timeout.opening;       // 5s 短超时
    } else {
        node.timeout = config.log_parser.l7_log_session_aggr_max_timeout.into();
    }
}
```

eBPF 流没有 TCP FIN/RST 信号，**通过"请求数 == 响应数"判断可以提前回收**。

##### 区别 3：超时时发送 socket close event

源码 `@agent/src/flow_generator/flow_map.rs:627-630`：

```rust
if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
    self.send_socket_close_event(&node);
}
```

eBPF 流超时时**额外发一条 close event**，让 Server 知道这个 socket 已经关闭。

#### 11.12.9 内存优化：双层池化

```rust
tcp_perf_pool: MemoryPool<TcpPerf>,                  // 对象池
flow_node_pool: MemoryPool<FlowNode>,                // 对象池
tagged_flow_allocator: Allocator<TaggedFlow>,        // 自定义 slab allocator
l7_stats_allocator: Allocator<L7Stats>,
```

**为什么需要这么多池**：FlowNode 是大对象（>200 字节），Agent 每秒可能创建/销毁数万个流。频繁的 `Box::new` / `drop` 会导致：

- malloc 锁竞争
- 堆碎片
- CPU cache 不友好

**对象池的工作方式**：

```rust
// 释放时：不真的 free，放回池子
flow_node_pool.put(node);

// 分配时：从池子拿，绕过 malloc
let node = flow_node_pool.get()
    .unwrap_or_else(|| Box::new(FlowNode::default()));
```

**效果**：减少 malloc/free 次数 90%+，避免内存碎片，CPU cache 友好。

#### 11.12.10 完整的性能监控指标

源码 `@agent/src/flow_generator/flow_map.rs:2582-2596`：

```rust
pub struct FlowMapCounter {
    new: AtomicU64,                  // 新建流数
    closed: AtomicU64,               // 关闭流数
    drop_by_window: AtomicU64,       // 因时间窗口被丢弃的包
    drop_by_capacity: AtomicU64,     // 因容量限制被丢弃的包
    packet_delay: AtomicI64,         // 包到达延迟（vs NTP）
    flush_delay: AtomicI64,          // flush 延迟
    flow_delay: AtomicI64,           // 流输出延迟
    concurrent: AtomicU64,           // 当前活跃流数
    slots: AtomicU64,                // HashMap 当前桶数
    slot_max_depth: AtomicU64,       // Vec 最大深度（hash 冲突指标）
    total_scan: AtomicU64,           // 总扫描次数
    time_set_shrinks: AtomicU64,     // 时间窗口收缩次数
    l7_perf_cache_counters: L7PerfCacheCounter,
}
```

##### 健康度判读

| 指标 | 健康值 | 异常含义 |
|------|-------|---------|
| `concurrent` | < 几十万 | 活跃流过多，可能有泄漏或大流量 |
| `drop_by_window` | 应为 0 | 包太老，时间窗口跟不上（NTP 不同步？）|
| `drop_by_capacity` | 应为 0 | 流数超过 capacity 上限 |
| `slot_max_depth` | 1-2 | 持续 > 5 说明哈希冲突严重 |
| `packet_delay` | ~ms | > 1s 说明 Dispatcher 处理慢 |
| `flush_delay` | ~ms | > 100ms 说明 flush 跟不上 |

#### 11.12.11 完整的流处理流程

```
MetaPacket 进入 inject_meta_packet()
  ↓
Step 1: inject_flush_ticker() 推进时间窗口
  └─ GC 即将过期的流，输出 FlowLog
  ↓
Step 2: FlowMapKey::new(meta_packet)
  └─ l3_hash + l4_hash（双向流自动归一）
  ↓
Step 3: AHashMap.get_mut(&pkt_key)
  ├─ Some(nodes) → 在 Vec 里调用 match_node() 精确匹配
  └─ None → 创建新 FlowNode
  ↓
Step 4: 更新 FlowNode
  ├─ TCP: update_tcp_node() → 查 state_machine 表
  ├─ UDP: update_udp_node()
  └─ 其他: update_other_node()
  ↓
Step 5: L7 协议处理 (collect_metric)
  ├─ check_payload() 识别协议
  ├─ parse_payload() 解析字段
  ├─ 提取 trace_id / x_request_id
  └─ 输出 L7 log 到 l7_log_output 队列
  ↓
Step 6: 检查流是否结束
  ├─ TCP: FIN/RST → flow_closed = true
  └─ 其他: 不会主动结束（靠超时）
  ↓
Step 7a: 流结束 → node_removed_aftercare()
  ├─ 计算最终统计（CloseType、duration、perf_stats）
  ├─ 通过 tflow_output 发送 TaggedFlow 到 Collector
  └─ FlowNode 还回 flow_node_pool
  
Step 7b: 流未结束 → 留在 AHashMap
  └─ 更新 timestamp_key 到 time_set 的对应 slot
```

#### 11.12.12 一句话总结

> **Stage 3（FlowGenerator）是 Agent 的状态核心：每个 Dispatcher worker 拥有自己独立的 AHashMap（share-nothing 架构，零跨核同步），通过"双向流哈希"技巧让一条流的两个方向自动归一，用 18×64 的二维查表实现 O(1) 的 TCP 状态机转换，按 TCP 状态智能分配超时（5s/35s/300s/...），用 slot-based 时间轮做高效批量 GC，最后通过双层对象池避免频繁 malloc。整套设计的核心哲学是：高吞吐 + 零锁 + 内存可控。**

### 11.13 Stage 4 深度剖析：Collector 聚合 + 成本模型

本节深入剖析 **Stage 4 的 Collector 聚合机制**——它的窗口模型、聚合维度、以及一个非常重要的工程认知：**采集成本与输出成本的解耦**。

#### 11.13.1 窗口模型：Tumbling Window（固定时间窗口）

**结论先行**：DeepFlow Collector 用的是**经典的固定时间窗口**（Tumbling Window），不是滑动窗口。窗口大小**硬编码为 1 秒和 60 秒，不可配置**。

源码 `@agent/src/collector/collector.rs:352-355`：

```rust
let (slot_interval, doc_flag) = match ctx.metric_type {
    MetricsType::SECOND => (1, DocumentFlag::PER_SECOND_METRICS),     // 1 秒窗口
    _ => (60, DocumentFlag::NONE),                                      // 60 秒窗口
};
```

##### 时间戳对齐

源码 `collector.rs:402`：

```rust
time_in_second = time_in_second / self.slot_interval * self.slot_interval;
```

**经典的整数除法截断**——把时间戳对齐到 slot 整数倍：

```
slot_interval = 60 (分钟级)

time_in_second = 1712587823  →  1712587823 / 60 * 60 = 1712587800
                                                       ↑
                                              对齐到 14:50:00 整点

所有在 [14:50:00, 14:51:00) 之间的数据归到 14:50:00 的 bucket
```

##### 窗口推进：事件驱动而非时间驱动

源码 `collector.rs:405-421`：

```rust
let start_time = self.start_time.as_secs();
if time_in_second > start_time {
    // 新数据时间戳超出当前窗口 → 触发 flush
    self.flush_stats();   // ★ 整个 stash 一次性 flush
    self.start_time = Duration::from_secs(time_in_second);
}
```

**这是事件驱动的窗口推进**：

```
窗口 [14:50:00, 14:51:00)
    │
    ├─ 14:50:01 数据到达 → 加入 stash
    ├─ 14:50:30 数据到达 → 加入 stash
    ├─ 14:50:59 数据到达 → 加入 stash
    │
    ├─ 14:51:02 数据到达 → ★ 超出窗口！
    │   ├─ flush_stats() — 把 [14:50:00, 14:51:00) 数据全部 flush
    │   ├─ start_time = 14:51:00
    │   └─ 14:51:02 的数据加入新窗口 [14:51:00, 14:52:00)
    └─ ...
```

**关键点**：窗口推进 = "新窗口的第一条数据到达的瞬间"，不是 wallclock 触发。如果完全没数据，窗口永远不会自动推进。

##### flush_stats：清空式 flush

源码 `collector.rs:826-864`：

```rust
fn flush_stats(&mut self) {
    let mut batch = Vec::with_capacity(QUEUE_BATCH_SIZE);

    // ★ 关键：drain() 把 HashMap 完全清空
    for (_, mut doc) in self.inner.drain() {
        doc.timestamp = self.start_time.as_secs() as u32;
        doc.flags |= self.doc_flag;
        batch.push(BoxedDocument(Box::new(doc)))
        // 批量发送...
    }

    // 自适应收缩 HashMap 容量（防内存膨胀）
    let stash_cap = self.inner.capacity();
    if stash_cap > 2 * max_history {
        self.inner.shrink_to(...);
    }
}
```

**典型的 Tumbling Window 行为**：整体 drain，不保留任何旧数据，新窗口从空开始。

##### Tumbling vs Sliding 对比

| 维度 | DeepFlow Tumbling Window | Sliding Window（未采用）|
|------|----------------------|---------------------|
| **窗口边界** | 固定整点对齐 | 跟随当前时间滑动 |
| **重叠** | 无 | 有 |
| **flush 时机** | 窗口结束整体 flush | 持续输出最近 N 秒 |
| **数据归属** | 一条数据只属 1 个窗口 | 一条数据可能在 N 个窗口 |
| **内存** | 单窗口 | 多窗口同时维护 |
| **典型用途** | "每分钟的总流量" | "最近 60 秒的滑动平均" |

##### 为什么不用滑动窗口

| 原因 | 说明 |
|------|------|
| **性能开销** | 滑动窗口需多个重叠窗口，内存和 CPU 都更贵 |
| **下游存储友好** | ClickHouse MergeTree 按时间分区，与固定窗口天然契合 |
| **查询语义清晰** | "14:50 那一分钟的总流量" 比"任意 60 秒滑动平均"更易复现 |
| **重复数据问题** | 滑动窗口同一 flow 落多窗口，存储成倍膨胀 |
| **滑动可在查询时实现** | 需要时让 ClickHouse 用窗口函数算 |

DeepFlow 的设计哲学：**Agent 只做最小聚合，复杂分析交给 Server**。

#### 11.13.2 窗口大小的可配置性

| 参数 | 是否可调 | 配置项 |
|------|---------|--------|
| **slot_interval = 1s** | ❌ 硬编码 | 无 |
| **slot_interval = 60s** | ❌ 硬编码 | 无 |
| **是否启用秒级 collector** | ✅ | `outputs.flow_metrics.filters.second_metrics` |
| **是否启用分钟级 collector** | ❌ 总是开 | 无 |
| **是否禁用 APM 指标** | ✅ | `outputs.flow_metrics.filters.apm_metrics` |
| **是否禁用并发指标** | ✅ | `outputs.flow_metrics.filters.npm_metrics_concurrent` |

源码 `@server/agent_config/template.yaml:7937-7953`：

```yaml
outputs:
  flow_metrics:
    filters:
      apm_metrics: true                # APM 指标开关
      npm_metrics_concurrent: true     # 并发指标开关
      second_metrics: true             # ★ 秒级指标整体开关（不能改间隔）
```

**没有任何配置可以把 1 秒改成 5 秒，或把 60 秒改成 300 秒**。如果需要更长粒度统计（5 分钟、1 小时），那是 Server ClickHouse 在查询时再聚合的事，不是 Agent 的职责。

#### 11.13.3 聚合维度：StashKey 与 9 种维度组合

聚合的本质是：**在固定时间窗口内，把 StashKey 相同的 Flow 的指标合并**。

源码 `@agent/src/collector/collector.rs:127-138`：

```rust
struct StashKey {
    fast_id: u128,        // ★ 编码了"维度组合 + 维度值"
    src_ip: IpAddr,
    dst_ip: IpAddr,
    src_gpid: u32,        // 源进程全局 ID
    dst_gpid: u32,        // 目的进程全局 ID
    endpoint_hash: u32,   // L7 endpoint 哈希
    time_span: u32,       // 请求-响应时间跨度
    biz_type: u8,         // 业务类型
}
```

##### fast_id：u128 位图编码

`fast_id` 把"哪些字段参与聚合 + 这些字段的值"全部编码到一个 128 位整数里。源码 `collector.rs:198-203` 的位图布局：

```
single point fast_id 布局：
128          72        64    59       56        48           32          24         16        0
+-------------+---------+-----+--------+---------+------------+-----------+----------+---------+
|             | L7Proto | MAC | CodeID | TapType | ServerPort | Direction | Protocol | L3EpcId |
+-------------+---------+-----+--------+---------+------------+-----------+----------+---------+

edge data fast_id 布局：
128    124        104          100        96          64          56           40         32         16        0
+------+----------+------------+----------+-----------+-----------+------------+----------+----------+---------+
| from | RESERVED | NAT SOURCE | TUN_TYPE | ip/id/mac | Direction | ServerPort | Protocol | L3EpcID1 | L3EpcId |
+------+----------+------------+----------+-----------+-----------+------------+----------+----------+---------+
```

**位图编码的好处**：比较两个 StashKey 时只需比较 16 字节，比按字段比较快得多。这是经典的元组压缩技巧。

##### 9 种维度组合（聚合粒度）

源码 `collector.rs:155-193` 定义了 9 种 StashKey 编码模式：

**Single 系列**（单端点视角，用于"某台服务器收到了多少请求"这类查询）：

| 模式 | 维度组合 | 用途 |
|------|---------|------|
| **SINGLE_IP** | IP + L3_EPC + GPID + VTAP + Protocol + Direction + TapType | 最粗：按"主机+协议"汇总 |
| **SINGLE_IP_PORT** | SINGLE_IP + ServerPort | 加上服务端口 |
| **SINGLE_MAC_IP_PORT** | SINGLE_IP_PORT + MAC | 加上 MAC（多网卡场景）|
| **SINGLE_IP_PORT_APP** | SINGLE_IP_PORT + L7_PROTOCOL | 加上 L7 协议 |
| **SINGLE_MAC_IP_PORT_APP** | 以上全部 | 最细 |

**Edge 系列**（双端点/通信对视角，用于"A 服务调了 B 服务多少次"这类查询）：

| 模式 | 维度组合 | 用途 |
|------|---------|------|
| **EDGE_IP** | IP_PATH + L3_EPC_PATH + GPID_PATH + Protocol + Direction + TapPort | 最粗：按"通信对+协议"汇总 |
| **EDGE_IP_PORT** | EDGE_IP + ServerPort | 加上服务端口 |
| **EDGE_MAC_IP_PORT** | EDGE_IP_PORT + MAC_PATH | 加上 MAC |
| **EDGE_IP_PORT_APP** | EDGE_IP_PORT + L7_PROTOCOL | 加上 L7 协议 |
| **EDGE_MAC_IP_PORT_APP** | 以上全部 | 最细 |

**ACL 模式**：

| 模式 | 维度组合 | 用途 |
|------|---------|------|
| **ACL** | ACL_GID + TUNNEL_IP_ID + VTAP_ID | 防火墙/PCAP 策略统计 |

##### Fan-out + Reduce：一条 Flow 扇出多个 Document

**最关键的认知**：一条 Flow 不是产生一个 Document，**而是被"扇出"成多个 Document**，每个对应一种维度组合，然后在各自的窗口里 reduce 合并。

```
                  一条 Flow
                       │
                       │  Fan-out
                       ▼
        ┌──────────────┼───────────────┐
        ▼              ▼               ▼
   StashKey-A      StashKey-B      StashKey-C  ...
   (SINGLE_IP)     (SINGLE_IP_     (EDGE_IP_
                    PORT_APP)        PORT_APP)
        │              │               │
        ▼              ▼               ▼
   HashMap[A] +=   HashMap[B] +=   HashMap[C] +=
```

**举例**：3 条 HTTP flow

```
Flow 1: 10.1.1.1:50000 → 10.2.2.2:80, packets=10, bytes=1500
Flow 2: 10.1.1.1:50001 → 10.2.2.2:80, packets=20, bytes=3000
Flow 3: 10.1.1.5:50000 → 10.2.2.2:80, packets=5,  bytes=750
```

**SINGLE_IP_PORT_APP 视角**（服务器视角）：

```
StashKey { ip=10.2.2.2, port=80, proto=TCP, app=HTTP, dir=server }
  → Document { packets: 35, bytes: 5250 }     ← 3 条 flow 合 1 条
```

**EDGE_IP_PORT_APP 视角**（端到端通信对）：

```
StashKey { src=10.1.1.1, dst=10.2.2.2, port=80, proto=TCP, app=HTTP }
  → Document { packets: 30, bytes: 4500 }     ← Flow1 + Flow2

StashKey { src=10.1.1.5, dst=10.2.2.2, port=80, proto=TCP, app=HTTP }
  → Document { packets: 5, bytes: 750 }       ← Flow3 单独
```

两种视图同时生成并送往 Server，因为查询端可能有不同的需求。

**单条流的扇出系数**：典型场景 6-10 个 Document（多种 Single + Edge 组合的总和）。这是为什么 Stash 在高 QPS 下膨胀很快。

#### 11.13.4 采集成本 vs 输出成本的解耦

这是 DeepFlow 性能模型中**最关键的工程认知**：

> **Agent 的资源占用主要在 Stage 1-3（与流量成正比，无法绕开），输出聚合（Stage 4-5）的成本相对小，但它决定的是网络上行带宽和下游 ClickHouse 存储压力。**

##### 5 个阶段的成本分布

```
Agent 总成本 ≈ 100%
├─ Stage 1 抓包          ~25%   (always on)
├─ Stage 2 协议解析       ~30%   (always on)
├─ Stage 3 流维护         ~25%   (always on)
├─ Stage 4 聚合          ~10-15% ← second_metrics 让这从 5% 变 15%
└─ Stage 5 发送          ~5-10%  ← second_metrics 让这从 3% 变 10%
```

**Stage 1-3 是与流量成正比的"必要开销"**，无论你开不开聚合都得做。

##### `second_metrics` 开关的作用边界

源码 `@agent/src/collector/quadruple_generator.rs:897-948`：

```rust
fn handle(&mut self, ..., tagged_flow: ..., time_in_second: Duration) {
    let mut second_inject = false;
    let mut minute_inject = false;

    if let Some(s) = self.second_quad_gen.as_mut() {
        if config.vtap_flow_1s_enabled {                     // ★ second_metrics 开关
            second_inject = s.move_window(time_in_second, ...);
        }
    }
    if let Some(s) = self.minute_quad_gen.as_mut() {
        minute_inject = s.move_window(time_in_second, ...);
    }

    if tagged_flow.is_none() || !(second_inject || minute_inject) {
        return;       // ★ 如果两个窗口都不需要，直接 return，零成本
    }

    if second_inject {
        self.second_quad_gen.as_mut().unwrap().inject_flow(...);   // 跳过这一段
    }
    if minute_inject {
        self.minute_quad_gen.as_mut().unwrap().inject_flow(...);
    }
}
```

**关掉 `second_metrics` 时跳过的工作**：

| 跳过 | 不跳过 |
|------|-------|
| ✅ 秒级 stash 维护（HashMap 操作）| ❌ Stage 1-3 全部照旧 |
| ✅ 秒级 Collector 每秒 flush | ❌ 分钟级 collector 照常 |
| ✅ 秒级 Document 序列化和发送 | ❌ L7 流日志（`l7_flow_log`）照常上报 |
| ✅ Server Ingester 写 1s 表 | |

**重要**：`second_metrics` 只影响"统计 metrics"，不影响"原始流日志"。关掉它仍能看到每条 HTTP 请求明细，只是看不到"按秒聚合的服务级 RED 指标"。

##### 配置开关与成本的对应关系

| 开关 | 影响阶段 | 改了 Agent 资源吗？|
|------|---------|-----------------|
| `second_metrics` | Stage 4-5 | 几乎不影响 |
| `apm_metrics` | Stage 4-5 | 几乎不影响 |
| `npm_metrics_concurrent` | Stage 4-5 | 几乎不影响 |
| `l4_throttle` | Stage 5 | 几乎不影响 |
| `l7_throttle` | Stage 5 | 几乎不影响 |
| `inputs.cbpf.disabled` | **Stage 1** | **大幅影响**（关掉抓包）|
| `inputs.ebpf.disabled` | **Stage 1** | **大幅影响**（关掉 eBPF）|

**真正影响 Agent 资源的是 Stage 1-3 的开关**（关数据源、降采样）。Stage 4-5 的开关只影响"产出"。

##### 运维决策原则

```
想省 Agent 资源 → 调 Stage 1-3 的开关
  ├─ 降低采样率
  ├─ 关闭特定 capture mode
  ├─ 缩小 BPF filter 范围
  └─ 关闭某种数据源（eBPF/抓包）

想省网络/存储 → 调 Stage 4-5 的开关
  ├─ 关闭 second_metrics
  ├─ 关闭 apm_metrics
  ├─ 调小 throttle 限速
  ├─ 增加压缩率
  └─ 缩短 ClickHouse 保留时间
```

**两类开关不能混淆**——Stage 4-5 的开关救不了 Stage 1-3 的资源问题。

#### 11.13.5 银行场景资源评估（second_metrics 案例）

以银行典型环境为例，估算开/关秒级聚合的成本差距。

##### 场景假设（中等股份行 / 大行单数据中心）

| 维度 | 估算 |
|------|------|
| 业务主机数 | ~3000 台 |
| 平均 vCPU/主机 | 8 vCore |
| TCP 并发流/主机 | 500-2000（高峰）|
| 每秒新建流/主机 | 50-200 |
| L7 协议覆盖 | HTTP/Dubbo/SOFA-RPC + MySQL/Oracle + Redis + Kafka/RocketMQ |

##### 单台主机的额外消耗

| 指标 | 不开秒级 | 开秒级 | 增量 |
|------|---------|--------|------|
| **CPU**（典型 500 流）| ~3% 单核 | ~3.5-4% 单核 | **+0.5-1% 单核** |
| **CPU**（高密度 2000+ 流）| ~6% 单核 | ~7.5-9% 单核 | **+1.5-3% 单核** |
| **常驻内存（RSS）** | ~250 MB | ~260-280 MB | **+10-30 MB** |
| **每秒 Document 输出** | ~30-60 docs | ~150-400 docs | **5-7 倍** |
| **网络上行带宽** | ~50-100 KB/s | ~250-700 KB/s | **5-7 倍** |

##### 3000 台 Agent 总增量

| 维度 | 不开秒级 | 开秒级 | 增量 |
|------|---------|--------|------|
| **总 CPU** | ~90 vCore | ~120 vCore | **+30 vCore** |
| **总 RSS** | ~750 GB | ~810 GB | **+60 GB** |
| **上报带宽** | ~150-300 MB/s | ~750-2100 MB/s | **+600-1800 MB/s** |
| **峰值带宽** | ~500 MB/s | ~3 GB/s | **+2.5 GB/s** |

##### ClickHouse 写入和存储

每条 Document 压缩后 ~150-300 字节。

**单 Agent 每天写入量**：

```
不开秒级（只 1m 表）：
  3000 × 2000 docs/min ≈ 6M docs/min ≈ 360M docs/day
  存储: 360M × 250B ≈ 90 GB/day

开秒级（1s + 1m 表）：
  1s 表: 3000 × 200 docs/sec × 86400 ≈ 52B docs/day → ~13 TB/day
  1m 表: 同上 90 GB/day
  合计: ~13.1 TB/day
```

**3000 台规模 1 个月总存储**（考虑 2 副本 + merge 空间放大 ×2.5）：

| 场景 | 1s 表 | 1m 表 | 实际占用 |
|------|-------|-------|---------|
| **不开秒级** | 0 | ~2.7 TB | **~7-8 TB** |
| **开秒级（保留 1 天）** | ~13 TB | ~2.7 TB | **~40 TB** |
| **开秒级（保留 7 天）** | ~91 TB | ~2.7 TB | **~235 TB** |

##### 综合资源预算

| 资源 | 不开秒级 | 开秒级（1 天）|
|------|---------|---------------|
| **Agent 总 CPU** | ~90 vCore | ~120 vCore（+33%）|
| **Agent 总内存** | ~750 GB | ~810 GB（+8%）|
| **管理网带宽** | ~300 MB/s | ~2 GB/s（+560%）|
| **CK 总磁盘** | ~8 TB | ~40 TB（+400%）|
| **CK CPU 节点数** | 4-6 节点 | 8-12 节点（+100%）|
| **每月运维成本** | 1× | ~3-4× |

##### 银行场景的部署建议

**默认应该关闭秒级聚合**：

| 理由 | 说明 |
|------|------|
| 存储成本 | 从 ~8 TB 涨到 40-235 TB，**5-30 倍** |
| 运维成本 | CK 集群需求翻倍 |
| 大部分查询用不上 | 业务排障和容量规划用 1m 粒度足够 |
| 高峰流量大 | 秒级写入是 CK 稳定性风险 |

**应该开启秒级的场景**：

| 场景 | 理由 |
|------|------|
| 核心交易系统（<100 台）| 局部开启，对 SLA 敏感 |
| 生产事故复盘期（短期）| 临时开 1-3 天，事后关闭 |
| 关键应用上线灰度 | 灰度期开，灰度结束关闭 |
| 压测期间 | 测试环境验证容量 |

##### 推荐的部署策略

```
策略 A：分级部署（推荐）

  全量主机（3000 台）         → 只开 1m 聚合，CK 保留 90 天
  核心交易主机（200-500 台）  → 开 1s 聚合，CK 1s 表保留 3 天

  通过 Agent Group 配置实现分组管理：
    默认: outputs.flow_metrics.filters.second_metrics: false
    核心组覆盖: outputs.flow_metrics.filters.second_metrics: true
```

```
策略 B：按需开启（备用）

  默认全部关闭秒级
  事故时通过 Controller 临时下发 second_metrics: true
  事后关闭
  (DeepFlow 配置是 hot_update，无需重启 Agent)
```

##### 重要免责声明

以上数字是**基于代码逻辑 + 业内常见规模的推算**，不是 benchmark 实测。实际数字会受影响：

| 因素 | 影响 |
|------|------|
| 业务流量峰值倍数 | 银行高峰可能是均值 5-10 倍 |
| L7 协议复杂度 | Oracle/MQ 解析比 HTTP 重 |
| 维度数量 | 开了 mac 维度，扇出系数翻倍 |
| 客户端 IP 收敛度 | IP 分散时 EDGE 维度暴增 |
| L7_metrics_enabled | 关掉再省 30-40% Document |

**强烈建议**：在你的环境里**先用 100 台主机做 PoC**，跑 24-48 小时，实测：

- Agent 的 CPU 和 RSS（用 cgroup 统计）
- Sender 的 `out` counter
- CK 的 `system.parts` 增长率
- 然后线性外推到全量

#### 11.13.6 Document 数据结构详解

前面提到 Stage 4 的产物是 Document，这里展开讲它的内部结构、Stage 4 vs Stage 5 的两种形态、以及完整的样例。

##### Document 在哪一步组织好的

**完全在 Stage 4 内组织好。Stage 5 (Sender) 只做格式转换 + 压缩 + 发送。**

```
Stage 4 (Collector)                  Stage 5 (Sender)
─────────────────────                ──────────────────

inject_flow()
   ↓
HashMap<StashKey, Document>          ┌────────────────┐
   - Document = Rust struct          │ encode() 调用时  │
   - sequential_merge() 累加         │ 才转成 protobuf  │
   ↓                                 └────────────────┘
flush_stats()                              ↓
   ↓                                 protobuf 字节流
send(BoxedDocument) ──────────────→         ↓
                                     zstd 压缩
                                          ↓
                                     TCP 发送给 Server
```

源码 `@agent/src/collector/collector.rs:826-852` 的 `flush_stats`：

```rust
fn flush_stats(&mut self) {
    let mut batch = Vec::with_capacity(QUEUE_BATCH_SIZE);
    for (_, mut doc) in self.inner.drain() {
        // ↓ Stage 4 内填充 timestamp 和 flag
        doc.timestamp = self.start_time.as_secs() as u32;
        doc.flags |= self.doc_flag;
        // ↓ 装进 BoxedDocument 后发出（Document 已完整）
        batch.push(BoxedDocument(Box::new(doc)))
    }
    if batch.len() > 0 {
        self.sender.send_all(&mut batch)?;
    }
}
```

`self.inner` 类型是 `HashMap<StashKey, Document>`——**Stage 4 内部一直存的就是 Document**。`flush_stats` 只是把它们 drain 出来送到下游队列。

源码 `@agent/src/metric/document.rs:89-103` 的 Sender 端：

```rust
impl Sendable for BoxedDocument {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, prost::EncodeError> {
        let pb_doc: metric::Document = (*self.0).into();   // ★ 这里才转 protobuf
        pb_doc.encode(buf).map(|_| pb_doc.encoded_len())
    }
    fn message_type(&self) -> SendMessageType {
        SendMessageType::Metrics
    }
}
```

##### Document 的两种形态

DeepFlow 里的 "Document" 实际上是**两个同名但完全不同的类型**：

| 类型 | 来源 | 作用 |
|------|------|------|
| **`crate::metric::Document`** | 手写的 Rust struct（`@agent/src/metric/document.rs:38`）| Agent 内部使用 |
| **`metric::Document`** | protobuf 生成（`prost-build` 编译 .proto）| 网络传输使用 |

它们之间靠 `impl From<Document> for metric::Document` 做转换。

**为什么要有两种形态？**

| 用 Rust 结构体的原因 | 用 protobuf 的原因 |
|------------------|------------------|
| 直接访问字段比操作 protobuf 字节快 | 跨语言通信（Server 是 Go 写的）|
| `sequential_merge` 频繁调用，需要高效字段访问 | 编码紧凑，适合压缩 |
| 内存布局紧凑（不需要 `Option<>` 包装大量字段）| Schema 演化机制，新字段不影响旧客户端 |

**Stage 4 用 Rust 结构体做"业务装配"，Stage 5 用 protobuf 做"传输编码"。**

##### .proto 源头定义

源码 `@message/metric.proto:63-68`：

```protobuf
message Document {
    uint32  timestamp = 1;
    MiniTag tag = 2;          // ← 维度（注意叫 MiniTag）
    Meter   meter = 3;        // ← 指标
    uint32  flags = 4;
}

message Meter {
    uint32     meter_id = 1;
    FlowMeter  flow = 2;       // meter_id == 1 时填这个
    UsageMeter usage = 3;      // meter_id == 4 时填这个
    AppMeter   app = 4;        // meter_id == 5 时填这个
}
```

**Meter 是 tag-based variant**——同时声明 3 个字段，但运行时只填一个，靠 `meter_id` 区分。

**.proto 是跨语言契约**：

```
@message/metric.proto  (Source of Truth)
       │
       ├──→ Rust prost-build → @agent/crates/public/src/proto/metric.rs
       │       └─ Agent 在 Sender 阶段使用
       │
       └──→ Go protoc       → @server/.../message/metric.pb.go
               └─ Server 在 Ingester 阶段反序列化
```

修改 `metric.proto` 需要重新生成两端代码。`@AGENTS.md` 反复强调"protobuf 是 source of truth"就是这个原因。

##### Rust 结构体字段详解

源码 `@agent/src/metric/document.rs:38-43`：

```rust
pub struct Document {
    pub timestamp: u32,        // 窗口的对齐时间戳（秒）
    pub tagger: Tagger,        // ★ 维度（"是谁的指标"）
    pub meter: Meter,          // ★ 指标值（"值是多少"）
    pub flags: DocumentFlag,   // PER_SECOND_METRICS / NONE
}
```

经典的"维度建模"——和 Prometheus、OpenTSDB 是同一类思路。

##### Tagger（维度）

源码 `@agent/src/metric/document.rs:344-382`：

```rust
pub struct Tagger {
    pub code: Code,                  // u64 位图：哪些字段有效

    // ── 网络层维度 ──
    pub ip: IpAddr,                  // 端点 1 的 IP
    pub ip1: IpAddr,                 // 端点 2 的 IP（仅 Edge 模式）
    pub is_ipv6: bool,
    pub mac: MacAddr,
    pub mac1: MacAddr,
    pub l3_epc_id: i16,              // 端点 1 的 EPC（虚拟网络）
    pub l3_epc_id1: i16,

    // ── 传输层维度 ──
    pub protocol: IpProtocol,        // TCP / UDP / ICMP
    pub server_port: u16,
    pub direction: Direction,
    pub tap_side: TapSide,

    // ── 元数据 ──
    pub agent_id: u16,
    pub global_thread_id: u8,
    pub tap_port: TapPort,
    pub tap_type: CaptureNetworkType,

    // ── 应用层 ──
    pub l7_protocol: L7Protocol,     // HTTP / MySQL / Kafka...
    pub endpoint: Option<String>,    // L7 endpoint
    pub biz_type: u8,
    pub time_span: u32,

    // ── 进程关联 ──
    pub gpid: u32,                   // 端点 1 的 global process id
    pub gpid_1: u32,                 // 端点 2 的 gpid
    pub pod_id: u32,

    // ── OTel 集成 ──
    pub otel_service: Option<String>,
    pub otel_instance: Option<String>,

    pub acl_gid: u16,
    pub signal_source: SignalSource, // Packet / EBPF / OTel
}
```

**`code: Code` 字段**是一个 u64 位图，标记**哪些字段有效**。源码 `document.rs:124-151`：

```rust
pub struct Code: u64 {
    // Single（单端点）相关
    const IP            = 1 << 0;
    const L3_EPC_ID     = 1 << 1;
    const MAC           = 1 << 11;
    const GPID          = 1 << 15;

    // Edge（双端点）相关
    const IP_PATH       = 1 << 20;
    const L3_EPC_PATH   = 1 << 21;
    const MAC_PATH      = 1 << 31;
    const GPID_PATH     = 1 << 35;

    // 通用维度
    const DIRECTION     = 1 << 40;
    const ACL_GID       = 1 << 41;
    const PROTOCOL      = 1 << 42;
    const SERVER_PORT   = 1 << 43;
    const TAP_TYPE      = 1 << 45;
    const VTAP_ID       = 1 << 47;
    const TAP_SIDE      = 1 << 48;
    const TAP_PORT      = 1 << 49;
    const L7_PROTOCOL   = 1 << 51;
    const TUNNEL_IP_ID  = 1 << 62;
}
```

##### Meter（指标值，3 种类型）

```rust
pub enum Meter {
    Flow(FlowMeter),    // 网络层指标（NPM）
    App(AppMeter),      // 应用层指标（APM/RED）
    Usage(UsageMeter),  // 用量指标（PCAP/分发统计）
}
```

**FlowMeter**（NPM）：

```rust
pub struct FlowMeter {
    pub traffic: Traffic,         // 流量
    pub latency: Latency,         // 时延
    pub performance: Performance, // 性能（重传等）
    pub anomaly: Anomaly,         // 异常
    pub flow_load: FlowLoad,      // 并发流数
}

pub struct Traffic {
    pub packet_tx: u64,    pub packet_rx: u64,
    pub byte_tx: u64,      pub byte_rx: u64,
    pub l3_byte_tx: u64,   pub l3_byte_rx: u64,
    pub l4_byte_tx: u64,   pub l4_byte_rx: u64,
    pub new_flow: u64,     pub closed_flow: u64,
    pub l7_request: u32,   pub l7_response: u32,
    pub syn: u32,          pub synack: u32,
    pub direction_score: u8,
}

pub struct Latency {
    // 8 种时延，每个都有 _max / _sum / _count
    pub rtt_max: u32,        pub rtt_sum: u64,        pub rtt_count: u32,
    pub rtt_client_max: u32, pub rtt_client_sum: u64, pub rtt_client_count: u32,
    pub rtt_server_max: u32, pub rtt_server_sum: u64, pub rtt_server_count: u32,
    pub srt_max: u32,        pub srt_sum: u64,        pub srt_count: u32,
    pub art_max: u32,        pub art_sum: u64,        pub art_count: u32,
    pub rrt_max: u32,        pub rrt_sum: u64,        pub rrt_count: u32,
    pub cit_max: u32,        pub cit_sum: u64,        pub cit_count: u32,
    pub tls_rtt_max: u32,    pub tls_rtt_sum: u64,    pub tls_rtt_count: u32,
}

pub struct Anomaly {
    pub client_rst_flow: u64,         pub server_rst_flow: u64,
    pub client_ack_miss: u64,         pub server_syn_miss: u64,
    pub client_half_close_flow: u64,  pub server_half_close_flow: u64,
    pub client_source_port_reuse: u64,
    pub client_establish_reset: u64,
    pub server_reset: u64,
    pub server_queue_lack: u64,
    pub server_establish_reset: u64,
    pub tcp_timeout: u64,
    pub l7_client_error: u32,
    pub l7_server_error: u32,
    pub l7_timeout: u32,
    pub client_ooo: u64,
    pub server_ooo: u64,
}
```

**AppMeter**（APM / RED 三元组）：

```rust
pub struct AppMeter {
    pub traffic: AppTraffic,    // R - Request rate（请求速率）
    pub latency: AppLatency,    // D - Duration（时延）
    pub anomaly: AppAnomaly,    // E - Errors（错误）
}
```

**关键观察：用 sum/count 而不是 histogram**

注意 `Latency` 字段都是 `_max / _sum / _count` 三元组，**没有直方图**。这意味着：

| 能算的 | 不能算的 |
|-------|---------|
| ✅ 平均值（sum / count）| ❌ 精确的 P99 / P95 |
| ✅ 最大值 | ❌ 精确的中位数 |
| ✅ 总数 | ❌ 长尾分布 |

DeepFlow 的精确百分位数靠**原始 L7 流日志**（`l7_flow_log`）支持，不是靠聚合 metrics。这是工程权衡：

- **聚合 metrics**：用于看趋势、做大盘、低成本存储
- **想看 P99**：查 `l7_flow_log` 表，让 ClickHouse 在查询时计算

##### 完整样例：3 个 HTTP 请求生成的 Document

假设 14:50:23 这一秒发生了：

```
10.1.1.1:50000 → 10.2.2.2:80  GET /api/users   耗时 50ms
10.1.1.1:50001 → 10.2.2.2:80  GET /api/orders  耗时 30ms
10.1.1.1:50002 → 10.2.2.2:80  GET /api/users   耗时 100ms (server error)
```

按 **EDGE_IP_PORT_APP 维度** 聚合后产生的 Document：

```rust
Document {
    timestamp: 1712587823,                     // 14:50:23 (Unix epoch)
    flags: PER_SECOND_METRICS,

    tagger: Tagger {
        code: IP_PATH | L3_EPC_PATH | GPID_PATH | VTAP_ID | PROTOCOL
            | DIRECTION | TAP_TYPE | TAP_PORT | SERVER_PORT | L7_PROTOCOL,

        // ── 双端点 ──
        ip: 10.1.1.1,                          // client
        ip1: 10.2.2.2,                         // server
        l3_epc_id: 100,                        // client EPC
        l3_epc_id1: 200,                       // server EPC

        // ── L4 ──
        protocol: TCP,
        server_port: 80,
        direction: ClientToServer,
        tap_side: Client,

        // ── L7 ──
        l7_protocol: Http1,

        // ── 进程 ──
        gpid: 12345,                           // client 进程
        gpid_1: 67890,                         // server 进程
        pod_id: 5,

        agent_id: 1,
        signal_source: Packet,
        // ...
    },

    meter: Meter::Flow(FlowMeter {
        traffic: Traffic {
            packet_tx: 18,              // 客户端发送
            packet_rx: 18,              // 服务端响应
            byte_tx: 1500,
            byte_rx: 12000,
            new_flow: 3,                // 3 个新流
            closed_flow: 3,
            l7_request: 3,              // 3 个 HTTP 请求
            l7_response: 3,
            syn: 3,
            synack: 3,
            direction_score: 100,
        },
        latency: Latency {
            rtt_max: 5000,              // 5ms (微秒)
            rtt_sum: 12000,
            rtt_count: 3,
            rrt_max: 100000,            // 100ms (最慢那个)
            rrt_sum: 180000,            // 50+30+100 ms
            rrt_count: 3,
            // 其他默认 0
        },
        performance: Performance {
            // 全部 0
        },
        anomaly: Anomaly {
            l7_server_error: 1,         // ★ 1 个 5xx 错误
            // 其他 0
        },
        flow_load: FlowLoad {
            load: 3,
            flow_count: 0,
        },
    }),
}
```

**关键观察**：

1. **3 条原始 flow 合成 1 条 Document**——因为 StashKey 相同
2. **指标都已累加**：`l7_request: 3`、`rrt_sum: 180000`
3. **能算平均**：avg_rrt = 180000 / 3 = 60ms
4. **能看错误**：`l7_server_error: 1`
5. **不能算 P99**——只有 sum/count/max
6. **同时还会生成另一条 SINGLE_IP_PORT_APP Document**（服务器视角），只有 `ip` 没有 `ip1`

##### Document 与 ClickHouse 表的对应

Server 端 Ingester 收到 Document 后，根据 `tagger.code` 决定写入哪张 ClickHouse 表：

| 维度组合 | code 包含 | ClickHouse 表 |
|---------|----------|--------------|
| **Single L4** | `IP` 但不含 `L7_PROTOCOL` | `flow_metrics.network` |
| **Single L7** | `IP` + `L7_PROTOCOL` | `flow_metrics.application` |
| **Edge L4** | `IP_PATH` 但不含 `L7_PROTOCOL` | `flow_metrics.network_map` |
| **Edge L7** | `IP_PATH` + `L7_PROTOCOL` | `flow_metrics.application_map` |
| **ACL** | `ACL_GID` | `flow_metrics.traffic_policy` |

每张表又有 `_1s` 和 `_1m` 两个粒度的子表，对应秒级和分钟级。

ClickHouse 把 Tagger 的字段映射为**维度列**，把 Meter 的字段映射为**指标列**：

```sql
-- flow_metrics.application_map.1s 表（简化）
CREATE TABLE application_map_1s (
    time DateTime,                          -- 来自 Document.timestamp

    -- Tagger 字段（维度列）
    agent_id UInt16,
    ip_0 IPv4,                              -- ip
    ip_1 IPv4,                              -- ip1
    l3_epc_id_0 Int16,
    l3_epc_id_1 Int16,
    server_port UInt16,
    protocol UInt8,
    l7_protocol UInt8,
    gpid_0 UInt32,
    gpid_1 UInt32,

    -- Meter 字段（指标列）
    packet_tx UInt64,    packet_rx UInt64,
    byte_tx UInt64,      byte_rx UInt64,
    l7_request UInt32,   l7_response UInt32,
    rtt_sum UInt64,      rtt_count UInt32,    rtt_max UInt32,
    rrt_sum UInt64,      rrt_count UInt32,    rrt_max UInt32,
    l7_client_error UInt32,
    l7_server_error UInt32,
    -- ...
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (time, l3_epc_id_0, ip_0, ...);
```

查询样例：

```sql
-- 查 14:50 这一分钟，10.2.2.2:80 的 HTTP 错误率
SELECT
    sum(l7_server_error) as errors,
    sum(l7_request) as total,
    errors / total as error_rate
FROM flow_metrics.application_1m
WHERE time >= '2024-04-08 14:50:00' AND time < '2024-04-08 14:51:00'
  AND ip = '10.2.2.2'
  AND server_port = 80
  AND l7_protocol = 20    -- HTTP1
```

#### 11.13.7 一句话总结

> **DeepFlow Collector 用的是固定时间窗口（Tumbling Window），1s 和 60s 两种粒度硬编码不可调，但秒级窗口可以整体启用/禁用。聚合的本质是：在窗口内把每条 Flow 按 9 种维度组合扇出成多个 Document（StashKey 用 u128 位图编码维度组合 + 维度值），相同 StashKey 的 Document 通过 sequential_merge 合并。开启秒级聚合对 Agent 本身的影响很小（CPU +1% 单核、RSS +30 MB），但会让网络上行翻 5-7 倍、ClickHouse 写入翻 ~145 倍、存储翻 5-30 倍。这是 DeepFlow 性能模型的关键认知：采集成本（Stage 1-3）和输出成本（Stage 4-5）是解耦的，运维调优时不要混淆这两类开关。**

### 11.14 Stage 5 深度剖析：UniformSender + 数据完整性

本节深入剖析 Stage 5 的 UniformSender——它的高性能设计、`OverwriteQueue` 背压机制、以及一个非常重要的工程认知：**限流对分布式 trace 完整性的影响**。

#### 11.14.1 Sender 是直接发往 Ingester 吗？

**是的，直接通过裸 TCP 长连接发到 Ingester，没有任何中间代理。** 端口默认 **30033**。

源码 `@agent/src/sender/uniform_sender.rs:386-395`：

```rust
impl Connection {
    pub fn new() -> Self {
        Self {
            tcp_stream: None,
            reconnect_interval: 10,
            dest_ip: "127.0.0.1".to_string(),
            dest_port: 30033,                   // ★ 默认端口
            reconnect: false,
            last_reconnect: Duration::ZERO,
        }
    }
}
```

源码 `uniform_sender.rs:587`：

```rust
conn.tcp_stream = TcpStream::connect((conn.dest_ip.clone(), conn.dest_port)).ok();
```

| 维度 | 实际情况 |
|------|---------|
| **协议** | **裸 TCP**（不是 gRPC，不是 HTTP）|
| **端口** | 30033（数据上报）vs 20035（管理面 gRPC，完全独立的两条链路）|
| **方向** | Agent → Ingester 单向 |
| **传输** | 无 TLS 默认（除非用 secure_transport）|
| **中间代理** | 无 |

#### 11.14.2 8 项高性能优化

##### 优化 1：256 KB 攒批 buffer

源码 `@agent/src/sender/uniform_sender.rs:159`：

```rust
impl<T: Sendable> Encoder<T> {
    const BUFFER_LEN: usize = 256 << 10;       // 256 KB

    pub fn cache_to_sender(&mut self, s: T) {
        // 第一个 item 时先写 header
        if self.buffer.is_empty() {
            self.set_msg_type(&s);
            self.add_header();
        }
        // 写 4 字节长度前缀
        let offset = self.buffer.len();
        self.buffer.extend_from_slice([0u8; 4].as_slice());
        // 写实际 protobuf 编码
        match s.encode(&mut self.buffer) {
            Ok(size) => self.buffer[offset..offset + 4]
                .copy_from_slice((size as u32).to_le_bytes().as_slice()),
            Err(e) => debug!("encode failed {}", e),
        };
    }
}
```

**关键设计**：

- 每个 Encoder 持有一块 256 KB 的可重用 buffer
- Document 以 length-prefixed protobuf 格式连续追加
- buffer 满或定时器到才 flush
- buffer 复用：每次 flush 后 `reset_buffer()`，不释放底层 Vec

##### 优化 2：双触发 flush（满 / 10 秒）

源码 `uniform_sender.rs:746-789`：

```rust
match self.input.recv_all(...) {
    Ok(_) => {
        // guaranteed to be sent every 10 seconds
        if start_cached.elapsed() >= Duration::from_secs(10) {
            start_cached = Instant::now();
            self.cached = false;       // ★ 强制下次 flush
        }
        // ...
    }
    Err(Error::Timeout) => {
        self.flush_encoder(&config);   // ★ 队列空闲也 flush
    }
    // ...
}
```

三条 flush 路径：

1. **buffer 满 256 KB** → 立即 flush
2. **超过 10 秒没 flush** → 强制 flush（防止低流量场景下数据滞留）
3. **channel timeout (3 秒空闲)** → 也 flush

经典的 size-based + time-based 双触发模式。

##### 优化 3：zstd 整批压缩

源码 `@agent/src/trident.rs:399-418`：

```rust
pub enum SenderEncoder {
    #[num_enum(default)]
    Raw = 0,
    Zstd = 3,
}

impl SenderEncoder {
    pub fn encode(&self, encode_buffer: &[u8], dst_buffer: &mut Vec<u8>) -> std::io::Result<()> {
        match self {
            SenderEncoder::Zstd => {
                let mut encoder = ZstdEncoder::new(dst_buffer, 0)?;
                encoder.write_all(&encode_buffer)?;
                encoder.finish()?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
```

源码 `uniform_sender.rs:227-247` 的压缩流程：

```rust
pub fn compress_buffer(&mut self) {
    self.compressed_buffer.clear();
    let buffer_len = self.buffer_len();
    match SenderEncoder::from(self.header.encoder).encode(
        &self.buffer[Header::HEADER_LEN..],     // ★ 跳过 header，只压缩 payload
        &mut self.compressed_buffer,
    ) {
        Ok(_) => {
            self.buffer.truncate(Header::HEADER_LEN);
            self.buffer.extend_from_slice(&self.compressed_buffer);
        }
        Err(e) => error!("compression failed {}", e),
    };
}
```

**关键设计**：

- 压缩在 256 KB 整批数据上做，不是单条 Document（压缩率高得多）
- 不压缩 header，让 Ingester 能直接读 header 判断类型
- 压缩级别 0（zstd 默认级别），延迟最低
- 实测压缩率通常 3-10 倍

##### 优化 4：三种连接共享模式

源码 `uniform_sender.rs:362-371`：

```rust
lazy_static! {
    static ref GLOBAL_CONNECTION: Arc<Mutex<Connection>> = Arc::new(Mutex::new(Connection::new()));
}

enum ConnectionType {
    Global,         // ★ 默认：全局共享一个 TCP 连接
    PrivateShared,  // 共享子集（按数据类型分组）
    Private,        // 每个 sender 独占
}
```

| 模式 | 特点 | 用途 |
|------|------|------|
| **Global** | 整个 Agent 进程共享一个 TCP 连接 | 默认。所有 Sender 都用同一个 socket |
| **PrivateShared** | 同类型 sender 共享 | 中等并发场景 |
| **Private** | 每个 sender 一个连接 | 高并发场景，开 `multiple_sockets_to_ingester: true` |

**默认 Global 的好处**：节省连接数、TCP 重用、简化 Ingester 端处理。

##### 优化 5：长连接 + 自动重连

源码 `uniform_sender.rs:566-616`：

```rust
if conn.reconnect || conn.tcp_stream.is_none() {
    // 控制重连频率（默认 10 秒）
    if conn.last_reconnect + Duration::from_secs(conn.reconnect_interval as u64) > now {
        return;
    }
    conn.tcp_stream = TcpStream::connect((conn.dest_ip.clone(), conn.dest_port)).ok();
    if let Some(tcp_stream) = conn.tcp_stream.as_mut() {
        // 设置 3 秒写超时
        tcp_stream.set_write_timeout(Some(Duration::from_secs(Self::TCP_WRITE_TIMEOUT)))?;
        // ...
    }
}
```

| 设计 | 作用 |
|------|------|
| **TCP 长连接** | 一次握手反复使用，避免每个 batch 的 3-way handshake |
| **重连间隔 10 秒** | 防止 Ingester 故障时疯狂重连 |
| **写超时 3 秒** | 避免网络问题让 Sender 永久阻塞 |

##### 优化 6：漏桶限流（Waiting / Drop 两种策略）

源码 `uniform_sender.rs:682-717`：

```rust
fn is_traffic_overflow(&mut self, config: &SenderConfig) -> bool {
    if self.max_throughput_mbps == 0 {
        return false;       // 0 表示不限速
    }
    let mut overflow = false;
    if config.ingester_traffic_overflow_action == TrafficOverflowAction::Waiting {
        // ★ Waiting 模式：阻塞等待
        let mut wait_times = 0;
        while !self.leaky_bucket.acquire(self.encoder.buffer_len() as u64)
            && wait_times < MAX_WAIT_TIMES   // 100 次
        {
            wait_times += 1;
            thread::sleep(Duration::from_millis(20));
            self.counter.waited.fetch_add(1, Ordering::Relaxed);
        }
        if wait_times == MAX_WAIT_TIMES {
            overflow = true;
        }
    } else {
        // ★ Drop 模式：超限直接丢
        if !self.leaky_bucket.acquire(self.encoder.buffer_len() as u64) {
            overflow = true;
            self.counter.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    // ...
}
```

| 模式 | 行为 | 适用场景 |
|------|------|---------|
| **Waiting** | 超限时阻塞等待令牌（最多重试 20ms × 100 次 = 2 秒）| 默认。容忍延迟，尽量不丢 |
| **Drop** | 超限直接丢弃，增加 `dropped` 计数 | 严格保护带宽 |

##### 优化 7：19 字节自定义二进制头

源码 `uniform_sender.rs:109-147`：

```
0          8          16         24         32         40         48         56         64
+----------+--------------------------------+----------+----------+----------+----------+
| frame_size                                | msg_type | version             | encoder  |
+----------+--------------------------------+----------+----------+----------+----------+
| team_id                                   | orgnization_id      | rsvd_1              |
+---------------------+----------+----------+---------------------+---------------------+
| agent_id            | rsvd_2   |
+--------------------------------+
```

**为什么不用 protobuf header**：

| 设计 | 收益 |
|------|------|
| **固定 19 字节** | Ingester 解析 header 是 O(1) 字节读取 |
| **`frame_size` 在最前** | 按帧切分数据流 |
| **`encoder` 在 header 内** | Ingester 不解压就知道是不是压缩的 |
| **`msg_type` 标识载荷类型** | 一个 TCP 连接混跑多种数据类型 |
| **多租户字段** | 单 Ingester 服务多个团队 |

经典的 TLV / 自描述帧设计——header 自描述、payload 按需解析。

##### 优化 8：批量 channel 读取

源码 `uniform_sender.rs:742-744`：

```rust
match self.input.recv_all(
    &mut batch,
    Some(Duration::from_secs(Self::QUEUE_READ_TIMEOUT)),
) {
```

`recv_all` 一次拉走多条消息，不是一条一条读，显著降低 channel 同步开销。

#### 11.14.3 OverwriteQueue：覆盖式队列（不是阻塞队列）

这是 DeepFlow 的一个**关键设计选择**——上下游之间的 channel 不是传统的阻塞队列，而是 `OverwriteQueue`（覆盖式队列）。

源码 `@agent/crates/public/src/queue/overwrite_queue.rs:115-142`：

```rust
// queue full
if end - start + count > self.size {
    let _lock = self.reader_lock.lock().unwrap();
    let start = self.start.load(Ordering::Acquire);
    // ...
    let free_space = self.size - (end - start);
    if free_space < count {
        let to_overwrite = count - free_space;
        for i in 0..to_overwrite {
            self.buffer
                .add((start + i) & (self.size - 1))
                .drop_in_place();          // ★ 直接 drop 旧消息
        }
        self.start.store(
            (start + to_overwrite) & (2 * self.size - 1),
            Ordering::Release,
        );
        self.counter
            .overwritten
            .fetch_add(to_overwrite as u64, Ordering::Relaxed);    // ★ 记录被覆盖数
    }
}
```

**关键行为**：

1. **send() 永远不阻塞**：上游永远能成功发送
2. **队列满时丢弃最旧的数据**（FIFO 丢弃）
3. **`overwritten` 计数器**记录丢弃数量，可被监控
4. **被覆盖对象通过 `drop_in_place()` 析构**，避免内存泄漏

队列容量配置 `@server/agent_config/template.yaml:7977`：

```yaml
outputs:
  flow_metrics:
    tunning:
      # range: [65536, 64000000]
      sender_queue_size: 65536       # ★ 默认 65536 条
```

#### 11.14.4 Waiting 模式的实际行为

如果 Sender 因为限流 sleep，上游会被阻塞吗？**不会**。让我用时间线说明：

```
T=0   Stage 4 持续往 OverwriteQueue 送数据
      ↓
T=0   Sender 线程: recv_all() 拉走一批，进入 for loop 处理
      ↓
T=10ms 处理到第 N 条时，encoder buffer 满 256 KB
      ↓
T=10ms flush_encoder() → compress → send_buffer()
      ↓
T=10ms send_buffer() 进入 is_traffic_overflow 检查
      ↓
T=10ms leaky_bucket.acquire() 失败（带宽超了）
      ↓
T=10ms Sender 线程开始 sleep(20ms)
      │
      │  ★ 此时 Sender 线程被卡住，不调用 recv_all
      │  ★ Stage 4 仍在 send，OverwriteQueue 持续被填充
      │
T=30ms 第 1 次重试 — 还是失败 → sleep(20ms)
      ...
T=2010ms 第 100 次重试，仍失败 → wait_times == MAX_WAIT_TIMES → overflow=true
      │
T=2010ms 触发 Exception::DataBpsThresholdExceeded
      │
T=2010ms send_buffer() return（数据没发出去）
      │
T=2010ms ★ flush_encoder 继续执行 reset_buffer() — 这 256 KB 直接丢了！
      │
T=2010ms Sender 继续下一轮 for loop / recv_all
      │  此时 OverwriteQueue 里多了 60K 条新数据
      │  期间被覆盖的旧数据 → counter.overwritten 增加
```

**关键事实**：

1. **Waiting 模式不会让上游阻塞**——Sender 在自己线程里 sleep
2. **数据积压在 OverwriteQueue 里**，满了开始覆盖最旧的
3. **Sender 内部那 256 KB batch 在限流失败后也会被 reset_buffer 丢掉**！

源码 `uniform_sender.rs:539-552` 揭示了这个微妙行为：

```rust
fn flush_encoder(&mut self, config: &SenderConfig) {
    if self.encoder.buffer_len() > 0 {
        // ...
        self.send_buffer(config);
        self.encoder.reset_buffer();   // ★ 不管发没发出去都 reset
    }
}
```

#### 11.14.5 不会内存爆炸的原因

整条数据流路径上**没有任何无界 buffer**：

```
                                    最大占用     单条大小    最大内存
─────────────────────────────────────────────────────────────────
Stage 4 stash HashMap (network 1m)  几千 entry  ~700 B      ~3-5 MB
Stage 4 stash HashMap (其他类型)    类似        ~700 B      ~10 MB
                                                              
OverwriteQueue × N 个                                        
  ├─ flow_metrics doc                65536 条   ~700 B      ~45 MB
  ├─ l4 flow log                     65536 条   ~1 KB       ~64 MB  
  ├─ l7 flow log                     65536 条   ~1 KB       ~64 MB
  └─ 其他类型                                                ~50 MB
                                                              
Sender encoder buffer × N            256 KB     —           ~10 MB
                                                              
─────────────────────────────────────────────────────────────────
                                                      合计  ~250 MB 上限
```

**这就是为什么 DeepFlow Agent 的 RSS 始终在 200-300 MB 量级**——所有 buffer 容量都是固定的。

#### 11.14.6 与传统背压的对比

| 设计 | 上游行为 | 内存风险 | 数据完整性 |
|------|---------|---------|----------|
| **传统阻塞 channel + 背压** | send() 阻塞 | 没有 | 100%（永不丢）|
| **无界队列 + 异步消费** | send() 永不阻塞 | 高（OOM）| 100%（直到 OOM）|
| **DeepFlow OverwriteQueue + 漏桶** | send() 永不阻塞 | 无（容量固定）| 不保证（覆盖最旧的）|

DeepFlow 选择第 3 种的原因：

| 决策 | 理由 |
|------|------|
| **Stage 4 不能阻塞** | 阻塞会反过来阻塞 Stage 3，最终丢网络包——更严重的问题 |
| **OOM 不能容忍** | Agent OOM 会被 K8s/cgroup 杀掉，所有观测能力中断 |
| **覆盖最旧的而非最新的** | 监控数据"越新越有价值" |
| **数据丢失要可观测** | overwritten/dropped/waited 计数器让运维知道发生了什么 |

#### 11.14.7 限流对分布式 Trace 完整性的影响

**这是一个非常严重的问题，必须正视**。

##### 危险场景：中间跳丢失导致链路断裂

考虑分布式调用：A → B → C，如果 B 的 log 因为限流丢失，会发生什么？

```
A → B → C 这条 trace

A 丢: 看不到入口，但 B → C 还在
B 丢: ★ 链路断成两段（A 独立，C 独立）
C 丢: 看不到末端，但 A → B 还在
```

**任何中间跳的丢失都会导致链路断裂**——而中间跳恰好是最容易出问题的（一个 B 跳故障可能引起大流量从而触发限流）。

##### 区分两种情况

| 问题 | 性质 | 严重程度 |
|------|------|---------|
| **网络延迟不一致** | 短暂的"最终一致性"问题 | 🟡 不可怕，自然恢复 |
| **限流真正丢数据** | 永久性的数据丢失 | 🔴 真问题 |

**网络延迟不一致**不会导致丢失——只是 B 的数据晚到了几秒。Querier 用时间窗口扩散查询，"等几秒再查"就能看到完整链路。这是分布式系统的标准最终一致性。

**限流丢数据**才是真问题——数据真的丢了，无法恢复。

##### 有 trace_id vs 无 trace_id 的对比

**无 trace_id 时**（只能靠 tcp_seq）：

```
A 的 log:                        C 的 log:
  resp_tcp_seq=100                 req_tcp_seq=200
  protocol=HTTP                    protocol=MySQL

Server 查询时:
  - 用 tcp_seq=100 → 只能找到 A 自己
  - 用 tcp_seq=200 → 只能找到 C 自己
  - 没有任何键能把 A 和 C 关联起来

UI 视图:
  ┌─────────┐                    ┌─────────┐
  │ A: HTTP │                    │ C: SQL  │
  └─────────┘                    └─────────┘
  ★ 两条独立的 log，看不出它们是同一请求
  ★ 你可能根本意识不到丢了 B
```

**有 trace_id 时**：

```
A 的 log:                        C 的 log:
  trace_id=abc                     trace_id=abc          ← ★ 共同标识
  resp_tcp_seq=100                 req_tcp_seq=200
  span_id=1                        span_id=3
  parent_span_id=                  parent_span_id=2      ← ★ 暗示有 B

Server 查询时:
  - WHERE trace_id='abc' → 找到 A 和 C
  - parent_span_id=2 但找不到 span_id=2 → 推断 B 丢了

UI 视图:
  ┌─────────┐    ┌──────────────┐    ┌─────────┐
  │ A: HTTP │ →  │ B: 未知节点  │ →  │ C: SQL  │
  │ /api/x  │    │ ⚠ 数据缺失  │    │ SELECT  │
  └─────────┘    └──────────────┘    └─────────┘
                     ↑
                 能看到"中间有东西丢了"
                 但 B 内部细节全部丢失
```

##### trace_id 能做和不能做的

| 维度 | 无 trace_id | 有 trace_id |
|------|------------|-------------|
| **能否关联 A 和 C** | ❌ | ✅ |
| **能否知道有 B** | ❌ 你以为 A 是叶子 | ✅ 看到"未知中间节点" |
| **能否看到 B 的耗时** | ❌ | ❌ 真的丢了 |
| **能否看到 B 的 SQL/请求体** | ❌ | ❌ 真的丢了 |
| **能否看到 B 的错误码** | ❌ | ❌ 真的丢了 |

**核心认知**：

```
trace_id 救的是"链路结构"，不是"节点细节"
trace_id 是减损手段，不是无损手段
```

##### 服务拓扑推断的局限

DeepFlow 在 Server 端的查询层有"自动服务拓扑"能力，能从历史数据中**统计性地**推断出"通常 A 会调 B"。即使本次 B 丢了，UI 也能从拓扑里把 B 灰显出来。

但这是**统计推断不是事实**：

| 维度 | 这一次的真实数据 | 历史拓扑推断 |
|------|---------------|-------------|
| B 这次的耗时 | ❌ 没有 | ❌ 没有 |
| B 这次的 SQL | ❌ 没有 | ❌ 没有 |
| B 这次的错误 | ❌ 没有 | ❌ 没有 |
| B 是不是真的存在过 | ❌ 不能确定 | 假设它一直存在 |

**最大的危险**：如果**正是 B 这次出问题**（重路由、跳过、故障），统计推断会**掩盖真实的故障**——你看到的"A → B → C"其实这次根本没经过 B。

##### DeepFlow 的多层防御

| 防御层 | 机制 |
|--------|------|
| **分类型限流** | metrics / l4_log / l7_log 各有独立限流，trace 主要靠 l7_log，可以单独调宽 |
| **多键冗余** | 6 种关联键（trace_id > x_request_id > syscall_trace_id > tcp_seq）互为兜底 |
| **OverwriteQueue 优先丢旧** | 用户最关心新数据，丢旧数据感知好 |
| **延迟容忍 + 时间窗口扩散** | 应对短暂延迟不一致 |
| **不阻塞上游** | 单 Agent 限流不会反向影响 Stage 1-3 的网络包采集 |

##### 银行场景的实战建议

| 目标 | 措施 |
|------|------|
| **保证链路结构能看到** | 应用集成 OpenTelemetry SDK 注入 trace_id（即使 B 丢，A→C 仍能关联）|
| **保证关键节点的细节不丢** | 关键服务的 Agent 单独部署 + 高 throttle 阈值 + 监控 dropped 计数 |
| **故障时能复盘 trace** | 应用日志做兜底（别只依赖 DeepFlow）|
| **避免被"统计假象"误导** | 看 trace 视图时区分"本次真实数据"和"历史拓扑推断" |

##### 关键监控指标

| 指标 | 告警阈值 | 含义 |
|------|---------|------|
| `sender.dropped` 持续 > 0 | 任何非零值 | trace 数据正在丢失 |
| `OverwriteQueue.overwritten` 持续 > 0 | 任何非零值 | log 队列被覆盖 |
| `sender.waited` 持续 > 1000/s | 持续 5 分钟 | Sender 经常被限流 |
| `Exception::DataBpsThresholdExceeded` | 发生 | 限流真的失效了 |

##### 一个让人不舒服的事实

**没有任何一个分布式追踪系统能在所有情况下保证 100% 的 trace 完整性**——这不是 DeepFlow 的局限，是分布式追踪本身的根本性挑战：

| 系统 | 丢失风险 |
|------|---------|
| **OpenTelemetry** | 取决于 SDK 采样率和 Collector 可用性 |
| **Jaeger** | 客户端到 Agent 用 UDP（更容易丢）|
| **Zipkin** | 依赖应用埋点 |
| **DataDog APM** | 商业版本也有限流和丢数据 |
| **DeepFlow** | OverwriteQueue 覆盖 + Sender 限流丢失 |

区别只在于：**怎么丢、丢多少、丢的时候有没有告警**。DeepFlow 的优势是**多键冗余 + 透明的丢失计数**——你能知道什么时候在丢、丢了多少。

#### 11.14.8 一句话总结

> **Stage 5 的 Sender 用裸 TCP 长连接直发 Ingester（端口 30033），核心是 8 项优化：256 KB 攒批 buffer、双触发 flush（满/10s）、zstd 整批压缩、全局共享 TCP 连接、长连接 + 自动重连、漏桶限流、19 字节自定义二进制头、批量 channel 读取。上下游之间用 OverwriteQueue 而非阻塞队列——队列满时覆盖最旧数据（FIFO 丢失），单 Agent 总内存上限 ~250 MB，绝不会 OOM。代价是限流时真的会丢数据（OverwriteQueue 覆盖 + 256 KB batch reset），其中中间跳丢失对分布式 trace 危害最大。trace_id 能保住链路结构（即使 B 丢，A 和 C 仍能关联到同一条 trace），但不能补回 B 的细节（耗时、SQL、错误码真的没了）。DeepFlow 是 Best Effort + Final Consistency 系统，不是 Exactly Once——所有丢失都通过 overwritten/dropped/waited 三个计数器暴露给运维，要保证关键 trace 不丢，唯一的办法是应用注入 OpenTelemetry trace_id。**

### 11.15 采集流程的瓶颈与优化（原 11.4）

**常见瓶颈**：

| 瓶颈 | 表现 | 优化方案 |
|------|------|---------|
| **Dispatcher 丢包** | 网卡链路速度 > 处理速度 | 升级到 `local-plus` 或 DPDK；增加 cpu affinity |
| **FlowMap 内存溢出** | 大象象流（100k+ 并发流）| 降低 flow timeout；启用 aggressive GC |
| **Collector 延迟** | 聚合窗口卡住 | 增加 Collector 线程数；优化 StashKey 哈希算法 |
| **Sender 堆积** | Document 队列满 | 增加压缩率；启用限流（LeakyBucket） |

如需进一步理解 Agent，可沿以下方向深入：

1. **采集器对比分析**：Dispatcher vs EbpfCollector vs IntegrationCollector，各自的优劣场景
2. **流聚合算法**：QuadrupleGenerator 的 1s/1m 聚合窗口实现，与 ClickHouse 表的对应关系
3. **配置热更新机制**：arc-swap 的无锁更新、broadcast notification 的事件分发
4. **性能瓶颈定位**：使用 flamegraph/perf 分析 CPU 热点和锁争用
5. **协议支持扩展**：如何新增一个 L7 协议解析器（e.g., gRPC、WebSocket）
6. **三块模块的隔离和重构**：如何进一步解耦块 1、块 2、块 3，提升可测试性
7. **Standalone 模式的扩展**：如何在无 Controller 场景下支持更多功能

---

**文档生成日期**: 2026-04-08
**文档版本**: 13.0（新增 Stage 5 深度剖析：Sender 8 项优化、OverwriteQueue 覆盖式背压、限流对 trace 完整性的影响、trace_id 减损机制的局限）
