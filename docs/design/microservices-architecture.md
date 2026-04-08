# DeepFlow 微服务架构

本文档描述 DeepFlow 的微服务组件组成、调用链路和数据流。

## 1. 组件清单

| 组件名称 | 语言 | 部署模式 | 职责 |
|---------|------|---------|------|
| **deepflow-agent** | Rust | 独立二进制 (DaemonSet) | 节点级采集：eBPF / AF_PACKET / OTLP 采集网络、应用流量和性能数据 |
| **deepflow-server** | Go | 独立二进制 (StatefulSet/Deployment) | **单进程包含三个子模块**：管理面 (controller) + 数据面 (ingester) + 查询面 (querier) |
| └ controller (子模块) | — | —— | 元数据管理、Agent 配置下发、与 agent 双向通信 |
| └ ingester (子模块) | — | —— | 接收 agent 数据、解码、标签富化、写入 ClickHouse |
| └ querier (子模块) | — | —— | HTTP/SQL/PromQL 查询接口、时序数据查询 |
| **deepflow-ctl** | Go | 独立二进制 (CLI) | 运维命令行工具，与 controller HTTP API 交互 |
| **message** | Protobuf | 共享库 | 不是运行时组件，定义 agent ↔ controller / ingester 通信协议 |

**关键点**：`controller / ingester / querier` 在同一进程启动，部署上是一个二进制、一个 Pod/Deployment，不是三个独立的微服务。

参考：@server/cmd/server/main.go

## 2. 各组件职责详述

### 2.1 deepflow-agent（采集层）

**位置**：@agent/src/main.rs

**核心职责**：
- 通过 eBPF syscall hook（内核态）采集网络流、协议识别、CPU 堆栈；通过 AF_PACKET（用户态）采集网络流量
- 支持 OTLP 集成（otel-collector、Java agent），Prometheus remote-write，Pyroscope profiling 摄入
- 本地流聚合（5s、1m 时间窗）生成 flow metrics 和 flow logs
- 通过私有二进制格式编码观测数据并推送至 ingester
- 与 controller 维持 gRPC 长连接拿配置、下发采集策略

**出站连接**：
- gRPC Synchronizer → controller (port 20035) 双向：Sync、Push、GenesisSync、Upgrade、Plugin
- TCP (protobuf) → ingester (port 20033) 单向推送：metrics、flow_log、ext_metrics、event、profile

参考：@agent/src/ebpf/，@agent/crates/

### 2.2 deepflow-server.controller（管理面）

**位置**：@server/controller/

**核心职责**：
- 维护全央行元数据：K8s 资源、云平台资源、主机、虚拟机、网络接口、IP，存储在 MySQL
- **gRPC Synchronizer 服务** (port 20035)：与 agent 双向通信
  - `Sync` / `Push`：Agent 注册、配置下发
  - `GenesisSync`：Agent 上报本地 IP/MAC 地址和 K8s 资源（若运行在 K8s 节点）
  - `GetKubernetesClusterID`：为 K8s 集群生成 cluster-id
  - `Upgrade` / `Plugin`：Agent 升级和插件管理
- **Genesis 子模块**：接收 agent  上报的资源变化，合并跨多个 agent 的元数据
- **Cloud 子模块**：与云平台 API 同步资源，处理云上虚拟机、子网等
- **Recorder 子模块**：将元数据落库到 MySQL
- **TagRecorder**：基于元数据生成标签注入规则
- **HTTP API** (port 20417)：供 deepflow-ctl 和 querier 调用
  - RBAC、资源管理、配置页面 API
  - 提供元数据查询给 querier (tag 翻译)

参考：@server/controller/grpc/synchronizer/service.go，@server/controller/http/

### 2.3 deepflow-server.ingester（数据面）

**位置**：@server/ingester/

**核心职责**：
- **信息接收** (port 20033)：监听 TCP，接收来自 agent 的观测数据（私有 protobuf 格式）
- **多类型解码器**：
  - flow_metrics.decoder：网络流指标（吞吐、包数、RTT、丢包）
  - flow_log.decoder：L4/L7 流日志（TCP、UDP、应用协议日志）
  - ext_metrics.decoder：扩展指标（Prometheus、Telegraf、StatsD）
  - event.decoder：事件日志（进程启停、系统性能 event）
  - profile.decoder：在线 CPU/内存 profiling
- **标签富化** (Tag Enrichment)：调用 controller 元数据 API，为数据打标签（Pod 名、Service、云资源标签）
- **批量写入 ClickHouse**：流聚合后向 ClickHouse 相应表追加數據
  - flow_metrics → flow_metrics
  - flow_log → flow_log
  - ext_metrics → ext_metrics
  - event → event
  - profile → profile (on_cpu、off_cpu)

参考：@server/ingester/ingester/ingester.go，@server/ingester/flow_log/

### 2.4 deepflow-server.querier（查询面）

**位置**：@server/querier/

**核心职责**：
- **HTTP API** (port 20416)：向外暴露查询接口
  - `/api/v1/**`：DeepFlow SQL 查询（自定义 SQL 方言，支持分布式追踪、性能剖析）
  - `/prom`：Prometheus PromQL 兼容接口（供 Grafana 等时序工具）
  - `/api/v1/event` 等：事件、profile 查询
- **查询引擎**：编译 DeepFlow SQL → ClickHouse 查询语句，执行 SQL 优化（列式存储优化）
- **元数据补全**：调用 controller HTTP API，获取标签翻译（IP → Pod 名、Service、标签）
- **多租户隔离**：基于 RBAC 和资源归属关系隔离数据

参考：@server/querier/router/，@server/querier/app/

### 2.5 deepflow-ctl（运维工具）

**位置**：@cli/

**核心职责**：
- 命令行工具，通过 HTTP REST 与 controller (port 20417) 交互
- 支持管理域、子域、云平台、Agent 版本库
- 创建、删除、更新资源（如创建 `Domain`、上传 Agent 版本）

参考：@cli/ctl/

## 3. 调用链路和通信协议

### 3.1 Agent ↔ Controller（管理面）

```
┌─────────────┐              ┌──────────────────────┐
│ deepflow-   │              │ deepflow-server      │
│ agent       │ ◄─gRPC:20035─►
│ (Rust)      │    Protobuf  │ .controller          │
│             │              │ .grpc.synchronizer   │
└─────────────┘              └──────────────────────┘

服务定义：message/agent.proto
方法列表：
  - GetKubernetesClusterID    (首次注册获取 cluster-id)
  - Sync                       (Agent 注册、上报 vtap-id)
  - Push                       (Controller 下发配置、ingester 地址)
  - GenesisSync                (Agent 上报本地 IP/MAC/K8s 资源)
  - Upgrade                    (Agent 升级指令)
  - Plugin                     (插件管理)

端口：20035 (TLS: 20135)
```

**典型流程**（参考 data-flow.md 第 3 节 "Agent Registration"）：
1. Agent 首次启动，调用 `GetKubernetesClusterID` 获取 cluster-id（若在 K8s 环境）
2. Agent 定期调用 `Sync` 上报本机 IP/MAC、vtap-id，获取 ConfigGroup
3. Agent 调用 `GenesisSync` 上报本机网卡、K8s Pod（若有 k8s_api_enabled）
4. Controller 将元数据存库到 MySQL，返回配置
5. Agent 根据配置调整 eBPF/采集策略；Controller 可通过 `Push` 实时下发策略更新

### 3.2 Agent → Ingester（数据面）

```
┌─────────────┐              ┌──────────────────────┐
│ deepflow-   │ TCP + pb     │ deepflow-server      │
│ agent       │ :20033 ──────►│ .ingester            │
│ (Rust)      │              │ (receiver.go:96)     │
└─────────────┘              └──────────────────────┘

协议：TCP + private protobuf format
定义：message/trident.proto

数据类型（多路上报）：
  - flow_metrics     : 网络流指标
  - flow_log         : TCP/UDP/应用层流日志
  - ext_metrics      : 扩展指标（Prom、Telegraf）
  - profile          : CPU/Memory profiling
  - event            : 系统 event（process start/stop）
  - telemetry        : 系统遥测（自身运行状态）

特点：
  - 单向推送
  - 支持多条并行连接（多 worker）
  - 背压控制（动态队列长度）
  - 本地先流聚合，减少上报量
```

### 3.3 Controller ↔ Ingester（内部通信）

```
┌──────────────┐
│ .controller  │        ╔════════════════╗
│              ├──────► ║ .ingester      ║
│ (元数据)      │ HTTP   ║ (Tag enrichment║
│              │ API    ║  会调)          ║
└──────────────┘        ╚════════════════╝

通信方式：HTTP API 调用，具体端点见 server/libs/tagrecorder

Tag Enrichment Pipeline：
  1. ingester 解码数据，得到 IP/MAC/Pod ID
  2. 调用 controller HTTP API 查询对应的资源标签
  3. 将标签字段拼接到数据行，写入 ClickHouse
```

### 3.4 Querier ↔ Controller（元数据查询）

```
┌──────────────┐
│ .querier     │        ┌──────────────┐
│              ├──HTTP──►│ .controller  │
│ (SQL 查询)    │ API    │ HTTP: 20417  │
│  :20416      │        │ (元数据翻译)  │
└──────────────┘        └──────────────┘

用途：
  - tag 翻译：IP → Pod、Service、云标签
  - 资源元数据：获取 VibIP 对应的实例信息
  - 监控范围验证：基于 RBAC 过滤可见数据
```

### 3.5 Querier ↔ ClickHouse（数据查询）

```
┌──────────────┐
│ .querier     │        ┌──────────────┐
│              ├──SQL───►│ ClickHouse   │
│              │ Native  │ :9000        │
│ :20416       │         │              │
└──────────────┘        └──────────────┘

ClickHouse 表：
  - flow_metrics     : 网络流指标时序
  - flow_log         : 应用层流日志
  - ext_metrics      : 外部指标（Prometheus）
  - event            : 事件日志
  - profile          : Profiling 堆栈
```

### 3.6 CLI → Controller（运维接口）

```
┌──────────────┐
│ deepflow-ctl │        ┌──────────────┐
│ (CLI工具)     │──HTTP──►│ .controller  │
│              │ :20417  │ HTTP API     │
└──────────────┘        └──────────────┘

支持操作：
  - 域 (Domain) 管理
  - 子域 (Sub-Domain) 管理
  - 云平台配置 (Cloud)
  - Agent 版本库 (VtapRepo)
  - RBAC 和用户管理
```

### 3.7 外部系统集成

```
┌─────────────┐
│ deepflow-   │
│ agent       │
│ 集成模块     │
└─────────────┘
      │
      ├─ otel-collector       ──OTLP──►┐
      │  otel-javaagent/sdk   ─OTLP──┐ │
      │  (Tracing 数据)               │ │
      │                              ▼ │
      ├─ Prometheus Server   ──prom-pb──┐
      │  Telegraf             ──influx──┤ agent Dispatcher
      │  (时序遥测)                    │  或 IntegrationCollector
      │                              │
      ├─ Pyroscope           ─http──┐
      │  (Profiling)               │
      │
      └─ StatsD              ────tb──┤
         (应用性能计数)              │
                                  │
                                  └─ Ingester 的各种 decoder
```

参考：@agent/src/ebpf/plugins/integration_skywalking/，@agent/crates/

## 4. 完整数据流（端到端）

### 4.1 观测数据采集到查询

```
┌───────────────────┐
│ 被监控主机/Pod    │
│                   │
│ Kernel / App      │
│   │ (syscall/     │
│   │  network)     │
│   ▼               │
│ deepflow-agent    │
│ ├─ eBPF Collector │
│ ├─ Dispatcher     │
│ ├─ FlowGenerator  │
│ └─ UniformSender  │
│   (encode pb)     │
└───────┬───────────┘
        │
        │ (2) TCP:20033 pb
        │
        ▼
┌──────────────────────────────────────┐
│ deepflow-server                      │
│ ┌────────────────────────────────┐  │
│ │ ingester                       │  │
│ │ ├─ flow_metrics.decoder        │  │
│ │ ├─ flow_log.decoder            │  │
│ │ ├─ ext_metrics.decoder         │  │
│ │ ├─ event.decoder               │  │
│ │ ├─ profile.decoder             │  │
│ │ │                              │  │
│ │ └─ tag_enrichment              │  │
│ │    (call controller HTTP API)  │  │
│ └────────────────────────────────┘  │
└──────────────┬───────────────────────┘
               │
               │ (3) ClickHouse native protocol
               │
               ▼
        ┌─────────────┐
        │ ClickHouse  │
        │ flow_*      │
        │ ext_metrics │
        │ event       │
        │ profile     │
        └──────┬──────┘
               │ (reversed)
               │
               ▼
        ┌──────────────┐
        │ querier      │
        │ HTTP:20416   │
        │ (SQL query)  │
        └──────┬───────┘
               │
               ▼
        User / Grafana
        (with labels translated
         from controller)
```

### 4.2 配置管理流程

```
controller (MySQL 元数据db)
    │
    │ (1) gRPC:20035 Push
    │ (server agent config)
    ▼
deepflow-agent
    ├─ 更新采集策略
    ├─ 启用/关闭 eBPF hooks
    ├─ 调整流聚合时间
    └─ 更新 ingester 连接地址

触发场景：
  - Agent 首次注册
  - 配置组 (ConfigGroup) 发生变化
  - Ingester 地址变更
  - Agent 版本升级
```

## 5. 关键文件导航

| 功能 | 文件路径 |
|-----|--------|
| Agent gRPC Synchronizer 定义 | @message/agent.proto |
| Agent 数据格式定义 | @message/trident.proto |
| Controller Synchronizer 实现 | @server/controller/grpc/synchronizer/service.go |
| Ingester 接收器主逻辑 | @server/ingester/ingester/ingester.go |
| Ingester Tag enrichment | @server/ingester/flow_log/ |
| Querier 路由入口 | @server/querier/router/query.go |
| Querier API handler | @server/querier/app/ |
| CLI 命令实现 | @cli/ctl/*.go |
| Agent 采集主入口 | @agent/src/main.rs |
| Agent eBPF 采集 | @agent/src/ebpf/ |
| Agent OTLP 集成 | @agent/crates/public/src/l7_protocol/opentelemetry.rs |

## 6. 未验证或未确认的技术点

1. **Agent 如何获得 Ingester 连接地址**
   - 推测：Controller 在 `Sync` 或 `Push` gRPC 响应中下发 ingester 地址列表
   - 未完全验证：需查看 `message/agent.proto` 的 `SyncResponse` 字段定义

2. **多个 Server 副本时的高可用**
   - 未发现明确的 etcd 选举机制
   - 推测：通过 MySQL `__lock` 表或 Redis 实现 leader 选举
   - 代码中有 election 包，但初始化细节未追踪

3. **Kafka 在 DeepFlow 中的角色**
   - L7 协议解析时识别 Kafka 消息格式，但 server 本身不消费 Kafka
   - Kafka 在 DeepFlow 中是被监控的对象，不是消息队列基础设施

4. **Querier 是否直接访问 ClickHouse，还是通过 Ingester**
   - 当前代码表明 Querier 直接连接 ClickHouse，不经过 Ingester
   - Ingester 仅作为数据入口，不提供查询路由

## 参考文档

- @docs/design/data-flow.md - 数据采集和流转详细流程图
- @docs/design/agent/agent.md - Agent 组件架构
- @docs/design/server/server.md - Server 组件架构
- @docs/design/cli/cli.md - CLI 工具
- @README.md - 项目顶层概述
- @server/README.md - Server 编译和运行指南
- @agent/README.md - Agent 编译和运行指南
