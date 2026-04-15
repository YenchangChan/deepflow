# DeepFlow Ingester 业务逻辑与架构

## 概述

Ingester 是 DeepFlow Server 的数据摄入核心，负责**接收 Agent 上报的各类遥测数据，经解码、平台数据富化后批量写入 ClickHouse，同时可选导出到外部系统**。

## 整体启动流程

入口：`@server/ingester/ingester/ingester.go` → `Start()`

```
Start()
  → 加载配置
  → 创建 TCP/UDP Receiver（默认端口 20033）
  → 初始化 PlatformData Manager（gRPC 连接 Controller）
  → 初始化 DFStatsd（指标采集）
  → Datasource Manager（HTTP API 端口 20106）
  → ClickHouse Schema 管理（ckissu）
  → 按优先级启动各子系统：
    FlowLog / FlowMetrics / ExtMetrics / Event /
    PCAP / Profile / Prometheus / AppLog
  → 最后启动 Receiver（防止消息无处理器）
```

## 统一处理模式

所有子系统遵循相同的流水线架构：

```
Receiver (TCP/UDP 20033)
    ↓  按 MessageType 分发
Decode Queue (N 个并行队列)
    ↓
Decoder (每队列一个，并行解码)
    ↓
Platform Data Enrichment (IP/MAC → 资源/Pod/Region 映射)
    ↓
DB Writer (批量写入 ClickHouse)
    ↓  (可选)
Exporters (OTLP / Prometheus / Kafka)
```

## 各子系统详情

### 1. Flow Log（流日志）

**目录**：`@server/ingester/flow_log/`

处理 L4/L7 流记录、应用 Span、事务日志：

| 消息类型 | 说明 |
|----------|------|
| `L4_FLOW_LOG` | 四层连接记录 |
| `L7_FLOW_LOG` | 七层应用流（HTTP、DNS、SQL、RPC 等） |
| `OPENTELEMETRY` | OTEL Trace Span |
| `SKYWALKING` | SkyWalking 协议 |
| `DATADOG` | Datadog APM Span |
| `PACKETSEQUENCE` | L4 包序列详情 |

**ClickHouse 库**：`flow_log`，表包括 `l4_flow_log_*`、`l7_flow_log_*`、`l4_packet_*`、`span_with_trace_id`

**关键处理**：解码 → 平台数据富化（加标签） → 限流（Throttler） → Span 去重 → 写入 + 导出

### 2. Flow Metrics（流指标）

**目录**：`@server/ingester/flow_metrics/`

处理 Agent 聚合后的网络流统计：

| 指标类型 | 说明 |
|----------|------|
| `vtap_flow_*` | 每流指标（packet、byte、duration、RTT） |
| `vtap_flow_port_*` | 端口级聚合 |
| `vtap_flow_edge_*` | 双向 src-dst 指标 |
| `vtap_acl_*` | 安全组指标 |

**处理链**：Unmarshaller 解码 protobuf → 平台数据富化（IP/MAC 映射到资源 ID） → 多时间窗口聚合（1s、1m） → 写入

### 3. External Metrics（外部指标）

**目录**：`@server/ingester/ext_metrics/`

| 消息类型 | 说明 |
|----------|------|
| `TELEGRAF` | Telegraf 插件指标 |
| `DFSTATS` | Agent 内部统计 |
| `SERVER_DFSTATS` | Server 端统计 |

**ClickHouse 库**：`ext_metrics`（外部指标）、`deepflow_admin`（管理指标，有平台富化）、`deepflow_tenant`（租户指标，无平台富化）

### 4. Event（事件）

**目录**：`@server/ingester/event/`

| 消息类型 | 说明 |
|----------|------|
| `RESOURCE_EVENT` | 资源生命周期事件（内存队列，非网络接收） |
| `FILE_EVENT` | 文件访问事件 |
| `K8S_EVENT` | Kubernetes 事件 |
| `ALERT_EVENT` | 告警通知 |
| `ALERT_RECORD` | 告警历史记录 |

**特点**：ResourceEvent 不走网络接收，从内存 OverwriteQueue 消费（由 Controller 的 Recorder 产生）

### 5. PCAP（抓包）

**目录**：`@server/ingester/pcap/`

- 处理 `RAW_PCAP` 消息，存储原始网络抓包数据
- 直接二进制存储到 ClickHouse，不做字段拆解
- 无需平台数据富化

### 6. Profile（性能剖析）

**目录**：`@server/ingester/profile/`

- 处理 CPU/内存 profiling 数据（火焰图、调用栈采样）
- 支持 Off-CPU profiling，可配置拆分粒度
- 关联应用实例（App Service Tag 富化）

### 7. Prometheus Metrics

**目录**：`@server/ingester/prometheus/`

**两阶段解码架构**：
- **Fast Decoder**：处理已知 label 定义的指标（快速路径）
- **Slow Decoder**：需要向 Controller 查询 label ID 的指标（延迟路径）

**特点**：动态 label 列分配、启动时请求全量 label 定义、label ID 转换检测

### 8. Application Log（应用日志）

**目录**：`@server/ingester/app_log/`

| 消息类型 | 说明 |
|----------|------|
| `SYSLOG` | 系统日志 |
| `AGENT_LOG` | Agent 内部日志 |
| `APPLICATION_LOG` | 应用产生的日志 |

三条并行 Logger 管道，共享 CKWriter 高效批量写入。

## 支撑子系统

### Receiver（接收层）

- TCP/UDP 双协议，默认端口 20033
- 自动解压（gzip、zlib、zstd）
- 6 级预分配 Buffer Pool（2K~512K），>512K 动态创建
- 按 VtapID/OrgID/TeamID 分流，消息序列号跟踪丢包

### Platform Data Enrichment（平台数据富化）

- gRPC 连接 Controller 获取实时元数据
- IP/MAC → 资源 ID、Pod、Host、Region、VPC 等映射
- 每个 Decoder 缓存 platform info 表

### Exporters（数据导出）

- **OTLP**：OpenTelemetry 标准 Trace 格式
- **Prometheus**：指标导出
- **Kafka**：消息流转发
- 支持 Tag 过滤和字段选择，Universal Tag Manager 管理 K8s 和自定义标签

### CK Monitor（ClickHouse 监控）

- 磁盘使用率阈值监控（默认 80%）
- 最小可用空间保障（默认 300GB）
- 按优先级自动清理过期表

### CK Issu（Schema 管理）

- 表创建、Schema 升级、数据迁移
- 冷存储策略管理
- 分布式 ClickHouse 副本处理

### Datasource Manager（数据源管理）

- HTTP API（端口 20106）运行时管理 ClickHouse 数据源
- 增删改保留策略（TTL、聚合函数）

## 数据流全景图

```
                        Agent (TCP/UDP 20033)
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Receiver        │
                    │  解压 · Buffer Pool │
                    │  VtapID/OrgID 分流  │
                    └────────┬────────────┘
                             │ 按 MessageType 分发
        ┌────────┬───────┬───┴───┬────────┬──────┬──────┬──────┐
        ▼        ▼       ▼       ▼        ▼      ▼      ▼      ▼
    FlowLog  FlowMetrics ExtMet  Event   PCAP  Profile Prom  AppLog
    (N队列)  (N队列)    (N队列) (队列)  (N队列) (N队列)(2阶段)(N队列)
        │        │       │       │        │      │      │      │
        ▼        ▼       ▼       ▼        ▼      ▼      ▼      ▼
     Decoder  Unmarsh  Decoder  Decoder  Decode Decode Fast/  Decode
     (并行)   (并行)   (并行)   (并行)   (并行) (并行) Slow   (并行)
        │        │       │       │        │      │      │      │
        └────────┴───────┴───┬───┴────────┴──────┴──────┴──────┘
                             │
                             ▼
                  Platform Data Enrichment
                  (gRPC ← Controller 元数据)
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         DB Writer       Exporters     Flow Tag
         (批量写入)    (OTLP/Prom/    (维度表
          ClickHouse    Kafka)        维护)
              │
              ▼
    ┌───────────────────────────────────┐
    │         ClickHouse                │
    │  flow_log · metrics · event      │
    │  ext_metrics · profile · app_log │
    │  prometheus · deepflow_admin     │
    └───────────────────────────────────┘
```

## ClickHouse 数据库全景

| 数据库 | 用途 | 关键表 |
|--------|------|--------|
| `flow_log` | 流记录和 Trace | `l4_flow_log_*`、`l7_flow_log_*`、`span_with_trace_id` |
| `metrics` | 时序指标 | `vtap_flow_*`、`vtap_flow_edge_*`、`vtap_acl_*` |
| `ext_metrics` | 外部指标 | Telegraf 和系统指标 |
| `deepflow_admin` | 管理指标 | 有平台富化的 Server 统计 |
| `deepflow_tenant` | 租户指标 | 多租户隔离指标 |
| `event` | 系统事件 | 资源/K8s/文件/告警事件 |
| `profile` | 性能剖析 | 调用栈和火焰图 |
| `app_log` | 应用日志 | Syslog/Agent/应用日志 |
| `prometheus` | Prometheus 指标 | 动态 label 存储 |

## 关键设计特点

| 特性 | 实现方式 |
|------|----------|
| **高吞吐** | N 队列并行解码 + Buffer Pool 零拷贝 + 批量写入 CK |
| **平台富化** | gRPC 获取 Controller 元数据，IP/MAC → 资源/Pod/Region |
| **灵活导出** | OTLP/Prometheus/Kafka 多协议，Tag 过滤与字段选择 |
| **容错** | 消息序列号跟踪丢包、CK 不可用时 StorageDisabled 降级 |
| **多租户** | 按 OrgID 隔离，Per-Org 数据库与追踪 |
| **存储管理** | 磁盘阈值自动清理、TTL 保留策略、冷存储分层 |

## 一句话总结

Ingester 的核心职责是：**通过 TCP/UDP 接收 Agent 上报的流日志、指标、事件、Trace、Profile 等多类遥测数据，经并行解码和平台数据富化后批量写入 ClickHouse，同时可选导出到 OTLP/Prometheus/Kafka 等外部系统**。
