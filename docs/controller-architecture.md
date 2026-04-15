# DeepFlow Controller 业务逻辑与架构

## 概述

Controller 是 DeepFlow Server 的控制面核心，负责**云资源采集、Agent 生命周期管理、配置下发、健康监控**。它支持两种部署模式：

- **Master Region Master Controller**：独占运行数据库迁移、资源 ID 管理、标签记录、健康检查、License 管理、Prometheus 编码等关键任务
- **Regional Controller**：为本 region 提供 API 服务和 Agent 同步
- **Standalone 模式**：单节点部署，无选举机制

## 整体启动流程

入口：`@server/controller/cmd/controller/main.go` → `controller.Start()`

```
Start()
  → HTTP Server
  → Leader Election（K8s 选主）
  → Database Migration（仅主控制器）
  → Metadb Init（按组织建库）
  → Redis / Statsd
  → Genesis（基础设施发现）
  → TagRecorder（标签字典）
  → Manager（云平台任务编排）
  → Trisolaris（Agent 配置下发）
  → Prometheus（指标编码缓存）
  → gRPC Services（Agent 通信）
  → Monitor（健康检查）
  → HTTP Router（REST API）
```

## 核心子系统

### 1. Election（选主）

**目录**：`@server/controller/election/`

- 基于 K8s leader election 机制，在主 region 中选出一个 master controller
- master 独占运行：健康检查、License 分配、资源清理、Prometheus 编码等
- 每 60s 检查选举状态，丢失主权立即取消独占任务
- Standalone 模式跳过选举
- 关键函数：`IsMasterController()` 返回选举状态
- 通过 `acquireTime` 确保跨 server 的版本号一致性

### 2. Manager + Cloud + Recorder（云资源采集链路）

这是 controller 最核心的数据采集链路。

**目录**：
- `@server/controller/manager/` — 全局任务编排
- `@server/controller/cloud/` — 云平台资源采集
- `@server/controller/recorder/` — 资源数据持久化

**架构**：

```
Manager（全局编排）
  → 从 DB 加载所有 domain（云平台）
  → 每个 domain 创建一个 Task = Cloud + Recorder

Cloud（采集）                    Recorder（入库）
  → 周期性调用云 API            → 接收 Cloud 的资源数据
  → 支持 AWS/阿里云/Azure 等    → 刷新到 metadb
  → K8s 集群有专门的            → 管理全局资源 ID
    KubernetesGatherTask        → 发布资源变更事件
```

**数据流**：

```
Manager.Start()
  → 加载 domains
  → 为每个 domain 创建 Task(Cloud + Recorder)
  → Task.Start()
    → Cloud.Start()：周期性资源采集
    → Cloud.run() 调用 platform API
    → domainRefreshSignal 触发
    → Task.startDomainRefreshMonitor()
    → Recorder.Refresh(target, cloudData)
    → Recorder.domainRefresher 更新 metadb
```

**Recorder 关键组件**：
- `Recorder` — 每个 domain 的资源写入器
- `Resource` — 全局资源管理器，包含：
  - `Cleaners` — 清理已删除/脏资源
  - `IDManagers` — 全局资源 ID 分配
- `Metadata` — 按组织的数据库抽象

**Cloud 支持的云平台**：AWS、阿里云、Azure、腾讯云等，各自实现在 `@server/controller/cloud/` 下的子目录中。

### 3. Genesis（基础设施发现）

**目录**：`@server/controller/genesis/`

- 从 K8s 集群和 Agent 上报中发现基础设施资源
- 提供 gRPC SynchronizerServer，Agent 通过此接口上报平台信息
- 数据存入 MySQL 或 Redis
- 5 分钟周期刷新缓存

**关键组件**：
- `GenesisSync` 接口 — 抽象同步策略
- `GenesisKubernetes` — K8s 资源采集
- `SynchronizerServer`（gRPC）— Agent 同步端点
- Store 实现：`store/sync/mysql/`、`store/sync/redis/`、`store/kubernetes/`

### 4. Trisolaris（Agent 配置下发）

**目录**：`@server/controller/trisolaris/`

- **按组织**维护 Agent、Controller、Analyzer 的元数据缓存
- Agent 通过 gRPC 拉取配置和拓扑信息

**关键组件**：
- `TrisolarisManager` — 管理每个组织的 Trisolaris 实例
- `Trisolaris` — 单组织的元数据和配置提供者
- `MetaData` — 拓扑元数据（网络、VPC、子网等）
- `VTapInfo` — Agent（采集器/探针）信息管理
- `NodeInfo` — 节点拓扑
- `KubernetesInfo` — K8s 集群信息
- `RefreshOP` — 处理拓扑刷新操作

**Agent 配置推送流程**：

```
Trisolaris.Start()
  → 从 metadb 加载 agents, controllers, analyzers
  → MetaData.Init()
  → gRPC Synchronizer 服务启动
  → Agent 通过 gRPC 请求配置
  → TrisolarisManager.GetMetaData(orgID)
  → 返回缓存的配置/拓扑
  → Monitor 变更时刷新
```

### 5. TagRecorder（标签字典）

**目录**：`@server/controller/tagrecorder/`

- **Dictionary**：标签字典数据（仅 all-region master 运行）
- **UpdaterManager**：标签更新（仅 master-region master 运行）
- **SubscriberManager**：接收标签更新（所有 controller 运行）
- 在 Manager 之前初始化，防止事件丢失

### 6. Monitor（健康监控）

**目录**：`@server/controller/monitor/`

- `ControllerCheck` — Controller 节点健康检查与 Agent 分配
- `AnalyzerCheck` — Analyzer 节点健康检查
- `VTapCheck` — Agent 健康监控
- `VTapRebalanceCheck` — Agent 负载均衡
- `VTapLicenseAllocation` — License 分配

### 7. gRPC Services（Agent 通信）

**目录**：`@server/controller/grpc/`

- **Controller gRPC** — 资源 ID、加密密钥、Prometheus 缓存
- **Synchronizer gRPC** — 配置和拓扑推送（Trisolaris 提供数据）
- **Agent Debug** — 远程调试
- 支持 TLS 加密
- Statsd 性能追踪

### 8. HTTP Router（REST API）

**目录**：`@server/controller/http/`

主要端点：

| 路径 | 功能 |
|------|------|
| `/agent/`, `/vtap/` | Agent 管理 |
| `/controller/` | Controller 状态 |
| `/analyzer/` | Analyzer（数据节点）状态 |
| `/domain/`, `/vpc/`, `/subnet/` | 云资源管理 |
| `/datasource/` | 数据源配置 |
| `/debug/` | 调试信息 |
| `/health/` | 健康检查 |

**分层**：
- `http/router/` — 端点处理器
- `http/service/` — 业务逻辑
- `http/common/` — 共享工具

### 9. Database（Metadb）

**目录**：`@server/controller/db/metadb/`

- 按组织独立数据库（多租户隔离）
- 支持 MySQL、PostgreSQL、DM（达梦）
- 基于 GORM 的 ORM 封装
- `DefaultDB` 用于系统组织（org_id=1）
- Schema 迁移：`metadb/migrator/`

### 10. Config（配置管理）

**目录**：`@server/controller/config/`

```
ControllerConfig
  ├── MetadbCfg (MySQL/PostgreSQL/DM)
  ├── RedisCfg
  ├── ManagerCfg
  ├── GenesisCfg
  ├── TrisolarisCfg
  ├── TagRecorderCfg
  ├── PrometheusCfg
  ├── MonitorCfg
  ├── StatsdCfg
  └── Specification (限制参数)
```

## Master Controller 独占功能

只有选主成功的 controller 运行（`@server/controller/master.go`），每 60s 检查：

```
checkAndStartMasterFunctions():
  → 检查选举状态
  → 如果当选：
    → tagRecorder.UpdaterManager     — 标签更新
    → ControllerCheck                — Controller 健康
    → AnalyzerCheck                  — Analyzer 健康
    → VTapCheck                      — Agent 监控
    → VTapRebalanceCheck             — Agent 负载均衡
    → VTapLicenseAllocation          — License 分配
    → Resource Cleaners              — 过期资源清理
    → Prometheus Encoders            — 指标编码
  → 如果失去主权：取消所有上述 goroutine
```

## 多租户（Multi-Organization）支持

- 每个组织拥有独立的 metadb 数据库
- Manager 为每个 domain（按组织）创建独立的 Cloud/Recorder 任务
- Trisolaris 按组织维护元数据
- TagRecorder Dictionary 跨组织（全局）
- Monitor 通过 `metadb.DoOnAllDBs()` 跨所有组织工作

## 关键设计模式

| 模式 | 应用 |
|------|------|
| **Leader Election** | 主控制器独占关键任务（健康检查、License、清理） |
| **Task = Cloud + Recorder** | 每个云平台域一对，Manager 统一编排 |
| **Per-Org 多租户** | 每个组织独立 DB，Manager/Trisolaris 按组织隔离 |
| **Signal Queue** | Cloud 采集完成 → OverwriteQueue → Recorder 入库 |
| **Event Subscriber** | 资源变更事件通知 TagRecorder 等下游 |
| **Singleton** | Genesis、Manager、Trisolaris、TagRecorder 全局单例 |

## 关键文件速查

| 文件 | 用途 |
|------|------|
| `@server/controller/controller.go` | 主初始化和启动编排 |
| `@server/controller/master.go` | Master controller 逻辑和选举处理 |
| `@server/controller/manager/manager.go` | 全局 Task（Cloud+Recorder）管理 |
| `@server/controller/cloud/cloud.go` | 云平台资源采集 |
| `@server/controller/recorder/recorder.go` | 资源数据持久化 |
| `@server/controller/trisolaris/trisolaris.go` | Agent 配置下发 |
| `@server/controller/genesis/genesis.go` | 基础设施资源发现 |
| `@server/controller/tagrecorder/tagrecorder.go` | 标签/字典数据管理 |
| `@server/controller/grpc/server.go` | gRPC 服务启动 |
| `@server/controller/http/server.go` | REST API 服务启动 |
| `@server/controller/monitor/controller.go` | Controller 健康和 Agent 分配 |
| `@server/controller/db/metadb/db.go` | 数据库生命周期管理 |
| `@server/controller/config/config.go` | 配置结构定义 |

## 一句话总结

Controller 的核心职责是：**通过 Cloud 子系统周期性采集多云资源，经 Recorder 持久化到 DB，由 Trisolaris 将配置和拓扑推送给 Agent，同时 Monitor 保障整个集群的健康运行**。选主机制确保关键管控任务不重复执行。
