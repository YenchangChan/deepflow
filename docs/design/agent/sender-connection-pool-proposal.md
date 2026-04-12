# Sender 连接池改造提案

> **状态**：设计评估中（建议先实现最小可行版本）
> **创建日期**：2026-04-08
> **更新日期**：2026-04-12
> **关联文档**：[architecture-deep-dive.md](./architecture-deep-dive.md) 11.14 Stage 5 深度剖析

## 结论

这个方向**可行，且值得做**，但第一版不建议一次性实现完整的健康检测、Slow Start、应用层 ACK 和持久化 buffer。

更稳妥的路径是先做：

1. 配置多个 Ingester endpoint；
2. `UniformSender` flush batch 时从连接池选择一个可用连接；
3. 当前 batch 发送失败后切换到另一个 endpoint 重试一次；
4. 保留单 IP 配置的向后兼容行为；
5. 增加最小必要的连接池统计和故障日志。

这样可以先解决当前最关键的单点问题：**某个 Ingester 或某条 Agent 到 Ingester 的 TCP 路径失败时，Agent 不必等待外部 LB 或下一轮单连接重连，能够在客户端主动换 endpoint 继续发送后续 batch。**

## 背景

当前 DeepFlow Agent 的 Sender 数据面存在几个高可用短板：

1. **单 IP 配置**：`global.communication.ingester_ip` 是单值字符串，不支持多个 Ingester endpoint。
2. **Sender 无客户端 failover**：Controller 侧有 `controller_ips` 和切换逻辑，Sender 侧只有一个 `dest_ip`。
3. **依赖外部 LB**：数据面高可用主要依赖 K8s Service、F5、LVS、VIP 等基础设施。
4. **失败 batch 仍可能丢失**：现有 `UniformSender` 在 flush 后会 reset encoder buffer；如果 TCP 写失败，没有跨 endpoint 的 batch 重试。
5. **无通用持久化 buffer**：内存队列只能缓冲尚未被 sender 消费的数据，不能兜住已经编码并 flush 失败的 batch。

详细分析见 [architecture-deep-dive.md 11.14.7 节](./architecture-deep-dive.md#11147-限流对分布式-trace-完整性的影响)。

### 为什么这是个问题

在传统数据中心，特别是银行、专有云和跨机房部署场景：

- 不一定有成熟的 4 层 LB 基础设施；
- LB / VIP 本身可能成为新的单点或运维依赖；
- 跨数据中心 LB 部署复杂；
- LB 配置错误、健康检查粒度不足是常见故障源；
- 4 层 LB 不理解 DeepFlow 数据面协议，看不到 Ingester 应用层写入异常。

业界同类系统的能力对比：

| 系统 | 客户端多 endpoint / failover | 持久化 buffer |
|------|-----------------------------|--------------|
| DeepFlow Agent 当前实现 | 否，数据面单 `ingester_ip` | 否，主要依赖内存队列 |
| OTel Collector | 支持多 endpoint / retry 组合 | 支持文件 queue |
| Datadog Agent | SaaS endpoint 模型为主 | 支持本地文件缓冲 |
| Fluent Bit | 支持多 output | 支持文件 buffer |

DeepFlow 当前的主要差距不是吞吐，而是**数据面客户端故障切换能力不足**。

## 方案概述

### 核心思路

配置多个 Ingester endpoint，Agent 在 Sender 侧维护一个轻量连接池。每次 batch flush 时，从池里选择一个健康或可尝试的连接发送；如果发送失败，立即标记该连接异常，并在同一个 encoded batch 上换另一个 endpoint 重试一次。

第一版建议保持现有 `UniformSender` 的 batching 模型：

```
Receiver<T> -> UniformSender 单 encoder -> batch flush -> ConnectionPool -> Ingester
```

不要在第一版引入“每连接独立 encoder”。这样改动范围更小，不改变现有 256 KB batch 和 10 秒兜底 flush 的基本语义。

### 关键前提

DeepFlow 上报数据没有强跨 frame 时序依赖，因此一个 sender 的不同 batch 可以发到不同 Ingester：

- **flow_metrics**：ClickHouse MergeTree 按 `ORDER BY (time, ...)` 等字段组织数据，不依赖到达顺序；
- **flow_log**：单条记录相对独立，trace 关联由 Querier 查询时完成；
- **l7_flow_log**：记录中带 trace_id、tcp_seq 等关联键。

因此，**batch 级乱序发送是可接受的**。不过仍需通过测试环境验证关键表和典型查询链路，避免某些边缘聚合逻辑隐含依赖单连接顺序。

## 具体收益

### 收益 1：降低单 Ingester 故障导致的数据丢失

当前单连接模型下，Agent 正在连接的 Ingester 不可用时，发送失败后只能等待下一轮重连。连接池后，如果还有其他 endpoint 可用，当前 batch 可以立即换 endpoint 重试一次，后续 batch 也会避开异常连接。

预期收益：

- 单个 Ingester 进程退出、节点重启、端口不可达时，丢失范围从“重连窗口内的一批或多批数据”收敛到“首个失败 batch 最多一次重试后仍失败才丢”；
- N 个 Ingester 中只要至少 1 个可用，Agent 仍可继续上报；
- 外部 LB 检测和切换不再是唯一恢复路径。

注意：这里不是严格的 0 ms 切换。已经选中的连接只有在 `write` 返回错误或写超时后才能感知故障。收益在于**故障被感知后可以立即尝试其他 endpoint**，而不是继续被单 IP 绑定。

### 收益 2：降低对外部 LB / VIP 的强依赖

在没有可靠数据面 LB 的环境里，可以直接配置多个 Ingester IP：

```yaml
global:
  communication:
    ingester_ips:
      - 10.1.1.11
      - 10.1.1.12
      - 10.1.1.13
    ingester_port: 30033
```

预期收益：

- 传统 IDC、裸金属、专有云场景更容易部署；
- LB 配置错误或 LB 自身故障不再直接阻断所有 Agent 数据面上报；
- 跨 DC 场景可以通过 endpoint 列表做显式容灾配置。

### 收益 3：负载分布更可控

外部 LB 通常按连接 hash 或五元组分流。Agent 侧连接池可以按 batch 做 round-robin，把流量更均匀地散到多个 Ingester。

预期收益：

- 对长连接场景更友好，不容易因为连接 hash 固定导致某些 Ingester 长期偏热；
- 后续可以扩展权重、Slow Start、失败率降权等策略；
- 统计可以直接按 endpoint 暴露，更容易定位某个 Ingester 慢或不稳定。

第一版建议只实现等权 round-robin，不做加权随机。先降低实现复杂度和 review 成本。

### 收益 4：改造成本可控，能保持向后兼容

现有代码已经把 TCP 连接集中在 `@agent/src/sender/uniform_sender.rs` 的 `Connection` 和 `send_buffer()` 附近。配置侧 `SenderConfig` 当前只有 `dest_ip` 和 `dest_port`，可以新增 `dest_ips` 或 `ingester_ips`，并在旧 `ingester_ip` 非空时生成单元素列表。

预期收益：

- 旧配置 `ingester_ip + ingester_port` 不需要迁移；
- 单 endpoint 时退化为现有行为；
- 可以只影响 TCP 数据面 sender，不触碰 NPB sender、文件输出和 server ingest 协议。

## 非目标

第一版不解决以下问题：

| 问题 | 说明 |
|------|------|
| 所有 Ingester 同时不可用 | 需要持久化 buffer 或上游限流配合 |
| Agent 重启后的数据恢复 | 需要本地磁盘队列和 replay 机制 |
| Ingester 接收后写 ClickHouse 失败 | 需要 Ingester 侧 ACK 或写入链路容错 |
| Exactly-once 语义 | DeepFlow 当前协议不提供 exactly-once |
| 应用层健康判断 | 需要扩展 Agent 与 Ingester 的协议 |

## 第一版设计

### 配置 Schema

建议第一版使用简单列表，避免同时引入 per-endpoint port、weight 等复杂字段：

```yaml
global:
  communication:
    # 新字段：多个 Ingester IP。为空时回退到 ingester_ip。
    ingester_ips:
      - "ingester-1.example.com"
      - "ingester-2.example.com"
      - "ingester-3.example.com"

    # 旧字段保留，兼容单 IP 配置。
    ingester_ip: ""
    ingester_port: 30033

outputs:
  socket:
    # 保留现有语义：是否让不同 sender 使用多个 socket。
    # 后续可以和连接池配置合并或重新命名。
    multiple_sockets_to_ingester: false
```

后续版本再考虑：

```yaml
global:
  communication:
    ingesters:
      - ip: "ingester-1.example.com"
        port: 30033
        weight: 100
```

### 数据结构

第一版连接池可以先不绑定 `Encoder<T>`：

```rust
pub struct ConnectionPool {
    endpoints: Vec<IngesterEndpoint>,
    connections: Vec<Connection>,
    next_index: usize,
}

pub struct IngesterEndpoint {
    ip: String,
    port: u16,
}

pub struct Connection {
    endpoint: IngesterEndpoint,
    tcp_stream: Option<TcpStream>,
    reconnect: bool,
    reconnect_interval: u8,
    last_reconnect: Duration,
    consecutive_failures: u8,
}
```

`UniformSender<T>` 继续持有一个 `Encoder<T>`。flush 时：

1. encoder 生成完整 frame；
2. connection pool 选择一个连接；
3. 写入完整 encoded buffer；
4. 失败则换下一个 endpoint 重试一次；
5. 成功后 reset encoder；
6. 重试仍失败则按现有逻辑计入 dropped，并 reset encoder。

### 连接选择算法

第一版使用等权 round-robin，跳过处于 reconnect 冷却期的连接：

```rust
fn pick_connection(&mut self) -> Option<usize> {
    let total = self.connections.len();
    for _ in 0..total {
        let index = self.next_index % total;
        self.next_index = self.next_index.wrapping_add(1);
        if self.connections[index].can_try_now() {
            return Some(index);
        }
    }
    None
}
```

这里不需要在第一版引入后台健康检测线程。发送线程在以下时机更新状态：

- 连接不存在时尝试 connect；
- connect 失败时设置 reconnect 间隔；
- write 失败时关闭 stream，增加失败计数；
- write 成功时清零失败计数。

这与现有 `Connection` 的重连模型接近，侵入性较小。

### 发送失败策略

第一版只做 batch 级重试：

```rust
fn send_buffer(&mut self, buffer: &[u8]) {
    if self.is_traffic_overflow() {
        return;
    }

    match self.pool.write_with_retry(buffer, 1) {
        Ok(()) => {
            self.counter.tx.fetch_add(1, Ordering::Relaxed);
            self.counter.tx_bytes.fetch_add(buffer.len() as u64, Ordering::Relaxed);
        }
        Err(e) => {
            self.counter.dropped.fetch_add(1, Ordering::Relaxed);
            self.exception_handler.set(Exception::AnalyzerSocketError, Some(e.to_string()));
        }
    }
}
```

不建议在第一版“fallback 到 OverwriteQueue”。原因是 `UniformSender` 从 `Receiver<T>` 消费后已经把多条 `T` 编码成一个 frame，发送失败时原始对象已不在手上。要把失败 batch 放回队列，需要额外保存原始 `T` 或引入 encoded-frame 队列，这会显著扩大改造范围。

### 健康判断

第一版只做被动健康判断：

- connect 成功：认为连接可用；
- write 成功：认为连接可用；
- connect/write 失败：标记为不可用并进入 reconnect 冷却；
- 连续失败：延长冷却或降低日志频率。

第二版再考虑主动健康检测：

- `TcpStream::take_error()`；
- TCP keepalive；
- idle 连接重连；
- per-endpoint 失败率降权；
- 后台 reconnect；
- 应用层 heartbeat / ACK。

需要强调：没有应用层 ACK 时，TCP 层健康检测不能证明 Ingester 已经成功写入后端存储，只能证明连接层面大概率可用。

## 后续增强

### 主动健康检测

第二版可以引入后台健康检测线程，但它应当是增强项，而不是第一版的必要条件。

```rust
fn health_check_loop(pool: Arc<Mutex<ConnectionPool>>) {
    loop {
        thread::sleep(HEALTH_CHECK_INTERVAL);
        // 检查连接错误、重连冷却、失败计数和 idle 时长。
        // 不承诺应用层可用性。
    }
}
```

### Slow Start

当某个 endpoint 刚恢复时，可以让它短时间内承担较少流量，避免所有 Agent 同时把流量打回同一个刚重启的 Ingester。

这适合放在 round-robin 和基础统计跑稳之后再做。

### 每连接独立 encoder

每连接独立 encoder 的优点是连接之间 flush 节奏独立，缺点是改动更大，也会改变现有 batch 聚合行为。

建议作为后续性能优化，而不是 HA 第一版必选项。

### 持久化 buffer

持久化 buffer 是另一个独立项目。它解决的是所有 Ingester 不可用、网络全分区、Agent 退出恢复等问题。

完整容错链路可以是：

```
Stage 4 -> 内存队列 -> UniformSender encoder -> ConnectionPool
                                              |-> Ingester 1
                                              |-> Ingester 2
                                              |-> Ingester 3

ConnectionPool 全部失败 -> encoded-frame 磁盘队列 -> 后台恢复后 replay
```

如果要做这个方向，建议新增 encoded-frame 级别的持久化队列，而不是把已经编码失败的 frame 反解回 `T`。

## 监控指标

第一版建议新增最小指标：

```rust
pub struct PoolStats {
    pool_total_connections: AtomicU64,
    pool_available_connections: AtomicU64,
    pool_connect_failures: AtomicU64,
    pool_write_failures: AtomicU64,
    pool_reconnects_total: AtomicU64,
    pool_retry_successes: AtomicU64,
    pool_all_endpoints_failed: AtomicU64,
}
```

告警建议：

| 指标 | 阈值 | 含义 |
|------|------|------|
| `pool_available_connections == 0` | 持续 1 分钟 | 所有 endpoint 当前不可用 |
| `pool_all_endpoints_failed > 0` | 任意非零 | batch 级重试仍失败 |
| `pool_retry_successes > 0` | 持续出现 | 已发生 endpoint 故障切换 |
| `pool_write_failures / tx` 明显升高 | 持续 5 分钟 | 某些 Ingester 或网络路径不稳定 |

后续再扩展 per-endpoint 指标：

- endpoint connect failures；
- endpoint write failures；
- endpoint successful batches；
- endpoint reconnect count；
- endpoint last successful send timestamp。

## 改造成本评估

| 维度 | 第一版估算 | 完整增强版估算 |
|------|------------|----------------|
| 代码改动量 | 300-500 行 Rust，另有配置和文档改动 | 800-1500 行，取决于健康检测和持久化 buffer 范围 |
| 主要文件 | `@agent/src/sender/uniform_sender.rs`，可能新增 `connection_pool.rs` | 额外涉及 stats、exception、持久化队列、server 协议 |
| 配置项 | `ingester_ips: Vec<String>` | `ingesters: [{ ip, port, weight }]`、健康检测、slow start、磁盘 buffer |
| 向后兼容 | 单 IP 配置退化为 1 endpoint | 仍可兼容，但迁移说明更多 |
| 性能影响 | 很小，主要是 endpoint 选择和失败重试 | 健康检测线程、更多连接和持久化 IO |
| 测试成本 | 单元测试 + 本地 mock TCP server | 需要 chaos test 和端到端压测 |

连接数压力需要保守评估。假设 3000 个 Agent，每个 Agent 面向 3 个 Ingester 各保持 1 个连接：

```
3000 Agent * 3 conn = 9000 TCP conn
每个 Ingester 约 3000 conn
```

如果后续改成每个 Ingester 3 个连接：

```
3000 Agent * 3 Ingester * 3 conn = 27000 TCP conn
每个 Ingester 约 9000 conn
```

这对 Linux 内核通常不是问题，但需要明确运维要求，例如 `ulimit -n`、conntrack、LB/firewall 连接数限制等。第一版建议默认每 endpoint 1 个连接，避免连接数激增。

## PR 拆分建议

1. **PR #1：配置兼容层**
   - 在 Agent 配置结构中新增 `ingester_ips: Vec<String>`；
   - `ingester_ips` 为空时使用旧 `ingester_ip`；
   - 更新 `@server/agent_config/template.yaml` 和生成文档；
   - 加配置解析测试。

2. **PR #2：ConnectionPool 基础结构**
   - 新增轻量 `ConnectionPool`；
   - 支持 round-robin 选择 endpoint；
   - 保留现有 reconnect 间隔和写超时语义；
   - 加单元测试。

3. **PR #3：UniformSender 接入连接池**
   - 保留单 encoder；
   - flush batch 时使用连接池；
   - 失败后换 endpoint 重试一次；
   - 单 endpoint 时行为与现有实现一致。

4. **PR #4：基础监控和异常**
   - 增加 pool 级 counter；
   - 增加 endpoint 全失败日志；
   - 必要时新增更明确的 exception。

5. **PR #5：故障测试**
   - mock TCP server 模拟 endpoint connect 失败；
   - 模拟 write 中断；
   - 验证单 endpoint 兼容；
   - 验证多 endpoint 下首个 endpoint 故障后重试成功。

后续增强可以独立成 PR：

- 主动健康检测；
- Slow Start；
- per-endpoint 权重；
- 应用层 ACK；
- encoded-frame 持久化 buffer。

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| 误以为 TCP 可写就代表 Ingester 写入成功 | 文档和代码注释明确第一版只保证连接层 failover，不保证应用层 ACK |
| 发送失败重试导致重复写入 | 只在 `write` 明确失败时重试；如果未来引入 ACK，需要重新定义幂等边界 |
| 多 endpoint 导致数据乱序 | 先验证关键查询链路；按 batch 级乱序设计，不做单条记录跨连接拆分 |
| 连接数增加影响 Ingester / firewall | 第一版默认每 endpoint 1 个连接；多连接作为后续显式配置 |
| 配置错误导致全部不可达 | 启动和运行时打印 endpoint 列表与失败原因；暴露 all endpoints failed 指标 |
| 改动破坏现有 `multiple_sockets_to_ingester` 语义 | 第一版不删除该配置，先把多 endpoint 和现有多 socket 语义分开处理 |
| 全部 Ingester 不可用仍丢数据 | 明确这是持久化 buffer 的范围，单独设计 encoded-frame 磁盘队列 |

## 待办事项清单

### Phase 1：设计验证

- [ ] 验证 batch 级乱序发送对 flow metrics、flow log、l7 flow log 查询结果无影响；
- [ ] 明确 `multiple_sockets_to_ingester` 与新连接池的组合语义；
- [ ] 确认配置中心、模板、生成文档对 `Vec<String>` 字段的支持方式；
- [ ] 与社区讨论 first PR 是否接受 `ingester_ips` 还是更偏好 `ingesters` 结构。

### Phase 2：最小可行实现

- [ ] 新增 `ingester_ips: Vec<String>` 配置项；
- [ ] 旧 `ingester_ip` 自动转换为单 endpoint；
- [ ] 实现 `ConnectionPool` 基础结构；
- [ ] 实现 round-robin 选择；
- [ ] 实现 batch 级失败重试一次；
- [ ] 单元测试连接选择和重试行为；
- [ ] 集成测试单 IP 配置兼容。

### Phase 3：监控和测试

- [ ] 添加连接池基础 counter；
- [ ] 添加 all endpoints failed 日志或 exception；
- [ ] mock 单 Ingester 故障；
- [ ] mock 多 Ingester 故障；
- [ ] 测试写超时和 reconnect 冷却；
- [ ] 对比单连接与连接池吞吐。

### Phase 4：增强能力

- [ ] 主动健康检测；
- [ ] per-endpoint 失败率统计；
- [ ] Slow Start；
- [ ] weighted endpoint；
- [ ] 应用层 heartbeat / ACK 可行性评估。

### Phase 5：持久化 buffer（独立项目）

- [ ] 设计 encoded-frame 磁盘队列格式；
- [ ] 设计 replay 顺序和限速；
- [ ] 设计磁盘容量上限和清理策略；
- [ ] 验证 Agent 重启恢复。

## 相关代码位置

- `@agent/src/sender/uniform_sender.rs` - 当前 `UniformSender`、`Connection`、TCP 写入逻辑；
- `@agent/src/sender/mod.rs` - Sender 模块入口；
- `@agent/src/config/config.rs:2653` - `Communication` 配置结构；
- `@agent/src/config/handler.rs:249` - `SenderConfig`；
- `@agent/src/config/handler.rs:1978` - `dest_ip` 的下发逻辑；
- `@server/agent_config/template.yaml:786` - `ingester_ip` 配置；
- `@server/agent_config/template.yaml:7536` - `multiple_sockets_to_ingester` 配置；
- `@agent/crates/public/src/queue/overwrite_queue.rs` - `OverwriteQueue` 实现。

## 参考资料

- [架构深度分析 - Stage 5 章节](./architecture-deep-dive.md#1114-stage-5-深度剖析uniformsender--数据完整性)
- [gRPC client-side load balancing](https://grpc.io/blog/grpc-load-balancing/)
- [OpenTelemetry Collector retry & queue](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/queued_retry.md)
- [Kafka Producer 的多 broker 连接管理](https://kafka.apache.org/documentation/#producerconfigs)

## 一句话总结

> Sender 连接池是一个低风险、高收益的数据面 HA 改造方向。第一版应聚焦“多 Ingester endpoint + batch 级失败重试 + 向后兼容”，预期能显著降低单 Ingester 或单网络路径故障造成的数据丢失，并减少对外部 LB 的强依赖；主动健康检测、Slow Start、应用层 ACK 和持久化 buffer 应作为后续独立增强项推进。

---

**文档版本**：2.0（可行性评估与最小可行方案）
