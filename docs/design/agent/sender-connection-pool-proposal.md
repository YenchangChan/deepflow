# Sender 连接池改造提案

> **状态**：📝 待办（提案阶段，尚未实现）
> **创建日期**：2026-04-08
> **关联文档**：[architecture-deep-dive.md](./architecture-deep-dive.md) 11.14 Stage 5 深度剖析

## 背景

当前 DeepFlow Agent 的 Sender 设计存在数据上报高可用方面的薄弱点：

1. **单 IP 配置**：`ingester_ip` 是单值字符串，不支持多 endpoint
2. **无客户端 failover**：和 Controller 不同（`controller_ips: Vec<String>` + `next_controller_ip()`），Sender 不会自动切换 Ingester
3. **依赖外部 LB**：高可用必须靠 K8s Service / F5 / LVS 等基础设施
4. **无持久化 buffer**：内存 OverwriteQueue 满了直接覆盖最旧的数据

详细分析见 [architecture-deep-dive.md 11.14.7 节](./architecture-deep-dive.md#11147-限流对分布式-trace-完整性的影响)。

### 为什么这是个问题

在传统数据中心（特别是银行场景）：

- 不一定有成熟的 4 层 LB 基础设施
- LB 自身是新的单点
- 跨数据中心 LB 部署复杂
- LB 配置错误是常见故障源
- 4 层 LB 看不到应用层错误（伪健康故障）

业界同类系统的对比：

| 系统 | 客户端 failover | 持久化 buffer |
|------|---------------|--------------|
| **DeepFlow Agent** | ❌ 单 IP | ❌ 仅内存 OverwriteQueue |
| **OTel Collector** | ✅ 多 endpoint | ✅ 文件 queue |
| **Datadog Agent** | ❌ SaaS endpoint | ✅ 本地文件 |
| **Fluent Bit** | ✅ 多 OUTPUT | ✅ 文件 buffer |

DeepFlow 在这个对比里**功能最简陋**，是真正的薄弱点。

## 方案概述

### 核心思路

> **配置多个 Ingester（如 3 个），每个 Ingester 维护多个 TCP 连接（如 3 个），共同构建一个连接池。后台线程做健康检测和自动重连。发送时从池里挑可用连接发送。**

### 关键洞察

DeepFlow 的数据**没有跨 frame 的时序依赖**：

- **flow_metrics**：CK MergeTree 引擎按 `ORDER BY (time, ...)` 自动排序
- **flow_log**：每条独立，trace 关联在 Querier 查询时通过 BFS 扩散完成
- **l7_flow_log**：每条自带所有关联键（trace_id、tcp_seq 等）

→ **数据可以乱序发到任意 Ingester**，所以连接池随机分发完全可行。

### 与现有方案对比

| 维度 | 当前架构（单连接 + 外部 LB）| 连接池方案 |
|------|------------------------|----------|
| **故障切换延迟** | 5-30 秒（LB 检测 + 切换）| **0 ms**（永远有可用连接）|
| **依赖外部组件** | LB / VIP / Service | **无** |
| **LB 自身单点** | 是 | **否** |
| **故障感知** | 4 层 LB 看不到应用错误 | **TCP 直接感知** |
| **负载均衡精度** | LB 按连接 hash | **应用层均匀分布** |
| **运维复杂度** | 需要 LB 团队 | **Agent 内置** |
| **跨 DC 部署** | LB 跨 DC 复杂 | **配置多 IP 即可** |
| **降级行为** | LB 切换期间数据丢失 | **N-1 个 Ingester 仍可用** |

## 设计细节

### 数据结构

```rust
const POOL_SIZE_PER_INGESTER: usize = 3;
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

pub struct ConnectionPool<T: Sendable> {
    ingesters: Vec<IngesterEndpoint>,         // 多个 Ingester
    connections: Vec<Arc<PooledConnection<T>>>,  // 扁平化的所有连接
    next_index: AtomicUsize,                  // round-robin 游标
    health_check_thread: JoinHandle<()>,
    stats: Arc<PoolStats>,
}

pub struct IngesterEndpoint {
    ip: String,
    port: u16,
    weight: u32,
}

pub struct PooledConnection<T: Sendable> {
    endpoint_idx: usize,
    tcp: Mutex<Option<TcpStream>>,
    state: AtomicU8,                          // Healthy / Dead / Reconnecting / SlowStart
    last_used_ts: AtomicU64,
    consecutive_failures: AtomicU32,
    total_sent: AtomicU64,
    total_failed: AtomicU64,

    // 每个连接独立的 256 KB encoder buffer
    encoder: Mutex<Encoder<T>>,
    
    connected_at: AtomicU64,                  // for slow-start
}
```

### 连接选择算法

**Round-robin + 健康过滤 + 加权（Slow Start）**：

```rust
impl<T: Sendable> ConnectionPool<T> {
    fn pick_connection(&self) -> Option<Arc<PooledConnection<T>>> {
        let total = self.connections.len();
        // 最多尝试 total 次，跳过不健康的
        for _ in 0..total {
            let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % total;
            let conn = &self.connections[idx];
            if conn.state.load(Ordering::Acquire) == STATE_HEALTHY {
                return Some(conn.clone());
            }
        }
        None  // 全部不可用
    }
}
```

### 健康检测：3 层判断

**关键工程坑：fd 可写 ≠ 对端可用**

需要 3 层组合判断：

```rust
fn health_check_loop(pool: Arc<ConnectionPool<T>>) {
    loop {
        thread::sleep(HEALTH_CHECK_INTERVAL);
        
        for conn in &pool.connections {
            let state = conn.state.load(Ordering::Relaxed);
            
            match state {
                STATE_HEALTHY => {
                    // L1: TCP 状态
                    if !is_tcp_alive(conn) {
                        conn.state.store(STATE_DEAD, Ordering::Release);
                    }
                    // L2: 失败率
                    if conn.consecutive_failures.load(Ordering::Relaxed) >= 3 {
                        conn.state.store(STATE_DEAD, Ordering::Release);
                    }
                    // L3: 活跃度（防 silent drop）
                    let last = conn.last_used_ts.load(Ordering::Relaxed);
                    if current_secs() - last > IDLE_TIMEOUT {
                        // 主动探测或降权
                    }
                }
                STATE_DEAD => {
                    // 异步重连
                    pool.spawn_reconnect(conn.clone());
                }
                _ => {}
            }
        }
    }
}
```

**TCP 配置必备**：

```rust
tcp_stream.set_keepalive(Some(Duration::from_secs(5)))?;       // 短 keepalive
tcp_stream.set_write_timeout(Some(Duration::from_secs(2)))?;   // 短写超时
```

**理想方案**：让 Ingester 增加应用层 ACK，Agent 心跳帧 + 等待响应，但这需要修改 Server。

### 发送流程

```rust
impl<T: Sendable> ConnectionPool<T> {
    fn send(&self, doc: T) -> Result<(), Error> {
        let conn = self.pick_connection()
            .ok_or(Error::AllConnectionsFailed)?;
        
        let mut encoder = conn.encoder.lock().unwrap();
        encoder.cache_to_sender(doc);
        
        if encoder.is_full() {
            self.flush_connection(&conn, &mut encoder)?;
        }
        Ok(())
    }
    
    fn flush_connection(
        &self,
        conn: &PooledConnection<T>,
        encoder: &mut Encoder<T>,
    ) -> Result<(), Error> {
        let mut tcp_guard = conn.tcp.lock().unwrap();
        let tcp = tcp_guard.as_mut().ok_or(Error::NotConnected)?;
        
        match tcp.write_all(encoder.buffer()) {
            Ok(_) => {
                conn.consecutive_failures.store(0, Ordering::Relaxed);
                conn.last_used_ts.store(current_secs(), Ordering::Relaxed);
                conn.total_sent.fetch_add(1, Ordering::Relaxed);
                encoder.reset_buffer();
                Ok(())
            }
            Err(e) => {
                let failures = conn.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                conn.total_failed.fetch_add(1, Ordering::Relaxed);
                if failures >= 3 {
                    conn.state.store(STATE_DEAD, Ordering::Release);
                    *tcp_guard = None;
                }
                Err(e.into())
            }
        }
    }
}
```

### Slow Start 机制

刚 reconnect 的连接先承担 1/N 流量，避免：

1. 故障 Ingester 重启后立即被打垮（thundering herd）
2. 让 Ingester 有时间做 warm-up（建立 CK 连接、初始化 cache 等）

```rust
fn pick_with_weight(&self) -> Option<Arc<PooledConnection<T>>> {
    let healthy: Vec<_> = self.connections.iter()
        .filter(|c| c.state.load(Ordering::Relaxed) == STATE_HEALTHY)
        .collect();
    
    let weights: Vec<u32> = healthy.iter().map(|c| {
        let age = current_secs() - c.connected_at.load(Ordering::Relaxed);
        if age < SLOW_START_DURATION {
            1   // 新连接权重 1
        } else {
            10  // 稳定连接权重 10
        }
    }).collect();
    
    Some(weighted_random(&healthy, &weights))
}
```

### 全挂时的降级策略

```rust
fn send(&self, doc: T) -> Result<(), Error> {
    match self.pick_connection() {
        Some(conn) => self.try_send(conn, doc),
        None => {
            // 所有连接都不可用 → 降级到现有 OverwriteQueue 行为
            self.exception_handler.set(Exception::AllIngestersDown, ...);
            self.fallback_overwrite_queue.push(doc);
            Ok(())
        }
    }
}
```

**最坏情况下退化到现有行为**，向后兼容。

### 配置 Schema

```yaml
global:
  communication:
    # 新字段：多 Ingester 端点
    ingesters:
      - ip: "ingester-1.example.com"
        port: 30033
        weight: 100              # 可选，用于加权
      - ip: "ingester-2.example.com"
        port: 30033
      - ip: "ingester-3.example.com"
        port: 30033
    connections_per_ingester: 3       # 每个 Ingester 维护几个连接
    health_check_interval: 5s         # 健康检查间隔
    slow_start_duration: 30s          # 新连接慢启动时长
    
    # 旧字段保留向后兼容（留空时使用 ingesters）
    ingester_ip: ""
    ingester_port: 30033
```

### 与现有 Sender 框架的整合

**改造方案 A（保守，推荐）**：保持现有按数据类型分线程的结构，只把"单连接"换成"连接池"。

```
metrics_sender 线程  → ConnectionPool(9 conn)
l4_log_sender 线程   → ConnectionPool(9 conn)  
l7_log_sender 线程   → ConnectionPool(9 conn)
```

**改造方案 B（彻底）**：所有 sender 共享一个连接池。

```
所有 sender 线程  → 共享 ConnectionPool(9 conn)
```

**推荐方案 A**——侵入性小，向后兼容。

## 关键设计选择

### 选择 1：每连接独立 buffer ✓

- 9 个连接 × 256 KB = 2.3 MB（可忽略）
- 无锁竞争
- 每个连接独立 flush 节奏

### 选择 2：发送失败时的策略 ✓

- **256 KB batch 级别**：失败时切换到另一个连接重试一次
- **单个 Document 级别**：不重试，直接丢（避免热路径开销）

### 选择 3：等权 round-robin（先实现）

- 第一版用等权 round-robin
- 等线上跑稳后再加加权（避免过早优化）

### 选择 4：连接数推荐配置

| Ingester 数 | 每 Ingester 连接数 | 总连接数 | 适用场景 |
|------------|------------------|---------|---------|
| 2 | 2 | 4 | 小规模 |
| 3 | 3 | 9 | **推荐**，平衡可用性和成本 |
| 5 | 4 | 20 | 大规模 |

**连接数压力评估**（3000 Agent）：

```
3000 Agent × 9 个连接 = 27000 个 TCP 连接
分布到 3 个 Ingester ≈ 每个 Ingester 9000 个连接

Linux 默认 fd limit: 1024
需要调整: ulimit -n 65536
```

9000 个连接对 Linux 内核压力很小。

## 与持久化 buffer 配合（推荐组合）

连接池解决了"单 Ingester 挂"，但不能解决"全部 Ingester 同时挂"或"网络全分区"的极端场景。建议同时增加持久化 buffer 兜底：

```
                                    ┌─→ Ingester1
Stage 4 → 内存缓冲 → 连接池池 ─────┼─→ Ingester2  
              │                     └─→ Ingester3
              │
              └─→ 全挂时 → 写本地磁盘文件
                              │
                              └─→ 后台线程恢复后回放
```

**完整方案的失败处理顺序**：

1. **连接池有可用连接** → 直接发送（99.9% 场景）
2. **某连接发送失败** → 切到另一个可用连接
3. **所有连接都失败** → 写本地磁盘文件
4. **磁盘也满了** → 落到 OverwriteQueue 覆盖（最后防线）

## 监控指标

需要新增的 stats counter：

```rust
pub struct PoolStats {
    pool_total_connections: AtomicU64,        // 总连接数
    pool_healthy_connections: AtomicU64,      // 健康连接数
    pool_dead_connections: AtomicU64,         // 死亡连接数
    pool_reconnects_total: AtomicU64,         // 累计重连次数
    pool_pick_failures: AtomicU64,            // 全挂导致 pick 失败次数
    pool_fallback_to_queue: AtomicU64,        // 降级到 OverwriteQueue 次数
    
    // 每连接级别
    per_conn_sent: Vec<AtomicU64>,
    per_conn_failed: Vec<AtomicU64>,
    per_conn_uptime: Vec<AtomicU64>,
}
```

告警建议：

| 指标 | 阈值 | 含义 |
|------|------|------|
| `pool_healthy_connections` < 总数的 50% | 持续 1 分钟 | 大量 Ingester 不可用 |
| `pool_pick_failures` > 0 | 任何非零值 | 所有连接都挂了 |
| `pool_fallback_to_queue` > 0 | 任何非零值 | 进入降级模式 |
| 某连接 `per_conn_failed / per_conn_sent` > 10% | 持续 5 分钟 | 某 Ingester 慢节点 |

## 改造成本评估

| 维度 | 估算 |
|------|------|
| **代码改动量** | ~600-800 行 Rust（含测试）|
| **修改的文件** | 主要 `uniform_sender.rs` + 新增 `connection_pool.rs` |
| **配置项新增** | `ingesters` 列表、`connections_per_ingester` 等 |
| **向后兼容** | 完全可行——单 IP 配置时退化为 1×1 池 |
| **性能影响** | CPU +1%（健康检测线程），内存 +2 MB（多 buffer），其他可忽略 |
| **测试成本** | 需要 chaos test 模拟各种网络故障 |
| **运维改动** | 几乎零——配置改一下就行 |

## PR 拆分建议

如果贡献给社区，建议拆成 5 个独立 PR，方便 review：

1. **PR #1**: 添加 `ingester_ips: Vec<String>` 配置项（向后兼容单 IP）
2. **PR #2**: 实现 `ConnectionPool` 结构 + 健康检测线程
3. **PR #3**: 改 `UniformSender` 用 `ConnectionPool` 替换单 `Connection`
4. **PR #4**: 添加监控指标（pool_size, healthy_connections, failed_sends 等）
5. **PR #5**: 添加 chaos test（模拟单/多 Ingester 故障）

每个 PR 独立可合，降低 review 难度。

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| **fd 可写不等于对端可用** | 短 keepalive + 写超时 + 连续失败计数 + （理想）应用层 ACK |
| **多连接增加 Ingester 连接数压力** | 默认 3×3=9，可控；推荐 ulimit 65536 |
| **健康检测延迟** | 5 秒间隔 + 写失败立即标记 |
| **重连风暴** | Slow Start + 重连间隔限制 |
| **配置错误导致全连不上** | 启动时连通性检测 + 至少 1 个连接成功才启动 |
| **改造引入 bug** | 完整 chaos test + 灰度发布 |

## 不解决的问题

**这个方案不能解决以下问题**，需要其他手段配合：

| 问题 | 需要的方案 |
|------|----------|
| 所有 Ingester 同时挂数小时 | 持久化 buffer（本地磁盘）|
| Agent OOM 重启 | 持久化状态恢复 |
| 跨 DC trace 完整性 | 应用层 trace_id 注入 |
| Ingester 端写 CK 失败 | Ingester 自身的高可用 |
| 单条 frame 永久丢失 | Exactly-once 语义（DeepFlow 不提供）|

## 待办事项清单

### Phase 1: 设计验证

- [ ] 在测试环境验证"乱序发送"对 CK 数据完整性无影响
- [ ] 测试单 Agent 的多 TCP 连接对 Ingester 端的实际影响
- [ ] 验证 TCP keepalive 在不同内核版本上的行为一致性
- [ ] 与社区讨论方案，获得 maintainer 的初步反馈

### Phase 2: 基础实现

- [ ] 实现 `ConnectionPool` 结构（不含健康检测）
- [ ] 实现 round-robin 选择算法
- [ ] 添加 `ingesters: Vec<...>` 配置项
- [ ] 单元测试连接池的核心行为
- [ ] 集成测试验证向后兼容（单 IP 仍然工作）

### Phase 3: 健康检测

- [ ] 实现健康检测后台线程
- [ ] 实现 3 层健康判断（TCP 状态、失败率、活跃度）
- [ ] 实现自动重连逻辑
- [ ] 实现 Slow Start 加权
- [ ] 单元测试健康状态机

### Phase 4: 监控和故障处理

- [ ] 添加 PoolStats 计数器
- [ ] 实现 fallback 到 OverwriteQueue 的降级逻辑
- [ ] 添加 Exception::AllIngestersDown 告警
- [ ] 集成到现有的 stats_collector 上报机制

### Phase 5: 测试和验证

- [ ] Chaos test：模拟单 Ingester 故障
- [ ] Chaos test：模拟多 Ingester 故障
- [ ] Chaos test：模拟网络分区
- [ ] Chaos test：模拟 Ingester hang 住
- [ ] 性能测试：vs 现有单连接方案的吞吐对比
- [ ] 内存测试：vs 现有方案的 RSS 增量

### Phase 6: 持久化 buffer（可选，更高级）

- [ ] 设计本地磁盘 buffer 的格式（segment-based？）
- [ ] 实现写磁盘的 fallback 逻辑
- [ ] 实现后台线程从磁盘恢复并回放
- [ ] 实现磁盘 buffer 的自我清理（避免无限增长）

### Phase 7: 文档和发布

- [ ] 更新 `architecture-deep-dive.md` 11.14 节
- [ ] 编写 [运维指南](./sender-connection-pool-guide.md)（待创建）
- [ ] 在 Release Notes 中说明新特性
- [ ] 提供从单 IP 迁移到连接池的步骤

## 相关代码位置

- `@agent/src/sender/uniform_sender.rs` - 当前 Sender 实现
- `@agent/src/sender/mod.rs` - Sender 模块入口
- `@agent/src/config/handler.rs:249` - SenderConfig
- `@agent/src/config/handler.rs:1978` - dest_ip 的下发逻辑
- `@server/agent_config/template.yaml:786` - ingester_ip 配置
- `@agent/crates/public/src/queue/overwrite_queue.rs` - OverwriteQueue 实现

## 参考资料

- [架构深度分析 - Stage 5 章节](./architecture-deep-dive.md#1114-stage-5-深度剖析uniformsender--数据完整性)
- [gRPC client-side load balancing](https://grpc.io/blog/grpc-load-balancing/)
- [HikariCP - Best Practices](https://github.com/brettwooldridge/HikariCP/wiki/Best-Practices)
- [OpenTelemetry Collector retry & queue](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/queued_retry.md)
- [Kafka Producer 的多 broker 连接管理](https://kafka.apache.org/documentation/#producerconfigs)

## 一句话总结

> **配置多 Ingester + 多 TCP 连接的连接池方案是 DeepFlow Sender 高可用改造的低成本高收益方向：零切换延迟、不依赖外部 LB、与 DeepFlow 数据无时序依赖的特性完美契合。核心要避开"fd 可写不等于对端可用"的工程坑（用短 keepalive + 写超时 + 失败计数 3 层防御）。改造成本约 600-800 行 Rust 代码，完全向后兼容，可拆 5 个 PR 渐进合入。配合持久化 buffer 兜底极端场景，就能成为企业级数据上报组件。**

---

**文档生成日期**: 2026-04-08
**文档版本**: 1.0（初版提案）
