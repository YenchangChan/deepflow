# DeepFlow ClickHouse 表结构文档

本文档梳理 DeepFlow 在 ClickHouse 中创建的所有表，包括建表 Schema、存储的数据内容，以及表引擎和聚合机制。

> 源码位置：`@server/libs/ckdb/` (建表框架)，`@server/ingester/` (各模块 dbwriter)

---

## 目录

- [1. 建表框架概览](#1-建表框架概览)
- [2. flow_log 数据库](#2-flow_log-数据库)
- [3. flow_metrics 数据库](#3-flow_metrics-数据库)
- [4. event 数据库](#4-event-数据库)
- [5. application_log 数据库](#5-application_log-数据库)
- [6. profile 数据库](#6-profile-数据库)
- [7. prometheus 数据库](#7-prometheus-数据库)
- [8. ext_metrics 数据库](#8-ext_metrics-数据库)
- [9. flow_tag 数据库](#9-flow_tag-数据库)
- [10. 表一览汇总](#10-表一览汇总)

---

## 1. 建表框架概览

### 1.1 多组织支持

- 默认组织 (OrgID=0 或 1)：数据库名不加前缀，如 `flow_log`
- 其他组织：加 `{OrgID}_` 前缀，如 `0042_flow_log`

### 1.2 表层次结构

每张数据表遵循如下层次：

| 层级 | 命名规则 | 引擎 | 说明 |
|------|---------|------|------|
| 本地表 | `{table}_local` | MergeTree 系列 | 实际存储数据 |
| 全局表 | `{table}` | Distributed | 跨 shard 的分布式视图 |
| 聚合表(可选) | `{table}.{interval}_agg` | AggregatingMergeTree | 聚合存储(1s/1h/1d) |
| 物化视图(可选) | `{table}.{interval}_mv` | MaterializedView | 从源表聚合到聚合表 |
| 聚合本地视图(可选) | `{table}.{interval}_local` | View | finalize 聚合结果 |
| 聚合全局表(可选) | `{table}.{interval}` | Distributed | 分布式聚合视图 |

### 1.3 引擎类型

| 引擎 | 用途 |
|------|------|
| MergeTree | 大多数原始数据表 |
| ReplacingMergeTree | 去重场景（如 alert_event、custom_field）|
| SummingMergeTree | 计数聚合（如 custom_field_value）|
| AggregatingMergeTree | 时间维度聚合表 |
| Distributed | 全局分布式查询 |

Byconity 模式下使用对应的 `Cnch*` 引擎变体。

### 1.4 分区函数

| 函数 | 粒度 | 典型使用场景 |
|------|------|-------------|
| TimeFuncHour | 1 小时 | flow_log、event、profile |
| TimeFuncTwoHour | 2 小时 | prometheus、ext_metrics |
| TimeFuncFourHour | 4 小时 | flow_metrics 秒级表 |
| TimeFuncTwelveHour | 12 小时 | flow_metrics 分钟级表、alert_record |
| TimeFuncDay | 1 天 | alert_event |

### 1.5 默认编码与索引

| 数据类型 | 默认编码 | 默认索引 |
|---------|---------|---------|
| UInt64/Int64 | T64 | MinMax |
| Float64 | Gorilla | MinMax |
| DateTime 系列 | DoubleDelta | MinMax |
| UInt8 | LZ4 | Set(300) |
| IPv4 | LZ4 | MinMax |
| String | LZ4 | 无 |

---

## 2. flow_log 数据库

源码：`@server/ingester/flow_log/`

### 2.1 l4_flow_log — 四层流日志

**用途**：存储 TCP/UDP 网络流日志，记录每条连接的完整生命周期信息。

**引擎**：MergeTree | **分区**：TimeFuncHour | **排序键**：`l3_epc_id_0, ip4_0, ip6_0, l3_epc_id_1, ip4_1, ip6_1, server_port`

| 列名 | 类型 | 说明 |
|------|------|------|
| **核心标识** |||
| `_id` | UInt64 | 流记录唯一 ID |
| `time` | DateTime | 秒精度时间戳，等于 end_time |
| `start_time` | DateTime64(6) | 流开始时间（微秒） |
| `end_time` | DateTime64(6) | 流结束时间（微秒） |
| `flow_id` | UInt64 | 流 ID |
| `close_type` | UInt16 | 流关闭原因 |
| **数据链路层** |||
| `mac_0`, `mac_1` | UInt64 | 源/目 MAC 地址 |
| `eth_type` | UInt16 | 以太网类型 |
| `vlan` | UInt16 | VLAN ID |
| **网络层** |||
| `is_ipv4` | UInt8 | IPv4 标志 |
| `ip4_0`, `ip4_1` | IPv4 | 源/目 IPv4 地址 |
| `ip6_0`, `ip6_1` | IPv6 | 源/目 IPv6 地址 |
| `protocol` | UInt8 | IP 协议号 |
| **隧道信息** |||
| `tunnel_tier` | UInt8 | 隧道层数 |
| `tunnel_type` | UInt16 | 隧道类型 |
| `tunnel_tx_id`, `tunnel_rx_id` | UInt32 | 隧道发送/接收 ID |
| `tunnel_tx_ip4_0/1`, `tunnel_rx_ip4_0/1` | IPv4 | 隧道端点 IPv4 |
| `tunnel_tx_ip6_0/1`, `tunnel_rx_ip6_0/1` | IPv6 | 隧道端点 IPv6 |
| `tunnel_is_ipv4` | UInt8 | 隧道 IPv4 标志 |
| `tunnel_tx_mac_0/1`, `tunnel_rx_mac_0/1` | UInt32 | 隧道 MAC |
| **传输层** |||
| `client_port` | UInt16 | 客户端端口 |
| `server_port` | UInt16 | 服务端端口 |
| `tcp_flags_bit_0`, `tcp_flags_bit_1` | UInt16 | TCP 标志位 |
| `syn_seq`, `syn_ack_seq` | UInt32 | TCP 握手序列号 |
| `last_keepalive_seq`, `last_keepalive_ack` | UInt32 | 最后 keepalive 序列号 |
| **应用层** |||
| `l7_protocol` | UInt8 | 应用层协议类型 |
| **知识图谱标签 (0=客户端, 1=服务端)** |||
| `region_id_0/1` | UInt16 | 云区域 ID |
| `az_id_0/1` | UInt16 | 可用区 ID |
| `host_id_0/1` | UInt16 | 宿主机 ID |
| `l3_device_type_0/1` | UInt8 | 资源类型 |
| `l3_device_id_0/1` | UInt32 | 资源 ID |
| `pod_node_id_0/1` | UInt32 | K8s 节点 ID |
| `pod_ns_id_0/1` | UInt16 | K8s 命名空间 ID |
| `pod_group_id_0/1` | UInt32 | K8s 工作负载 ID |
| `pod_id_0/1` | UInt32 | K8s Pod ID |
| `pod_cluster_id_0/1` | UInt16 | K8s 集群 ID |
| `l3_epc_id_0/1` | Int32 | VPC ID |
| `epc_id_0/1` | Int32 | EPC ID |
| `subnet_id_0/1` | UInt16 | 子网 ID |
| `service_id_0/1` | UInt32 | 服务 ID |
| `auto_instance_id_0/1` | UInt32 | 自动实例 ID |
| `auto_instance_type_0/1` | UInt8 | 自动实例类型 |
| `auto_service_id_0/1` | UInt32 | 自动服务 ID |
| `auto_service_type_0/1` | UInt8 | 自动服务类型 |
| `tag_source_0/1` | UInt8 | 标签来源 |
| `team_id` | UInt16 | 团队 ID |
| **地理信息** |||
| `province_0`, `province_1` | LowCardinality(String) | 省份 |
| **流信息** |||
| `signal_source` | UInt16 | 信号来源 |
| `aggregated_flow_ids` | String | 聚合的流 ID 列表 |
| `init_ipid` | UInt32 | 初始 IP ID |
| `capture_network_type_id` | UInt8 | 采集网络类型 |
| `nat_source` | UInt8 | NAT 来源 |
| `capture_nic_type` | UInt8 | 采集网卡类型 |
| `capture_nic` | UInt32 | 采集网卡 |
| `observation_point` | LowCardinality(String) | 观测点 |
| `agent_id` | UInt16 | Agent ID |
| `l2_end_0/1`, `l3_end_0/1` | UInt8 | L2/L3 端点标记 |
| `duration` | UInt64 | 流持续时间（微秒） |
| `is_new_flow` | UInt8 | 新建流标志 |
| `status` | UInt8 | 状态(0=正常,1=异常,3=服务端错误,4=客户端错误) |
| `acl_gids` | Array(UInt16) | ACL 组 ID |
| `gprocess_id_0/1` | UInt32 | 全局进程 ID |
| `nat_real_ip4_0/1` | IPv4 | NAT 真实 IP |
| `nat_real_port_0/1` | UInt16 | NAT 真实端口 |
| `direction_score` | UInt8 | 方向置信度 |
| `request_domain` | String | 请求域名 |
| **吞吐指标** |||
| `packet_tx/rx` | UInt64 | 发送/接收包数 |
| `byte_tx/rx` | UInt64 | 发送/接收字节数 |
| `l3_byte_tx/rx` | UInt64 | L3 有效载荷字节 |
| `l4_byte_tx/rx` | UInt64 | L4 有效载荷字节 |
| `total_packet_tx/rx` | UInt64 | 总包数 |
| `total_byte_tx/rx` | UInt64 | 总字节数 |
| **应用指标** |||
| `l7_request`, `l7_response` | UInt32 | 应用请求/响应数 |
| `l7_parse_failed` | UInt32 | 协议解析失败数 |
| **延迟指标** |||
| `rtt`, `rtt_client`, `rtt_server`, `tls_rtt` | Float64 | RTT（微秒） |
| `srt_sum/count/max` | Float64/UInt64/UInt32 | 系统响应时间 |
| `art_sum/count/max` | Float64/UInt64/UInt32 | 应用响应时间 |
| `rrt_sum/count/max` | Float64/UInt64/UInt32 | 请求响应时间 |
| `cit_sum/count/max` | Float64/UInt64/UInt32 | 客户端空闲时间 |
| **TCP 异常指标** |||
| `retrans_tx/rx` | UInt32 | 重传次数 |
| `zero_win_tx/rx` | UInt32 | 零窗口次数 |
| `syn_count`, `synack_count` | UInt32 | SYN/SYNACK 包数 |
| `retrans_syn`, `retrans_synack` | UInt32 | SYN/SYNACK 重传 |
| `fin_count` | UInt32 | FIN 包数 |
| `l7_client_error/server_error/server_timeout/error` | UInt32 | 应用层错误 |
| `ooo_tx/rx` | UInt32 | 乱序次数 |

### 2.2 l7_flow_log — 七层流日志

**用途**：存储应用层协议日志（HTTP、DNS、MySQL、Redis、Kafka、Dubbo 等 20+ 种协议的请求/响应记录）。

**引擎**：MergeTree | **分区**：TimeFuncHour | **排序键**：`l3_epc_id_0, ip4_0, ip6_0, l3_epc_id_1, ip4_1, ip6_1, server_port`

| 列名 | 类型 | 说明 |
|------|------|------|
| **核心标识** |||
| `_id` | UInt64 | 记录唯一 ID |
| `time` | DateTime | 秒精度时间戳 |
| `start_time`, `end_time` | DateTime64(6) | 请求开始/结束时间（微秒） |
| `flow_id` | UInt64 | 关联的流 ID |
| **知识图谱标签** ||| (与 l4_flow_log 相同的 36 列) |
| **网络信息** |||
| `is_ipv4` | UInt8 | IPv4 标志 |
| `ip4_0/1`, `ip6_0/1` | IPv4/IPv6 | 源/目 IP |
| `protocol` | UInt8 | IP 协议 |
| `client_port`, `server_port` | UInt16 | 客户端/服务端端口 |
| **采集信息** |||
| `capture_network_type_id` | UInt8 | 采集网络类型 |
| `nat_source` | UInt8 | NAT 来源 |
| `capture_nic_type` | UInt8 | 采集网卡类型 |
| `signal_source` | UInt16 | 信号来源 |
| `tunnel_type` | UInt8 | 隧道类型 |
| `capture_nic` | UInt32 | 采集网卡 |
| `observation_point` | LowCardinality(String) | 观测点 |
| `agent_id` | UInt16 | Agent ID |
| **应用协议信息** |||
| `l7_protocol` | UInt8 | 协议类型(20=HTTP1,21=HTTP2,40=Dubbo,60=MySQL,80=Redis,100=Kafka,120=DNS 等) |
| `biz_protocol` | LowCardinality(String) | 业务协议 |
| `version` | LowCardinality(String) | 协议版本 |
| `type` | UInt8 | 类型(0=请求,1=响应,2=会话) |
| `is_tls` | UInt8 | TLS 加密标志 |
| `is_async` | UInt8 | 异步调用标志 |
| `is_reversed` | UInt8 | 方向反转标志 |
| **请求/响应** |||
| `request_type` | LowCardinality(String) | 请求方法(HTTP Method、SQL命令等) |
| `request_domain` | String | 请求域名 |
| `request_resource` | String | 请求资源(HTTP路径、SQL语句等) |
| `endpoint` | String | 端点 |
| `request_id` | Nullable(UInt64) | 请求 ID |
| `response_status` | UInt8 | 响应状态(0=正常,3=服务端错误,4=客户端错误) |
| `response_code` | Nullable(Int32) | 响应码(HTTP/RPC/DNS) |
| `response_exception` | String | 响应异常信息 |
| `response_result` | String | 响应结果(如 DNS 解析地址) |
| **分布式追踪** |||
| `trace_id` | String | Trace ID |
| `_trace_id_2` | String | Trace ID (备用编码) |
| `trace_id_index` | UInt64 | Trace ID 索引 |
| `span_id` | String | Span ID |
| `parent_span_id` | String | 父 Span ID |
| `span_kind` | Nullable(UInt8) | Span 类型 |
| `x_request_id_0/1` | String | X-Request-ID |
| `http_proxy_client` | String | HTTP 代理客户端 |
| **业务标记** |||
| `biz_type` | UInt8 | 业务类型 |
| `biz_code`, `biz_scenario`, `biz_response_code` | String | 业务码/场景/响应码 |
| **服务信息** |||
| `app_service` | LowCardinality(String) | 应用服务名 |
| `app_instance` | LowCardinality(String) | 应用实例名 |
| **进程信息** |||
| `gprocess_id_0/1` | UInt32 | 全局进程 ID |
| `process_id_0/1` | Int32 | 进程 ID |
| `process_kname_0/1` | String | 进程名 |
| **系统调用** |||
| `req_tcp_seq`, `resp_tcp_seq` | UInt32 | 请求/响应 TCP 序列号 |
| `syscall_trace_id_request/response` | UInt64 | 系统调用 Trace ID |
| `syscall_thread_0/1` | UInt32 | 系统调用线程 |
| `syscall_coroutine_0/1` | UInt64 | 系统调用协程 |
| `syscall_cap_seq_0/1` | UInt32 | 系统调用序列号 |
| **指标** |||
| `response_duration` | UInt64 | 响应耗时（微秒） |
| `request_length` | Nullable(Int64) | 请求长度 |
| `response_length` | Nullable(Int64) | 响应长度 |
| `sql_affected_rows` | Nullable(UInt64) | SQL 影响行数 |
| `direction_score` | UInt8 | 方向置信度 |
| `captured_request_byte` | UInt32 | 已采集请求字节数 |
| `captured_response_byte` | UInt32 | 已采集响应字节数 |
| **自定义属性** |||
| `attribute_names` | Array(LowCardinality(String)) | 属性名列表 |
| `attribute_values` | Array(String) | 属性值列表 |
| `metrics_names` | Array(LowCardinality(String)) | 指标名列表 |
| `metrics_values` | Array(Float64) | 指标值列表 |
| `events` | String | OTel 事件 |

### 2.3 l4_packet — 四层数据包

**用途**：存储 L4 层网络包序列信息，用于数据包级别的网络分析。

**引擎**：MergeTree | **分区**：TimeFuncHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `start_time` | DateTime64(6) | 开始时间（微秒） |
| `end_time` | DateTime64(6) | 结束时间（微秒） |
| `flow_id` | UInt64 | 关联流 ID |
| `agent_id` | UInt16 | Agent ID |
| `team_id` | UInt16 | 团队 ID |
| `packet_count` | UInt32 | 包数 |
| `packet_batch` | String | 编码后的包序列数据 |

### 2.4 l7_packet — 七层数据包

**用途**：存储 L7 层网络包的 PCAP 原始数据，用于应用层协议的包级分析。

**源码**：`@server/ingester/pcap/dbwriter/pcap.go`

**引擎**：MergeTree | **分区**：TimeFuncHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `start_time` | DateTime64(6) | 开始时间（微秒） |
| `end_time` | DateTime64(6) | 结束时间（微秒） |
| `flow_id` | UInt64 | 关联流 ID |
| `agent_id` | UInt16 | Agent ID |
| `team_id` | UInt16 | 团队 ID |
| `packet_count` | UInt32 | 包数 |
| `packet_batch` | String | PCAP 格式的包数据 |
| `acl_gids` | Array(UInt16) | ACL 组 ID |

### 2.5 span_with_trace_id — Span 追踪索引

**用途**：存储 Span 与 Trace ID 的关联关系，加速 Trace 查询。

**引擎**：MergeTree | **排序键**：`search_index, time`

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `trace_id` | String | Trace ID |
| `_trace_id_2` | String | Trace ID (备用编码) |
| `search_index` | UInt64 | 搜索索引 |
| `encoded_span` | String | 编码后的 Span 数据 |

### 2.6 trace_tree — 追踪树

**用途**：存储分布式追踪的树形结构数据，用于 Trace 可视化。

**引擎**：MergeTree | **排序键**：`search_index, time`

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `search_index` | UInt64 | 搜索索引 |
| `trace_id` | String | Trace ID（含 Bloom 过滤器索引）|
| `_trace_id_2` | String | Trace ID 备用编码（含 Bloom 过滤器索引）|
| `encoded_span_list` | String | 编码后的追踪树节点列表 |

---

## 3. flow_metrics 数据库

源码：`@server/libs/flow-metrics/`、`@server/ingester/flow_metrics/`

该数据库存储预聚合的网络和应用性能指标。每个表有 1m（分钟级）和 1s（秒级）两种粒度变体，分钟级表额外支持 1h/1d 的自动聚合。

### 3.1 通用知识图谱标签列

以下标签列在 network/application 系列表中通用（`_0/_1` 后缀用于 map 表表示路径两端）：

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `_tid` | UInt8 | 表类型标识 |
| `region_id` | UInt16 | 云区域 ID |
| `az_id` | UInt16 | 可用区 ID |
| `host_id` | UInt16 | 宿主机 ID |
| `l3_device_type` | UInt8 | 资源类型 |
| `l3_device_id` | UInt32 | 资源 ID |
| `pod_node_id` | UInt32 | K8s 节点 ID |
| `pod_ns_id` | UInt16 | K8s 命名空间 ID |
| `pod_group_id` | UInt32 | K8s 工作负载 ID |
| `pod_id` | UInt32 | K8s Pod ID |
| `pod_cluster_id` | UInt16 | K8s 集群 ID |
| `l3_epc_id` | Int32 | VPC ID |
| `subnet_id` | UInt16 | 子网 ID |
| `service_id` | UInt32 | 服务 ID |
| `auto_instance_id/type` | UInt32/UInt8 | 自动实例 |
| `auto_service_id/type` | UInt32/UInt8 | 自动服务 |
| `ip4` | IPv4 | IPv4 地址 |
| `ip6` | IPv6 | IPv6 地址 |
| `is_ipv4` | UInt8 | IPv4 标志 |
| `tag_source` | UInt8 | 标签来源 |
| `protocol` | UInt8 | IP 协议 |
| `server_port` | UInt16 | 服务端口 |
| `role` | UInt8 | 方向(0=c2s,1=s2c,2=local,3=rest) |
| `gprocess_id` | UInt32 | 全局进程 ID |
| `signal_source` | UInt16 | 信号来源 |
| `agent_id` | UInt16 | Agent ID |
| `team_id` | UInt16 | 团队 ID |

### 3.2 network.1m / network.1s — 网络指标

**用途**：存储网络流聚合指标（吞吐量、延迟、重传、异常等）。

**分区**：1m=TimeFuncTwelveHour, 1s=TimeFuncFourHour | **排序键**：`time, l3_epc_id, ip4, ip6, server_port`

**指标列：**

| 分类 | 列名 | 类型 | 说明 |
|------|------|------|------|
| **吞吐** | `packet_tx/rx/total` | UInt64 | 发送/接收/总包数 |
| | `byte_tx/rx/total` | UInt64 | 发送/接收/总字节数 |
| | `l3_byte_tx/rx` | UInt64 | L3 有效载荷字节 |
| | `l4_byte_tx/rx` | UInt64 | L4 有效载荷字节 |
| | `new_flow`, `closed_flow` | UInt64 | 新建/关闭连接数 |
| | `l7_request`, `l7_response` | UInt64 | 应用请求/响应数 |
| | `syn_count`, `synack_count` | UInt64 | SYN/SYNACK 数 |
| | `direction_score` | UInt8 | 方向置信度 |
| **延迟** | `rtt_sum/count/max` | Float64/UInt64/UInt32 | 连接 RTT（微秒）|
| | `rtt_client_sum/count/max` | Float64/UInt64/UInt32 | 客户端 RTT |
| | `rtt_server_sum/count/max` | Float64/UInt64/UInt32 | 服务端 RTT |
| | `srt_sum/count/max` | Float64/UInt64/UInt32 | 系统响应时间 |
| | `art_sum/count/max` | Float64/UInt64/UInt32 | 应用响应时间 |
| | `rrt_sum/count/max` | Float64/UInt64/UInt32 | 请求响应时间 |
| | `cit_sum/count/max` | Float64/UInt64/UInt32 | 客户端空闲时间 |
| **TCP 性能** | `retrans_tx/rx/total` | UInt64 | 重传次数 |
| | `zero_win_tx/rx/total` | UInt64 | 零窗口次数 |
| | `retrans_syn`, `retrans_synack` | UInt64 | SYN/SYNACK 重传 |
| **异常** | `client_rst_flow` | UInt64 | 客户端 RST |
| | `server_rst_flow` | UInt64 | 服务端 RST |
| | `server_syn_miss` | UInt64 | 服务端 SYN 缺失 |
| | `client_ack_miss` | UInt64 | 客户端 ACK 缺失 |
| | `client_half_close_flow` | UInt64 | 客户端半关闭 |
| | `server_half_close_flow` | UInt64 | 服务端半关闭 |
| | `client_source_port_reuse` | UInt64 | 客户端端口复用 |
| | `server_reset` | UInt64 | 服务端直接 RST |
| | `server_queue_lack` | UInt64 | 服务端队列溢出 |
| | `tcp_timeout` | UInt64 | 连接超时 |
| | `client_establish_fail` | UInt64 | 客户端建连失败 |
| | `server_establish_fail` | UInt64 | 服务端建连失败 |
| | `tcp_establish_fail` | UInt64 | TCP 建连失败(总) |
| | `tcp_transfer_fail` | UInt64 | TCP 传输失败 |
| | `tcp_rst_fail` | UInt64 | TCP RST 数 |
| | `ooo_tx/rx` | UInt64 | 乱序次数 |
| | `l7_client_error/server_error` | UInt32 | 应用层错误 |
| | `l7_timeout`, `l7_error` | UInt32 | 应用超时/总错误 |
| **流负载** | `flow_load` | UInt64 | 活跃连接数 |

### 3.3 network_map.1m / network_map.1s — 网络路径指标

**用途**：与 network 表相同的指标，但标签使用 `_0/_1` 后缀表示路径两端（如 `ip4_0`, `ip4_1`），用于绘制网络拓扑和路径分析。

**指标列**：与 network 完全相同。

### 3.4 application.1m / application.1s — 应用指标

**用途**：存储应用层聚合指标（请求数、响应时间、错误率等）。

**额外标签列**（在通用标签基础上）：

| 列名 | 类型 | 说明 |
|------|------|------|
| `l7_protocol` | UInt8 | 应用协议类型 |
| `app_service` | LowCardinality(String) | 应用服务名 |
| `app_instance` | LowCardinality(String) | 应用实例名 |
| `endpoint` | String | 端点 |
| `biz_type` | UInt8 | 业务类型 |

**指标列：**

| 列名 | 类型 | 说明 |
|------|------|------|
| `request` | UInt32 | 累计请求数 |
| `response` | UInt32 | 累计响应数 |
| `direction_score` | UInt8 | 方向置信度 |
| `rrt_max` | UInt32 | 最大请求响应时间（微秒） |
| `rrt_sum` | Float64 | 累计请求响应时间（微秒） |
| `rrt_count` | UInt64 | 请求响应时间计数 |
| `client_error` | UInt64 | 客户端错误数 |
| `server_error` | UInt64 | 服务端错误数 |
| `timeout` | UInt64 | 超时数 |
| `error` | UInt64 | 总错误数 |

### 3.5 application_map.1m / application_map.1s — 应用路径指标

**用途**：与 application 相同的指标，使用 `_0/_1` 后缀标签表示调用路径两端，用于服务拓扑图。

### 3.6 traffic_policy.1m — 流量策略指标

**用途**：存储基于 ACL/流量策略的流量统计。

**标签列：**

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `acl_gid` | UInt16 | ACL 组 ID |
| `tunnel_ip_id` | UInt16 | 隧道分发点 ID |
| `agent_id` | UInt16 | Agent ID |

**指标列：**

| 列名 | 类型 | 说明 |
|------|------|------|
| `packet_tx/rx/total` | UInt64 | 包数 |
| `byte_tx/rx/total` | UInt64 | 字节数 |
| `l3_byte_tx/rx` | UInt64 | L3 字节数 |
| `l4_byte_tx/rx` | UInt64 | L4 字节数 |

**注意**：该表不支持 1h/1d 自动聚合，默认 TTL 3 天。

---

## 4. event 数据库

源码：`@server/ingester/event/dbwriter/`

### 4.1 event — 资源事件

**用途**：存储 K8s 事件、资源变更事件。

**引擎**：MergeTree | **分区**：TimeFuncHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `_id` | UInt64 | 唯一 ID |
| `start_time`, `end_time` | DateTime64(6) | 事件时间范围 |
| `tagged` | UInt8 | 调试标志 |
| `signal_source` | UInt8 | 事件来源 |
| `event_type` | LowCardinality(String) | 事件类型 |
| `event_desc` | String | 事件描述 |
| `process_kname` | String | 进程名 |
| `gprocess_id` | UInt32 | 全局进程 ID |
| `region_id` | UInt16 | 云区域 ID |
| `az_id` | UInt16 | 可用区 ID |
| `l3_epc_id` | Int32 | VPC ID |
| `host_id` | UInt16 | 宿主机 ID |
| `pod_id` | UInt32 | Pod ID |
| `pod_node_id` | UInt32 | 节点 ID |
| `pod_ns_id` | UInt16 | 命名空间 ID |
| `pod_cluster_id` | UInt16 | 集群 ID |
| `pod_group_id` | UInt32 | 工作负载 ID |
| `l3_device_type` | UInt8 | 资源类型 |
| `l3_device_id` | UInt32 | 资源 ID |
| `service_id` | UInt32 | 服务 ID |
| `agent_id` | UInt16 | Agent ID |
| `subnet_id` | UInt16 | 子网 ID |
| `is_ipv4` | UInt8 | IPv4 标志 |
| `ip4` | IPv4 | IPv4 地址 |
| `ip6` | IPv6 | IPv6 地址 |
| `team_id` | UInt16 | 团队 ID |
| `auto_instance_id/type` | UInt32/UInt8 | 自动实例 |
| `auto_service_id/type` | UInt32/UInt8 | 自动服务 |
| `app_instance` | String | 应用实例 |
| `attribute_names` | Array(LowCardinality(String)) | 额外属性名 |
| `attribute_values` | Array(String) | 额外属性值 |

### 4.2 file_event — 文件事件

**用途**：存储文件系统操作事件（读写、挂载等）。

**引擎**：MergeTree | **分区**：TimeFuncTwelveHour

在 event 表所有列的基础上，额外增加：

| 列名 | 类型 | 说明 |
|------|------|------|
| `bytes` | UInt32 | 读写字节数 |
| `duration` | UInt64 | 耗时（微秒） |
| `file_name` | String | 文件名 |
| `file_type` | UInt8 | 文件类型 |
| `offset` | UInt64 | 读写偏移量 |
| `syscall_thread` | UInt32 | 线程 ID |
| `syscall_coroutine` | UInt32 | 协程 ID |
| `mount_source` | LowCardinality(String) | 挂载源 |
| `mount_point` | LowCardinality(String) | 挂载点 |
| `file_dir` | String | 文件目录 |

### 4.3 alert_event — 告警事件

**用途**：存储告警事件记录，使用 ReplacingMergeTree 进行去重。

**引擎**：ReplacingMergeTree(_id) | **分区**：TimeFuncDay

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `_id` | UInt64 | 去重版本号 |
| `policy_id` | UInt32 | 策略 ID |
| `policy_type` | UInt8 | 策略类型 |
| `alert_policy` | LowCardinality(String) | 告警策略名 |
| `metric_value` | Float64 | 指标值 |
| `metric_value_str` | String | 指标值(字符串) |
| `event_level` | UInt8 | 告警级别 |
| `target_tags` | String | 目标标签 |
| `tag_string_names/values` | Array | 字符串标签 |
| `tag_int_names/values` | Array | 整数标签 |
| `trigger_threshold` | LowCardinality(String) | 触发阈值 |
| `metric_unit` | LowCardinality(String) | 指标单位 |
| `custom_tag_names/values` | Array | 自定义标签 |
| `_target_uid` | String | 目标 UID |
| `_query_region` | LowCardinality(String) | 查询区域 |
| `team_id` | UInt16 | 团队 ID |
| `user_id` | UInt32 | 用户 ID |
| `event_id` | String | 事件 ID |
| `start_time`, `end_time` | DateTime | 告警时间范围 |
| `duration` | UInt32 | 持续时长 |
| `state` | UInt32 | 告警状态 |
| `alert_time` | UInt64 | 告警触发时间 |

### 4.4 alert_record — 告警记录

**用途**：存储告警历史记录（与 alert_event 列类似，但不含 start_time/end_time/duration/state/alert_time）。

**引擎**：MergeTree | **分区**：TimeFuncTwelveHour

---

## 5. application_log 数据库

源码：`@server/ingester/app_log/dbwriter/log.go`

### 5.1 log — 应用日志

**用途**：存储应用日志（对接 OpenTelemetry Logs），支持全文搜索。

**引擎**：MergeTree | **分区**：TimeFuncHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `timestamp` | DateTime64(6) | 精确时间戳（微秒） |
| `_id` | UInt64 | 唯一 ID |
| `_type` | Enum8 | 日志类型(user/system/audit/agent) |
| `trace_id` | String | Trace ID（Bloom 过滤器+ZSTD） |
| `span_id` | String | Span ID（Bloom 过滤器+ZSTD） |
| `trace_flags` | UInt32 | W3C Trace Flags |
| `severity_number` | UInt8 | 日志级别 ID |
| `body` | String | 日志正文（TokenBF 索引+ZSTD） |
| `app_service` | LowCardinality(String) | 应用服务名 |
| `gprocess_id` | UInt32 | 全局进程 ID |
| `agent_id` | UInt16 | Agent ID |
| `region_id` | UInt16 | 云区域 ID |
| `az_id` | UInt16 | 可用区 ID |
| `l3_epc_id` | Int32 | VPC ID |
| `host_id` | UInt16 | 宿主机 ID |
| `pod_id` | UInt32 | Pod ID |
| `pod_node_id` | UInt32 | 节点 ID |
| `pod_ns_id` | UInt16 | 命名空间 ID |
| `pod_cluster_id` | UInt16 | 集群 ID |
| `pod_group_id` | UInt32 | 工作负载 ID |
| `l3_device_type` | UInt8 | 资源类型 |
| `l3_device_id` | UInt32 | 资源 ID |
| `service_id` | UInt32 | 服务 ID |
| `subnet_id` | UInt16 | 子网 ID |
| `is_ipv4` | UInt8 | IPv4 标志 |
| `ip4` | IPv4 | IPv4 地址 |
| `ip6` | IPv6 | IPv6 地址 |
| `team_id` | UInt16 | 团队 ID |
| `user_id` | UInt32 | 用户 ID |
| `auto_instance_id/type` | UInt32/UInt8 | 自动实例 |
| `auto_service_id/type` | UInt32/UInt8 | 自动服务 |
| `attribute_names` | Array(LowCardinality(String)) | 属性名列表 |
| `attribute_values` | Array(String) | 属性值列表（ZSTD） |
| `metrics_names` | Array(LowCardinality(String)) | 指标名列表 |
| `metrics_values` | Array(Float64) | 指标值列表 |

---

## 6. profile 数据库

源码：`@server/ingester/profile/dbwriter/profile.go`

### 6.1 in_process — 持续剖析

**用途**：存储应用 Profiling 数据（CPU、内存、goroutine 等持续剖析），支持火焰图分析。

**引擎**：MergeTree（支持聚合表）| **分区**：TimeFuncHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `_id` | UInt64 | 唯一 ID |
| `ip4` | IPv4 | IPv4 地址 |
| `ip6` | IPv6 | IPv6 地址 |
| `is_ipv4` | UInt8 | IPv4 标志 |
| `app_service` | LowCardinality(String) | 应用服务名 |
| `profile_location_str` | String | 调用栈位置 |
| `profile_value` | Int64 | 采样值（self value） |
| `profile_value_unit` | LowCardinality(String) | 值单位 |
| `profile_event_type` | LowCardinality(String) | 剖析类型(cpu/memory等) |
| `profile_create_timestamp` | DateTime64(6) | 客户端聚合时间 |
| `profile_in_timestamp` | DateTime64(6) | 写入时间 |
| `profile_language_type` | LowCardinality(String) | 语言类型 |
| `profile_id` | String | Profile ID |
| `trace_id` | String | 关联 Trace ID |
| `span_name` | String | 关联 Span 名 |
| `app_instance` | LowCardinality(String) | 应用实例名 |
| `tag_names` | Array(LowCardinality(String)) | 标签名 |
| `tag_values` | Array(String) | 标签值 |
| `compression_algo` | LowCardinality(String) | 压缩算法 |
| `process_id` | UInt32 | 进程 ID |
| `process_start_time` | DateTime64(3) | 进程启动时间 |
| `gprocess_id` | UInt32 | 全局进程 ID |
| `agent_id` | UInt16 | Agent ID |
| `region_id` | UInt16 | 云区域 ID |
| `az_id` | UInt16 | 可用区 ID |
| `subnet_id` | UInt16 | 子网 ID |
| `l3_epc_id` | Int32 | VPC ID |
| `host_id` | UInt16 | 宿主机 ID |
| `pod_id` | UInt32 | Pod ID |
| `pod_node_id` | UInt32 | 节点 ID |
| `pod_ns_id` | UInt16 | 命名空间 ID |
| `pod_cluster_id` | UInt16 | 集群 ID |
| `pod_group_id` | UInt32 | 工作负载 ID |
| `auto_instance_id/type` | UInt32/UInt8 | 自动实例 |
| `auto_service_id/type` | UInt32/UInt8 | 自动服务 |
| `l3_device_type` | UInt8 | 资源类型 |
| `l3_device_id` | UInt32 | 资源 ID |
| `service_id` | UInt32 | 服务 ID |
| `team_id` | UInt16 | 团队 ID |

**聚合表**：`in_process_metrics.1s_agg`，对 `profile_value` 进行 sum 聚合。

---

## 7. prometheus 数据库

源码：`@server/ingester/prometheus/dbwriter/prometheus_sample.go`

### 7.1 samples — Prometheus 指标样本

**用途**：存储 Prometheus 格式的时序指标数据。

**引擎**：MergeTree | **分区**：TimeFuncTwoHour

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `metric_id` | UInt32 | 编码后的指标名 ID |
| `target_id` | UInt32 | 编码后的 Target ID |
| `team_id` | UInt16 | 团队 ID |
| `app_label_value_id_1` ~ `_N` | UInt32 | 动态 Label 值 ID（数量可配置） |
| `value` | Float64 | 指标值 |
| **通用标签（可选，带 UniversalTag 时）** |||
| `region_id` | UInt16 | 云区域 ID |
| `az_id` | UInt16 | 可用区 ID |
| `l3_epc_id` | Int32 | VPC ID |
| `host_id` | UInt16 | 宿主机 ID |
| `pod_id` | UInt32 | Pod ID |
| `pod_node_id` | UInt32 | 节点 ID |
| `pod_ns_id` | UInt16 | 命名空间 ID |
| `pod_cluster_id` | UInt16 | 集群 ID |
| `pod_group_id` | UInt32 | 工作负载 ID |
| `l3_device_type` | UInt8 | 资源类型 |
| `l3_device_id` | UInt32 | 资源 ID |
| `service_id` | UInt32 | 服务 ID |
| `agent_id` | UInt16 | Agent ID |
| `subnet_id` | UInt16 | 子网 ID |
| `is_ipv4` | UInt8 | IPv4 标志 |
| `ip4` | IPv4 | IPv4 地址 |
| `ip6` | IPv6 | IPv6 地址 |

---

## 8. ext_metrics 数据库

源码：`@server/ingester/ext_metrics/dbwriter/ext_metrics.go`

### 8.1 metrics — 扩展指标

**用途**：存储外部/自定义指标数据，支持灵活的 tag/metric 结构。

**引擎**：MergeTree | **分区**：TimeFuncTwoHour

**数据库变体**：
- `ext_metrics.metrics` — 通用外部指标
- `deepflow_tenant.deepflow_collector` — DeepFlow 租户指标
- `deepflow_admin.deepflow_server` — DeepFlow 管理指标

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `virtual_table_name` | LowCardinality(String) | 虚拟表名 |
| `team_id` | UInt16 | 团队 ID |
| `tag_names` | Array(LowCardinality(String)) | 标签名列表 |
| `tag_values` | Array(LowCardinality(String)) | 标签值列表 |
| `metrics_float_names` | Array(LowCardinality(String)) | 浮点指标名列表 |
| `metrics_float_values` | Array(Float64) | 浮点指标值列表 |
| **通用标签（非 dfstats 类型时包含）** |||
| `region_id` ~ `ip6` | (同 prometheus) | 通用知识图谱标签 |

---

## 9. flow_tag 数据库

源码：`@server/ingester/flow_tag/flow_tag.go`

### 9.1 custom_field — 自定义字段元数据

**用途**：记录各数据表中出现的动态字段名和类型，用于查询时的字段发现。

**引擎**：ReplacingMergeTree

| 列名 | 类型 | 说明 |
|------|------|------|
| `time` | DateTime | 时间戳 |
| `table` | LowCardinality(String) | 虚拟表名 |
| `vpc_id` | Int32 | VPC ID |
| `pod_ns_id` | UInt16 | K8s 命名空间 ID |
| `field_type` | LowCardinality(String) | 字段分类(tag/custom_tag/metrics) |
| `field_name` | LowCardinality(String) | 字段名 |
| `field_value_type` | LowCardinality(String) | 值类型(string/float/int) |
| `team_id` | UInt16 | 团队 ID |

### 9.2 custom_field_value — 自定义字段值

**用途**：记录各字段的已知取值，用于查询时的值补全和过滤。

**引擎**：SummingMergeTree(count)

| 列名 | 类型 | 说明 |
|------|------|------|
| (包含 custom_field 的所有列) |||
| `field_value` | String | 字段值 |
| `count` | UInt64 | 出现次数（自动求和聚合） |

---

## 10. 表一览汇总

| 数据库 | 表名 | 存储内容 | 引擎 | 聚合 |
|--------|------|---------|------|------|
| flow_log | l4_flow_log | TCP/UDP 网络流日志 | MergeTree | 无 |
| flow_log | l7_flow_log | 应用层协议日志(HTTP/DNS/MySQL 等) | MergeTree | 无 |
| flow_log | l4_packet | L4 数据包序列 | MergeTree | 无 |
| flow_log | l7_packet | L7 PCAP 数据包 | MergeTree | 无 |
| flow_log | span_with_trace_id | Span-Trace 关联索引 | MergeTree | 无 |
| flow_log | trace_tree | 分布式追踪树 | MergeTree | 无 |
| flow_metrics | network.1m/1s | 网络指标(吞吐/延迟/异常) | MergeTree | 1m→1h→1d |
| flow_metrics | network_map.1m/1s | 网络路径指标 | MergeTree | 1m→1h→1d |
| flow_metrics | application.1m/1s | 应用指标(请求/响应/错误) | MergeTree | 1m→1h→1d |
| flow_metrics | application_map.1m/1s | 应用路径指标 | MergeTree | 1m→1h→1d |
| flow_metrics | traffic_policy.1m | 流量策略指标 | MergeTree | 无 |
| event | event | 资源/K8s 事件 | MergeTree | 无 |
| event | file_event | 文件系统事件 | MergeTree | 无 |
| event | alert_event | 告警事件(去重) | ReplacingMergeTree | 无 |
| event | alert_record | 告警历史记录 | MergeTree | 无 |
| application_log | log | 应用日志(OTel) | MergeTree | 无 |
| profile | in_process | 持续剖析(CPU/内存等) | MergeTree | 1s_agg |
| prometheus | samples | Prometheus 指标 | MergeTree | 无 |
| ext_metrics | metrics | 扩展/自定义指标 | MergeTree | 无 |
| flow_tag | custom_field | 动态字段元数据 | ReplacingMergeTree | 无 |
| flow_tag | custom_field_value | 动态字段值 | SummingMergeTree | 无 |
