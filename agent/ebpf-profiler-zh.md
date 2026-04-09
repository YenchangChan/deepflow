# deepflow-agent eBPF Profiler 专题（中文）

> 本文档聚焦 deepflow-agent 内置 eBPF profiler 的能力、定位、适用场景与已知局限。作为性能剖析专题的入口，后续相关调研、实验、配置技巧、火焰图分析经验都可以沉淀到本目录下的配套文档。

## 1. 它是什么

deepflow-agent 在 `@agent/src/ebpf/` 内实现了一个完整的 on-CPU eBPF profiler，而不是简单地"用 eBPF 采了几个性能计数器"。关键入口：

- `@agent/src/ebpf/kernel/perf_profiler.bpf.c`：内核侧 BPF 程序
- `@agent/src/ebpf/user/profile/perf_profiler.c`：用户态采样与上报
- `@agent/src/ebpf/user/profile/stringifier.c`：栈帧符号化
- `@agent/src/ebpf/user/profile/java/`：Java（JVM）符号解析专用分支

### 技术路线

- **BPF 程序类型**：`BPF_PROG_TYPE_PERF_EVENT`（和 `perf record` / parca / pyroscope 同路数）
- **触发方式**：`perf_event_open` 按固定频率采样，默认 **99 Hz**
  - 代码注释原文：`@freq sample frequency, Hertz. (e.g. 99 profile stack traces at 99 Hertz)`
- **抓取内容**：同时抓 **kernel stack** 和 **user stack**（`BPF_F_USER_STACK`）
- **去重聚合**：在 agent 本地用 `stack_trace_msg_hash` 聚合相同栈，再批量上报 server
- **产出形态**：server 侧组装成可按 进程 / 线程 / 容器 / Pod 聚合的火焰图
  - 代码函数 `process_stack_trace_data_for_flame_graph` 明确了这个目标

### 运行时要求

| 依赖 | 最低版本 | 推荐版本 |
|---|---|---|
| Linux kernel | 4.9（CO-RE / BTF 最低）| ≥ 4.14 |
| BTF 信息 | 内核启用 `CONFIG_DEBUG_INFO_BTF` 或配套外部 BTF | — |
| agent 配置 | `ebpf` 功能块未被禁用 | — |

kernel 3.10（如 CentOS 7）**原生不支持**，即便厂商 backport 了部分 eBPF 能力，profiler 依赖的 stack trace map + perf_event 组合基本跑不起来。

## 2. 它适合做什么

### 甜点场景

1. **生产环境持续性能画像（always-on profiling）**
   - 99 Hz 采样 + eBPF 栈抓取开销极低，单核 CPU 占用通常 < 1%
   - 可以一直开着，不必为"定位问题时临时接 profiler"做准备
2. **把"慢"和"热点代码"关联起来**
   - 这是 deepflow 相对 parca / pyroscope 最独特的价值
   - 同一套 agent 同时采集：network flow、L7 调用链（trace）、profile
   - 可以从"这条 HTTP trace 慢" → "这条 trace 时间窗内的进程火焰图" → "定位到具体热点函数"
   - 单纯的 profiling 工具做不到这种跨域联动
3. **对业务透明、零 SDK 接入**
   - 无需修改业务代码、无需重启进程、无需语言 runtime agent
   - 尤其适合传统应用、闭源服务、第三方中间件
4. **多语言 on-CPU 剖析**
   - C / C++ / Go / Rust：栈帧天然可抓
   - Java：有 `profile/java/` 专门分支，走 perf-`<pid>`.map 风格的 JIT 符号解析
5. **天然 Pod / 容器维度**
   - agent 本身感知 cgroup / Pod 归属
   - profile 数据可直接按容器 / Pod 聚合，无需另搭 collector 或 sidecar

### 典型使用姿势

- **"某服务 p99 变差，想知道 CPU 花哪了"**：看对应时间窗的火焰图
- **"新版本上线后 CPU 涨了 20%"**：前后两个时间窗的火焰图对比
- **"某条慢 trace 是不是卡在业务代码"**：从 trace 下钻到进程栈
- **"内核态消耗是不是异常"**：火焰图里内核栈占比一目了然

## 3. 它不适合做什么

这部分避免期望错位。deepflow profiler 是 **on-CPU 采样 profiler**，不是"万能剖析平台"。

| 需求 | 支持情况 | 说明 |
|---|---|---|
| on-CPU 热点分析 | ✅ 正是为此设计 | — |
| off-CPU 分析（等锁、等 IO 的时间）| ❌ | 源码里未见 sched switch / blocked state tracer 路径 |
| wall-clock / 墙钟时间 profiling | ❌ | 同上，只有 CPU 时间维度 |
| 堆 / 内存分配 profiling | ❌ | 无 malloc / kmem hook |
| 精确函数调用计数 | ❌ | 采样式，不是 instrumentation，只能出 CPU 时间占比 |
| 微秒级短毛刺定位 | ⚠️ | 99 Hz 采样粒度约 10 ms，短抖动易漏；调高频率有开销成本 |
| Python / Node / Ruby 栈解析 | ⚠️ | 代码中只看到 Java 专门处理；其它解释型语言大概率只能看到解释器内部 C 栈 |
| stripped / 无符号二进制 | ⚠️ | 符号化在 agent 本地做，目标符号缺失时只能显示地址 |
| 单次 benchmark 深挖 | ⚠️ | 能用，但这种一次性场景 `perf record` / `parca` 更专业 |
| PMU 事件（cache miss、分支预测等）| ❌ | 不涉及硬件 PMU counter |

## 4. 定位对比

| 工具 | 定位 | deepflow profiler 相对优劣 |
|---|---|---|
| `perf record` / `perf top` | 本地深挖专业工具 | perf 深度更好（off-CPU、PMU、更丰富事件），但不是 always-on、不跨主机、不联动 trace；deepflow 反之 |
| parca | 纯 continuous profiling 产品 | parca 专注度高、UI 成熟；deepflow 独特点是**同时有 flow/trace 数据**可联动 |
| pyroscope / Grafana Cloud Profiles | 多语言 continuous profiling | 多语言和 UI 更丰富；deepflow 优势在一套 agent 解决网络 + APM + profile |
| Grafana Beyla | eBPF 为主的观测套件 | 定位最接近，都是"顺带做 profiling 的 eBPF 全家桶" |
| bpftrace / bcc tools | 灵活的 ad-hoc eBPF 工具 | bpftrace 灵活、一次性场景强；deepflow 是固定 pipeline 的生产化产品 |
| async-profiler / Java Flight Recorder | 语言 runtime profiler | 深入 JVM 内部的能力更强（如 lock contention、alloc 采样）；deepflow 提供与 JVM 无关的外部视角 |

## 5. 实用建议

1. **已部署 deepflow-agent 做网络/APM 观测的环境**：直接打开 profiler，几乎是零成本的增量价值。重点用在"慢调用 → 热点代码"下钻。
2. **专业性能工程工作流**（off-CPU、内存泄漏、短时 benchmark、PMU）：deepflow 只能覆盖其中 on-CPU 一部分，需要组合 `perf` / `bpftrace` / `parca` / `pyroscope` / `memray` 等专业工具，别期待它一把梭。
3. **解释型语言栈**（Python、Node、Ruby 等）：除非确认 deepflow 对该语言有显式支持（目前代码里只看到 Java），否则看到的栈可能不可读，建议配合语言自身的 profiler。
4. **kernel 版本不达标的老系统**：profiler 必须关闭（见 `agent/build-docker-zh.md` 第 7.5 节），可考虑在应用侧接语言级 profiler 作为替代。

## 6. 后续调研 / 实验 Backlog（占位）

> 随专题深入再补具体内容；每一项都可以独立拆成一篇子文档。

- [ ] 采样频率 vs CPU 开销的实测曲线（99 / 199 / 499 Hz）
- [ ] 与 server 侧火焰图 UI 的数据流与压缩格式
- [ ] Java 符号解析在 JIT、CDS、Class Data Sharing 下的可靠性
- [ ] 动态语言（Python / Node）栈解析的扩展可能性
- [ ] off-CPU profiling 的可行性评估（需要内核 ≥ 4.9 的 `sched_switch` tracepoint + `bpf_get_stackid`）
- [ ] 和 deepflow L7 trace 的 time-window 关联查询路径
- [ ] 低内核版本（4.9 / 4.14 / 4.18）上的兼容性矩阵
- [ ] 符号缺失 / stripped 场景的补救手段（debuginfod、离线符号化）

## 7. 参考文件索引

- `@agent/src/ebpf/kernel/perf_profiler.bpf.c` — 内核 BPF 程序
- `@agent/src/ebpf/kernel/include/perf_profiler.h`
- `@agent/src/ebpf/user/profile/perf_profiler.c` — 用户态采样与上报
- `@agent/src/ebpf/user/profile/profile_common.c`
- `@agent/src/ebpf/user/profile/stringifier.c` — 符号化
- `@agent/src/ebpf/user/profile/java/` — JVM 符号解析
- `@agent/src/ebpf/samples/rust/profiler` — Rust 侧示例
- `@agent/build-docker-zh.md` — 构建与跨平台速查（第 7.5 节对低内核系统的处理）
