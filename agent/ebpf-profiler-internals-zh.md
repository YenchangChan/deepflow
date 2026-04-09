# eBPF Profiler 内核级工作原理（中文）

> 本文档是 `agent/ebpf-profiler-zh.md` 专题的内核机制子篇，聚焦一次采样从"定时触发"到"火焰图上出现一行"的完整内部链路。内容偏通用 eBPF profiling 原理，不限于 deepflow 实现，文末给出 deepflow 源码对照表。

## 0. 全景图

一次采样经过 7 个阶段：

```
[1] 定时触发       perf_event_open / PERF_COUNT_SW_CPU_CLOCK
      ↓           内核按固定频率在当前 CPU 上触发采样
[2] BPF 程序运行   BPF_PROG_TYPE_PERF_EVENT，拿到 pt_regs
      ↓
[3] 栈回溯         bpf_get_stackid() 分别抓内核栈和用户栈
      ↓
[4] 栈存入 map     BPF_MAP_TYPE_STACK_TRACE：stackid → 地址数组
      ↓
[5] 计数           HASH map：(pid, tgid, k_stackid, u_stackid) → count
      ↓
[6] 用户态读取     周期性扫 map，拿 (key, count) + stackid → 地址数组
      ↓
[7] 符号化         地址 → 函数名 → 聚合 → 火焰图
```

记住这张图，下面逐段展开。

## 1. 触发源：`perf_event_open` + `CPU_CLOCK`

sampling profiler 的"心跳"来自 Linux 2.6.31 引入的 `perf_event_open(2)` 系统调用（同时也是 `perf` 工具的后端）。

### 1.1 关键调用

```c
struct perf_event_attr attr = {
    .type = PERF_TYPE_SOFTWARE,
    .config = PERF_COUNT_SW_CPU_CLOCK,   // 只在 CPU 上跑时计时
    .sample_period = 0,
    .sample_freq = 99,                   // 目标 99 Hz
    .freq = 1,                           // 告诉内核用 freq 而不是 period
    .size = sizeof(attr),
};
int fd = perf_event_open(&attr, /*pid=*/-1, /*cpu=*/N,
                         /*group_fd=*/-1, /*flags=*/0);
```

- `pid=-1, cpu=N`：每个 CPU 各开一个事件，实现 system-wide per-CPU 采样（profiler 的标准姿势）
- `CPU_CLOCK` 事件：**该 CPU 上有进程运行时才滴答**，空闲不计数——这就是"on-CPU"的物理来源
- `freq=1` + `sample_freq=99`：让内核根据运行时 CPU 时钟反馈自动调节 period，保证平均 ~99 Hz

### 1.2 为什么是 99 Hz

- **避免锁相**：很多系统定时器按 100/1000 Hz 工作，如果 profiler 也用 100 Hz，采样点可能持续落在时钟中断 handler 上，污染数据。选质数 99、199、499、997 是经典做法。
- **开销可控**：99 Hz × 单次采样成本（栈回溯 + map 更新）≈ 单核 < 1%
- **时间粒度 ≈ 10 ms**：短于这个时间的函数会被概率性漏掉。要抓短毛刺需要把频率提高到 499 或 999 Hz，开销线性上涨。

### 1.3 把 BPF 程序挂上去

```c
ioctl(fd, PERF_EVENT_IOC_SET_BPF, bpf_prog_fd);
ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
```

之后，内核每次"该采样了"就会在**中断 / NMI 上下文**里调用这段 BPF 程序。该上下文限制极其严格：

- 不能睡眠
- 不能等锁
- 不能触发 page fault（这对用户栈回溯是个大坑，见 §3.3）
- 只能用 BPF helper 集合里的函数

## 2. BPF 程序：内核侧只做 O(1) 的事

BPF 程序类型是 `BPF_PROG_TYPE_PERF_EVENT`，入参是 `struct bpf_perf_event_data *ctx`，其中的 `regs` 字段保存触发采样时的寄存器状态（即"案发现场"）。

典型骨架：

```c
SEC("perf_event")
int oncpu(struct bpf_perf_event_data *ctx) {
    __u64 id  = bpf_get_current_pid_tgid();
    __u32 tgid = id >> 32;
    __u32 pid  = id & 0xffffffff;

    // ① 过滤
    if (should_skip(tgid)) return 0;

    // ② 抓栈
    __s32 u_stackid = bpf_get_stackid(ctx, &stack_map, BPF_F_USER_STACK);
    __s32 k_stackid = bpf_get_stackid(ctx, &stack_map, 0);

    // ③ 组装 key
    struct key_t key = {
        .pid = pid, .tgid = tgid,
        .u_stackid = u_stackid, .k_stackid = k_stackid,
    };
    bpf_get_current_comm(&key.comm, sizeof(key.comm));

    // ④ 计数
    __u64 *cnt = bpf_map_lookup_elem(&counts, &key);
    if (cnt) {
        __sync_fetch_and_add(cnt, 1);
    } else {
        __u64 one = 1;
        bpf_map_update_elem(&counts, &key, &one, BPF_ANY);
    }
    return 0;
}
```

**内核里只做 O(1) 的哈希更新**，不做符号化、不做上报、不做过滤逻辑以外的处理。这是 eBPF profiler 相对 `perf record` 的根本性能优势——后者把每次采样作为原始记录写到环形缓冲区，数据量是 eBPF 路线的几十到几百倍。

## 3. 栈回溯：`bpf_get_stackid`

这是整个 profiler 最复杂的一步。

### 3.1 `bpf_get_stackid` 的职责

```c
long bpf_get_stackid(void *ctx, struct bpf_map *stack_map, u64 flags);
```

行为：

1. **确定方向**：看 `flags` 是否含 `BPF_F_USER_STACK`
   - 无 → 抓**内核栈**
   - 有 → 抓**用户栈**
2. **走栈**：从 `ctx->regs` 出发循环读内存，拿到一串返回地址
3. **哈希去重**：把地址数组哈希得到 stack id，若 map 中已存在同哈希栈则直接返回旧 id
4. **存储**：若是新栈，把地址数组写入 `stack_map`

### 3.2 走栈的三种技术

| 技术 | 原理 | eBPF 可用性 |
|---|---|---|
| **Frame pointer walking** | 读 `rbp` 寄存器，每个栈帧的 `saved rbp` 指向上一帧，像链表 | ✅ 极快，`bpf_get_stackid` 的默认实现 |
| **DWARF unwinding** | 读 ELF `.eh_frame` 的 CFI 表，按规则计算上一帧 | ❌ BPF 里跑不了 DWARF 解释器，需要把 CFI 预处理成扁平表后推进 BPF map（Parca Agent、Polar Signals、Pyroscope 走这条路） |
| **LBR (Last Branch Record)** | Intel CPU 硬件记录最近 16/32 次分支 | ⚠️ 深度受限，仅部分 Intel/AMD CPU，作为辅助 |

**关键结论：eBPF profiler 默认吃 frame pointer**。现代编译器默认 `-fomit-frame-pointer`（把 `rbp` 当普通寄存器用），导致 frame chain 断裂，profiler 会看到半截栈或大量 `[unknown]`。

这就是 2023 年以来 Fedora 38、Ubuntu 24.04、Arch、openSUSE Tumbleweed 陆续**默认为系统库重新启用 frame pointer** 的原因——付出 ~1–2% 的性能代价，换取 continuous profiling 可用。

### 3.3 用户栈的额外难点：page fault

内核态走栈时，用户栈内存**可能被换出或尚未缺页加载**。在中断/NMI 上下文里**不能触发 page fault**（会 panic），所以 `bpf_get_stackid` 遇到不在物理内存里的地址就会：

- 直接停止走栈
- 返回不完整的栈或 `-EFAULT`

这是 profiler 中"栈截断"的根源。deepflow 在用户态有 `stack_trace_err` 计数器专门记录这种失败次数，作为健康度指标。

### 3.4 哈希冲突

`stack_map` 用 stack id（哈希）作 key，**不同栈哈希到同一 id 时会冲突**——后到者覆盖或失败。

改进 helper：**`bpf_get_stack`**（内核 4.18+），直接把地址数组写进调用者提供的 buffer，不经过哈希表，无冲突问题。用户态需自行去重。新版 profiler 倾向于用 `bpf_get_stack`。

### 3.5 栈回溯的天坑清单

| 问题 | 原因 | 影响 |
|---|---|---|
| Inlined 函数 | 编译器内联后栈帧消失 | 火焰图看不到被内联函数 |
| Tail call | 尾调用优化复用栈帧 | 上一层函数"被吃" |
| JIT 代码 | JIT 代码地址不在 ELF 里 | 需要 perf map 才能符号化 |
| Stripped 二进制 | 符号表被去掉 | 只能显示地址 |
| 解释器栈 | Python/Node 业务栈在解释器 heap 上 | 只能看到 `_PyEval_EvalFrameDefault` |
| 栈深度上限 | `PERF_MAX_STACK_DEPTH` 默认 127 | 深递归程序栈被截断 |
| 短命进程 + ASLR | 进程退出后 `/proc/<pid>/maps` 消失 | agent 来不及读映射，无法符号化 |

## 4. 两个关键的 BPF map

### 4.1 `BPF_MAP_TYPE_STACK_TRACE`

内核为栈回溯专门设计的 map 类型：

```c
struct {
    __uint(type, BPF_MAP_TYPE_STACK_TRACE);
    __uint(max_entries, 16384);
    __type(key, __u32);                       // stack id
    __type(value, __u64[PERF_MAX_STACK_DEPTH]); // 地址数组
} stack_map SEC(".maps");
```

- **key**：`bpf_get_stackid` 返回的 id
- **value**：`__u64` 地址数组，长度默认 127
- `max_entries`：能存多少条唯一栈——对应 deepflow `perf_profiler.c` 的 `stack_trace_map_capacity()` 计算：`每 CPU × 期望采样频率 × 安全系数`
- 容量不足时新栈被拒绝，`stack_trace_err` 递增

### 4.2 计数 map

```c
struct key_t {
    __u32 pid;
    __u32 tgid;
    __s32 u_stackid;
    __s32 k_stackid;
    char  comm[16];
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1 << 20);
    __type(key, struct key_t);
    __type(value, __u64);
} counts SEC(".maps");
```

内核里只做一件事：**这个 key 的 count 原子 +1**。

最终上报数据天然已聚合，无需用户态按时间戳对齐或再去重。

### 4.3 为什么要两张 map

因为 BPF map key 大小有限（典型 <= 512 字节），而一个 127 层的栈 × 8 字节 = 1KB，塞不进 key。把栈单独存到 stack_map，counts map 的 key 只存一个 4 字节 stack id 引用即可。

## 5. 用户态读取

用户态每隔 N 秒（通常 3–10 秒）做一轮：

```c
// 1. 遍历 counts map
while (bpf_map_get_next_key(counts_fd, prev, &cur) == 0) {
    bpf_map_lookup_elem(counts_fd, &cur, &count);
    // 2. 用 key 里的 stackid 去 stack_map 取地址数组
    bpf_map_lookup_elem(stack_map_fd, &cur.u_stackid, user_addrs);
    bpf_map_lookup_elem(stack_map_fd, &cur.k_stackid, kernel_addrs);
    // 3. 拼成 (pid, [k_addr...], [u_addr...], count) 记录
    prev = cur;
}
```

内核 5.6+ 可以用 `BPF_MAP_LOOKUP_AND_DELETE_BATCH` 一次性取出并清空，比逐条迭代快得多。

### 5.1 读完之后要清空吗

两种策略：

- **累积式**：不清空，每次上报 delta。实现简单但 counts map 会越撑越大。
- **清空式**：读完就清。deepflow 用这种，代价是"读取 → 清空"窗口内的采样会丢，但几秒窗口内的损失通常可忽略。

### 5.2 deepflow 特有的二次聚合

deepflow 不直接上报内核 map，而是在用户态再建一个 `stack_trace_msg_hash`，把"相同用户栈、不同 pid"进一步合并——server 侧火焰图按"代码"聚合而非"进程实例"。

## 6. 符号化：地址 → 函数名

**整个流水线最脏、最容易出问题的一步。** 到这里只有一串裸地址（例如 `0x7f8a2c4b1320`），要翻译成 `do_sys_openat2+0xa0` 这类可读形式。

### 6.1 内核符号（简单）

- 源：`/proc/kallsyms`
- 格式：`ffffffff812345ab T do_sys_openat2`
- 做法：读一次 → 建排序数组 → 二分查找
- **KASLR**：每次启动内核地址都变，但 kallsyms 反映的是当前启动的真实地址，不需要特殊处理
- **权限坑**：`sysctl kernel.kptr_restrict = 0` 或 CAP_SYSLOG 才能看到真实地址。否则整张表全是 `0000000000000000`，会**悄无声息地**让内核栈符号化全部失败。

### 6.2 用户态符号化流水线

对每个用户 IP，经过四步：

#### 步骤 1：定位映射

读 `/proc/<pid>/maps`：

```
7f8a2c400000-7f8a2c500000 r-xp 00000000 08:01 12345 /usr/lib/libc.so.6
```

确定地址落在 `libc.so.6` 这段映射里，文件偏移 = `0x7f8a2c4b1320 - 0x7f8a2c400000 = 0xb1320`。

#### 步骤 2：查符号表

打开对应 ELF，读符号表：

- `.symtab`：完整符号表，通常被 `strip` 掉
- `.dynsym`：动态链接符号表，strip 后仍存在，但只含导出符号

stripped 二进制只有 `.dynsym`，命中率低很多。这是 profiler 对已 strip 系统二进制效果不佳的主要原因。

#### 步骤 3：找外部 debuginfo

Linux 约定：

- **按 Build-ID**：`/usr/lib/debug/.build-id/<aa>/<bb...>.debug`
- **按路径**：`/usr/lib/debug/<原路径>.debug`
- **debuginfod**：Fedora/RHEL 推动的 HTTP 服务，按 build-id 按需下载 debug 信息。`DEBUGINFOD_URLS` 环境变量控制。`perf`、`gdb`、`eu-unstrip` 均支持。

#### 步骤 4：行号信息（可选）

若要定位到"函数 + 行号"，需要读 DWARF 的 `.debug_line` 段。成本高，一般只在用户放大具体函数时解析。

### 6.3 JIT / 解释型语言

#### perf map 方案

JIT 代码地址不在 ELF 里，靠 `/tmp/perf-<pid>.map`：

```
7f8a2c500000 100 java/util/HashMap.get
7f8a2c500100 80  com/example/Service.handle
```

格式：`<十六进制地址> <十六进制长度> <符号名>`

- **Java**：`-XX:+PreserveFramePointer -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints`，配合 async-profiler agent 写 perf map。这是 deepflow `profile/java/` 在做的事。
- **Node.js**：`node --perf-basic-prof`，V8 负责写
- **.NET**：`dotnet-trace` / PerfCollect
- **V8 通用**：`--perf-prof`

#### 解释型语言（Python/Ruby/PHP）

perf map 救不了解释型语言——业务"函数"不是 JIT 代码，而是解释器 heap 上的数据结构。解决路径：

- **用户态 profiler**（py-spy 类）：`ptrace` 附加进程，直接读 `PyFrameObject` 链表
- **eBPF + 解释器偏移表**：BPF 里读 Python 进程的 `PyThreadState → frame → f_code → co_filename` 指针链。Parca、Pyroscope 的 Python eBPF profiler 走这条路，需要针对每个 Python 版本维护偏移表。
- **deepflow 当前主线**：源码里未见对 Python/Ruby 的显式解释器解析，实际只能看到解释器内部 C 栈。

### 6.4 常见符号化失败表现

| 现象 | 原因 |
|---|---|
| `[unknown]` | 地址落在 `/proc/<pid>/maps` 之外，或栈已走丢 |
| 只有库名没有函数名 | 库被 strip，`.dynsym` 也没命中 |
| `libjvm.so [unknown]` | Java JIT 代码，需要 perf map |
| 半截栈（2–3 层）| 用户二进制缺 frame pointer |
| 地址和函数名错位 | 符号缓存过期、进程 `mmap` 变更后未失效 |

## 7. 聚合成火焰图

到这一步数据长这样：

```
(pid=1234, comm=nginx,
 kstack=[schedule, sys_futex, ...],
 ustack=[main, handle_req, parse_http, ...],
 count=42)
```

后续处理：

1. **按栈聚合**：把"相同栈不同 pid"的计数合并
2. **折叠格式**（Brendan Gregg 的 `stackcollapse` 约定）：
   ```
   main;handle_req;parse_http 42
   main;handle_req;db_query;__libc_send 128
   ```
3. **生成火焰图**：按分号拆节点，相同前缀共享矩形，宽度 = 计数总和
   - SVG 版：`flamegraph.pl`
   - 交互式：`d3-flame-graph`

deepflow UI 展示的就是这个数据结构，server 侧按时间窗 + Pod + 进程做 on-demand 聚合。

## 8. 几个关键"为什么"

### 为什么栈回溯必须在内核中完成

被采样的进程在被内核中断的**那一刻**，寄存器状态是准确的。等 BPF 程序返回、进程继续执行后，`rsp/rbp` 早就变了，无法事后重建当时的栈。栈回溯必须就地完成。

### 为什么 stack 要单独建 map

BPF map key 大小通常上限 512 字节，而 127 层栈 × 8 字节 = 1KB，塞不进 counts map 的 key。所以栈单独存、用 stack id 做引用。同时哈希去重也大幅节省空间。

### 为什么 on-CPU profiler 看不到 off-CPU 时间

`PERF_COUNT_SW_CPU_CLOCK` 事件**只在该 CPU 上有进程运行时**才滴答。进程被调度出 CPU（等锁、等 IO、sleep）期间没有 `CPU_CLOCK` 事件发生，BPF 程序根本不会被触发。

做 off-CPU 必须换触发源——挂 `sched_switch` tracepoint，在进程"下车"时记录时间戳、"上车"时计算 delta、以 delta 作为该栈的 weight。实现复杂度高于 on-CPU。

### 为什么 continuous profiling 这几年才流行

三件事凑齐了：

1. **eBPF** 让栈回溯开销 < 1%（过去 `perf record` 写大量数据到磁盘）
2. **libbpf + CO-RE + BTF** 解决跨内核版本部署
3. **pprof 格式 + 对象存储**让"全年 profile 数据"的存储成本可接受

三者分别在 2018、2020、2021 前后成熟。

## 9. deepflow 源码对照表

| 文件 | 对应本文 |
|---|---|
| `@agent/src/ebpf/kernel/perf_profiler.bpf.c` | §2 BPF 程序 + §3 栈回溯 |
| `@agent/src/ebpf/kernel/include/perf_profiler.h` | §4 数据结构 |
| `@agent/src/ebpf/user/profile/perf_profiler.c` | §1 perf_event_open + §5 用户态读取 |
| `@agent/src/ebpf/user/profile/profile_common.c` | §5 聚合 + §6 符号化框架 |
| `@agent/src/ebpf/user/profile/stringifier.c` | §6 符号化 |
| `@agent/src/ebpf/user/profile/java/` | §6.3 JIT 语言 perf map |

建议阅读顺序：先看 `perf_profiler.bpf.c` 顶部几百行——能看到 `BPF_F_USER_STACK`、`stack_trace_key_t`、`bpf_get_stackid` 的用法，整个内核侧逻辑在 500 行以内，麻雀虽小五脏俱全。然后再看 `perf_profiler.c` 的用户态读取循环与 `stringifier.c` 的符号缓存实现。

## 10. 参考资料

- Brendan Gregg《Systems Performance》第 6 章（CPU）、第 13 章（perf）
- Brendan Gregg："Flame Graphs" 系列博文
- Linux 内核源码：`kernel/bpf/stackmap.c`（`bpf_get_stackid` 实现）
- libbpf + CO-RE 官方文档
- Parca Agent 源码（DWARF CFI 预处理的参考实现）
- `agent/ebpf-profiler-zh.md`：deepflow profiler 专题主文档
- `agent/build-docker-zh.md` §7.5：低内核系统的 profiler 关闭说明
