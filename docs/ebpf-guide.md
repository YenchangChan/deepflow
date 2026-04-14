# eBPF 知识普及：从入门到 DeepFlow 实践

## 目录

- [第一章：eBPF 是什么？](#第一章ebpf-是什么)
- [第二章：eBPF 能做什么？](#第二章ebpf-能做什么)
- [第三章：eBPF 如何工作？](#第三章ebpf-如何工作)
- [第四章：eBPF 程序类型详解](#第四章ebpf-程序类型详解)
- [第五章：eBPF 数据传递机制](#第五章ebpf-数据传递机制)
- [第六章：实操代码示例](#第六章实操代码示例)
- [第七章：DeepFlow 的 eBPF 实践](#第七章deepflow-的-ebpf-实践)
- [第八章：DeepFlow eBPF 架构深度剖析](#第八章deepflow-ebpf-架构深度剖析)
- [第九章：DeepFlow 如何利用 eBPF 做 Profiling 和分布式链路追踪](#第九章deepflow-如何利用-ebpf-做-profiling-和分布式链路追踪)
- [附录：术语表](#附录术语表)

---

## 第一章：eBPF 是什么？

### 1.1 一个比喻

想象 Linux 内核是一座高速运转的工厂。以前，如果你想在生产线上加一个质检环节，你必须停下整条生产线、改造设备、重新启动——这就是传统的「改内核代码、重新编译、重启」的方式。

**eBPF** 就像是给这座工厂装了一套「可热插拔的传感器系统」。你可以在工厂运行的时候，把小型传感器（eBPF 程序）安装到生产线的任意节点上，实时收集数据或做微调，而且：

- **不需要停工**（不用重启内核）
- **不会弄坏生产线**（有安全验证器保护）
- **随装随拆**（动态加载和卸载）

### 1.2 正式定义

**eBPF**（extended Berkeley Packet Filter）是 Linux 内核中的一个**沙箱化虚拟机**。它允许用户在**不修改内核源码、不加载内核模块**的情况下，在内核空间安全地运行自定义程序。

关键特性：

| 特性 | 说明 |
|------|------|
| **安全** | 所有程序必须通过内核验证器（Verifier）检查，保证不会崩溃或死循环 |
| **高效** | 程序被 JIT 编译为原生机器码，性能接近内核原生代码 |
| **动态** | 运行时加载/卸载，无需重启系统 |
| **可编程** | 用 C（受限子集）编写，通过 LLVM/Clang 编译为 eBPF 字节码 |

### 1.3 历史简述

```
1992  BPF 诞生（Berkeley Packet Filter）
      └─ 最初只用于网络包过滤（tcpdump 背后的技术）
      └─ 本质是一个简单的指令集虚拟机

2014  eBPF 进入 Linux 内核 3.18
      └─ "extended" = 扩展了寄存器、指令集、Map 等
      └─ 不再局限于网络，可以挂载到几乎任何内核事件

2016+ 生态爆发
      └─ kprobe/uprobe/tracepoint 支持
      └─ XDP（高性能网络处理）
      └─ BCC/bpftrace 等开发工具
      └─ Cilium、Falco、DeepFlow 等项目涌现

2020+ CO-RE（Compile Once, Run Everywhere）
      └─ BTF 让 eBPF 程序可以跨内核版本运行
```

---

## 第二章：eBPF 能做什么？

eBPF 的应用可以归纳为四大领域：

### 2.1 网络观测与处理

```
┌─────────────────────────────────────────┐
│  应用层                                  │
│  ┌─────┐ ┌─────┐ ┌─────┐               │
│  │Nginx│ │MySQL│ │Redis│               │
│  └──┬──┘ └──┬──┘ └──┬──┘               │
│     │       │       │                    │
│  ───┼───────┼───────┼──── syscall 边界 ──│
│     │       │       │                    │
│  内核层                                  │
│  ┌──▼───────▼───────▼──┐                │
│  │   TCP/IP 协议栈      │  ← eBPF 可以  │
│  │   │                  │    在这里拦截  │
│  │   ▼                  │    和观测      │
│  │   网络设备驱动        │               │
│  └─────────────────────┘                │
└─────────────────────────────────────────┘
```

**典型用途**：
- **流量观测**：不修改应用代码，自动采集 HTTP、DNS、MySQL 等协议的请求/响应数据
- **负载均衡**：在内核层直接转发数据包（如 Cilium 替代 kube-proxy）
- **DDoS 防护**：在网卡驱动层（XDP）丢弃恶意流量，速度极快

### 2.2 安全监控

- **系统调用审计**：监控所有进程的文件打开、网络连接、权限提升等行为
- **容器安全**：检测容器逃逸、异常进程执行（如 Falco）
- **运行时防护**：阻止已知的恶意行为模式

### 2.3 性能分析

- **CPU Profiling**：采样每个 CPU 上正在运行的函数调用栈，生成火焰图
- **延迟追踪**：测量函数执行耗时，定位性能瓶颈
- **I/O 分析**：追踪磁盘和网络 I/O 的延迟分布

### 2.4 网络策略

- **微服务网格**：替代 Sidecar 代理，直接在内核实现服务间的网络策略
- **透明加密**：对 Pod 间流量做 WireGuard 加密
- **带宽控制**：精细化的 QoS 策略

---

## 第三章：eBPF 如何工作？

### 3.1 整体工作流程

```
用户空间                              内核空间
┌──────────────────┐                ┌──────────────────────┐
│                  │                │                      │
│  1. 编写 C 代码   │                │  3. 验证器检查         │
│     │            │                │     │                │
│     ▼            │                │     ▼                │
│  2. Clang 编译    │  ── 加载 ──►  │  4. JIT 编译为机器码   │
│     为 eBPF      │                │     │                │
│     字节码        │                │     ▼                │
│                  │                │  5. 挂载到 Hook 点     │
│                  │                │     │                │
│  7. 用户程序      │  ◄── 读取 ──  │  6. 事件触发时执行     │
│     读取数据      │    （Map）     │     写入数据到 Map    │
│                  │                │                      │
└──────────────────┘                └──────────────────────┘
```

让我们逐步展开：

### 3.2 第一步：编写 eBPF 程序（受限 C 语言）

eBPF 程序用 C 的一个**受限子集**编写。限制包括：

- **不能有无限循环**（必须有明确的循环上界）
- **栈空间有限**（最大 512 字节）
- **不能随意访问内存**（必须用 `bpf_probe_read` 等辅助函数）
- **早期有指令数限制**（4096 条，Linux 5.2 后放宽到 100 万条）

一个简单的例子——监控所有 `write` 系统调用：

```c
// 这是一个挂载在 write() 系统调用入口的 eBPF 程序
SEC("tracepoint/syscalls/sys_enter_write")
int trace_write(struct trace_event_raw_sys_enter *ctx)
{
    // 获取当前进程 ID
    __u32 pid = bpf_get_current_pid_tgid() >> 32;

    // 获取 write() 的参数：文件描述符和写入长度
    int fd = (int)ctx->args[0];
    size_t count = (size_t)ctx->args[2];

    // 把数据写入 Map，让用户空间程序可以读取
    struct event_t event = {
        .pid = pid,
        .fd = fd,
        .count = count,
    };
    bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU,
                          &event, sizeof(event));
    return 0;
}
```

### 3.3 第二步：编译

使用 Clang/LLVM 将 C 代码编译为 eBPF 字节码（ELF 格式）：

```bash
clang -O2 -target bpf -c trace_write.c -o trace_write.o
```

### 3.4 第三步：验证器（Verifier）

这是 eBPF 最核心的安全机制。当程序被加载到内核时，验证器会：

```
                   eBPF 字节码
                       │
                       ▼
              ┌────────────────┐
              │   验证器检查    │
              │                │
              │ ✓ 没有无限循环  │
              │ ✓ 所有路径可达  │
              │ ✓ 内存访问安全  │
              │ ✓ 指令数合规    │
              │ ✓ 辅助函数合法  │
              └───────┬────────┘
                      │
              ┌───────┴───────┐
              │               │
              ▼               ▼
          通过 ✅          拒绝 ❌
          │               │
          ▼               ▼
      JIT 编译         返回错误
      挂载执行         （程序不会被加载）
```

验证器通过**模拟执行所有可能的代码路径**来确保安全。这意味着你的 eBPF 程序不可能导致内核崩溃——最差的情况也只是被拒绝加载。

### 3.5 第四步：JIT 编译

通过验证后，eBPF 字节码被 JIT（Just-In-Time）编译器翻译成目标架构的原生机器码（x86、ARM 等），运行速度接近内核原生函数。

### 3.6 第五步：挂载到 Hook 点

eBPF 程序需要挂载到内核中的「钩子（Hook）」上。当相应事件发生时，程序自动执行。

```
内核事件发生
     │
     ▼
┌─────────────────────────────────────────────┐
│                内核 Hook 点                   │
│                                             │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  │
│  │ kprobe  │  │tracepoint│  │   uprobe   │  │
│  │ 函数入口 │  │ 静态埋点  │  │用户态函数  │  │
│  └────┬────┘  └────┬─────┘  └─────┬─────┘  │
│       │            │              │          │
│       ▼            ▼              ▼          │
│    ┌─────────────────────────────────┐      │
│    │      你的 eBPF 程序在此执行      │      │
│    └─────────────────────────────────┘      │
│                                             │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  │
│  │  XDP    │  │    TC    │  │perf_event │  │
│  │网卡驱动  │  │ 流量控制  │  │ CPU 采样  │  │
│  └─────────┘  └──────────┘  └───────────┘  │
│                                             │
└─────────────────────────────────────────────┘
```

---

## 第四章：eBPF 程序类型详解

### 4.1 Kprobe / Kretprobe — 内核函数探针

**什么是 Kprobe？**

Kprobe 可以挂载到内核中**几乎任何函数**的入口处。当该函数被调用时，你的 eBPF 程序先于原函数执行。Kretprobe 则是在函数**返回时**执行。

```
普通执行流:   调用者 ──► tcp_sendmsg() ──► 返回

加了 Kprobe:  调用者 ──► [你的 eBPF 程序] ──► tcp_sendmsg() ──► [你的 Kretprobe] ──► 返回
```

**优点**：几乎可以挂载到任何内核函数，非常灵活。
**缺点**：依赖内核内部函数签名，内核升级可能导致函数名或参数变化。

**典型用途**：追踪 TCP 连接建立、文件系统操作、内存分配等。

### 4.2 Tracepoint — 静态追踪点

**什么是 Tracepoint？**

Tracepoint 是内核开发者预先埋好的**稳定追踪点**。它们有明确的接口定义，不会随内核版本轻易变化。

```
Kprobe:       可以挂在任何函数上，但接口不稳定
Tracepoint:   只能挂在预定义的点上，但接口稳定
```

**常见的 Tracepoint 示例**：
- `tracepoint/syscalls/sys_enter_write` — write 系统调用入口
- `tracepoint/syscalls/sys_exit_read` — read 系统调用返回
- `tracepoint/sched/sched_process_exec` — 进程执行
- `tracepoint/sched/sched_process_exit` — 进程退出

**优点**：接口稳定，跨内核版本兼容性好。
**缺点**：只能用在内核预定义的追踪点上。

### 4.3 Uprobe / Uretprobe — 用户态函数探针

**什么是 Uprobe？**

Uprobe 跟 Kprobe 类似，但挂载的是**用户态程序**的函数。可以追踪 OpenSSL、Go 运行时、Java 等应用程序的内部函数。

```
用户态程序:   main() ──► SSL_write(data) ──► 返回

加了 Uprobe:  main() ──► [eBPF 拦截到 data] ──► SSL_write(data) ──► 返回
```

**典型用途**：
- 拦截 `SSL_write` / `SSL_read`，在加密前/解密后捕获明文数据
- 追踪 Go 语言的 goroutine 调度
- 监控数据库客户端库的查询操作

**优点**：无需修改应用代码即可观测应用内部行为。
**缺点**：依赖二进制符号表，需要应用程序未被 strip。

### 4.4 XDP — 极速网络处理

**什么是 XDP？**

XDP（eXpress Data Path）在网卡驱动层处理数据包——这是 Linux 网络栈中**最早**可以触及数据包的地方。

```
网络数据包到达
     │
     ▼
  ┌──────┐
  │ 网卡  │
  │ 驱动  │──► XDP eBPF 程序 ──┬── XDP_PASS  → 继续进入协议栈
  └──────┘                    ├── XDP_DROP  → 直接丢弃
                              ├── XDP_TX    → 从同一网卡发回
                              └── XDP_REDIRECT → 转发到其他接口
```

**典型用途**：DDoS 防护（百万包/秒级丢弃）、高性能负载均衡。

### 4.5 Perf Event — CPU 性能采样

**什么是 Perf Event eBPF？**

挂载到 CPU 硬件性能计数器上，以固定频率或计数间隔触发 eBPF 程序。

**典型用途**：
- CPU 热点分析（采集函数调用栈，生成火焰图）
- Cache miss 分析
- 分支预测分析

### 4.6 各类型对比总结

| 类型 | 挂载位置 | 稳定性 | 性能开销 | 典型场景 |
|------|---------|--------|---------|---------|
| Kprobe | 任意内核函数 | 低（函数可能变） | 低 | 追踪内核行为 |
| Tracepoint | 预定义追踪点 | 高 | 极低 | 系统调用监控 |
| Uprobe | 用户态函数 | 依赖二进制 | 中 | 应用内部观测 |
| XDP | 网卡驱动 | 高 | 极低 | 高性能网络处理 |
| TC | 流量控制层 | 高 | 低 | 网络策略实施 |
| Perf Event | CPU 计数器 | 高 | 可调 | 性能分析 |

---

## 第五章：eBPF 数据传递机制

eBPF 程序运行在内核空间，用户程序运行在用户空间。它们之间通过 **eBPF Map** 通信。

### 5.1 eBPF Map — 内核与用户空间的桥梁

Map 是一个内核中的键值数据结构，两边都可以读写：

```
用户空间                           内核空间
┌──────────┐    read/write    ┌──────────────┐
│          │ ◄──────────────► │              │
│ 用户程序  │                  │  eBPF 程序    │
│          │    ┌────────┐    │              │
│          │───►│eBPF Map│◄───│              │
│          │    └────────┘    │              │
└──────────┘                  └──────────────┘
```

### 5.2 常用 Map 类型

#### Hash Map（哈希表）
```
用途：存储键值对，如 "连接ID → 连接状态"
特点：O(1) 查找
示例：socket_info_map[pid_fd] = socket_info
```

#### Array Map（数组）
```
用途：通过索引快速访问，如 "CPU编号 → 配置"
特点：固定大小，O(1) 索引
示例：config_map[0] = global_config
```

#### Per-CPU Array / Hash
```
用途：同 Array/Hash，但每个 CPU 有独立副本
特点：无锁，极高性能
示例：每个 CPU 核心有自己的数据缓冲区
```

#### Perf Event Array（性能事件数组）
```
用途：从内核向用户空间高效流式传输事件数据
特点：环形缓冲区，per-CPU，异步读取

内核:  bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU, &data, size)
用户:  perf_buffer_read() / epoll_wait() 接收事件
```

这是大量数据从内核传输到用户空间最常用的方式。

#### Ring Buffer（环形缓冲区，Linux 5.8+）
```
用途：替代 Perf Event Array 的新一代方案
特点：所有 CPU 共享一个缓冲区，减少内存浪费，支持变长数据
```

#### LRU Hash（最近最少使用哈希表）
```
用途：缓存场景，自动淘汰旧数据
特点：空间满时自动删除最久未使用的条目
示例：HTTP/2 TCP 序列号追踪
```

#### Prog Array（程序数组）
```
用途：实现 Tail Call（尾调用）——一个 eBPF 程序跳转到另一个
特点：规避单程序指令数限制
示例：将协议解析逻辑拆分为多个子程序
```

### 5.3 数据传输模式对比

```
方式1: 轮询读取 Map（主动拉取）
  用户程序 ──定时──► 读取 Hash/Array Map ──► 处理数据
  适合：统计计数器、状态查询

方式2: Perf Event / Ring Buffer（被动推送）
  内核 eBPF ──事件──► perf buffer / ring buffer ──► 用户程序收到通知
  适合：高频事件流（网络包、系统调用追踪）
```

---

## 第六章：实操代码示例

下面提供 5 个可以直接编译运行的 eBPF 示例，从简单到复杂，帮助你建立实际的动手经验。

> **环境要求**：
> - Linux 内核 5.4+（推荐 5.15+）
> - 安装 `clang`、`llvm`、`libbpf-dev`（或 `libbpf-devel`）、`bpftool`、`linux-headers`
> - 以 root 权限运行
>
> **Ubuntu/Debian 一键安装**：
> ```bash
> sudo apt install clang llvm libbpf-dev linux-headers-$(uname -r) bpftool
> ```
>
> **CentOS/Fedora 一键安装**：
> ```bash
> sudo dnf install clang llvm libbpf-devel kernel-devel bpftool
> ```

---

### 示例 1：Hello World — 追踪 execve 系统调用

这是最简单的 eBPF 程序：每当有进程被执行时，打印一行日志。

#### eBPF 内核侧程序 `hello.bpf.c`

```c
// hello.bpf.c — 追踪所有 execve 调用
#include "vmlinux.h"           // 内核类型定义（由 bpftool 生成）
#include <bpf/bpf_helpers.h>

// SEC() 宏告诉加载器：这个函数要挂载到 sys_enter_execve tracepoint
SEC("tracepoint/syscalls/sys_enter_execve")
int handle_execve(struct trace_event_raw_sys_enter *ctx)
{
    // bpf_get_current_pid_tgid() 返回 64 位值
    // 高 32 位 = TGID (进程 ID)，低 32 位 = TID (线程 ID)
    u32 pid = bpf_get_current_pid_tgid() >> 32;

    // 获取进程名（最多 16 字节）
    char comm[16];
    bpf_get_current_comm(&comm, sizeof(comm));

    // bpf_printk 输出到 /sys/kernel/debug/tracing/trace_pipe
    // 生产环境不用这个（性能差），但学习阶段非常方便
    bpf_printk("Hello eBPF! pid=%d comm=%s", pid, comm);

    return 0;
}

// 所有 eBPF 程序必须声明 License，GPL 才能使用所有辅助函数
char LICENSE[] SEC("license") = "GPL";
```

#### 用户侧加载程序 `hello.c`

```c
// hello.c — 加载 eBPF 程序并持续运行
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <bpf/libbpf.h>
#include "hello.skel.h"        // 由 bpftool gen skeleton 生成

static volatile bool running = true;

static void sig_handler(int sig)
{
    running = false;
}

int main(void)
{
    struct hello_bpf *skel;
    int err;

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    // 1. 打开：解析 ELF，准备数据结构
    skel = hello_bpf__open();
    if (!skel) {
        fprintf(stderr, "Failed to open BPF skeleton\n");
        return 1;
    }

    // 2. 加载：将程序送入内核，经过验证器检查
    err = hello_bpf__load(skel);
    if (err) {
        fprintf(stderr, "Failed to load BPF program: %d\n", err);
        goto cleanup;
    }

    // 3. 挂载：将程序附加到 tracepoint
    err = hello_bpf__attach(skel);
    if (err) {
        fprintf(stderr, "Failed to attach BPF program: %d\n", err);
        goto cleanup;
    }

    printf("eBPF program loaded. Run this to see output:\n");
    printf("  sudo cat /sys/kernel/debug/tracing/trace_pipe\n");
    printf("Press Ctrl+C to stop.\n");

    // 4. 保持运行，直到收到信号
    while (running)
        sleep(1);

cleanup:
    // 5. 卸载：清理所有 eBPF 资源
    hello_bpf__destroy(skel);
    return err < 0 ? 1 : 0;
}
```

#### 编译与运行

```bash
# 1. 生成 vmlinux.h（内核类型定义，只需要做一次）
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h

# 2. 编译 eBPF 程序为字节码
clang -g -O2 -target bpf -c hello.bpf.c -o hello.bpf.o

# 3. 生成 skeleton 头文件（自动生成加载代码）
bpftool gen skeleton hello.bpf.o > hello.skel.h

# 4. 编译用户态程序
clang -g -O2 -o hello hello.c -lbpf -lelf -lz

# 5. 运行（需要 root）
sudo ./hello

# 6. 在另一个终端查看输出
sudo cat /sys/kernel/debug/tracing/trace_pipe
# 输出示例：
#   <...>-12345 [002] d... 1234.567890: bpf_trace_printk: Hello eBPF! pid=12345 comm=ls
#   <...>-12346 [001] d... 1234.567891: bpf_trace_printk: Hello eBPF! pid=12346 comm=bash
```

**要点**：
- `SEC("tracepoint/syscalls/sys_enter_execve")` 决定了挂载位置
- `bpf_printk` 是调试利器，但生产环境应改用 Map 传递数据
- skeleton（`.skel.h`）自动生成了 open/load/attach/destroy 的样板代码

---

### 示例 2：Kprobe — 追踪 TCP 连接建立

追踪所有 `tcp_connect` 内核函数调用，获取连接的目标地址和端口。

#### eBPF 内核侧程序 `tcp_connect.bpf.c`

```c
// tcp_connect.bpf.c — 追踪 TCP 连接建立
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>  // CO-RE 读取辅助宏

// 定义传递给用户空间的事件结构
struct event {
    u32 pid;
    u32 uid;
    u16 dport;       // 目标端口
    u32 daddr;       // 目标 IPv4 地址
    char comm[16];   // 进程名
};

// 定义一个 Perf Event Array Map，用于向用户空间推送事件
struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, sizeof(u32));
} events SEC(".maps");

// 挂载到 tcp_connect 内核函数入口
SEC("kprobe/tcp_connect")
int BPF_KPROBE(trace_tcp_connect, struct sock *sk)
{
    struct event evt = {};

    // 获取进程信息
    evt.pid = bpf_get_current_pid_tgid() >> 32;
    evt.uid = bpf_get_current_uid_gid() & 0xFFFFFFFF;
    bpf_get_current_comm(&evt.comm, sizeof(evt.comm));

    // 使用 CO-RE 安全读取内核结构体字段
    // BPF_CORE_READ 会根据当前内核的 BTF 信息自动适配字段偏移
    evt.dport = __builtin_bswap16(BPF_CORE_READ(sk, __sk_common.skc_dport));
    evt.daddr = BPF_CORE_READ(sk, __sk_common.skc_daddr);

    // 通过 Perf Event 推送到用户空间
    bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU,
                          &evt, sizeof(evt));
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
```

#### 用户侧程序 `tcp_connect.c`

```c
// tcp_connect.c — 接收并打印 TCP 连接事件
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include "tcp_connect.skel.h"

struct event {
    __u32 pid;
    __u32 uid;
    __u16 dport;
    __u32 daddr;
    char comm[16];
};

static volatile bool running = true;

static void sig_handler(int sig) { running = false; }

// 每当 eBPF 推送一个事件，这个回调函数就会被调用
static void handle_event(void *ctx, int cpu, void *data, __u32 size)
{
    struct event *evt = data;
    char addr_str[INET_ADDRSTRLEN];

    // 将二进制 IP 转为可读字符串
    inet_ntop(AF_INET, &evt->daddr, addr_str, sizeof(addr_str));

    printf("%-8d %-16s → %s:%d\n",
           evt->pid, evt->comm, addr_str, evt->dport);
}

int main(void)
{
    struct tcp_connect_bpf *skel;
    struct perf_buffer *pb = NULL;
    int err;

    signal(SIGINT, sig_handler);

    skel = tcp_connect_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "Failed to open and load BPF skeleton\n");
        return 1;
    }

    err = tcp_connect_bpf__attach(skel);
    if (err) {
        fprintf(stderr, "Failed to attach: %d\n", err);
        goto cleanup;
    }

    // 创建 Perf Buffer 读取器
    // 参数：bpf_map fd, 回调函数, 丢失事件回调, 上下文, 页数
    pb = perf_buffer__new(
        bpf_map__fd(skel->maps.events),
        16,              // 每个 CPU 的缓冲区页数
        handle_event,    // 收到事件的回调
        NULL,            // 丢失事件的回调（可选）
        NULL,            // 用户上下文
        NULL             // 选项
    );
    if (!pb) {
        fprintf(stderr, "Failed to create perf buffer\n");
        goto cleanup;
    }

    printf("%-8s %-16s   %s\n", "PID", "COMM", "DESTINATION");
    printf("%-8s %-16s   %s\n", "---", "----", "-----------");

    while (running) {
        // poll Perf Buffer，超时 100ms
        err = perf_buffer__poll(pb, 100);
        if (err < 0 && err != -EINTR) {
            fprintf(stderr, "Error polling perf buffer: %d\n", err);
            break;
        }
    }

cleanup:
    perf_buffer__free(pb);
    tcp_connect_bpf__destroy(skel);
    return 0;
}
```

#### 运行效果

```bash
sudo ./tcp_connect
PID      COMM               DESTINATION
---      ----               -----------
3521     curl               142.250.80.100:443
3522     wget               151.101.1.6:80
1234     python3            127.0.0.1:5432
8080     node               10.0.0.5:6379
```

**要点**：
- `BPF_KPROBE` 宏让你可以像写普通 C 函数一样访问被 Hook 函数的参数
- `BPF_CORE_READ` 是 CO-RE 的关键——同一份编译产物可以运行在不同内核版本上
- Perf Event Array 是高频事件传输的标准方式

---

### 示例 3：Uprobe — 拦截 OpenSSL 明文数据

这个示例展示了 DeepFlow 捕获加密流量的核心原理：在 `SSL_write` 加密前拦截明文。

#### eBPF 内核侧程序 `ssl_sniff.bpf.c`

```c
// ssl_sniff.bpf.c — 拦截 SSL_write，在加密前捕获明文
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

#define MAX_DATA_LEN 256

struct ssl_event {
    u32 pid;
    u32 tid;
    u32 len;            // 实际数据长度
    u8  data[MAX_DATA_LEN];  // 明文数据（截断到 256 字节）
    char comm[16];
};

// Perf Event Map，推送明文数据到用户空间
struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, sizeof(u32));
} events SEC(".maps");

// 临时存储 SSL_write 的参数（因为 uprobe 和 uretprobe 不共享栈）
// 用 Per-CPU Array 避免锁竞争
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, u64);          // pid_tgid
    __type(value, const void *);// buf 指针
} ssl_write_args SEC(".maps");

// Uprobe: 挂载到 SSL_write 函数入口
// SSL_write 原型: int SSL_write(SSL *ssl, const void *buf, int num)
SEC("uprobe/SSL_write")
int handle_ssl_write_entry(struct pt_regs *ctx)
{
    u64 pid_tgid = bpf_get_current_pid_tgid();

    // 第二个参数 buf 是明文数据指针
    // 在 x86_64 上，函数参数依次在 rdi, rsi, rdx, rcx...
    const void *buf = (const void *)PT_REGS_PARM2(ctx);

    // 保存 buf 指针，等 SSL_write 返回时再读取
    // （也可以在入口直接读取，但在返回时可以确认调用是否成功）
    bpf_map_update_elem(&ssl_write_args, &pid_tgid, &buf, BPF_ANY);
    return 0;
}

// Uretprobe: SSL_write 返回时触发
SEC("uretprobe/SSL_write")
int handle_ssl_write_return(struct pt_regs *ctx)
{
    u64 pid_tgid = bpf_get_current_pid_tgid();

    // 查找之前保存的 buf 指针
    const void **buf_ptr = bpf_map_lookup_elem(&ssl_write_args, &pid_tgid);
    if (!buf_ptr)
        return 0;

    // 获取返回值（实际写入字节数），返回值 <= 0 表示失败
    int ret = PT_REGS_RC(ctx);
    if (ret <= 0)
        goto cleanup;

    // 构造事件
    struct ssl_event evt = {};
    evt.pid = pid_tgid >> 32;
    evt.tid = (u32)pid_tgid;
    evt.len = (u32)ret;
    bpf_get_current_comm(&evt.comm, sizeof(evt.comm));

    // 从用户空间内存读取明文数据
    u32 read_len = ret < MAX_DATA_LEN ? ret : MAX_DATA_LEN;
    bpf_probe_read_user(&evt.data, read_len, *buf_ptr);

    // 推送到用户空间
    bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU,
                          &evt, sizeof(evt));

cleanup:
    bpf_map_delete_elem(&ssl_write_args, &pid_tgid);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
```

#### 挂载 Uprobe（关键步骤）

Uprobe 不像 Tracepoint 那样自动挂载，需要指定**目标二进制文件和函数名**：

```c
// 在用户态加载代码中手动挂载 uprobe
// 找到系统上 libssl.so 的路径
const char *libssl_path = "/usr/lib/x86_64-linux-gnu/libssl.so.3";

// 挂载到 SSL_write 函数入口（offset 自动由 libbpf 解析符号表）
skel->links.handle_ssl_write_entry =
    bpf_program__attach_uprobe(
        skel->progs.handle_ssl_write_entry,
        false,           // false=uprobe（入口），true=uretprobe（返回）
        -1,              // -1 = 所有进程
        libssl_path,     // 目标共享库路径
        0                // 函数偏移（0 表示自动查找符号）
    );

skel->links.handle_ssl_write_return =
    bpf_program__attach_uprobe(
        skel->progs.handle_ssl_write_return,
        true,            // true = uretprobe
        -1,
        libssl_path,
        0
    );
```

#### 运行效果

```bash
sudo ./ssl_sniff
# 在另一个终端执行:
#   curl https://example.com

# 输出示例（加密前的明文 HTTP 请求）:
PID=4521 COMM=curl LEN=78
  GET / HTTP/1.1
  Host: example.com
  User-Agent: curl/7.81.0
  Accept: */*
```

**要点**：
- Uprobe 入口捕获函数参数（buf 指针），Uretprobe 捕获返回值并读取数据
- `bpf_probe_read_user` 安全地从用户空间内存复制数据到 eBPF 栈
- 这正是 DeepFlow 无需私钥即可解密 HTTPS 流量的核心原理

---

### 示例 4：Perf Event — CPU 火焰图采样

定频采样 CPU 上正在运行的函数调用栈，这是 DeepFlow Continuous Profiling 的基础。

#### eBPF 内核侧程序 `profiler.bpf.c`

```c
// profiler.bpf.c — CPU 性能采样，采集函数调用栈
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

#define MAX_STACK_DEPTH 128

// 栈追踪 Map：内核自动填充调用栈帧
struct {
    __uint(type, BPF_MAP_TYPE_STACK_TRACE);
    __uint(key_size, sizeof(u32));     // stack_id
    __uint(value_size, MAX_STACK_DEPTH * sizeof(u64));  // 栈帧地址数组
    __uint(max_entries, 16384);
} stack_traces SEC(".maps");

// 采样事件结构
struct sample_key {
    u32 pid;
    u32 tid;
    int kernel_stack_id;   // 内核态调用栈 ID
    int user_stack_id;     // 用户态调用栈 ID
    char comm[16];
};

// 计数 Map：相同调用栈出现的次数
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 65536);
    __type(key, struct sample_key);
    __type(value, u64);               // 采样次数
} counts SEC(".maps");

// 挂载到 Perf Event（定频 CPU 采样）
SEC("perf_event")
int do_sample(struct bpf_perf_event_data *ctx)
{
    struct sample_key key = {};
    u64 *count;
    u64 one = 1;

    u64 pid_tgid = bpf_get_current_pid_tgid();
    key.pid = pid_tgid >> 32;
    key.tid = (u32)pid_tgid;

    // 跳过内核线程（pid=0）
    if (key.pid == 0)
        return 0;

    bpf_get_current_comm(&key.comm, sizeof(key.comm));

    // 采集内核态调用栈
    // 返回值是 stack_id（Map 中的索引），< 0 表示失败
    key.kernel_stack_id =
        bpf_get_stackid(ctx, &stack_traces, 0);

    // 采集用户态调用栈
    // BPF_F_USER_STACK 标志表示获取用户空间的栈
    key.user_stack_id =
        bpf_get_stackid(ctx, &stack_traces, BPF_F_USER_STACK);

    // 累加计数：相同的 (pid, 内核栈, 用户栈) 组合加 1
    count = bpf_map_lookup_elem(&counts, &key);
    if (count)
        __sync_fetch_and_add(count, 1);
    else
        bpf_map_update_elem(&counts, &key, &one, BPF_ANY);

    return 0;
}

char LICENSE[] SEC("license") = "GPL";
```

#### 用户侧程序核心逻辑

```c
// profiler.c 核心片段 — 挂载到所有 CPU 的 perf_event 并定期读取

#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>

// 打开 perf_event，设置 99Hz 采样频率
static int open_perf_event(int cpu)
{
    struct perf_event_attr attr = {
        .type = PERF_TYPE_SOFTWARE,
        .config = PERF_COUNT_SW_CPU_CLOCK,
        .sample_freq = 99,        // 每秒 99 次采样（质数避免共振）
        .freq = 1,                // 使用频率模式而非周期模式
    };
    // 系统调用打开 perf event fd
    return syscall(SYS_perf_event_open, &attr, -1, cpu, -1, 0);
}

// 对每个 CPU 挂载 eBPF 程序
int nr_cpus = libbpf_num_possible_cpus();
for (int cpu = 0; cpu < nr_cpus; cpu++) {
    int perf_fd = open_perf_event(cpu);

    // 将 eBPF 程序关联到这个 perf event
    bpf_program__attach_perf_event(skel->progs.do_sample, perf_fd);
    ioctl(perf_fd, PERF_EVENT_IOC_ENABLE, 0);
}

// 定期从 counts Map 读取数据并生成火焰图
while (running) {
    sleep(10);  // 每 10 秒采集一次

    // 遍历 counts Map
    struct sample_key key, next_key;
    u64 count;

    while (bpf_map_get_next_key(map_fd, &key, &next_key) == 0) {
        bpf_map_lookup_elem(map_fd, &next_key, &count);

        // 通过 stack_id 从 stack_traces Map 读取实际的栈帧地址
        u64 stack[MAX_STACK_DEPTH];
        bpf_map_lookup_elem(stack_map_fd, &next_key.user_stack_id, &stack);

        // stack[] 中的每个地址可通过 /proc/<pid>/maps 解析为函数名
        // 输出格式兼容 brendangregg/FlameGraph 工具
        print_stack(next_key.comm, stack, count);

        // 清除已读数据
        bpf_map_delete_elem(map_fd, &next_key);
        key = next_key;
    }
}
```

#### 生成火焰图

```bash
# 运行采样 30 秒
sudo timeout 30 ./profiler > stacks.txt

# 用 FlameGraph 工具生成 SVG
git clone https://github.com/brendangregg/FlameGraph.git
cat stacks.txt | FlameGraph/stackcollapse.pl | FlameGraph/flamegraph.pl > flame.svg

# 浏览器打开 flame.svg 即可交互式查看
```

**要点**：
- `BPF_MAP_TYPE_STACK_TRACE` 是内核提供的专用 Map，自动采集调用栈
- `bpf_get_stackid` 一次调用即可获取完整调用栈，内核自动去重
- 99Hz 是经典的采样频率——足够精确，又避免与系统时钟共振
- DeepFlow 的 `perf_profiler.bpf.c` 在此基础上加了双缓冲和更精细的过滤

---

### 示例 5：Tail Call — 拆分大型程序

当你的 eBPF 程序太大（超过指令限制）时，可以用 Tail Call 拆分为多个子程序。
这正是 DeepFlow `socket_trace.bpf.c` 的核心设计模式。

#### eBPF 内核侧程序 `tailcall_demo.bpf.c`

```c
// tailcall_demo.bpf.c — 演示 Tail Call 拆分协议解析
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

// 子程序索引
#define PROG_PARSE_HTTP  0
#define PROG_PARSE_MYSQL 1
#define PROG_OUTPUT      2

// Per-CPU 临时存储（在 Tail Call 链中共享数据）
struct parse_ctx {
    u32 pid;
    u32 protocol;    // 识别出的协议
    char data[128];  // 请求数据片段
    char comm[16];
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, struct parse_ctx);
} ctx_map SEC(".maps");

// 程序跳转表（Prog Array）
struct {
    __uint(type, BPF_MAP_TYPE_PROG_ARRAY);
    __uint(max_entries, 8);
    __type(key, u32);
    __type(value, u32);
} jmp_table SEC(".maps");

// Perf Event 输出
struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, sizeof(u32));
} events SEC(".maps");

// ─── 入口程序 ───
SEC("tracepoint/syscalls/sys_enter_write")
int trace_write(struct trace_event_raw_sys_enter *ctx)
{
    u32 zero = 0;
    struct parse_ctx *pctx = bpf_map_lookup_elem(&ctx_map, &zero);
    if (!pctx)
        return 0;

    pctx->pid = bpf_get_current_pid_tgid() >> 32;
    pctx->protocol = 0;
    bpf_get_current_comm(&pctx->comm, sizeof(pctx->comm));

    // 读取 write() 的 buf 参数（前 128 字节）
    const void *buf = (const void *)ctx->args[1];
    bpf_probe_read_user(&pctx->data, sizeof(pctx->data), buf);

    // Tail Call 跳转到 HTTP 解析子程序
    bpf_tail_call(ctx, &jmp_table, PROG_PARSE_HTTP);
    // 如果 tail_call 失败（比如子程序未加载），继续执行到这里
    return 0;
}

// ─── 子程序 1: HTTP 协议检测 ───
SEC("tracepoint/syscalls/sys_enter_write")  // SEC 必须与入口相同
int parse_http(struct trace_event_raw_sys_enter *ctx)
{
    u32 zero = 0;
    struct parse_ctx *pctx = bpf_map_lookup_elem(&ctx_map, &zero);
    if (!pctx)
        return 0;

    // 检查是否是 HTTP 请求
    // "GET " = 0x47455420, "POST" = 0x504f5354, "HTTP" = 0x48545450
    u32 first_word = *(u32 *)pctx->data;
    if (first_word == 0x20544547 ||     // "GET " (小端序)
        first_word == 0x54534f50 ||     // "POST"
        first_word == 0x50545448) {     // "HTTP"
        pctx->protocol = 1;  // HTTP
        // 识别成功，跳到输出
        bpf_tail_call(ctx, &jmp_table, PROG_OUTPUT);
    }

    // 不是 HTTP，尝试下一个协议
    bpf_tail_call(ctx, &jmp_table, PROG_PARSE_MYSQL);
    return 0;
}

// ─── 子程序 2: MySQL 协议检测 ───
SEC("tracepoint/syscalls/sys_enter_write")
int parse_mysql(struct trace_event_raw_sys_enter *ctx)
{
    u32 zero = 0;
    struct parse_ctx *pctx = bpf_map_lookup_elem(&ctx_map, &zero);
    if (!pctx)
        return 0;

    // MySQL 协议：第 5 个字节是命令类型
    // 0x03 = COM_QUERY（SQL 查询）
    if (pctx->data[4] == 0x03) {
        pctx->protocol = 2;  // MySQL
    }

    // 无论是否识别，都跳到输出
    bpf_tail_call(ctx, &jmp_table, PROG_OUTPUT);
    return 0;
}

// ─── 子程序 3: 输出结果 ───
SEC("tracepoint/syscalls/sys_enter_write")
int output_result(struct trace_event_raw_sys_enter *ctx)
{
    u32 zero = 0;
    struct parse_ctx *pctx = bpf_map_lookup_elem(&ctx_map, &zero);
    if (!pctx || pctx->protocol == 0)
        return 0;

    // 推送识别结果到用户空间
    bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU,
                          pctx, sizeof(*pctx));
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
```

#### 用户侧：注册 Tail Call 跳转表

```c
// 加载后，需要把子程序的 fd 填入跳转表
int jmp_fd = bpf_map__fd(skel->maps.jmp_table);

// PROG_PARSE_HTTP=0 → parse_http 程序
int prog_fd = bpf_program__fd(skel->progs.parse_http);
u32 key = PROG_PARSE_HTTP;
bpf_map_update_elem(jmp_fd, &key, &prog_fd, BPF_ANY);

// PROG_PARSE_MYSQL=1 → parse_mysql 程序
prog_fd = bpf_program__fd(skel->progs.parse_mysql);
key = PROG_PARSE_MYSQL;
bpf_map_update_elem(jmp_fd, &key, &prog_fd, BPF_ANY);

// PROG_OUTPUT=2 → output_result 程序
prog_fd = bpf_program__fd(skel->progs.output_result);
key = PROG_OUTPUT;
bpf_map_update_elem(jmp_fd, &key, &prog_fd, BPF_ANY);
```

#### 执行流程图

```
write() 系统调用触发
       │
       ▼
  trace_write (入口)
  ├── 采集 pid, comm, 前 128 字节数据
  └── tail_call → parse_http
                     │
                     ├── 是 HTTP? ──是──► tail_call → output_result → 推送事件
                     │
                     └── 不是 ──► tail_call → parse_mysql
                                                │
                                                ├── 是 MySQL? → protocol=2
                                                │
                                                └── tail_call → output_result → 推送事件
```

**要点**：
- 所有子程序的 `SEC()` 类型必须与入口程序相同
- 子程序间通过 Per-CPU Map（`ctx_map`）共享数据，因为 Tail Call 不共享栈
- `bpf_tail_call` 失败时会继续执行后续代码（不会崩溃），这提供了安全降级
- DeepFlow 的实际实现用了 5 个 Tail Call 阶段，检测 20+ 种协议

---

### 示例间的递进关系

```
示例 1 (Hello World)
  │  学会：SEC 挂载、bpf_printk 调试、skeleton 编译流程
  ▼
示例 2 (TCP Connect)
  │  学会：Kprobe、CO-RE 跨内核适配、Perf Event 数据推送
  ▼
示例 3 (SSL Sniff)
  │  学会：Uprobe、用户态内存读取、入口/返回配对追踪
  ▼
示例 4 (CPU Profiler)
  │  学会：Perf Event 采样、Stack Trace Map、火焰图生成
  ▼
示例 5 (Tail Call)
     学会：程序拆分、跳转表、Per-CPU 数据共享 — DeepFlow 的核心模式
```

---

## 第七章：DeepFlow 的 eBPF 实践

现在让我们看看 DeepFlow 是如何利用 eBPF 实现**零侵入、全栈可观测**的。

### 7.1 DeepFlow 为什么需要 eBPF？

传统的应用可观测性（APM）需要在每个服务中插入 SDK 或 Agent：

```
传统方式（侵入式）：
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Service A │     │ Service B │     │ Service C │
│ +SDK埋点  │────►│ +SDK埋点  │────►│ +SDK埋点  │
└──────────┘     └──────────┘     └──────────┘
问题：侵入业务代码、语言绑定、维护成本高

DeepFlow 方式（eBPF 零侵入）：
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Service A │     │ Service B │     │ Service C │
│（无需改动）│────►│（无需改动）│────►│（无需改动）│
└────┬─────┘     └─────┬────┘     └────┬─────┘
     │                 │               │
  ───┼─────────────────┼───────────────┼─── syscall 边界
     │                 │               │
┌────▼─────────────────▼───────────────▼────┐
│              DeepFlow Agent eBPF           │
│         自动采集所有服务的通信数据           │
└───────────────────────────────────────────┘
```

### 7.2 DeepFlow eBPF 采集了什么？

DeepFlow 通过 eBPF 采集四大类数据：

#### (1) 网络 Socket 数据
通过 Hook 系统调用，捕获所有进出的网络流量：

```
应用程序
    │
    ├── write() / sendto() / sendmsg()     ← 发送数据
    ├── read() / recvfrom() / recvmsg()    ← 接收数据
    ├── connect()                           ← 建立连接
    ├── close()                             ← 关闭连接
    │
    ▼
DeepFlow eBPF 在系统调用入口/出口拦截，采集：
  - 源/目标 IP 和端口
  - 协议类型
  - 请求/响应数据（用于 L7 协议解析）
  - 时间戳
  - 进程 PID/TGID/名称
```

#### (2) 加密流量（TLS/SSL）
通过 Uprobe Hook 加密库函数，**在加密前/解密后**捕获明文：

```
应用程序发送 HTTPS 请求:

  app ──► SSL_write(明文) ──► [加密] ──► 网卡发送(密文)
              ↑
              └── DeepFlow Uprobe 在此拦截明文数据

支持：
  - OpenSSL: SSL_write / SSL_read
  - Go crypto/tls: Write / Read
```

#### (3) 应用层协议（L7）
在内核中通过 DPI（深度包检测）自动识别 **20+ 种应用协议**：

```
已支持协议：
┌─────────────────────────────────────────────────────┐
│ HTTP/1.1  │ HTTP/2  │ gRPC   │ Dubbo    │ TARS     │
│ MySQL     │ Postgres│ Redis  │ MongoDB  │ Memcached│
│ Kafka     │ MQTT    │ RabbitMQ│ NATS    │ Pulsar   │
│ DNS       │ TLS     │ FastCGI│ BRPC    │ SOFArpc  │
│ RocketMQ  │ ZMTP    │ OpenWire│ SOME/IP │ ISO8583  │
│ Oracle    │ Pulsar  │ WebSphereMQ │ ...           │
└─────────────────────────────────────────────────────┘
```

#### (4) CPU Profiling
通过 Perf Event 采样 CPU 调用栈，生成持续性能剖析数据：

```
CPU 0: 每秒采样 N 次
  ├── 采样时刻 1: [main → handleRequest → queryDB → mysql_exec]
  ├── 采样时刻 2: [main → handleRequest → serialize → json_encode]
  └── 采样时刻 3: [main → handleRequest → compress → zlib_deflate]

结果：生成 On-CPU 火焰图，可视化热点函数
```

### 7.3 DeepFlow eBPF 挂载点全景图

```
┌──────────────────────── 用户空间 ──────────────────────────┐
│                                                           │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌─────────┐  │
│  │Go 程序   │  │OpenSSL   │  │ Java 程序  │  │ 其他应用 │  │
│  │          │  │ 链接库    │  │           │  │         │  │
│  └────┬─────┘  └────┬─────┘  └─────┬─────┘  └────┬────┘  │
│       │             │              │              │        │
│  ┌────▼─────┐  ┌────▼─────┐       │              │        │
│  │ Uprobe:  │  │ Uprobe:  │       │              │        │
│  │ go_tls   │  │ openssl  │       │              │        │
│  │ go_http2 │  │ SSL_r/w  │       │              │        │
│  │ runtime  │  │          │       │              │        │
│  └──────────┘  └──────────┘       │              │        │
│                                    │              │        │
├────────────────────────────────────┼──────────────┼────────┤
│               系统调用边界          │              │        │
├────────────────────────────────────┼──────────────┼────────┤
│                                    │              │        │
│  ┌─────────────────────────────────▼──────────────▼─────┐  │
│  │             Tracepoint: syscalls                      │  │
│  │  sys_enter/exit_write     sys_enter/exit_sendto       │  │
│  │  sys_enter/exit_read      sys_enter/exit_recvfrom     │  │
│  │  sys_enter/exit_sendmsg   sys_enter/exit_recvmsg      │  │
│  │  sys_enter/exit_sendmmsg  sys_enter/exit_recvmmsg     │  │
│  │  sys_enter_connect        sys_exit_socket             │  │
│  │  sys_enter_close                                      │  │
│  │  sys_enter/exit_pread64   sys_enter/exit_pwrite64     │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │             Tracepoint: sched & process               │  │
│  │  sched_process_exec    sched_process_exit             │  │
│  │  sys_exit_fork         sys_exit_clone                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │             Kprobe: 内核函数                           │  │
│  │  __sys_sendmsg   __sys_sendmmsg   __sys_recvmsg      │  │
│  │  do_writev       do_readv                             │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │             Perf Event: CPU 采样                       │  │
│  │  定频采样 → 采集用户态+内核态调用栈                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
└─────────────────────── 内核空间 ───────────────────────────┘
```

---

## 第八章：DeepFlow eBPF 架构深度剖析

### 8.1 代码组织结构

```
agent/src/ebpf/
├── kernel/                    # eBPF C 程序（运行在内核空间）
│   ├── socket_trace.bpf.c     # 核心：Socket 追踪主程序
│   ├── go_tls.bpf.c           # Go TLS 流量拦截
│   ├── go_http2.bpf.c         # Go HTTP/2 协议拦截
│   ├── openssl.bpf.c          # OpenSSL 流量拦截
│   ├── uprobe_base.bpf.c      # Go runtime 追踪基础
│   ├── perf_profiler.bpf.c    # CPU 性能采样
│   ├── files_rw.bpf.c         # 文件 I/O 追踪
│   └── include/               # 共享头文件
│       ├── socket_trace.h     # 数据结构定义
│       └── ...
│
├── user/                      # 用户空间 C 代码（加载和管理 eBPF）
│   ├── load.c                 # eBPF 程序加载
│   ├── tracer.c               # 主追踪管理器
│   ├── probe.c                # 探针挂载逻辑
│   ├── socket.c               # Socket 数据收集
│   └── btf_core.c             # BTF 类型信息处理
│
├── docs/                      # 文档
│   ├── probes-and-maps.md     # 探针和 Map 说明
│   └── kernel-versions.md     # 内核版本兼容性
│
└── mod.rs                     # Rust FFI 绑定定义

agent/src/
├── ebpf_dispatcher.rs         # Rust 侧 eBPF 数据分发器
└── common/ebpf.rs             # eBPF 类型定义（EbpfType 枚举）
```

### 8.2 Tail Call 机制 — 突破指令限制

DeepFlow 面临的一个关键挑战：早期 Linux 内核（< 5.2）限制单个 eBPF 程序最多 **4096 条指令**，而 DeepFlow 的协议解析逻辑远超这个限制。

**解决方案：Tail Call（尾调用）**

```
Tail Call 原理：
一个 eBPF 程序结束时，不返回，而是「跳转」到另一个 eBPF 程序继续执行。
每个子程序都有独立的 4096 条指令配额。

DeepFlow 的 Tail Call 链：

┌─────────────┐    tail_call    ┌─────────────────┐
│ 入口程序     │ ──────────────► │ 协议推断 阶段2   │
│ 数据采集     │                 │ (PROTO_INFER_2)  │
│ 初始解析     │                 └────────┬────────┘
└─────────────┘                          │ tail_call
                                         ▼
                                ┌─────────────────┐
                                │ 协议推断 阶段3   │
                                │ (PROTO_INFER_3)  │
                                └────────┬────────┘
                                         │ tail_call
                                         ▼
                                ┌─────────────────┐
                                │ 数据提交         │
                                │ (DATA_SUBMIT)    │
                                └────────┬────────┘
                                         │ tail_call
                                         ▼
                                ┌─────────────────┐
                                │ 输出到用户空间    │
                                │ (OUTPUT_DATA)    │
                                └─────────────────┘
```

每个阶段负责不同的协议检测，避免单个程序过大：
- 阶段 2：HTTP、MySQL、Redis、Kafka 等常见协议
- 阶段 3：Dubbo、MQTT、NATS 等更多协议
- 数据提交：将识别结果写入缓冲区
- 输出：通过 Perf Event 发送到用户空间

### 8.3 Burst Mode — 高效数据传输

每次 `bpf_perf_event_output()` 都是一次从内核到用户空间的跨域传输，开销不小。DeepFlow 使用 **Burst Mode** 优化：

```
不用 Burst Mode（每个事件单独发送）：
  事件1 → perf_output → 用户空间
  事件2 → perf_output → 用户空间
  事件3 → perf_output → 用户空间
  ...
  每个事件都有跨域传输开销

使用 Burst Mode（批量发送）：
  事件1 → 写入 Per-CPU 缓冲区
  事件2 → 写入 Per-CPU 缓冲区
  ...
  事件32（或定时 10ms）→ 批量 perf_output → 用户空间

  每次传输 32KB 数据块，大幅减少跨域调用次数
```

实现细节：
- 每个 CPU 有一个 `__data_buf`（Per-CPU Array Map）
- 最多积累 32 个事件或等待 10ms
- 达到阈值后通过 `bpf_perf_event_output` 一次性发送 32768 字节

### 8.4 Per-CPU 设计 — 无锁高性能

多个 CPU 核心可能同时触发 eBPF 程序。为了避免锁竞争，DeepFlow 大量使用 Per-CPU 数据结构：

```
CPU 0: ┌─ data_buf_0 ─┐ ┌─ tracer_ctx_0 ─┐
CPU 1: ┌─ data_buf_1 ─┐ ┌─ tracer_ctx_1 ─┐
CPU 2: ┌─ data_buf_2 ─┐ ┌─ tracer_ctx_2 ─┐
CPU 3: ┌─ data_buf_3 ─┐ ┌─ tracer_ctx_3 ─┐
       └──────────────┘ └───────────────┘

每个 CPU 操作自己的副本，完全无锁
```

### 8.5 Go 语言特殊处理

Go 语言的协程（goroutine）调度模型给 eBPF 追踪带来了独特挑战：

```
传统线程模型（C/Java/Python）：
  线程 1 → 始终处理请求 A → 完成
  线程 2 → 始终处理请求 B → 完成
  eBPF 通过 TID 即可关联请求的 send 和 recv

Go 协程模型：
  线程 1 → 处理请求 A(goroutine 5) → 调度切换 → 处理请求 B(goroutine 8)
  线程 2 → 处理请求 B(goroutine 8) → 调度切换 → 处理请求 A(goroutine 5)
  同一请求的 send 和 recv 可能在不同线程上！
```

**DeepFlow 的解决方案**：

1. **Uprobe `runtime.execute`**：追踪 goroutine 调度，维护 `线程ID → goroutine ID` 映射
2. **Uprobe `runtime.newproc1`**：追踪 goroutine 创建，维护父子关系
3. **`goroutines_map`**：eBPF Map 存储线程到协程的实时映射

```
goroutines_map:
  thread_100 → goroutine_5
  thread_101 → goroutine_8

当 thread_100 执行 write() 时：
  1. 查 goroutines_map → 得知是 goroutine_5
  2. 用 goroutine_5 关联之前 thread_101 上的 recv()
  3. 完整还原 goroutine_5 的请求链路
```

### 8.6 加密流量捕获

DeepFlow 通过 Uprobe 在加密边界前后拦截明文数据：

```
┌─────────────────── OpenSSL 拦截 ────────────────────┐
│                                                      │
│  应用层: HTTP 请求                                    │
│     │                                                │
│     ▼                                                │
│  SSL_write(明文 HTTP)  ◄── Uprobe 拦截，采集明文      │
│     │                                                │
│     ▼                                                │
│  TLS 加密引擎                                         │
│     │                                                │
│     ▼                                                │
│  send(密文)  ◄── Tracepoint 看到的是加密后的数据       │
│                   (无法解析协议)                       │
│                                                      │
│  结论：Uprobe 让 DeepFlow 无需私钥即可获取 HTTPS 明文  │
└──────────────────────────────────────────────────────┘

┌─────────────────── Go TLS 拦截 ─────────────────────┐
│                                                      │
│  crypto/tls.(*Conn).Write(明文)                      │
│     │                                                │
│     ├── Uprobe 拦截 ──► 采集明文                      │
│     │                                                │
│     ▼                                                │
│  内部 TLS 加密 → syscall write(密文)                  │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 8.7 双缓冲 Profiler

CPU Profiling 采用双缓冲（Double Buffering）设计，确保采样和读取互不干扰：

```
时间片 1:
  eBPF 写入 → Buffer A (活跃)    |   用户空间读取 ← Buffer B
                                 |
时间片 2:                         |   切换！
  eBPF 写入 → Buffer B (活跃)    |   用户空间读取 ← Buffer A

永远不会出现同时读写同一个 Buffer 的情况
```

### 8.8 内核版本适配

DeepFlow 需要运行在各种 Linux 内核版本上。内核结构体（如 `task_struct`、`socket`）的字段偏移量在不同版本中可能不同。

**适配策略**：

```
┌──────────────────────────────────────────────┐
│          内核版本检测与适配                     │
│                                              │
│  Linux 5.2+ (有 BTF)                         │
│  └─► 从 BTF 信息直接获取结构体偏移             │
│      安全、准确、自动适配                      │
│                                              │
│  Linux 5.2 以下 (无 BTF)                      │
│  └─► 运行时探测内核结构体偏移                  │
│      通过 DWARF 调试信息推断                   │
│                                              │
│  特殊版本 (CentOS 7 / Linux 3.10)            │
│  └─► 预编译适配，使用条件编译                  │
│      不同的 Hook 函数名                       │
│                                              │
│  偏移信息存储在 __members_offset Map 中        │
│  eBPF 程序通过读取 Map 适配不同内核            │
└──────────────────────────────────────────────┘
```

### 8.9 完整数据流总结

从系统调用触发到最终数据进入 DeepFlow 的处理管线，完整路径如下：

```
① 应用程序调用 write(fd, buf, len)
        │
        ▼
② Tracepoint sys_enter_write 触发
   eBPF 程序记录: pid, fd, buf 指针, 时间戳
        │
        ▼
③ Tail Call → 协议推断
   读取 buf 内容前几个字节，判断协议:
   "GET /api" → HTTP    "SELECT" → MySQL
   0x16 0x03 → TLS      ...
        │
        ▼
④ Tail Call → 数据提交
   将事件写入 Per-CPU Burst 缓冲区 (__data_buf)
        │
        ▼
⑤ 缓冲区满(32个) 或 定时(10ms)
   Tail Call → 输出
   bpf_perf_event_output() 发送到 Perf Event Buffer
        │
        ▼
⑥ 用户空间 C 代码 (socket.c)
   perf_buffer_read() → epoll_wait()
   从 Perf Event 读取数据，解析 __socket_data 结构
   放入 dispatch worker 队列
        │
        ▼
⑦ Rust EbpfCollector (ebpf_dispatcher.rs)
   通过 FFI 从 C 层获取数据
   转换为 Rust 数据结构 (AppProtoLogsData)
        │
        ▼
⑧ DeepFlow Agent 处理管线
   ├── FlowGenerator: 流状态维护
   ├── Collector: 指标聚合
   └── Sender: 编码压缩，上报 Server
```

---

## 第九章：DeepFlow 如何利用 eBPF 做 Profiling 和分布式链路追踪

前面的章节介绍了 eBPF 的通用知识和 DeepFlow 的整体架构。这一章将深入 DeepFlow 最具特色的两个高级功能：**持续性能剖析（Continuous Profiling）** 和 **零侵入分布式链路追踪（AutoTracing）**。

---

### 9.1 持续性能剖析（Continuous Profiling）

#### 9.1.1 为什么需要持续性能剖析？

传统的性能分析是"出问题了再去抓"——手动运行 `perf record`，采样几十秒，分析火焰图。问题是：

- 很多性能问题是**偶发的**，你不知道什么时候该抓
- 手动采样有侵入性，可能影响生产环境
- 历史数据无法回溯

**持续性能剖析**是"永远在采样"——以极低的开销持续运行，随时可以回溯任意时间段的 CPU 热点。

```
传统方式：
  问题发生 → 人工介入 → perf record → 分析
  └── 延迟数分钟，问题可能已消失

DeepFlow 方式：
  持续采样（97Hz） → 自动聚合 → 存储到 Server
  问题发生 → 打开 UI → 查看该时刻的火焰图
  └── 秒级回溯，问题现场完整保留
```

#### 9.1.2 采样原理：Perf Event + eBPF

DeepFlow 的 Profiler 基于 Linux 内核的 Perf Event 子系统。核心思想很简单：

```
每秒 97 次（每 ~10.3ms 一次），对每个 CPU 核心：
  1. 暂停当前运行的线程（极短暂，纳秒级）
  2. 记录此刻的函数调用栈
  3. 恢复执行

采样足够多次后，统计每个函数出现的频率：
  出现次数多 = 占用 CPU 时间多 = 热点函数
```

**为什么是 97Hz？** 用质数是为了避免与系统中的周期性操作"共振"产生采样偏差。如果用 100Hz，恰好与某个 10ms 定时器对齐，你可能总是采到同一个函数。

#### 9.1.3 内核态实现：perf_profiler.bpf.c

DeepFlow 的 eBPF Profiler 程序挂载在 Perf Event 上，每次 CPU 采样触发时执行：

```
Perf Event 触发（97Hz × CPU 核心数）
     │
     ▼
┌─────────────────────────────────────────────┐
│ oncpu_profile() 入口                         │
│                                             │
│  1. 获取当前进程 pid/tgid、CPU ID、进程名     │
│  2. 记录纳秒级时间戳                         │
│                                             │
│  3. 解释型语言栈展开（如果是 Python/PHP 等）  │
│     └── 调用 extended_interpreter_unwind()   │
│         读取 Python PyFrameObject 链         │
│         读取 PHP execute_data 链             │
│                                             │
│  4. DWARF 栈展开（如果有调试信息）            │
│     └── tail_call → df_PE_dwarf_unwind      │
│         通过 .eh_frame 信息逐帧回溯          │
│                                             │
│  5. 内核态调用栈采集                         │
│     └── bpf_get_stackid(ctx, stack_map, 0)  │
│                                             │
│  6. 用户态调用栈采集                         │
│     └── bpf_get_stackid(ctx, stack_map,     │
│                          BPF_F_USER_STACK)   │
│                                             │
│  7. 写入 Perf Ring Buffer                    │
│     └── bpf_perf_event_output()             │
└─────────────────────────────────────────────┘
```

**关键数据结构**：

```c
// 每次采样生成的数据
struct stack_trace_key_t {
    __u32 pid;               // 线程 ID
    __u32 tgid;              // 进程 ID
    __u32 cpu;               // CPU 核心 ID
    char  comm[16];          // 进程名
    int   kernstack;         // 内核态调用栈 ID（指向 stack_map）
    int   userstack;         // 用户态调用栈 ID
    int   intpstack;         // 解释器调用栈 ID（Python/PHP/Lua 等）
    __u32 flags;             // DWARF/CUDA 等标志位
    __u64 timestamp;         // 采样时间（纳秒）
};
```

`bpf_get_stackid()` 是内核提供的辅助函数，一次调用就能采集完整的调用栈（最多 127 帧），并返回一个去重后的 stack_id。相同的调用栈在同一周期内只存储一次。

#### 9.1.4 双缓冲机制：永不丢数据

持续 Profiling 的核心挑战：**eBPF 在不停写入数据，用户空间在不停读取数据**。如果读写同一块内存，会出现竞争。

DeepFlow 的解决方案是**双缓冲（Double Buffering）**——借鉴了 GPU 渲染中的"前后帧缓冲"思想：

```
有两套完全相同的数据结构：

    Buffer A                     Buffer B
┌──────────────────┐        ┌──────────────────┐
│ profiler_output_a │        │ profiler_output_b │  ← Perf Ring Buffer
│ stack_map_a       │        │ stack_map_b       │  ← 调用栈存储
│ sample_count_a    │        │ sample_count_b    │  ← 采样计数
└──────────────────┘        └──────────────────┘
```

工作流程：

```
     时间 ──────────────────────────────────────►

阶段 1:  eBPF 写入 Buffer A      用户空间读取 Buffer B
         ┌──写──► A              B ──读──┐
         │                               │
         ▼                               ▼
阶段 2:  ─── 翻转！(transfer_count++) ───
         
阶段 3:  eBPF 写入 Buffer B      用户空间读取 Buffer A
         ┌──写──► B              A ──读──┐
         │                               │
         ▼                               ▼
阶段 4:  ─── 翻转！(transfer_count++) ───

         ... 如此循环 ...
```

**翻转控制**非常简单——一个原子计数器：

```c
// 内核侧：根据 transfer_count 的奇偶决定写入哪个 Buffer
__u64 transfer_count = profiler_state_map[TRANSFER_CNT_IDX];
if (transfer_count & 0x1) {
    stack_map   = stack_map_b;
    output_buf  = profiler_output_b;
} else {
    stack_map   = stack_map_a;
    output_buf  = profiler_output_a;
}

// 用户侧：读完后翻转
ctx->transfer_count++;
bpf_table_set_value(state_map, TRANSFER_CNT_IDX, &ctx->transfer_count);
```

#### 9.1.5 用户空间处理：聚合与符号解析

用户空间 C 代码（`profile_common.c`）每次翻转后处理上一轮的数据：

```
从 Perf Ring Buffer 读取原始采样
           │
           ▼
    ┌──────────────────────────┐
    │   聚合（Aggregation）     │
    │                          │
    │  相同的 (tgid, pid,      │
    │   kernstack, userstack)  │
    │  合并为一条记录，count++  │
    │                          │
    │  例如：                   │
    │  main→handleReq→queryDB  │
    │  出现了 15 次 → count=15 │
    └────────┬─────────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │   符号解析（Symbolize）   │
    │                          │
    │  stack_id → 地址数组      │
    │  [0x4012a0, 0x401180...] │
    │       │                  │
    │       ▼                  │
    │  /proc/<pid>/maps 查找   │
    │  0x4012a0 → queryDB      │
    │  0x401180 → handleReq    │
    │       │                  │
    │       ▼                  │
    │  生成折叠栈字符串：       │
    │  "main;handleReq;queryDB"│
    └────────┬─────────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │   输出到 Rust 层          │
    │                          │
    │  stack_trace_msg_t:      │
    │    tgid, pid, cpu        │
    │    count = 15            │
    │    data = "main;handle   │
    │     Req;queryDB"         │
    │    container_id          │
    │    process_name          │
    └──────────────────────────┘
             │
             ▼
      DeepFlow Agent Rust 管线
      → 编码 → 压缩 → 上报 Server
      → Server 存储 → UI 展示火焰图
```

#### 9.1.6 多语言调用栈支持

DeepFlow 不仅能采集 C/C++/Rust/Go 等编译型语言的调用栈，还支持**解释型语言**：

```
┌──────────────────────────────────────────────────────┐
│  编译型语言（C/C++/Go/Rust/Java JIT）                 │
│                                                      │
│  方式 1: bpf_get_stackid() — 硬件帧指针回溯          │
│          适合：带 -fno-omit-frame-pointer 编译的程序  │
│                                                      │
│  方式 2: DWARF 展开 — 读取 .eh_frame 调试信息        │
│          适合：没有帧指针但有调试信息的程序            │
│          实现：通过 Tail Call 在 eBPF 中做二分查找     │
│                查找每个指令地址对应的 CFA 和返回地址   │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│  解释型语言（Python/PHP/Lua/V8/Node.js）              │
│                                                      │
│  方式: Uprobe 读取解释器内部数据结构                   │
│                                                      │
│  Python: 遍历 PyFrameObject 链表                     │
│    PyFrameObject.f_back → 上一帧                     │
│    PyFrameObject.f_code.co_filename → 文件名          │
│    PyFrameObject.f_code.co_name → 函数名              │
│                                                      │
│  PHP: 遍历 execute_data 链表                          │
│    execute_data->prev_execute_data → 上一帧           │
│    execute_data->func->common.function_name → 函数名  │
│                                                      │
│  结果：混合栈 = 原生帧 + 解释器帧                     │
│    "python3;native_call;PyEval;my_module.py:process"  │
└──────────────────────────────────────────────────────┘
```

#### 9.1.7 完整数据流总结

```
                         内核空间
                    ┌────────────────┐
  CPU 0 ──97Hz──►  │                │
  CPU 1 ──97Hz──►  │ oncpu_profile  │
  CPU 2 ──97Hz──►  │   eBPF 程序    │
  CPU 3 ──97Hz──►  │                │
                    │  采集调用栈     │
                    │  写入 Buffer X │
                    └───────┬────────┘
                            │ perf_event_output
                            ▼
                    ┌────────────────┐
                    │  Buffer A / B  │ ← 双缓冲
                    └───────┬────────┘
                            │
                         用户空间
                    ┌───────▼────────┐
                    │  epoll_wait    │
                    │  读取 Buffer Y │ ← 读对面的 Buffer
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │  聚合 + 符号化  │
                    │  "main;func;.."│
                    │  count = N     │
                    └───────┬────────┘
                            │ FFI 回调
                    ┌───────▼────────┐
                    │  Rust Agent    │
                    │  编码压缩上报   │
                    └───────┬────────┘
                            │ TCP:20033
                    ┌───────▼────────┐
                    │  DeepFlow      │
                    │  Server        │
                    │  存储 + 查询    │
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │  Web UI        │
                    │  交互式火焰图   │
                    └────────────────┘
```

---

### 9.2 零侵入分布式链路追踪（AutoTracing）

#### 9.2.1 分布式追踪的传统难题

在微服务架构中，一个用户请求可能经过十几个服务：

```
用户 → API Gateway → 用户服务 → 订单服务 → 库存服务 → 支付服务
                                    │
                                    └──► 通知服务
```

传统的分布式追踪（Jaeger、Zipkin、SkyWalking）要求每个服务**主动传递 Trace Header**：

```
服务 A 发出请求:
  GET /api/order HTTP/1.1
  traceparent: 00-abcdef1234567890-1234567890abcdef-01  ← 必须手动注入
  x-b3-traceid: abcdef1234567890                         ← 或者用 Zipkin 格式
```

这意味着：
- 每个服务都要集成 SDK（侵入业务代码）
- 不同语言需要不同的 SDK
- 遗留系统、第三方服务无法追踪
- 任何一环缺失，链路就断了

**DeepFlow 的 AutoTracing 的目标：不修改任何应用代码，自动构建完整的调用链路。**

#### 9.2.2 追踪的三个层次

DeepFlow 的分布式追踪在三个层次上同时工作，层层递进：

```
┌─────────────────────────────────────────────────────────────────┐
│  层次 3: 应用层 Trace Header 关联（锦上添花）                     │
│  当应用本身就有 traceparent/x-b3-traceid 等 header 时，          │
│  DeepFlow 解析并整合到自己的追踪链路中                            │
├─────────────────────────────────────────────────────────────────┤
│  层次 2: 进程内请求关联（线程/协程级追踪）                        │
│  eBPF 追踪同一进程内的 "收到请求 → 处理 → 发出请求" 因果链       │
├─────────────────────────────────────────────────────────────────┤
│  层次 1: 跨服务网络关联（连接级追踪）                             │
│  通过 TCP 连接的五元组，将服务 A 的出口流量与服务 B 的入口流量匹配 │
└─────────────────────────────────────────────────────────────────┘
```

下面逐层详解。

#### 9.2.3 层次 1：跨服务网络关联

这是最基础的一层。原理：**同一条 TCP 连接的两端，数据是相同的**。

```
服务 A（客户端）                         服务 B（服务端）
┌──────────────┐                       ┌──────────────┐
│              │   TCP 连接              │              │
│  write()  ───┼──────────────────────►─┼── read()     │
│  (请求数据)  │  src=10.0.0.1:45678    │  (请求数据)  │
│              │  dst=10.0.0.2:8080     │              │
│              │                        │              │
│  read()   ◄──┼────────────────────────┼── write()    │
│  (响应数据)  │                        │  (响应数据)  │
└──────────────┘                       └──────────────┘

DeepFlow Agent 在两端同时采集：
  Agent@服务A: write(fd=5, "GET /order") → (10.0.0.1:45678 → 10.0.0.2:8080)
  Agent@服务B: read(fd=12, "GET /order") → (10.0.0.1:45678 → 10.0.0.2:8080)

五元组完全相同 → 这两条记录属于同一次调用！
```

DeepFlow Server 在收到两端数据后，通过**五元组（源IP、源端口、目标IP、目标端口、协议）** 进行匹配，自动建立服务 A → 服务 B 的调用关系。

在 Kubernetes 环境中，DeepFlow 还会结合 **Pod 元数据** 和 **DNS 解析记录** 增强关联的准确性。

#### 9.2.4 层次 2：进程内请求关联（核心难点）

层次 1 解决了"A 调用了 B"的问题。但如果 A 同时处理多个请求，如何知道"处理请求 X 时调了 B，处理请求 Y 时调了 C"？

这需要在**进程内部**追踪一个请求的完整处理过程。

##### 核心概念：thread_trace_id

DeepFlow 在 eBPF 层面为每个"请求处理流程"分配一个唯一的 **thread_trace_id**：

```
线程 T1 处理请求 X 的过程:

  ① read(fd=10, 请求X)                      ← 收到入站请求
     eBPF: 分配 thread_trace_id = 42
     trace_map[(tgid, tid)] = {id: 42, peer_fd: 10}

  ② [业务逻辑处理...]

  ③ write(fd=20, 调用服务B)                  ← 发出出站请求
     eBPF: 查找 trace_map[(tgid, tid)] → id=42
     为这次 write 打上 thread_trace_id = 42

  ④ read(fd=20, 服务B的响应)                 ← 收到服务B的响应
     eBPF: thread_trace_id = 42

  ⑤ write(fd=10, 响应X)                     ← 回复原始请求
     eBPF: 查找 trace_map[(tgid, tid)] → id=42
     trace_map 清除条目
```

**关键洞察**：在同一个线程中，read(入站请求) 和后续的 write(出站请求) 之间存在**因果关系**。eBPF 通过 `trace_map` 将它们串联起来。

##### eBPF 数据结构

```c
// 追踪关联的 Key：标识"谁在处理请求"
struct trace_key_t {
    __u32 tgid;   // 进程 ID
    __u32 pid;    // 线程 ID（非 Go 程序使用）
    __u64 goid;   // Goroutine ID（Go 程序使用）
};

// 追踪关联的 Value：记录当前请求的追踪上下文
struct trace_info_t {
    __u32 update_time;         // 更新时间
    __u32 peer_fd;             // 关联的对端 socket fd
    __u64 thread_trace_id;     // 本次请求的唯一追踪 ID
    __u64 socket_id;           // 关联的 socket ID
};

// trace_map: 核心 Map，每个线程/协程一个条目
// Key: trace_key_t  →  Value: trace_info_t
```

##### 处理流程（trace_process 函数）

```
                    收到入站请求（INGRESS + REQUEST）
                           │
                           ▼
                ┌──────────────────────┐
                │ 分配新 thread_trace_id│
                │ = ++全局计数器        │
                │                      │
                │ 记录 peer_fd = 当前fd │
                │ （供后续关联使用）     │
                │                      │
                │ 写入 trace_map       │
                └──────────┬───────────┘
                           │
                    ... 业务逻辑 ...
                           │
                    发出出站请求（EGRESS + REQUEST）
                           │
                           ▼
                ┌──────────────────────┐
                │ 查找 trace_map       │
                │ → 获取 thread_trace_id│
                │                      │
                │ 将 id 附加到出站数据  │
                │ 上报给 Server        │
                └──────────┬───────────┘
                           │
                    收到出站响应（INGRESS + RESPONSE）
                           │
                           ▼
                    发送入站响应（EGRESS + RESPONSE）
                           │
                           ▼
                ┌──────────────────────┐
                │ 查找 trace_map       │
                │ → 获取 thread_trace_id│
                │                      │
                │ 清除 trace_map 条目  │
                │ （请求处理完毕）      │
                └──────────────────────┘
```

**结果**：同一请求处理流程中的所有 syscall（入站 read、出站 write、出站 read、入站 write）都携带相同的 `thread_trace_id`。Server 端据此还原因果链。

##### 反向代理场景（NGINX）的特殊处理

NGINX 的转发模型特殊：**前端连接和后端连接在同一个线程中处理，但通过不同的 fd**。DeepFlow 用 `peer_fd` 机制解决：

```
NGINX 线程处理一个请求:

  ① read(fd=10, 客户端请求)     ← 前端入站
     trace_map[(tgid,tid)] = {id:42, peer_fd:10}

  ② write(fd=20, 转发给后端)    ← 后端出站
     查 trace_map → id=42
     socket_info[fd=20].peer_fd = 10       // 后端记住前端 fd
     socket_info[fd=20].trace_id = 42

  ③ read(fd=20, 后端响应)       ← 后端入站
     查 socket_info[fd=20].peer_fd = 10
     反查 socket_info[fd=10] → 通知前端

  ④ write(fd=10, 回复客户端)    ← 前端出站
     查 socket_info[fd=10].trace_id = 42   // 从后端传来的 id
     使用同一个 thread_trace_id

结果：客户端请求 → NGINX → 后端服务，三者共享 thread_trace_id = 42
```

#### 9.2.5 Go 语言的特殊挑战与解法

Go 语言的 goroutine 调度模型给进程内追踪带来了独特挑战：

```
传统线程模型（Java/C++/Python）:
  线程 100: [read请求X] → [处理] → [write调用B] → [read响应B] → [write响应X]
  └── 整个过程在同一线程，tid=100 始终不变

Go 协程模型:
  线程 100: [read请求X(goroutine 5)] → [调度切换] → [处理请求Y(goroutine 8)]
  线程 101: [处理请求X(goroutine 5)] → [write调用B(goroutine 5)]
  └── 同一请求在不同线程上执行！用 tid 关联会断链
```

**DeepFlow 的解法：用 Goroutine ID 替代 Thread ID**

```c
// eBPF 侧：获取追踪 key 时优先使用 goroutine ID
static __inline struct trace_key_t get_trace_key(...)
{
    __u64 goid = get_current_goroutine_id();  // 从 Uprobe 获取

    struct trace_key_t key = {};
    key.tgid = current_tgid;

    if (goid) {
        key.goid = goid;    // Go 程序：用 goroutine ID
    } else {
        key.pid = current_tid;  // 非 Go 程序：用线程 ID
    }
    return key;
}
```

**Goroutine ID 怎么获取？**

通过 Uprobe 挂载 Go 运行时的两个关键函数：

```
1. runtime.execute(gp *g)
   └── 每次 goroutine 被调度执行时触发
   └── eBPF 更新 goroutines_map[当前线程ID] = gp.goid
   └── 之后同线程的 syscall 就能查到当前的 goroutine ID

2. runtime.newproc1(fn, callergp, callerpc)
   └── 每次创建新 goroutine 时触发
   └── eBPF 记录 go_ancestor_map[新goid] = 父goid
   └── 用于追踪 goroutine 的父子关系
```

```
完整 Go 追踪流程:

  线程 100 调度 goroutine 5:
    runtime.execute(g5) → goroutines_map[100] = 5

  线程 100 执行 read():
    查 goroutines_map[100] → goid=5
    trace_map[(tgid, goid=5)] = {id: 42}

  [Go 运行时调度切换，goroutine 5 迁移到线程 101]

  线程 101 调度 goroutine 5:
    runtime.execute(g5) → goroutines_map[101] = 5

  线程 101 执行 write():
    查 goroutines_map[101] → goid=5
    查 trace_map[(tgid, goid=5)] → id=42  ✅ 关联成功！
```

#### 9.2.6 层次 3：应用层 Trace Header 整合

当应用本身已经接入了分布式追踪（如 Jaeger、SkyWalking），DeepFlow 可以**解析 HTTP 头中的 Trace Header**，将其与 eBPF 层的追踪数据整合。

DeepFlow 支持的 Trace Header 格式：

| 格式 | Header 名 | 示例 |
|------|----------|------|
| W3C TraceContext | `traceparent` | `00-abcdef...-123456...-01` |
| Zipkin B3 | `x-b3-traceid` | `abcdef1234567890` |
| Zipkin B3 Single | `b3` | `traceid-spanid-1` |
| Jaeger | `uber-trace-id` | `traceid:spanid:parentid:flags` |
| SkyWalking | `sw8` | `1-traceid-segmentid-...` |
| 自定义 | 可配置 | 任意 Header 名和解析规则 |

```
整合流程:

  eBPF 采集到 HTTP 请求数据:
    write(fd, "GET /api HTTP/1.1\r\n
               traceparent: 00-abc123-def456-01\r\n
               ...")

         │
         ▼

  Rust 层 L7 协议解析:
    1. 识别为 HTTP 协议
    2. 解析 Header → 发现 traceparent
    3. 提取 trace_id = "abc123", span_id = "def456"
    4. 同时有 eBPF 的 thread_trace_id = 42

         │
         ▼

  Server 端关联:
    ┌────────────────────────────────────────┐
    │  同一个 Span 同时拥有:                  │
    │  - app_trace_id = "abc123"  (来自应用)  │
    │  - syscall_trace_id = 42    (来自eBPF)  │
    │                                        │
    │  两种 ID 共同构建完整链路:               │
    │  - app_trace_id 关联有 SDK 的服务       │
    │  - syscall_trace_id 关联无 SDK 的服务   │
    │  - 结合后实现完整覆盖                   │
    └────────────────────────────────────────┘
```

#### 9.2.7 三层协作的完整示例

```
用户请求 → API Gateway(Go) → 用户服务(Java+Jaeger) → 数据库(MySQL)

┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  用户 ──HTTP──► API Gateway(Go)                                      │
│                    │                                                 │
│    eBPF 采集:      │                                                 │
│    ├ read(fd=10) goroutine 5, thread_trace_id=100                   │
│    ├ write(fd=20) goroutine 5, thread_trace_id=100                  │
│    └ 五元组: 10.0.0.1:45678 → 10.0.0.2:8080                        │
│                    │                                                 │
│                    │  层次1: 五元组匹配                               │
│                    ▼                                                 │
│  API Gateway ──HTTP──► 用户服务(Java)                                │
│                    │   Header: traceparent: 00-abc123-...            │
│    eBPF 采集:      │                                                 │
│    ├ read(fd=5) tid=200, thread_trace_id=201                        │
│    ├ 解析到 traceparent → app_trace_id=abc123                       │
│    ├ write(fd=8) tid=200, thread_trace_id=201                       │
│    └ 五元组: 10.0.0.2:39876 → 10.0.0.3:3306                        │
│                    │                                                 │
│                    │  层次1: 五元组匹配                               │
│                    │  层次2: thread_trace_id 关联 read→write         │
│                    │  层次3: app_trace_id 整合                       │
│                    ▼                                                 │
│  用户服务 ──MySQL协议──► MySQL数据库                                  │
│                                                                      │
│    eBPF 采集:                                                        │
│    ├ read(fd=12) tid=300, 识别为 MySQL COM_QUERY                    │
│    └ "SELECT * FROM users WHERE id=123"                             │
│                                                                      │
│  最终链路:                                                           │
│  用户 → API Gateway → 用户服务 → MySQL                               │
│  (全部自动关联，无需任何代码修改)                                      │
│  (用户服务的 Jaeger trace_id 也被整合进来)                            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

#### 9.2.8 对比传统方案

| 维度 | 传统方案 (Jaeger/Zipkin) | DeepFlow AutoTracing |
|------|------------------------|---------------------|
| **代码侵入** | 需要在每个服务中集成 SDK | 完全零侵入 |
| **语言支持** | 每种语言需要独立 SDK | 语言无关（OS 层采集） |
| **遗留系统** | 无法追踪 | 自动覆盖 |
| **中间件** | 需要特定插件 | 自动识别（MySQL、Redis、Kafka 等） |
| **链路完整性** | 一环缺失则断链 | 网络层兜底，不会断 |
| **Trace Header** | 必需 | 可选（有则整合，无则自动生成） |
| **性能开销** | SDK 本身有开销 | eBPF 采集开销 < 1% |
| **部署方式** | 改代码 + 重新部署 | 部署 Agent 即生效 |

#### 9.2.9 代码位置速查

| 组件 | 文件位置 |
|------|---------|
| trace_map 与核心追踪逻辑 | `@agent/src/ebpf/kernel/socket_trace.bpf.c` |
| 追踪数据结构定义 | `@agent/src/ebpf/kernel/include/socket_trace_common.h` |
| Go goroutine 追踪 | `@agent/src/ebpf/kernel/uprobe_base.bpf.c` |
| Go TLS 追踪 | `@agent/src/ebpf/kernel/go_tls.bpf.c` |
| Go HTTP/2 追踪 | `@agent/src/ebpf/kernel/go_http2.bpf.c` |
| Trace Header 解析配置 | `@agent/src/config/handler.rs` |
| L7 协议追踪 ID 提取 | `@agent/src/flow_generator/protocol_logs/` |
| Profiler eBPF 程序 | `@agent/src/ebpf/kernel/perf_profiler.bpf.c` |
| Profiler 用户空间处理 | `@agent/src/ebpf/user/profile/` |

---

## 附录：术语表

| 术语 | 全称 | 说明 |
|------|------|------|
| **eBPF** | extended Berkeley Packet Filter | Linux 内核中的可编程沙箱虚拟机 |
| **BPF** | Berkeley Packet Filter | eBPF 的前身，最初用于包过滤 |
| **JIT** | Just-In-Time Compilation | 将 eBPF 字节码编译为原生机器码 |
| **Verifier** | — | eBPF 安全验证器，确保程序不会危害内核 |
| **Map** | — | eBPF 的键值数据结构，用于内核/用户空间通信 |
| **Kprobe** | Kernel Probe | 动态挂载到内核函数的探针 |
| **Uprobe** | User-space Probe | 动态挂载到用户态函数的探针 |
| **Tracepoint** | — | 内核中预定义的稳定追踪点 |
| **XDP** | eXpress Data Path | 网卡驱动层的 eBPF 程序类型 |
| **TC** | Traffic Control | Linux 流量控制层 |
| **BTF** | BPF Type Format | 描述内核数据类型的元信息，支持 CO-RE |
| **CO-RE** | Compile Once, Run Everywhere | 一次编译即可在不同内核版本运行 |
| **Tail Call** | — | eBPF 程序间的跳转机制 |
| **Per-CPU** | — | 每个 CPU 核心独立的数据副本 |
| **Perf Event** | Performance Event | Linux 性能事件子系统 |
| **DPI** | Deep Packet Inspection | 深度包检测，解析应用层协议 |
| **FFI** | Foreign Function Interface | 跨语言函数调用接口（此处指 Rust 调用 C） |
| **Goroutine** | — | Go 语言的轻量级协程 |
| **TLS** | Transport Layer Security | 传输层安全协议（HTTPS 使用） |
| **Continuous Profiling** | — | 持续性能剖析，以极低开销 7×24 小时采样 CPU 调用栈 |
| **AutoTracing** | — | DeepFlow 的零侵入分布式链路追踪技术 |
| **thread_trace_id** | — | DeepFlow eBPF 层为每个请求处理流程分配的唯一追踪 ID |
| **Trace Header** | — | HTTP 头中的追踪标识（如 traceparent、x-b3-traceid） |
| **Span** | — | 分布式追踪中的一次调用操作，多个 Span 组成完整 Trace |
| **DWARF** | Debugging With Attributed Record Formats | 调试信息格式，用于还原没有帧指针的调用栈 |
| **火焰图** | Flame Graph | 将函数调用栈频率可视化的图表，宽度代表 CPU 占用比例 |
