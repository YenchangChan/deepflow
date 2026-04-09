# deepflow-agent 容器化构建速查（中文）

> 本文档是 `agent/build.md` 的中文补充，重点整理使用官方 Docker 镜像构建 agent、以及跨架构构建的可行方案。源码级手动编译请仍以 `agent/build.md` 为准。

## 1. 为什么走 Docker

Agent 本地手动编译链路很长：需要 clang/LLVM 11 或 12、libpcap-dev、libelf-dev，以及 5 个必须从源码编译的静态库（bcc、bddisasm、libdwarf、elfutils、libGoReSym）。官方因此提供预装好全部依赖的构建镜像，强烈推荐直接使用。

## 2. 官方构建镜像

官方并不提供"单镜像跨架构编译"的方案，而是 **每种目标架构一个原生镜像**：

| 目标架构 | 镜像 | 备注 |
|---|---|---|
| x86_64  | `ghcr.io/deepflowio/rust-build:1.31`        | CI 中配合 `agent/docker/dockerfile-build` 使用 |
| aarch64 | `ghcr.io/deepflowio/rust-build:1.31-arm64`  | CI 中配合 `agent/docker/dockerfile-build-aarch64` 使用，需 `source /opt/rh/devtoolset-8/enable` |

另有一个 yunshan 私有 registry 的别名，`agent/build.md` 中引用的是它（等价于 x86_64 镜像）：

```
hub.deepflow.yunshan.net/public/rust-build
```

> 两个镜像内部都只跑 `cargo build --release`，没有任何 `--target` 交叉编译参数。CI 对 arm64 使用的是**原生 arm64 runner**（`cirun-aws-arm64-32c`）。

### 为什么不是"一个镜像交叉编译"
Agent 含大量 eBPF / C 代码（`@agent/src/ebpf`）和预编译静态库，这些 C 工件都按主机架构预装到镜像里。交叉编译需要给 eBPF 和所有静态库各准备 sysroot，维护成本高，官方因此选择"双镜像 + 原生 runner"路线。

`agent/.cargo/config.toml` 里确实配置了 `aarch64-unknown-linux-gnu` 的 linker，但那是 **arm64 镜像内部**使用的（`/opt/rh/devtoolset-8` 只存在于 arm64 镜像里），不是给 x86_64 host 做交叉用的。

## 3. 在 x86_64 主机上构建 x86_64 agent

```bash
cd /path/to/deepflow
docker run --privileged --rm -it \
    -v "$(pwd):/deepflow" \
    ghcr.io/deepflowio/rust-build:1.31 \
    bash -c "cd /deepflow/agent && cargo build --release"
```

产物：`agent/target/release/deepflow-agent`

说明：
- `--privileged` 是 eBPF 相关工具链所必需的。
- 首次拉取镜像体积较大（含 LLVM + 多份静态库），建议挂后台。
- debug 构建把 `--release` 去掉即可，产物在 `agent/target/debug/`。

## 4. 在 x86_64 主机上构建 aarch64 agent（QEMU 模拟）

没有原生 arm64 机器时，可用 QEMU 用户态模拟。**能跑通但较慢（约为原生的 1/5 ~ 1/10）**，仅适合偶尔出包，不建议日常开发。

```bash
# 一次性注册 binfmt_misc，使 docker 能跑非本机架构镜像
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

# 使用 aarch64 镜像构建
docker run --privileged --rm -it --platform linux/arm64 \
    -v "$(pwd):/deepflow" \
    ghcr.io/deepflowio/rust-build:1.31-arm64 \
    bash -c "cd /deepflow/agent && source /opt/rh/devtoolset-8/enable && cargo build --release"
```

产物同样落在 `agent/target/release/deepflow-agent`，但是 aarch64 ELF。

## 5. 原生 aarch64 环境构建

若有原生 arm64 机器（云上 arm 实例 / Apple Silicon + Docker Desktop / 树莓派 5 等），直接执行与第 4 节相同的 `docker run`（去掉 `--platform linux/arm64` 也可）。这是官方 CI 的做法，也是最快的路径。

## 6. 常见问题

- **拉不到 `ghcr.io/deepflowio/rust-build`**：改用 yunshan 私有 registry 的 `hub.deepflow.yunshan.net/public/rust-build`（目前仅 x86_64）。
- **需要 aarch64 但只能用 yunshan 镜像**：目前没有 arm64 的 yunshan 镜像对应，需从 ghcr 拉。
- **容器内 cargo 下载慢**：可复制 `agent/docker/rust-proxy-config` 为 `/usr/local/cargo/config`（CI 正是这么做的）。

## 7. 跨平台能力速查

Agent 在设计上是 **Linux 优先、x86_64/aarch64 优先、新内核优先**。具体支持面如下：

### 7.1 支持矩阵

| 平台 | 状态 | 说明 |
|---|---|---|
| Linux x86_64 (glibc ≥ 2.17) | ✅ 一等公民 | 官方 CI 构建、有预编译二进制 |
| Linux aarch64 (glibc ≥ 2.17) | ✅ 一等公民 | 官方 CI 构建，使用原生 arm64 runner |
| Linux x86_64 / aarch64 musl 静态 | ✅ cargo 配置已支持 | `.cargo/config.toml` 中有对应 target；适合老系统 |
| Android aarch64 | ⚠️ 有源码分支 | `.cargo/config.toml` 中配置了 NDK linker，非主力 |
| Windows x86_64 | ⚠️ 源码有、官方不发布 | 见 7.2 |
| Linux i686 / 其它 32 位 | ❌ 不支持 | 见 7.3 |
| glibc ≤ 2.11（如 RHEL 6 之前） | ❌ 不支持 | 见 7.4 |

### 7.2 Windows 支持现状

- `agent/Cargo.toml` 中有完整的 `[target.'cfg(target_os = "windows")'.dependencies]`，依赖 `winapi` 和 `windows` crate。
- `agent/src/dispatcher/recv_engine/mod.rs` 中 Windows 默认走 `RecvEngine::Libpcap(None)`，即依赖 **WinPcap / Npcap**。
- `agent/src/common/flow.rs` 注释明确："Packet data from AF_PACKET/Winpcap"。
- 但以下核心能力是 **Linux-only**（`cfg(target_os = "linux")` 门控）：
  - eBPF（`@agent/src/ebpf`，集成 bcc、libbpf）
  - cgroups 管理 / 资源限制
  - Kubernetes 元数据发现（`kube`, `k8s-openapi`）
  - procfs 读取（进程、网络统计）
  - DPDK 接收引擎 / VhostUser 接收引擎
  - trace-utils 符号解析
- **`.github/workflows/agent-build.yml` 只构建 linux/amd64 和 linux/arm64**，没有任何 Windows job。
- 结论：Windows 分支可编译（需自己准备 MSVC 工具链 + Npcap SDK），能完成"包捕获 + L4/L7 flow 分析"这条主线，但会损失 eBPF、k8s 感知、容器发现等关键能力。**官方不发布 Windows 产物，也不保证回归。**

### 7.3 32 位 Linux 不支持

- `agent/.cargo/config.toml` 仅列出：
  - `x86_64-unknown-linux-gnu` / `x86_64-unknown-linux-musl`
  - `aarch64-unknown-linux-gnu` / `aarch64-unknown-linux-musl`
  - `aarch64-linux-android`
- **没有任何 `i686-*` / `i586-*` target**，代码中也没有 `cfg(target_pointer_width = "32")` 分支。
- DashMap、tokio multi-thread 运行时、eBPF map、AF_PACKET `TPACKET_V3` 的结构体布局都默认 64 位。强行编 i686 会出现大量 trait bound 与布局错误。

### 7.4 glibc 2.11（及更老）不可用

两道硬门槛：

1. **Rust 工具链自身**：官方 stable 预编译 rustc/cargo 要求 **glibc ≥ 2.17**（对应 CentOS 7 基线）。glibc 2.11 的机器上连编译器都起不来。
2. **即便交叉编译出二进制**：
   - 走 `x86_64-unknown-linux-musl` 出静态产物（cargo 配置留了这条路）可以**绕过 glibc 限制**，但目标机器仍受 **内核** 限制：
     - `TPACKET_V3` 需要内核 ≥ 3.2；
     - eBPF 相关功能需要内核 ≥ 4.x（视具体 probe 不同）；
     - 代码中有注释提到 `procfs 0.16.0` 在 kernel 2.6.32 上 `Process::fd().iter()` 结果不正确（见 Cargo.toml 注释），说明 2.6.x 内核会踩 bug。
- glibc 2.11 大约是 2009 年发布（Ubuntu 10.04 / RHEL 5 时代），对应内核通常是 2.6.x，AF_PACKET 默认实现就跑不起来，agent 的默认 `Tpacket::new` 会直接失败。
- **真正的下限其实是内核版本，不是 glibc**。要支持老系统，优先看内核是否 ≥ 3.2 且有 AF_PACKET TPACKET_V3，再考虑用 musl 规避 glibc。

### 7.5 老系统实战路径（kernel 3.10 + glibc 2.12 等）

典型场景：某些国产化系统或 backport 新内核的 CentOS 6（kernel 3.10 + glibc 2.12），以及 CentOS 7（kernel 3.10 + glibc 2.17）。

**核心思路：现代机器上用 Docker + musl 交叉出静态二进制，拷到老机器上运行，同时在配置中关闭 eBPF。**

#### 步骤 1：交叉编译（在任意支持 Docker 的现代 x86_64 机器上）

仓库已提供静态链接专用 Dockerfile：`agent/docker/dockerfile-build-static-link`，其中关键一行是：

```
cargo build --release --target=x86_64-unknown-linux-musl
```

手动触发构建：

```bash
cd /path/to/deepflow
docker run --privileged --rm -it \
    -v "$(pwd):/deepflow" \
    ghcr.io/deepflowio/rust-build:1.31 \
    bash -c "cd /deepflow/agent && \
             rustup target add x86_64-unknown-linux-musl && \
             cargo build --release --target=x86_64-unknown-linux-musl && \
             cargo build --release --bin deepflow-agent-ctl --target=x86_64-unknown-linux-musl"
```

产物路径：

```
agent/target/x86_64-unknown-linux-musl/release/deepflow-agent
agent/target/x86_64-unknown-linux-musl/release/deepflow-agent-ctl
```

这两个文件是 **完全静态链接** 的 ELF，不依赖目标机器上的 glibc / libpcap / libelf 等，拷过去就能跑。可用 `file` 确认：输出里应包含 `statically linked`。

> aarch64 同理，用 `ghcr.io/deepflowio/rust-build:1.31-arm64` 镜像并改成 `--target=aarch64-unknown-linux-musl`（需配合 QEMU 或原生 arm64 环境）。

#### 步骤 2：目标机器上的运行环境要求

即使 glibc 被 musl 绕开，**内核能力** 仍然是硬门槛。kernel 3.10 上的实际情况：

| 子系统 | kernel 3.10 可用性 | 处理方式 |
|---|---|---|
| AF_PACKET TPACKET_V3（抓包主线）| ✅ 原生支持（3.2+）| 正常使用 |
| cgroups v1 | ✅ | 正常使用 |
| procfs | ✅ | 正常使用 |
| eBPF `bpf()` syscall | ❌ 主线 3.18 才进（RHEL 7.6+ 有部分 backport）| **必须关闭** |
| libbpf CO-RE / BTF | ❌ 需 ≥ 4.9 | 必须关闭 |
| uprobe / kprobe tracer、socket-tracer、on-CPU profiler | ❌ 需 ≥ 4.14 | 必须关闭 |
| DPDK / VhostUser 接收引擎 | ⚠️ 视具体版本 | 默认不启用即可 |

#### 步骤 3：在 agent 配置中关闭 eBPF 相关能力

拷贝过去后，确保运行配置中禁用 eBPF 子系统，否则 agent 启动时会尝试加载 probe 并报错。具体字段以 `@server/agent_config/template.yaml` 中 `ebpf` 段为准（例如 `ebpf.disabled: true` 及各 tracer 的开关），下发策略时关掉整个 ebpf 功能块。

dispatcher / flow_generator / collector / sender 主干不受影响，agent 仍可完成：
- 网络抓包（L2–L4）
- L7 协议识别（HTTP、DNS、MySQL 等）
- Flow 维护与指标聚合
- 通过 TCP:20033 上报 server

#### 已知会损失的能力（相对完整版）

- 无 eBPF 进程级观测（syscall、socket、file、on-CPU profile 等）
- 无基于 uprobe 的应用层追踪（如 Go runtime、HTTPS 解密等）
- 无 kprobe 派生的额外指标
- Kubernetes 容器发现仍可用（走 API server / kubelet，不依赖 eBPF），但如果老系统同时没有 k8s 环境可自然忽略

#### 小结

- **glibc 2.12 上本地编译 agent：不可行**（Rust stable 工具链最低 glibc 2.17）。
- **现代机器 Docker + musl 静态交叉编译 → 拷贝运行：可行**，走 `dockerfile-build-static-link` 的思路。
- **运行时必须关闭 eBPF**，只用抓包主线；这是 kernel 3.10 的硬限制而非编译问题。

## 8. 参考

- `agent/build.md`：官方英文构建文档（含手动编译步骤）
- `agent/docker/dockerfile-build`：x86_64 CI 构建用 Dockerfile
- `agent/docker/dockerfile-build-aarch64`：aarch64 CI 构建用 Dockerfile
- `.github/workflows/agent-build.yml`：完整 CI 构建流水线
- `agent/.cargo/config.toml`：各目标 triple 的 linker 配置
