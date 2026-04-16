# DeepFlow Agent

DeepFlow Agent (deepflow-agent) 是 DeepFlow 的数据采集组件，负责网络流量采集、应用协议解析、eBPF 数据采集等。

## 命令行参数

```
deepflow-agent [OPTIONS]
```

| 参数 | 缩写 | 说明 | 默认值 |
|---|---|---|---|
| `--config-file <PATH>` | `-f` / `-c` | 指定配置文件路径 | `/etc/deepflow-agent.yaml` |
| `--standalone` | | 独立模式运行 | `/etc/deepflow-agent-standalone.yaml` |
| `--version` | `-v` | 显示版本信息 | |
| `--dump-ifs` | | 输出网卡信息 | |
| `--if-mac-source <TYPE>` | | MAC 来源类型，配合 `--dump-ifs` 使用 | `mac` |
| `--xml-path <PATH>` | | libvirt XML 路径，配合 `--dump-ifs` 使用 | `/etc/libvirt/qemu` |
| `--check-privileges` | | 检查 K8s 环境下的权限 | |
| `--add-cap` | | 授予网络相关 capabilities | |
| `--sidecar` | | sidecar 模式运行 | |
| `--cgroups-disabled` | | 禁用 cgroups 资源限制 | |

### 使用示例

```bash
# 使用默认配置启动
deepflow-agent

# 指定配置文件
deepflow-agent -f /opt/deepflow/deepflow-agent.yaml

# 独立模式
deepflow-agent --standalone

# 查看版本
deepflow-agent -v
```

## 配置文件

Agent 有两层配置：

### 静态配置（启动时加载）

文件路径通过命令行 `-f` 参数指定，默认 `/etc/deepflow-agent.yaml`。包含 controller 地址、日志路径、pid 文件等启动必需的配置项。

### 运行时配置（server 下发）

由 DeepFlow Server 通过 gRPC 动态下发，支持热更新。包括采集策略、输出配置、资源限制等。完整配置项参见 `server/agent_config/template.yaml`。

## 日志配置

### 静态配置（`deepflow-agent.yaml`）

```yaml
log_file: /var/log/deepflow-agent/deepflow-agent.log
```

支持绝对路径和相对路径。相对路径会自动基于进程工作目录（cwd）转换为绝对路径。

默认值：
- Linux: `/var/log/deepflow-agent/deepflow-agent.log`
- Windows: `C:\DeepFlow\deepflow-agent\log\deepflow-agent.log`

### 运行时配置（server 下发）

```yaml
global:
  alerts:
    log_level: INFO
    log_file: /var/log/deepflow-agent/deepflow-agent.log
    log_backhaul_enabled: true
  limits:
    max_local_log_file_size: 1000M
    local_log_retention: 300d
```

| 配置项 | 说明 | 默认值 |
|---|---|---|
| `log_level` | 日志级别 | `INFO` |
| `log_file` | 日志文件路径 | `/var/log/deepflow-agent/deepflow-agent.log` |
| `log_backhaul_enabled` | 是否回传日志到 server | `true` |
| `max_local_log_file_size` | 本地日志文件大小上限 | `1000M` |
| `local_log_retention` | 本地日志保留时长 | `300d` |

## 进程锁（PID 文件）

通过静态配置文件的 `pid_file` 字段启用：

```yaml
pid_file: /var/run/deepflow-agent.pid
```

默认值为空（不创建 pid 文件）。

### 工作机制

1. **启动时**检查 pid 文件是否存在，并验证对应进程是否存活
2. 如果有同 PID 的进程在运行，**直接报错退出**，防止重复启动
3. 写入当前进程 PID 到文件
4. **进程退出时自动清理** pid 文件

### 使用示例

```yaml
# deepflow-agent.yaml
controller-ips:
  - 10.1.2.3
pid_file: /var/run/deepflow-agent.pid
log_file: /var/log/deepflow-agent/deepflow-agent.log
```

```bash
deepflow-agent -f /opt/deepflow/deepflow-agent.yaml
```

## 动态库依赖

### X86

1. linux-vdso.so.1
2. libpthread.so.0
3. libz.so.1
4. libstdc++.so.6
5. libgcc_s.so.1
6. librt.so.1
7. libm.so.6
8. libdl.so.2
9. libc.so.6
10. ld-linux-x86-64.so.2

### ARM

1. linux-vdso.so.1
2. libpthread.so.0
3. libz.so.1
4. libstdc++.so.6
5. libgcc_s.so.1
6. librt.so.1
7. libm.so.6
8. libdl.so.2
9. libc.so.6
10. ld-linux-aarch64.so.1
11. libibverbs.so.1
12. libnl-route-3.so.200
13. libnl-3.so.200
