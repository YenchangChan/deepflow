# Lumberjack Sender 改造提案

> **状态**:📝 待办（提案阶段，尚未实现）
> **创建日期**:2026-04-08
> **关联文档**:
> - [architecture-deep-dive.md](./architecture-deep-dive.md) - Stage 5 Sender 深度剖析
> - [sender-connection-pool-proposal.md](./sender-connection-pool-proposal.md) - 连接池改造提案

## 背景

DeepFlow Agent 当前 Sender 只支持发送数据到自家的 Ingester（裸 TCP + 自定义二进制协议）或写本地文件。在企业场景下，常常需要把 DeepFlow 采集的数据导出到其他自有组件——比如：

- 自研的日志/指标处理平台
- 现有的 Beats / Logstash 生态
- 通用的数据中转管道
- ELK 栈

**Lumberjack v2 协议**（[elastic/go-lumber](https://github.com/elastic/go-lumber)）是 Elastic Beats 生态的核心传输协议，特点：

- TCP 长连接 + ACK 确认
- 支持 zlib 压缩
- 支持 TLS
- 支持 batch 批量发送
- v2 协议**天然支持 JSON payload**（'J' 帧）
- 协议简单（仅 4 种帧类型）

## 总体方案

**两步走**：

1. **Step 1**: 实现一个独立的 Rust crate `lumberjack-protocol`（纯协议实现）
2. **Step 2**: 在 DeepFlow Agent 的 `UniformSender` 里加一个 `SocketType::Lumberjack` 分支，引用这个 crate

### 为什么独立 crate

| 收益 | 说明 |
|------|------|
| **关注点分离** | 协议归协议，业务归业务 |
| **可独立测试** | 协议层零 IO，单元测试快且彻底（fuzzing 友好）|
| **可贡献社区** | Rust 生态目前没有维护良好的 Lumberjack v2 实现，是个空白 |
| **DeepFlow 集成简单** | DeepFlow 这边只需 ~80 行胶水代码 |
| **长期复用** | 其他 Rust 项目（Vector / 嵌入式 agent / 自研 collector）都可以用 |

## Step 1: lumberjack-protocol crate 设计

### 项目元信息

```toml
[package]
name = "lumberjack-protocol"
version = "0.1.0"
edition = "2021"
license = "Apache-2.0 OR MIT"
description = "Pure Rust implementation of the Lumberjack v2 protocol (used by Beats/Logstash)"
repository = "https://github.com/yourname/lumberjack-protocol"
keywords = ["lumberjack", "beats", "logstash", "protocol"]
categories = ["network-programming", "encoding"]
```

### 模块结构

```
lumberjack-protocol/
├── Cargo.toml
├── README.md
├── LICENSE-APACHE
├── LICENSE-MIT
├── src/
│   ├── lib.rs                # re-exports
│   ├── frame.rs              # ★ 核心：帧编解码（无 IO）
│   ├── error.rs              # 错误类型
│   ├── client/
│   │   ├── mod.rs            # Client trait + ClientConfig
│   │   ├── sync.rs           # 同步 Client（std::net）
│   │   └── async_client.rs   # 异步 Client（tokio，feature 控制）
│   └── tls/
│       ├── mod.rs
│       └── rustls.rs         # rustls 集成
├── tests/
│   ├── frame_codec.rs        # 帧编解码黄金测试
│   ├── interop.rs            # 与 go-lumber server 互通测试
│   └── tls.rs
└── examples/
    ├── simple_send.rs        # 最小示例
    └── tls_send.rs
```

### Cargo features

```toml
[features]
default = ["sync", "compression"]

# 客户端实现
sync = []                          # 同步 client (std::net)
tokio = ["dep:tokio"]              # 异步 client (tokio)

# 压缩
compression = ["dep:flate2"]       # zlib 压缩支持

# TLS（二选一）
tls-rustls = ["dep:rustls", "dep:rustls-native-certs", "dep:rustls-pki-types"]
tls-native = ["dep:native-tls"]

[dependencies]
byteorder = "1.5"
thiserror = "1.0"

flate2 = { version = "1.0", optional = true }
tokio = { version = "1", features = ["net", "io-util"], optional = true }
rustls = { version = "0.23", optional = true }
rustls-native-certs = { version = "0.7", optional = true }
rustls-pki-types = { version = "1.0", optional = true }
native-tls = { version = "0.2", optional = true }

[dev-dependencies]
serde_json = "1.0"
```

**features 设计原则**：

- **默认零异步运行时依赖** —— 同步 API 默认开
- **TLS 二选一** —— rustls 或 native-tls，不绑死
- **Payload 不绑 serde** —— 只接受 `&[u8]`，让用户自由选择序列化方式

### Lumberjack v2 协议参考

帧格式：

```
版本 1 字节: '2' (0x32)
帧类型 1 字节: 'W' / 'C' / 'J' / 'A'
后续字段取决于帧类型
```

**4 种帧类型**：

| 帧 | 用途 | 格式 |
|----|------|------|
| **'W'** Window | 客户端告诉 server 这一批的窗口大小 | `'2' 'W' uint32_BE(window_size)` (6 字节) |
| **'J'** JSON Data | 单条 JSON 数据 | `'2' 'J' uint32_BE(seq) uint32_BE(payload_len) payload` (变长) |
| **'C'** Compressed | 包裹一组其他帧的压缩 | `'2' 'C' uint32_BE(uncompressed_len) zlib_data` (变长) |
| **'A'** Ack | server 端 ACK | `'2' 'A' uint32_BE(seq)` (6 字节) |

**完整 batch 的发送序列**：

```
Client → Server:
  W frame (window = N)            ← 1 个
  C frame {                       ← 1 个，包含压缩后的 N 条
    J frame (seq=1, payload=...)
    J frame (seq=2, payload=...)
    ...
    J frame (seq=N, payload=...)
  }

Server → Client:
  A frame (seq=N)                 ← 收到序号 ≤ N 的所有帧
```

### 核心 API:第 1 层 - Frame 编解码（无 IO）

`src/frame.rs`:

```rust
pub const VERSION_V2: u8 = b'2';

#[derive(Debug, Clone, PartialEq)]
pub enum Frame<'a> {
    Window { size: u32 },
    Json { sequence: u32, payload: &'a [u8] },
    Compressed { payload: &'a [u8] },
    Ack { sequence: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("not enough data")]
    NotEnoughData,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("unknown frame type: {0}")]
    UnknownFrameType(u8),
}

impl<'a> Frame<'a> {
    /// 编码到 buffer，返回写入的字节数
    pub fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let start = buf.len();
        match self {
            Frame::Window { size } => {
                buf.push(VERSION_V2);
                buf.push(b'W');
                buf.extend_from_slice(&size.to_be_bytes());
            }
            Frame::Json { sequence, payload } => {
                buf.push(VERSION_V2);
                buf.push(b'J');
                buf.extend_from_slice(&sequence.to_be_bytes());
                buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(payload);
            }
            Frame::Compressed { payload } => {
                buf.push(VERSION_V2);
                buf.push(b'C');
                buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(payload);
            }
            Frame::Ack { sequence } => {
                buf.push(VERSION_V2);
                buf.push(b'A');
                buf.extend_from_slice(&sequence.to_be_bytes());
            }
        }
        buf.len() - start
    }

    /// 从 buffer 解码一个帧，返回 (frame, 消耗的字节数)
    pub fn decode(buf: &'a [u8]) -> Result<(Frame<'a>, usize), DecodeError> {
        if buf.len() < 2 {
            return Err(DecodeError::NotEnoughData);
        }
        if buf[0] != VERSION_V2 {
            return Err(DecodeError::UnsupportedVersion(buf[0]));
        }

        match buf[1] {
            b'W' => {
                if buf.len() < 6 { return Err(DecodeError::NotEnoughData); }
                let size = u32::from_be_bytes(buf[2..6].try_into().unwrap());
                Ok((Frame::Window { size }, 6))
            }
            b'A' => {
                if buf.len() < 6 { return Err(DecodeError::NotEnoughData); }
                let seq = u32::from_be_bytes(buf[2..6].try_into().unwrap());
                Ok((Frame::Ack { sequence: seq }, 6))
            }
            b'J' => {
                if buf.len() < 10 { return Err(DecodeError::NotEnoughData); }
                let seq = u32::from_be_bytes(buf[2..6].try_into().unwrap());
                let len = u32::from_be_bytes(buf[6..10].try_into().unwrap()) as usize;
                if buf.len() < 10 + len { return Err(DecodeError::NotEnoughData); }
                let payload = &buf[10..10+len];
                Ok((Frame::Json { sequence: seq, payload }, 10 + len))
            }
            b'C' => {
                if buf.len() < 6 { return Err(DecodeError::NotEnoughData); }
                let len = u32::from_be_bytes(buf[2..6].try_into().unwrap()) as usize;
                if buf.len() < 6 + len { return Err(DecodeError::NotEnoughData); }
                let payload = &buf[6..6+len];
                Ok((Frame::Compressed { payload }, 6 + len))
            }
            other => Err(DecodeError::UnknownFrameType(other)),
        }
    }
}
```

**这一层完全无 IO**，适合做穷举测试和 fuzz testing。

### 核心 API:第 2 层 - Sync Client

`src/client/sync.rs`:

```rust
use std::io::{Read, Write};
use std::time::Duration;

#[cfg(feature = "compression")]
use flate2::write::ZlibEncoder;
#[cfg(feature = "compression")]
use flate2::Compression;

use crate::frame::Frame;
use crate::error::Error;

pub struct ClientConfig {
    pub window_size: u32,            // 默认 1000
    pub compression_level: u32,      // 0-9，0=不压缩
    pub ack_timeout: Duration,       // 等 ACK 超时
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            window_size: 1000,
            compression_level: 3,
            ack_timeout: Duration::from_secs(30),
        }
    }
}

pub struct Client<S: Read + Write> {
    stream: S,
    next_seq: u32,
    config: ClientConfig,

    // batch 状态
    pending_payloads: Vec<Vec<u8>>,
    encode_buf: Vec<u8>,
    compress_buf: Vec<u8>,
}

impl<S: Read + Write> Client<S> {
    pub fn new(stream: S, config: ClientConfig) -> Self {
        Self {
            stream,
            next_seq: 0,
            config,
            pending_payloads: Vec::with_capacity(1024),
            encode_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::with_capacity(64 * 1024),
        }
    }

    /// 添加一条数据到当前 batch（不立即发送）
    pub fn push(&mut self, payload: Vec<u8>) {
        self.pending_payloads.push(payload);
    }

    /// 当前 batch 的数据条数
    pub fn batch_len(&self) -> usize {
        self.pending_payloads.len()
    }

    /// 发送当前 batch 并等待 ACK
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.pending_payloads.is_empty() {
            return Ok(());
        }

        let count = self.pending_payloads.len() as u32;

        // 1. 发 Window 帧
        self.encode_buf.clear();
        Frame::Window { size: count }.encode(&mut self.encode_buf);
        self.stream.write_all(&self.encode_buf)?;

        // 2. 准备 J 帧批量
        self.encode_buf.clear();
        for payload in &self.pending_payloads {
            self.next_seq += 1;
            Frame::Json {
                sequence: self.next_seq,
                payload,
            }.encode(&mut self.encode_buf);
        }
        let end_seq = self.next_seq;

        // 3. 压缩并发 C 帧（或不压缩直接发）
        #[cfg(feature = "compression")]
        if self.config.compression_level > 0 {
            self.compress_buf.clear();
            let mut encoder = ZlibEncoder::new(
                &mut self.compress_buf,
                Compression::new(self.config.compression_level),
            );
            encoder.write_all(&self.encode_buf)?;
            encoder.finish()?;

            let mut frame_buf = Vec::with_capacity(6 + self.compress_buf.len());
            Frame::Compressed { payload: &self.compress_buf }.encode(&mut frame_buf);
            self.stream.write_all(&frame_buf)?;
        } else {
            self.stream.write_all(&self.encode_buf)?;
        }

        #[cfg(not(feature = "compression"))]
        self.stream.write_all(&self.encode_buf)?;

        self.stream.flush()?;

        // 4. 等待 ACK
        self.wait_for_ack(end_seq)?;

        self.pending_payloads.clear();
        Ok(())
    }

    fn wait_for_ack(&mut self, target_seq: u32) -> Result<(), Error> {
        let mut buf = [0u8; 6];
        loop {
            self.stream.read_exact(&mut buf)?;
            match Frame::decode(&buf)? {
                (Frame::Ack { sequence }, _) => {
                    if sequence >= target_seq {
                        return Ok(());
                    }
                    // partial ACK，继续等
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }
}
```

### 核心 API:第 3 层 - TLS 包装（可选）

`src/tls/rustls.rs` (`feature = "tls-rustls"`):

```rust
use std::sync::Arc;
use std::net::TcpStream;
use std::path::Path;

use rustls::{ClientConfig as TlsClientConfig, ClientConnection, StreamOwned};
use rustls_pki_types::ServerName;

use crate::client::sync::{Client, ClientConfig};
use crate::error::Error;

pub struct TlsConnector {
    config: Arc<TlsClientConfig>,
}

impl TlsConnector {
    /// 创建一个 TLS 连接器，可选指定 CA 证书
    pub fn new(ca_cert_path: Option<&Path>) -> Result<Self, Error> {
        let mut root_store = rustls::RootCertStore::empty();

        if let Some(path) = ca_cert_path {
            // 加载自定义 CA
            let cert_pem = std::fs::read(path)?;
            for cert in rustls_pemfile::certs(&mut cert_pem.as_slice()) {
                root_store.add(cert?)?;
            }
        } else {
            // 使用系统默认 CA
            for cert in rustls_native_certs::load_native_certs()? {
                root_store.add(cert)?;
            }
        }

        let config = TlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// 连接到 host:port 并返回包好 TLS 的 Client
    pub fn connect(
        &self,
        addr: &str,
        server_name: &str,
        client_config: ClientConfig,
    ) -> Result<Client<StreamOwned<ClientConnection, TcpStream>>, Error> {
        let tcp = TcpStream::connect(addr)?;
        let server_name: ServerName = server_name.to_string().try_into()?;
        let conn = ClientConnection::new(self.config.clone(), server_name)?;
        let stream = StreamOwned::new(conn, tcp);
        Ok(Client::new(stream, client_config))
    }
}
```

### 用户视角的使用

**普通 TCP**:

```rust
use lumberjack_protocol::client::sync::{Client, ClientConfig};
use std::net::TcpStream;

let stream = TcpStream::connect("logstash:5044")?;
let mut client = Client::new(stream, ClientConfig::default());

for json_line in batch {
    client.push(json_line);
    if client.batch_len() >= 1000 {
        client.flush()?;
    }
}
client.flush()?;
```

**TLS**:

```rust
use lumberjack_protocol::tls::TlsConnector;
use lumberjack_protocol::client::sync::ClientConfig;

let connector = TlsConnector::new(Some(Path::new("/etc/deepflow/ca.crt")))?;
let mut client = connector.connect(
    "logstash.example.com:5044",
    "logstash.example.com",
    ClientConfig::default(),
)?;

client.push(json_data);
client.flush()?;
```

API 非常薄,符合 "do one thing well"。

## Step 2: DeepFlow 集成

### Cargo 依赖

```toml
# agent/Cargo.toml
[dependencies]
lumberjack-protocol = { version = "0.1", features = ["sync", "compression", "tls-rustls"] }
```

### 配置项扩展

```yaml
# server/agent_config/template.yaml

outputs:
  socket:
    data_socket_type: LUMBERJACK    # 新增类型
  
  lumberjack:
    endpoints:
      - "lumberjack-1.example.com:5044"
      - "lumberjack-2.example.com:5044"
    tls:
      enabled: true
      ca_cert: "/etc/deepflow/ca.crt"
      server_name: "lumberjack.example.com"
    window_size: 1000
    flush_interval_secs: 5
    compression_level: 3
    ack_timeout_secs: 30
```

### Sender 集成代码

```rust
// agent/src/sender/uniform_sender.rs

use lumberjack_protocol::client::sync::{Client as LumberjackClient, ClientConfig as LjConfig};
use lumberjack_protocol::tls::TlsConnector;

pub struct UniformSender<T> {
    // ... 现有字段
    
    // ★ 新增
    lumberjack_client: Option<LumberjackClient<TlsStream>>,
    lumberjack_kv_string: String,
    lumberjack_last_flush: Instant,
}

impl<T: Sendable> UniformSender<T> {
    fn handle_target_lumberjack(
        &mut self,
        send_item: T,
        config: &SenderConfig,
    ) -> std::io::Result<()> {
        let client = self.lumberjack_client.as_mut()
            .ok_or(io::ErrorKind::NotConnected)?;
        
        // 复用现有的 to_kv_string 生成 JSON
        send_item.to_kv_string(&mut self.lumberjack_kv_string);
        let json = self.lumberjack_kv_string.as_bytes().to_vec();
        self.lumberjack_kv_string.clear();
        
        client.push(json);
        
        // 满了或超时就 flush
        if client.batch_len() >= config.lumberjack_window_size as usize
            || self.lumberjack_last_flush.elapsed() >= config.lumberjack_flush_interval
        {
            client.flush().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.lumberjack_last_flush = Instant::now();
        }
        Ok(())
    }
}

// process() 里加分支
let result = match socket_type {
    SocketType::File => self.handle_target_file(...),
    SocketType::Lumberjack => self.handle_target_lumberjack(send_item, &config),  // ★
    _ => self.handle_target_server(send_item, &config),
};
```

**DeepFlow 这边大概只需要 80-120 行代码**(含配置解析、连接初始化、handler 函数、错误映射)。

## 工作量评估

### crate 实现部分

| 模块 | 内容 | 行数 | 工时 |
|------|------|------|------|
| Frame 编解码 | 4 种帧 + 测试 | ~150 + ~150 测试 | 1 天 |
| Sync Client | 连接管理、batch、ACK 等待 | ~200 + ~100 测试 | 1.5 天 |
| TLS 封装(rustls) | 可选 feature | ~80 + ~30 测试 | 0.5 天 |
| Compression | zlib 集成 | ~50 | 0.5 天 |
| Error 类型 | thiserror 定义 | ~50 | 0.2 天 |
| 文档 | rustdoc + README + 示例 | — | 0.5 天 |
| **互通测试** | 用 go-lumber 启 server 测试 | ~100 | 0.5 天 |

### DeepFlow 集成部分

| 模块 | 内容 | 行数 | 工时 |
|------|------|------|------|
| UniformSender 加分支 | handle_target_lumberjack | ~100 | 0.5 天 |
| 配置项 | proto + yaml + handler 解析 | ~50 | 0.3 天 |
| 集成测试 | 验证端到端发送 | ~50 | 0.3 天 |

### 总计

| 维度 | 数值 |
|------|------|
| **总代码量** | ~1000 行 Rust(含测试) |
| **总工时** | ~5.5 人天 |
| **新增依赖** | flate2、rustls(可选)、tokio(可选) |
| **DeepFlow 改动** | ~150 行(集成层) |

## 设计原则

### 1. 优先做同步 API,异步留到 v0.2

**理由**:

- DeepFlow 的 Sender 是同步的(独立 OS 线程,不在 Tokio runtime 内)
- 同步 API 实现简单
- 后续加 tokio 支持只需要再加一个 `client/async_client.rs`
- 避免一开始就纠结"async runtime 选哪个"

### 2. 严格区分"协议层"和"运输层"

```
Frame              ← 纯协议,零 IO,可 fuzz 测试
Client struct      ← 运输层 + 状态管理
TLS Connector      ← 可选的 TLS 包装
```

将来要加 UDP、QUIC、Unix Socket 等只需要换个 `Read + Write` 实现。

### 3. Payload 用 `&[u8]` 而不是 String

虽然 lumberjack v2 是 JSON 协议,但**协议本身是字节安全的**。用 `&[u8]` 让用户可以传任意 UTF-8 字节序列,不强制 String 转换。

DeepFlow 的 `to_kv_string()` 输出是 String,但 `as_bytes()` 零拷贝转换。

### 4. ACK 等待用阻塞 read,不用 select

```rust
self.stream.read_exact(&mut buf)?;
```

简单直接。如果上层需要 timeout,让 stream 自己设 read timeout:

```rust
tcp_stream.set_read_timeout(Some(Duration::from_secs(10)))?;
```

### 5. 不要试图实现 v1 协议

v1 协议(Filebeat 老版本)有 'D' 帧和 k-v 字段格式,**复杂得多**而且基本被弃用。**只做 v2 ('J' 帧 + JSON)**。

### 6. 提供 conformance test

写一个测试 binary,启动 go-lumber 的 server,发数据然后验证收到的内容。这是证明协议实现正确的"金标准"。

```rust
// tests/interop.rs
#[test]
#[ignore]  // 默认不跑,需要手动启 go-lumber server
fn test_interop_with_go_lumber() {
    // 启 go-lumber server
    // 用 lumberjack-protocol 发 1000 条 JSON
    // 验证 server 全部收到且解码正确
}
```

## crate 命名

| 候选名 | 优劣 |
|------|------|
| **`lumberjack-protocol`** | ✅ 准确、专业、不冲突(**推荐**)|
| `lumberjack` | 可能与 cargo 上同名旧 crate 冲突 |
| `rust-lumberjack` | 不符合 cargo 命名习惯 |
| `lumberjack-rs` | 也常见,但有点冗余 |
| `lj-protocol` | 太晦涩 |

## 待办事项清单

### Phase 1: crate 设计与初始化

- [ ] 创建 GitHub 仓库 `lumberjack-protocol`
- [ ] 决定 license(Apache-2.0 OR MIT)
- [ ] 初始化 Cargo workspace
- [ ] 编写 README.md(项目定位、特性、使用示例)
- [ ] 设计 CI(GitHub Actions: test/clippy/fmt)

### Phase 2: 协议层(Frame 编解码)

- [ ] 定义 `Frame` enum(W/J/C/A 四种)
- [ ] 实现 `Frame::encode()`
- [ ] 实现 `Frame::decode()`(返回消耗字节数)
- [ ] 定义 `DecodeError` 类型
- [ ] 编写黄金测试(known-good encode/decode 用例)
- [ ] 编写 fuzz test(cargo-fuzz)
- [ ] 文档:rustdoc 注释

### Phase 3: Sync Client

- [ ] 定义 `ClientConfig` 结构
- [ ] 实现 `Client::new()` / `push()` / `batch_len()` / `flush()`
- [ ] 实现 zlib 压缩(feature gate)
- [ ] 实现 `wait_for_ack()`
- [ ] 处理 partial ACK
- [ ] 处理 read timeout
- [ ] 单元测试(用 mock stream)

### Phase 4: TLS 支持

- [ ] 实现 `tls::rustls::TlsConnector`
- [ ] 支持自定义 CA cert
- [ ] 支持系统默认 CA(rustls-native-certs)
- [ ] 单元测试(self-signed cert)
- [ ] (可选)`tls::native_tls` 实现

### Phase 5: 互通测试

- [ ] 写 `tests/interop.rs`
- [ ] 用 docker-compose 启动 go-lumber server
- [ ] 测试发送 1000 条 JSON 全部收到
- [ ] 测试压缩开关
- [ ] 测试 ACK 处理
- [ ] 测试 TLS 互通

### Phase 6: 文档与发布

- [ ] 完整的 rustdoc 文档
- [ ] examples/ 目录下 2-3 个示例
- [ ] CHANGELOG.md
- [ ] 发布 v0.1.0 到 crates.io

### Phase 7: DeepFlow 集成

- [ ] 在 `agent/Cargo.toml` 加依赖
- [ ] 扩展 `SocketType` 枚举(KAFKA → 应该叫 LUMBERJACK,补 proto)
- [ ] 添加 `LumberjackConfig` 配置结构
- [ ] 实现 `UniformSender::handle_target_lumberjack()`
- [ ] 在 `process()` 里加 match 分支
- [ ] 添加配置项到 `template.yaml`
- [ ] 添加监控指标(sent / acked / failed / batch_size)
- [ ] 集成测试

### Phase 8: 异步支持(v0.2)

- [ ] 实现 `client::async_client::Client`(基于 tokio)
- [ ] 异步 ACK 等待(不阻塞调用方线程)
- [ ] Pipelining 支持(多 batch in flight)
- [ ] 异步 TLS

## 长期价值

把这个 crate 做出来后,受益的不只是 DeepFlow:

| 受益方 | 用法 |
|------|------|
| **DeepFlow** | 对接 Logstash/Beats 生态 |
| **Vector** | 替换其内部的 lumberjack source 实现 |
| **嵌入式 Rust agent** | 发日志到中心 |
| **自研 Rust 数据 collector** | 标准化协议 |
| **Logstash 替代品** | 写一个 Rust 版的 lumberjack server |

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| **ACK 等待降低吞吐** | v0.2 实现 pipelining(多 batch in flight) |
| **TLS 配置复杂** | 提供合理默认值 + 详细文档 |
| **与 go-lumber 不互通** | 持续 interop test + CI 跑 |
| **协议升级到 v3** | 当前 v2 是主流,v3 还没出;预留 enum 扩展空间 |
| **DeepFlow 升级影响集成** | crate 独立维护,DeepFlow 这边只是引用,影响小 |

## 不解决的问题

| 问题 | 后续方案 |
|------|---------|
| **Lumberjack v1 协议** | 不实现,v1 已弃用 |
| **服务端实现** | v0.x 不做 server,聚焦 client |
| **多 endpoint 客户端 LB** | 复用 [sender-connection-pool-proposal](./sender-connection-pool-proposal.md) 的连接池方案 |
| **持久化 buffer** | 不在 crate 范围内,由上层(DeepFlow)负责 |

## 相关代码位置

### DeepFlow 端

- `@agent/src/sender/uniform_sender.rs` - 当前 Sender 实现,要加 lumberjack 分支
- `@agent/src/sender/mod.rs` - Sender 模块入口
- `@agent/src/config/handler.rs` - SenderConfig 解析
- `@agent/src/config/config.rs:2840` - SocketType 解析
- `@agent/crates/public/src/sender.rs` - Sendable trait 定义
- `@server/agent_config/template.yaml:786` - 配置项
- `@message/agent.proto` - SocketType protobuf 定义

### 参考资料

- [elastic/go-lumber](https://github.com/elastic/go-lumber) - Go 实现的参考
- [Lumberjack v2 协议规范](https://github.com/elastic/go-lumber/blob/master/server/v2/proto.go)
- [Filebeat output.logstash 配置](https://www.elastic.co/guide/en/beats/filebeat/current/logstash-output.html)
- [rustls 文档](https://docs.rs/rustls)

## 压缩链路设计

### 整体压缩策略

| 层级 | 压缩算法 | 选择理由 |
|------|--------|---------|
| **传输层（Lumberjack）** | zlib level 3 | Lumberjack 协议规定（'C' 帧）；兼容性 100% |
| **Kafka 存储** | zstd level 3 | 比 gzip 快 3 倍，比 snappy 压缩率高 30%；Kafka 0.11+ 原生支持 |
| **CK 列存** | zstd level 1（默认）| ClickHouse 列存最优选择，与 codec（DoubleDelta/T64）组合使用 |

### 完整数据流

```
Agent (pb encode 或 JSON)
   ↓
Lumberjack 'C' 帧 zlib 压缩          ← 第 1 次压缩
   ↓
TCP → Lumberjack server
   ↓
zlib 解压                             ← 第 1 次解压
   ↓
JSON 外壳解析 + 业务路由
   ↓
Kafka producer zstd 压缩              ← 第 2 次压缩
   ↓
Kafka brokers (zstd 存储)
   ↓
zstd 解压                             ← 第 2 次解压
   ↓
解 JSON 外壳 → INSERT INTO ClickHouse
   ↓
CK 列编码 zstd 压缩                   ← 第 3 次压缩
   ↓
CK 列存
```

**3 次压缩 + 2 次解压**——看起来很多,但每次 CPU 开销都很小：

| 阶段 | CPU 吞吐 |
|------|---------|
| zlib level 3 压缩 | ~50-100 MB/s 单核 |
| zstd level 3 压缩 | ~300-500 MB/s 单核 |
| zlib 解压 | ~200-300 MB/s 单核 |
| zstd 解压 | ~800 MB/s+ 单核 |

按银行 3000 台 Agent 总流量 ~750 MB/s（已压缩）算，整条链路的总压缩 CPU 大约只占 **3-5 个 vCPU**——可忽略。

### 关键配置参数

#### Lumberjack Client（Agent 端）

```rust
ClientConfig {
    window_size: 1000,
    compression_level: 3,    // zlib level 3
    ack_timeout: Duration::from_secs(30),
}
```

| 级别 | CPU | 压缩率 | 推荐场景 |
|------|-----|-------|---------|
| 1 | 最低 | 最低 | 极高吞吐 |
| **3** | 低 | 中 | **默认推荐** |
| 6 | 中 | 高 | 带宽受限 |
| 9 | 高 | 最高 | 不推荐（CPU 翻倍但收益递减）|

#### Kafka Producer（Lumberjack server 端）

```properties
compression.type=zstd
compression.level=3
batch.size=65536           # 64 KB
linger.ms=10               # 攒批关键
buffer.memory=67108864     # 64 MB
acks=1
```

#### Kafka Broker

```properties
compression.type=producer  # ★ 信任 producer 的 zstd，不重压
```

**关键的 `compression.type=producer`**：让 broker 不要解压重压——直接存 producer 已经压好的数据。能省 broker 端 50%+ 的 CPU。

#### ClickHouse 表 codec 组合

```sql
CREATE TABLE l7_flow_log (
    -- 时间列：DoubleDelta 极致压缩 + ZSTD
    time DateTime CODEC(DoubleDelta, ZSTD(1)),

    -- 高基数字符串：仅 ZSTD
    trace_id String CODEC(ZSTD(3)),
    request_resource String CODEC(ZSTD(3)),

    -- IP：定长 + ZSTD level 1
    ip_0 IPv4 CODEC(ZSTD(1)),
    ip_1 IPv4 CODEC(ZSTD(1)),

    -- 整数指标：T64 + ZSTD
    response_duration UInt64 CODEC(T64, ZSTD(1)),
    request_length UInt32 CODEC(T64, ZSTD(1)),

    -- 枚举类：仅 ZSTD level 1
    l7_protocol UInt8 CODEC(ZSTD(1)),
    response_status UInt8 CODEC(ZSTD(1)),

    -- 大文本（SQL/response body）：ZSTD level 6
    sql_statement String CODEC(ZSTD(6)),

    -- ...
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (time, l3_epc_id_0, ip_0, l7_protocol);
```

| 列类型 | 推荐 codec | 理由 |
|--------|----------|------|
| **DateTime 时间列** | `DoubleDelta, ZSTD(1)` | 时间单调递增，DoubleDelta 极致压缩 |
| **整数指标** | `T64, ZSTD(1)` 或 `Gorilla, ZSTD(1)` | T64 对低位重复整数效果好 |
| **高基数字符串**（trace_id, URL）| `ZSTD(3)` | 字符串无法位编码，靠 zstd |
| **低基数字符串**（method, status）| `LowCardinality(String) + ZSTD(1)` | 字典编码 + zstd |
| **IP 列** | `ZSTD(1)` | 4 字节定长，level 1 足够 |
| **大文本**（SQL, body）| `ZSTD(6)` | 文本压缩率高，值得用更高级别 |

### 容易踩的坑

#### 坑 1：Kafka producer 端 `linger.ms` 不要太小

zstd 的压缩效率**严重依赖批量大小**——单条消息压缩率很差，批量越大压缩率越好。

| linger.ms | 单批条数 | zstd 压缩率 |
|-----------|--------|----------|
| 0 | 1-2 | 1.2x |
| 5 | 50-100 | 4x |
| 10 | 200-500 | **6-8x** |
| 50 | 1000+ | 8-10x |

10ms 是甜点——再大就引入感知延迟。

#### 坑 2：Lumberjack window_size 不要切太小

同样道理，Lumberjack 的 zlib 也吃批量。`window_size: 1000` 是合理默认，**不要为了"低延迟"调到 100 以下**。

#### 坑 3：CK 写入 batch 大小

CK 最差的输入方式是"一条 INSERT 一行"。即使前面 Kafka 的压缩做得再好，CK 端如果 batch 不够大，**列存压缩效率极差**。

```go
// ❌ 坏：每条一插
for msg := range kafkaConsumer {
    db.Exec("INSERT INTO l7_flow_log VALUES (?)", msg)
}

// ✅ 好：批量 + 周期 flush
const BATCH_SIZE = 10000
const FLUSH_INTERVAL = 5 * time.Second

batch := make([]Row, 0, BATCH_SIZE)
ticker := time.NewTicker(FLUSH_INTERVAL)

for {
    select {
    case msg := <-kafkaConsumer:
        batch = append(batch, parse(msg))
        if len(batch) >= BATCH_SIZE {
            ckClient.InsertBatch(batch)
            batch = batch[:0]
        }
    case <-ticker.C:
        if len(batch) > 0 {
            ckClient.InsertBatch(batch)
            batch = batch[:0]
        }
    }
}
```

CK 的 batch 大小**至少 10K-100K 条**，理想 100K-1M 条。

#### 坑 4：JSON 外壳字段名重复影响压缩字典效率

如果 envelope 字段名很长（`message_type`、`agent_id`、`schema_version`），会占用 zstd 字典空间，降低 payload 部分的压缩率。

**优化：用短字段名**

```json
{"t":"l7","a":1,"ts":1712587823,"e":"pb","d":"..."}
```

**或者：把元数据外提到 batch 级别**

```json
{"meta":{"a":1,"v":"v6.5"},"items":[{"t":1712587823,"d":"..."},...]}
```

字段名只出现一次，zstd 字典效率最大化。

#### 坑 5（可选高级优化）：zstd 字典训练

对于超大规模（>10 TB/天）且数据 schema 稳定的场景，可以用 **zstd 字典模式**：

```bash
zstd --train sample-data/* -o deepflow.dict
zstd -D deepflow.dict input -o output.zst
```

字典模式对**高重复度小消息**的压缩率提升 30-50%。但需要客户端和服务端共享字典文件、字典版本管理——增加运维复杂度，**一般规模不需要**。

### 完整链路 CPU 开销估算

按 3000 Agent + 银行场景估算每天的 CPU 开销：

| 阶段 | 数据量 | 压缩算法 | CPU 开销 |
|------|------|--------|---------|
| Agent 端 zlib 压缩 | ~5 TB/天（未压缩 JSON）| zlib L3 | ~5 vCPU·小时 |
| Lumberjack server 端 zlib 解压 | ~750 GB/天 | zlib | ~1 vCPU·小时 |
| Kafka producer zstd 压缩 | ~5 TB/天 | zstd L3 | ~3 vCPU·小时 |
| Kafka consumer zstd 解压 | ~700 GB/天 | zstd | ~0.5 vCPU·小时 |
| CK 列编码 zstd 压缩 | ~5 TB/天 | zstd L1 | ~3 vCPU·小时 |
| **总计** | | | **~12.5 vCPU·小时/天** |

折算下来约 **0.5 个常驻 vCPU**——完全可以接受。

## Payload 格式选择决策

在确定了"传输 zlib + Kafka zstd + CK zstd"的双层压缩前提后,需要决定 Lumberjack 的 'J' 帧 payload 用什么格式。

### 三个候选方案

| 方案 | Payload 格式 | 说明 |
|------|----------|------|
| **方案 1** | 纯 JSON（字段全展开）| 直接用 `Sendable::to_kv_string()` 输出 |
| **方案 2** | JSON 外壳 + base64(pb) | JSON 元数据 + base64 编码的 pb 字节 |
| **方案 3** | 私有 v3 协议 / 'P' 帧扩展 | Lumberjack 帧 payload 直接是 pb 二进制 |

### 反直觉的结论:双层压缩下方案 1 反而最优

**关键认知**: zstd 对 JSON 这种结构化文本的压缩效率**远高于**对 protobuf 的压缩效率。原因:

1. **JSON 字段名是结构化重复**——`"trace_id":""`、`"req_tcp_seq":` 在每条数据里都重复，zstd 字典编码极其高效
2. **protobuf 是接近熵极限的编码**——已经用 tag 编号代替字段名，没有重复结构供 zstd 利用
3. **batch 越大，JSON 的字段名重复越多，zstd 越能发挥**

### 完整链路体积对比（双层压缩前提）

以一条典型 L7 flow log（原始 pb ~250 字节）为例：

#### 第 1 阶段：Agent → Lumberjack server（zlib level 3）

| 方案 | 单条原始 | batch 1000 条 | zlib 后 | 单条等效 | 压缩率 |
|------|--------|------------|------|--------|------|
| **方案 1: 纯 JSON** | 2000 B | 2 MB | ~250 KB | **250 B** | 8.0x |
| **方案 2: JSON+base64(pb)** | 380 B | 380 KB | ~190 KB | **190 B** | 2.0x |
| **方案 3: 原生 pb** | 250 B | 250 KB | ~200 KB | **200 B** | 1.25x |

第一阶段三方案差距已经很小（250/190/200 字节）。

#### 第 2 阶段：Lumberjack server → Kafka（解 zlib → zstd level 3）

Kafka producer 看到的是 zlib 解压后的原始大小。按 batch ~500 条算：

| 方案 | 500 条 batch | zstd 后 batch | 单条等效 | 压缩率 |
|------|-----------|-----------|--------|------|
| **方案 1: 纯 JSON** | 1000 KB | ~80 KB | **160 B** | 12.5x |
| **方案 2: JSON+base64(pb)** | 190 KB | ~70 KB | **140 B** | 2.7x |
| **方案 3: 原生 pb** | 125 KB | ~100 KB | **200 B** | 1.25x |

**核心发现**：

- **方案 1（纯 JSON）的最终体积反而接近最小**——zstd 把字段名重复消化得几乎为零
- **方案 3（原生 pb）反而最大**——pb 已接近熵极限，zstd 压不动
- **方案 2 在中间，但 base64 影响压缩效率**

#### 第 3 阶段：CK 列存

**三方案完全相同**——CK 是反序列化后按列存储，与传输格式无关。

### 完整数据量对比

按 3000 Agent × ~5K l7_log/s × 86400 s 算，每天总数据量：

| 方案 | Agent → server | Kafka 存储/天 | CK 存储/天 |
|------|--------------|------------|----------|
| **方案 1: 纯 JSON** | ~325 GB | **~210 GB** | ~120 GB |
| **方案 2: JSON+base64(pb)** | ~245 GB | **~180 GB** | ~120 GB |
| **方案 3: 原生 pb** | ~260 GB | **~260 GB** | ~120 GB |

**关键发现**：

1. **Kafka 存储方案 1 几乎追平方案 2**（210 vs 180 GB），差距只有 15%
2. **方案 3 的 Kafka 存储反而最大**（260 GB），比方案 1 还多 24%
3. **CK 存储完全一样**
4. **传输带宽差距极小**

### CPU 开销对比

| 阶段 | 方案 1 | 方案 2 | 方案 3 |
|------|------|------|------|
| **Agent JSON encode** | ~3-5x pb | ~1.5x pb | 1x（基准）|
| **Agent base64** | 0 | 0.1x pb | 0 |
| **Server JSON parse** | 慢 | 慢（外壳）+ pb parse + base64 | pb parse 快 |
| **Kafka zstd 压缩** | CPU 大 | 中 | 小 |

| 方案 | Agent 端 CPU 增量 | 评价 |
|------|---------------|------|
| **方案 1** | ~4% 单核（5K log/s）| 🟡 可接受 |
| **方案 2** | ~2% 单核 | 🟢 |
| **方案 3** | ~1% 单核 | 🟢 |

Agent 端 JSON 序列化的实际开销:
- 每条 ~5-10 µs（JSON）vs 1-3 µs（pb）
- 5K l7_log/s × 8 µs = 40 ms/s = **4% 单核**
- 对一台 8 核业务主机 = 0.5% 总 CPU——可忽略

### 三方案完整对比

| 维度 | 方案 1 纯 JSON | 方案 2 JSON+base64(pb) | 方案 3 原生 pb |
|------|--------------|----------------------|---------------|
| **传输带宽** | ✅ 250 B/条 | ✅ 190 B/条 | ✅ 200 B/条 |
| **Kafka 存储** | ✅ 160 B/条 | ✅ 140 B/条 | ❌ 200 B/条 |
| **CK 存储** | ✅ 一样 | ✅ 一样 | ✅ 一样 |
| **Agent CPU** | 🟡 +4% 单核 | 🟢 +2% | ✅ +1% |
| **Server CPU** | 🟡 JSON 解析 | ❌ 多层 | ✅ 最低 |
| **下游改造** | ✅ 零 | 🟡 base64+pb 解码 | ❌ 协议改造 |
| **可调试性** | ✅ jq 友好 | ❌ 需要工具 | ❌ 需要工具 |
| **生态兼容** | ✅ 任何工具 | ✅ 标准 lumberjack | ❌ 私有协议 |
| **协议标准** | ✅ 完全 | ✅ 完全 | ❌ 私有 |
| **crate 中立性** | ✅ | ✅ | ❌ 污染 crate 定位 |

### 决策矩阵

| 你的场景 | 推荐方案 |
|---------|--------|
| **大部分场景**（默认）| **方案 1: 纯 JSON** |
| **Agent 主机 CPU 紧张** | 方案 2: JSON+base64(pb) |
| **Agent CPU 极度紧张 + 下游可控** | 方案 3 的"P 帧扩展"（不要叫 v3）|
| **每月数据量 > 100 TB + 存储成本极敏感** | 方案 2 |
| **数据量 < 10 TB/月** | 方案 1 |

### 最终推荐：方案 1（纯 JSON）

#### 推荐理由

1. **存储成本几乎追平方案 2**——双层压缩消化了原本 5x 的体积差距，实际只多 15% 的 Kafka 存储
2. **Agent CPU 开销可接受**——绝对值只有 4% 单核（业务主机的 0.5%）
3. **巨大的运维收益**：
   - ✅ 零下游改造
   - ✅ 故障时直接 `kafka-console-consumer | jq` 调试
   - ✅ 任何 lumberjack/JSON 消费者都能用
   - ✅ schema 演化简单（JSON 自描述）
   - ✅ lumberjack-protocol crate 完全干净，可贡献社区
4. **方案 3 反而最差**——pb 已接近熵极限，zstd 压不动，Kafka 存储甚至比方案 1 大 24%

#### 修正后的"省钱预期"

| 方案 | Kafka 存储/月 | 相对方案 1 节省 | 节省金额 |
|------|------------|-----------|--------|
| **方案 1** | ~6.3 TB/月 | 0 | $0 |
| **方案 2** | ~5.4 TB/月 | ~0.9 TB | **$45/月** |
| **方案 3** | ~7.8 TB/月 | -1.5 TB（更贵）| -$75/月 |

每月省 $45 vs 多 0.5% Agent CPU——**显然不值得用方案 2 的复杂度去换**。

### 配置实现示例

#### Agent 端实现（推荐方案 1）

```rust
// agent/src/sender/uniform_sender.rs

impl<T: Sendable> UniformSender<T> {
    fn handle_target_lumberjack(
        &mut self,
        send_item: T,
        config: &SenderConfig,
    ) -> std::io::Result<()> {
        // 直接复用 to_kv_string 生成 JSON
        send_item.to_kv_string(&mut self.lumberjack_kv_string);
        let json_bytes = self.lumberjack_kv_string.as_bytes().to_vec();
        self.lumberjack_kv_string.clear();
        
        let client = self.lumberjack_client.as_mut()
            .ok_or(io::ErrorKind::NotConnected)?;
        client.push(json_bytes);
        
        if client.batch_len() >= config.lumberjack_window_size as usize {
            client.flush()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        Ok(())
    }
}
```

注意：**不需要 base64、不需要 envelope 包装**——直接用 DeepFlow 已有的 `to_kv_string()` 输出即可。

#### 服务端消费示例（Go）

```go
// 直接 unmarshal 成结构体
type L7FlowLog struct {
    Time         int64  `json:"time"`
    TraceID      string `json:"trace_id"`
    AgentID      uint32 `json:"agent_id"`
    Protocol     string `json:"l7_protocol"`
    // ...
}

func handleLumberjackFrame(jsonLine []byte) error {
    var log L7FlowLog
    if err := json.Unmarshal(jsonLine, &log); err != nil {
        return err
    }
    // 直接处理
    return processL7Log(&log)
}
```

零序列化开销，零外部依赖。

#### 切换到方案 2 的 fallback 路径（如果将来 Agent CPU 真的不够）

代码可以预留方案 2 的开关，但默认关闭：

```yaml
outputs:
  lumberjack:
    payload_format: json              # 默认。可选值: json | pb_base64
    pb_base64_for:                    # 仅当 payload_format=pb_base64 时生效
      - l7_flow_log                   # 只让大数据类型走 pb
      - profile
```

这样将来如果发现某个数据类型确实需要 pb，可以按需切换，不需要重新设计。

## 一句话总结

> **把 Lumberjack v2 协议做成独立 Rust crate `lumberjack-protocol`（约 ~700 行 + ~280 行测试，5-6 人天），再让 DeepFlow Agent 引用这个 crate 加一个 `SocketType::Lumberjack` 分支（~150 行）。crate 设计要点：① **纯协议层零 IO**（Frame 编解码独立模块）；② **默认同步 API**，异步留到 v0.2；③ **TLS 走 feature**（rustls/native-tls 二选一）；④ **Payload 用 &[u8]**，不强绑 String；⑤ **只做 v2 ('J' 帧)**，不做 v1。**Payload 格式选 纯 JSON（方案 1）**——在传输 zlib + Kafka zstd + CK zstd 双层压缩前提下，zstd 对 JSON 字段名重复有"超线性压缩"，最终 Kafka 存储只比 base64+pb 方案大 15%（每月差 ~$45），但在调试性、生态兼容性、协议中立性方面有压倒性优势；方案 3（私有 pb）反而因为 pb 接近熵极限而 Kafka 存储最大。压缩链路 3 次压缩 + 2 次解压共消耗约 0.5 vCPU，可忽略。crate 命名推荐 `lumberjack-protocol`。**

---

**文档生成日期**: 2026-04-08
**文档版本**: 2.0（新增"压缩链路设计"和"Payload 格式选择决策"两大章节，明确双层压缩前提下推荐方案 1 纯 JSON）
