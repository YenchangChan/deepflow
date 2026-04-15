use std::pin::Pin;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::codec::LumberjackCodec;
use crate::error::{Error, Result};
use crate::frame::Frame;

pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncStream for T {}

pub(crate) type BoxedStream = Pin<Box<dyn AsyncStream>>;

pub struct ClientBuilder {
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
    local_port_range: Option<(u16, u16)>,
    #[cfg(feature = "tls")]
    tls: Option<(tokio_rustls::TlsConnector, String)>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            compression_level: 3,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(30)),
            local_port_range: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

impl ClientBuilder {
    pub fn compression_level(mut self, level: u32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn write_timeout(mut self, d: Duration) -> Self {
        self.write_timeout = Some(d);
        self
    }

    pub fn ack_timeout(mut self, d: Duration) -> Self {
        self.ack_timeout = Some(d);
        self
    }

    pub fn local_port_range(mut self, start: u16, end: u16) -> Self {
        self.local_port_range = Some((start, end));
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls(mut self, connector: tokio_rustls::TlsConnector, domain: impl Into<String>) -> Self {
        self.tls = Some((connector, domain.into()));
        self
    }

    pub async fn connect<A: ToSocketAddrs>(self, addr: A) -> Result<Client> {
        if let Some((start, end)) = self.local_port_range {
            if start > end {
                return Err(Error::InvalidConfig("local_port_range start > end"));
            }
        }
        let target = tokio::net::lookup_host(addr)
            .await?
            .next()
            .ok_or(Error::InvalidConfig("no addresses resolved"))?;

        let stream: TcpStream = match self.local_port_range {
            None => TcpStream::connect(target).await?,
            Some((start, end)) => connect_with_port_range(target, start, end).await?,
        };
        // Disable Nagle: ACK frames are tiny and would otherwise be delayed,
        // crippling per-batch round-trip latency.
        let _ = stream.set_nodelay(true);

        let boxed: BoxedStream = {
            #[cfg(feature = "tls")]
            {
                if let Some((connector, domain)) = self.tls {
                    let server_name =
                        tokio_rustls::rustls::pki_types::ServerName::try_from(domain)
                            .map_err(|_| Error::InvalidConfig("invalid TLS server name"))?;
                    let tls_stream = connector
                        .connect(server_name, stream)
                        .await
                        .map_err(Error::Io)?;
                    Box::pin(tls_stream)
                } else {
                    Box::pin(stream)
                }
            }
            #[cfg(not(feature = "tls"))]
            {
                Box::pin(stream)
            }
        };

        Ok(Client {
            framed: Framed::new(boxed, LumberjackCodec::new()),
            compression_level: self.compression_level,
            write_timeout: self.write_timeout,
            ack_timeout: self.ack_timeout,
        })
    }
}

pub struct Client {
    framed: Framed<BoxedStream, LumberjackCodec>,
    compression_level: u32,
    write_timeout: Option<Duration>,
    ack_timeout: Option<Duration>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        ClientBuilder::default().connect(addr).await
    }

    /// Send a batch of events and block until the server has acked them.
    /// Returns the number of events acknowledged.
    pub async fn send<T: Serialize>(&mut self, events: &[T]) -> Result<u32> {
        if events.is_empty() {
            return Ok(0);
        }
        let n = events.len() as u32;

        // Encode J frames into a buffer.
        let mut payload = BytesMut::new();
        for (i, ev) in events.iter().enumerate() {
            let bytes = serde_json::to_vec(ev)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Frame::Json {
                seq: (i as u32) + 1,
                data: Bytes::from(bytes),
            }
            .encode(&mut payload);
        }

        self.send_with_optional_timeout(Frame::Window(n)).await?;

        if self.compression_level > 0 {
            self.send_with_optional_timeout(Frame::Compressed(payload.freeze()))
                .await?;
        } else {
            let mut buf = payload;
            while let Some(frame) = Frame::decode(&mut buf)? {
                self.send_with_optional_timeout(frame).await?;
            }
        }
        self.flush_with_optional_timeout().await?;

        // Receive frames until Ack(seq) where seq >= n. Tolerate Ack(0) and partial acks.
        loop {
            let recv = match self.ack_timeout {
                Some(t) => tokio::time::timeout(t, self.framed.next())
                    .await
                    .map_err(|_| Error::AckTimeout)?,
                None => self.framed.next().await,
            };
            match recv {
                Some(Ok(Frame::Ack(seq))) if seq >= n => return Ok(n),
                Some(Ok(Frame::Ack(_))) => continue,
                Some(Ok(_)) => return Err(Error::UnexpectedFrame("expected Ack")),
                Some(Err(e)) => return Err(e),
                None => return Err(Error::ConnectionClosed),
            }
        }
    }

    pub async fn close(mut self) -> Result<()> {
        self.framed.close().await
    }

    async fn send_with_optional_timeout(&mut self, frame: Frame) -> Result<()> {
        match self.write_timeout {
            Some(t) => tokio::time::timeout(t, self.framed.feed(frame))
                .await
                .map_err(|_| Error::WriteTimeout)?,
            None => self.framed.feed(frame).await,
        }
    }

    async fn flush_with_optional_timeout(&mut self) -> Result<()> {
        match self.write_timeout {
            Some(t) => tokio::time::timeout(t, self.framed.flush())
                .await
                .map_err(|_| Error::WriteTimeout)?,
            None => self.framed.flush().await,
        }
    }
}

async fn connect_with_port_range(
    target: std::net::SocketAddr,
    start: u16,
    end: u16,
) -> Result<TcpStream> {
    use rand::Rng;
    let count = (end - start) as u32 + 1;
    let offset = rand::thread_rng().gen_range(0..count);

    for i in 0..count {
        let port = start + ((offset + i) % count) as u16;
        let socket = match target {
            std::net::SocketAddr::V4(_) => TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => TcpSocket::new_v6()?,
        };
        let bind_addr: std::net::SocketAddr = match target {
            std::net::SocketAddr::V4(_) => format!("0.0.0.0:{port}").parse().unwrap(),
            std::net::SocketAddr::V6(_) => format!("[::]:{port}").parse().unwrap(),
        };
        if socket.bind(bind_addr).is_err() {
            continue;
        }
        match socket.connect(target).await {
            Ok(stream) => return Ok(stream),
            Err(e)
                if e.kind() == std::io::ErrorKind::AddrInUse
                    || e.kind() == std::io::ErrorKind::AddrNotAvailable =>
            {
                continue;
            }
            Err(e) => return Err(Error::Io(e)),
        }
    }
    Err(Error::NoLocalPortAvailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::io::duplex;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn invalid_port_range_errors() {
        let res = ClientBuilder::default()
            .local_port_range(50000, 49000)
            .connect("127.0.0.1:1")
            .await;
        assert!(matches!(res, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn send_uncompressed_writes_window_then_json_then_returns_on_ack() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(5)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            assert_eq!(peer.next().await.unwrap().unwrap(), Frame::Window(2));
            match peer.next().await.unwrap().unwrap() {
                Frame::Json { seq, data } => {
                    assert_eq!(seq, 1);
                    let v: serde_json::Value = serde_json::from_slice(&data).unwrap();
                    assert_eq!(v["a"], 1);
                }
                other => panic!("expected Json, got {other:?}"),
            }
            match peer.next().await.unwrap().unwrap() {
                Frame::Json { seq, .. } => assert_eq!(seq, 2),
                other => panic!("expected Json, got {other:?}"),
            }
            peer.send(Frame::Ack(2)).await.unwrap();
        });

        let n = client.send(&[json!({"a": 1}), json!({"b": 2})]).await.unwrap();
        assert_eq!(n, 2);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn send_compressed_emits_window_then_compressed_frame() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 3,
            write_timeout: None,
            ack_timeout: Some(Duration::from_secs(5)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            assert_eq!(peer.next().await.unwrap().unwrap(), Frame::Window(2));
            match peer.next().await.unwrap().unwrap() {
                Frame::Compressed(inner) => {
                    let mut buf = bytes::BytesMut::from(&inner[..]);
                    let f1 = Frame::decode(&mut buf).unwrap().unwrap();
                    let f2 = Frame::decode(&mut buf).unwrap().unwrap();
                    assert!(matches!(f1, Frame::Json { seq: 1, .. }));
                    assert!(matches!(f2, Frame::Json { seq: 2, .. }));
                }
                other => panic!("expected Compressed, got {other:?}"),
            }
            peer.send(Frame::Ack(2)).await.unwrap();
        });

        let n = client.send(&[json!({"a": 1}), json!({"b": 2})]).await.unwrap();
        assert_eq!(n, 2);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn send_returns_ack_timeout_when_peer_silent() {
        let (client_io, _peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_millis(50)),
        };
        let res = client.send(&[json!({"x": 1})]).await;
        assert!(matches!(res, Err(Error::AckTimeout)));
    }

    #[tokio::test]
    async fn send_tolerates_ack0_keepalives() {
        let (client_io, peer_io) = duplex(64 * 1024);
        let mut client = Client {
            framed: Framed::new(Box::pin(client_io) as BoxedStream, LumberjackCodec::new()),
            compression_level: 0,
            write_timeout: None,
            ack_timeout: Some(Duration::from_millis(200)),
        };

        let peer = tokio::spawn(async move {
            let mut peer = Framed::new(peer_io, LumberjackCodec::new());
            assert!(matches!(peer.next().await.unwrap().unwrap(), Frame::Window(1)));
            assert!(matches!(peer.next().await.unwrap().unwrap(), Frame::Json { .. }));
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(80)).await;
                peer.send(Frame::Ack(0)).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
            peer.send(Frame::Ack(1)).await.unwrap();
        });

        let n = client.send(&[json!({"x": 1})]).await.unwrap();
        assert_eq!(n, 1);
        peer.await.unwrap();
    }

    async fn listen_loopback() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    fn pick_free_port_in(range_start: u16, range_end: u16) -> u16 {
        for p in range_start..=range_end {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p);
            if std::net::TcpListener::bind(addr).is_ok() {
                return p;
            }
        }
        panic!("no free port in {range_start}..={range_end}");
    }

    #[tokio::test]
    async fn local_port_range_uses_port_within_range() {
        let (listener, target) = listen_loopback().await;
        let port = pick_free_port_in(45000, 45200);

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let client = ClientBuilder::default()
            .local_port_range(port, port)
            .connect(target)
            .await
            .unwrap();
        let (server_side, _peer_addr) = accept.await.unwrap();
        assert_eq!(server_side.peer_addr().unwrap().port(), port);
        drop(client);
    }

    #[tokio::test]
    async fn local_port_range_skips_busy_port() {
        let (listener, target) = listen_loopback().await;
        // Reserve one port; the client should pick the other.
        let busy = pick_free_port_in(45300, 45301);
        let _hold = std::net::TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            busy,
        ))
        .unwrap();
        let other = if busy == 45300 { 45301 } else { 45300 };

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let client = ClientBuilder::default()
            .local_port_range(45300, 45301)
            .connect(target)
            .await
            .unwrap();
        let (server_side, _peer_addr) = accept.await.unwrap();
        assert_eq!(server_side.peer_addr().unwrap().port(), other);
        drop(client);
    }

    #[tokio::test]
    async fn local_port_range_exhausted_errors() {
        let (_listener, target) = listen_loopback().await;
        let port = pick_free_port_in(45400, 45400);
        let _hold = std::net::TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            port,
        ))
        .unwrap();
        let res = ClientBuilder::default()
            .local_port_range(port, port)
            .connect(target)
            .await;
        assert!(matches!(res, Err(Error::NoLocalPortAvailable)));
    }
}
