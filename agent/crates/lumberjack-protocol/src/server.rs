use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::warn;

use crate::codec::LumberjackCodec;
use crate::error::{Error, Result};
use crate::frame::{Frame, DEFAULT_MAX_FRAME_SIZE};

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

pub struct Batch {
    events: Vec<Value>,
    last_seq: u32,
    ack: Option<oneshot::Sender<u32>>,
}

impl Batch {
    pub(crate) fn new(events: Vec<Value>, last_seq: u32, ack: oneshot::Sender<u32>) -> Self {
        Self {
            events,
            last_seq,
            ack: Some(ack),
        }
    }

    pub fn events(&self) -> &[Value] {
        &self.events
    }

    pub fn into_events(mut self) -> Vec<Value> {
        std::mem::take(&mut self.events)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn ack(mut self) {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(self.last_seq);
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(self.last_seq);
        }
    }
}

pub(crate) type BatchSender = mpsc::Sender<Batch>;
pub(crate) type BatchReceiver = mpsc::Receiver<Batch>;

// ---------------------------------------------------------------------------
// Per-connection state machine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ConnectionConfig {
    pub max_frame_size: usize,
    pub keepalive: Option<Duration>,
}

pub(crate) async fn run_connection<S>(
    stream: S,
    cfg: ConnectionConfig,
    out: BatchSender,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let codec = LumberjackCodec::with_max_frame_size(cfg.max_frame_size);
    let mut framed = Framed::new(stream, codec);

    loop {
        // Expect a Window frame to begin a batch.
        let window = match framed.next().await {
            Some(Ok(Frame::Window(n))) => n,
            Some(Ok(_)) => {
                return Err(Error::UnexpectedFrame("expected Window at batch start"));
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()), // clean EOF
        };

        let mut events: Vec<Value> = Vec::with_capacity(window as usize);
        let mut received: u32 = 0;
        let mut last_seq: u32 = 0;
        let mut prev_seq: u32 = 0;

        while received < window {
            let frame = match framed.next().await {
                Some(Ok(f)) => f,
                Some(Err(e)) => return Err(e),
                None => return Err(Error::ConnectionClosed),
            };
            match frame {
                Frame::Json { seq, data } => {
                    if seq <= prev_seq {
                        return Err(Error::SeqOutOfOrder { got: seq, prev: prev_seq });
                    }
                    prev_seq = seq;
                    last_seq = seq;
                    received += 1;
                    push_json_event(&mut events, &data);
                }
                Frame::Compressed(inner) => {
                    let mut buf = bytes::BytesMut::from(&inner[..]);
                    while !buf.is_empty() && received < window {
                        let inner_frame = Frame::decode_with_limit(&mut buf, cfg.max_frame_size)?;
                        match inner_frame {
                            Some(Frame::Json { seq, data }) => {
                                if seq <= prev_seq {
                                    return Err(Error::SeqOutOfOrder {
                                        got: seq,
                                        prev: prev_seq,
                                    });
                                }
                                prev_seq = seq;
                                last_seq = seq;
                                received += 1;
                                push_json_event(&mut events, &data);
                            }
                            Some(_) => {
                                return Err(Error::UnexpectedFrame(
                                    "compressed payload must contain only J frames",
                                ));
                            }
                            None => {
                                return Err(Error::InvalidFrame("compressed payload truncated"));
                            }
                        }
                    }
                }
                Frame::Window(_) | Frame::Ack(_) => {
                    return Err(Error::UnexpectedFrame("expected J or C inside window"));
                }
            }
        }

        // Dispatch the batch and wait for ack, sending Ack(0) keepalives if configured.
        let (ack_tx, mut ack_rx) = oneshot::channel();
        let batch = Batch::new(events, last_seq, ack_tx);
        if out.send(batch).await.is_err() {
            // Receiver dropped — server is shutting down.
            return Ok(());
        }

        let acked = loop {
            let recv_result = match cfg.keepalive {
                Some(interval) => {
                    tokio::select! {
                        biased;
                        r = &mut ack_rx => Some(r),
                        _ = tokio::time::sleep(interval) => None,
                    }
                }
                None => Some((&mut ack_rx).await),
            };
            match recv_result {
                Some(Ok(seq)) => break seq,
                Some(Err(_)) => break last_seq,
                None => {
                    framed.send(Frame::Ack(0)).await?;
                }
            }
        };
        framed.send(Frame::Ack(acked)).await?;
    }
}

fn push_json_event(out: &mut Vec<Value>, data: &Bytes) {
    match serde_json::from_slice::<Value>(data) {
        Ok(v) => out.push(v),
        Err(e) => warn!("dropping event with invalid JSON: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Server / ServerBuilder
// ---------------------------------------------------------------------------

pub struct ServerBuilder {
    keepalive: Option<Duration>,
    channel_capacity: usize,
    max_frame_size: usize,
    #[cfg(feature = "tls")]
    tls: Option<tokio_rustls::TlsAcceptor>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            keepalive: Some(Duration::from_secs(15)),
            channel_capacity: 128,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

impl ServerBuilder {
    pub fn keepalive(mut self, interval: Duration) -> Self {
        self.keepalive = Some(interval);
        self
    }

    pub fn no_keepalive(mut self) -> Self {
        self.keepalive = None;
        self
    }

    pub fn channel_capacity(mut self, n: usize) -> Self {
        self.channel_capacity = n;
        self
    }

    pub fn max_frame_size(mut self, n: usize) -> Self {
        self.max_frame_size = n;
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls(mut self, acceptor: tokio_rustls::TlsAcceptor) -> Self {
        self.tls = Some(acceptor);
        self
    }

    pub async fn bind<A: ToSocketAddrs>(self, addr: A) -> Result<Server> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel::<Batch>(self.channel_capacity);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let cfg = Arc::new(ConnectionConfig {
            max_frame_size: self.max_frame_size,
            keepalive: self.keepalive,
        });

        #[cfg(feature = "tls")]
        let tls_acceptor: Arc<Option<tokio_rustls::TlsAcceptor>> = Arc::new(self.tls);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _peer)) => {
                                // Disable Nagle so per-batch ACK frames are not delayed.
                                let _ = stream.set_nodelay(true);
                                let cfg = cfg.clone();
                                let tx = tx.clone();
                                #[cfg(feature = "tls")]
                                let tls = tls_acceptor.clone();
                                tokio::spawn(async move {
                                    let result: Result<()> = async {
                                        #[cfg(feature = "tls")]
                                        {
                                            if let Some(acceptor) = tls.as_ref() {
                                                let tls_stream = acceptor
                                                    .accept(stream)
                                                    .await
                                                    .map_err(Error::Io)?;
                                                return run_connection(
                                                    tls_stream,
                                                    (*cfg).clone(),
                                                    tx,
                                                )
                                                .await;
                                            }
                                        }
                                        run_connection(stream, (*cfg).clone(), tx).await
                                    }
                                    .await;
                                    if let Err(e) = result {
                                        warn!("connection terminated: {e}");
                                    }
                                });
                            }
                            Err(e) => {
                                warn!("accept failed: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Server {
            rx,
            shutdown: Some(shutdown_tx),
            local_addr,
        })
    }
}

pub struct Server {
    rx: BatchReceiver,
    shutdown: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
}

impl Server {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Server> {
        ServerBuilder::default().bind(addr).await
    }

    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn recv(&mut self) -> Option<Batch> {
        self.rx.recv().await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::duplex;

    #[tokio::test]
    async fn explicit_ack_sends_last_seq() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![Value::Null], 7, tx);
        batch.ack();
        assert_eq!(rx.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn drop_without_ack_still_sends_last_seq() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![Value::Null, Value::Null], 12, tx);
        drop(batch);
        assert_eq!(rx.await.unwrap(), 12);
    }

    #[tokio::test]
    async fn double_ack_is_a_noop() {
        let (tx, rx) = oneshot::channel();
        let batch = Batch::new(vec![], 3, tx);
        batch.ack();
        assert_eq!(rx.await.unwrap(), 3);
    }

    #[tokio::test]
    async fn server_processes_uncompressed_batch() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);

        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig {
                    max_frame_size: 1024,
                    keepalive: None,
                },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json {
                seq: 1,
                data: Bytes::from_static(b"{\"a\":1}"),
            })
            .await
            .unwrap();
        client
            .send(Frame::Json {
                seq: 2,
                data: Bytes::from_static(b"{\"b\":2}"),
            })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.events()[0]["a"], 1);
        assert_eq!(batch.events()[1]["b"], 2);
        batch.ack();

        match client.next().await.unwrap().unwrap() {
            Frame::Ack(seq) => assert_eq!(seq, 2),
            other => panic!("expected Ack, got {other:?}"),
        }

        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_skips_invalid_json_event() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);
        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig {
                    max_frame_size: 1024,
                    keepalive: None,
                },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json {
                seq: 1,
                data: Bytes::from_static(b"not json"),
            })
            .await
            .unwrap();
        client
            .send(Frame::Json {
                seq: 2,
                data: Bytes::from_static(b"{\"ok\":true}"),
            })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events()[0]["ok"], true);
        batch.ack();
        // Drain the ack so the server's write completes before we drop the client.
        match client.next().await.unwrap().unwrap() {
            Frame::Ack(seq) => assert_eq!(seq, 2),
            other => panic!("expected Ack, got {other:?}"),
        }
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_rejects_seq_out_of_order() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, _out_rx) = mpsc::channel::<Batch>(8);
        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig {
                    max_frame_size: 1024,
                    keepalive: None,
                },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(2)).await.unwrap();
        client
            .send(Frame::Json {
                seq: 5,
                data: Bytes::from_static(b"{}"),
            })
            .await
            .unwrap();
        client
            .send(Frame::Json {
                seq: 3,
                data: Bytes::from_static(b"{}"),
            })
            .await
            .unwrap();

        let res = server.await.unwrap();
        assert!(matches!(res, Err(Error::SeqOutOfOrder { got: 3, prev: 5 })));
    }

    #[tokio::test]
    async fn server_sends_ack0_keepalive_while_user_holds_batch() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (out_tx, mut out_rx) = mpsc::channel::<Batch>(8);

        let server = tokio::spawn(async move {
            run_connection(
                server_io,
                ConnectionConfig {
                    max_frame_size: 1024,
                    keepalive: Some(Duration::from_millis(20)),
                },
                out_tx,
            )
            .await
        });

        let mut client = Framed::new(client_io, LumberjackCodec::new());
        client.send(Frame::Window(1)).await.unwrap();
        client
            .send(Frame::Json {
                seq: 1,
                data: Bytes::from_static(b"{}"),
            })
            .await
            .unwrap();

        let batch = out_rx.recv().await.unwrap();

        let mut zeros = 0u32;
        for _ in 0..3 {
            match client.next().await.unwrap().unwrap() {
                Frame::Ack(0) => zeros += 1,
                Frame::Ack(seq) => panic!("got real ack {seq} too early"),
                other => panic!("got {other:?}"),
            }
            if zeros >= 2 {
                break;
            }
        }
        assert!(zeros >= 2);

        batch.ack();
        loop {
            match client.next().await.unwrap().unwrap() {
                Frame::Ack(0) => continue,
                Frame::Ack(1) => break,
                other => panic!("got {other:?}"),
            }
        }

        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_bind_accepts_real_tcp_connection() {
        let mut server = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();

        let client_task = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut client = Framed::new(stream, LumberjackCodec::new());
            client.send(Frame::Window(1)).await.unwrap();
            client
                .send(Frame::Json {
                    seq: 1,
                    data: Bytes::from_static(b"{\"x\":42}"),
                })
                .await
                .unwrap();
            loop {
                match client.next().await.unwrap().unwrap() {
                    Frame::Ack(0) => continue,
                    Frame::Ack(1) => return,
                    other => panic!("unexpected: {other:?}"),
                }
            }
        });

        let batch = server.recv().await.unwrap();
        assert_eq!(batch.events()[0]["x"], 42);
        batch.ack();
        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn server_drop_stops_accept_loop() {
        let server = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        drop(server);

        // Give the accept loop a moment to react to the shutdown signal.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let res = tokio::time::timeout(
            Duration::from_millis(500),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .unwrap();
        assert!(res.is_err(), "expected connection refused after drop");
    }

}
