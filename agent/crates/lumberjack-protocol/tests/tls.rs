#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn make_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        cert.key_pair.serialize_der(),
    ));
    (cert_der, key_der)
}

#[tokio::test]
async fn tls_round_trip() {
    // Install the default crypto provider once for the process.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (cert, key) = make_self_signed();

    let server_cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    let mut server = Server::builder()
        .tls(acceptor)
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr();

    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .ack_timeout(Duration::from_secs(5))
            .compression_level(0)
            .tls(connector, "localhost")
            .connect(addr)
            .await
            .unwrap();
        let n = client.send(&[json!({"hello": "tls"})]).await.unwrap();
        assert_eq!(n, 1);
    });

    let batch = server.recv().await.unwrap();
    assert_eq!(batch.events()[0]["hello"], "tls");
    batch.ack();
    client_task.await.unwrap();
}
