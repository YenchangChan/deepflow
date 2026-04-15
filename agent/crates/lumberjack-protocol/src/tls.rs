//! Convenience constructors for rustls. Use these or build `TlsAcceptor` /
//! `TlsConnector` directly with rustls APIs — both work with this crate.

use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::{Error, Result};

pub fn server_acceptor_from_pem(cert: &Path, key: &Path) -> Result<TlsAcceptor> {
    let cert_pem = std::fs::read(cert)?;
    let key_pem = std::fs::read(key)?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::io::Result<Vec<_>>>()?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_pem.as_slice())?
        .ok_or(Error::InvalidConfig("no private key in PEM file"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Error::Tls)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub fn client_connector_with_native_roots() -> Result<TlsConnector> {
    let mut roots = RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    for cert in result.certs {
        let _ = roots.add(cert);
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}
