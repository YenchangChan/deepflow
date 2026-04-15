//! Async Rust implementation of the Lumberjack v2 protocol.
//!
//! See [`Server`] and [`Client`] for the public API. Plain TCP works out of the
//! box; rustls TLS is available with the `tls` feature, and zlib batch
//! compression with the `compression` feature.

pub mod client;
pub mod codec;
pub mod error;
pub mod frame;
pub mod server;

#[cfg(feature = "tls")]
pub mod tls;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use frame::Frame;
pub use server::{Batch, Server, ServerBuilder};
