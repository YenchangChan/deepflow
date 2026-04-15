use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid frame: {0}")]
    InvalidFrame(&'static str),

    #[error("frame too large: {size} > {max}")]
    FrameTooLarge { size: usize, max: usize },

    #[error("compression error: {0}")]
    Compression(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("unexpected frame: {0}")]
    UnexpectedFrame(&'static str),

    #[error("seq out of order: got {got}, expected > {prev}")]
    SeqOutOfOrder { got: u32, prev: u32 },

    #[error("write timeout")]
    WriteTimeout,

    #[error("ack timeout")]
    AckTimeout,

    #[error("connection closed by peer")]
    ConnectionClosed,

    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("no local port available in configured range")]
    NoLocalPortAvailable,

    #[cfg(feature = "tls")]
    #[error("tls error: {0}")]
    Tls(#[from] tokio_rustls::rustls::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    #[test]
    fn io_error_converts_via_from() {
        let io = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        let err: Error = io.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.source().is_some());
    }

    #[test]
    fn display_format_includes_context() {
        let err = Error::FrameTooLarge { size: 100, max: 10 };
        assert_eq!(err.to_string(), "frame too large: 100 > 10");
    }
}
