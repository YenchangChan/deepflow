use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

pub const PROTOCOL_VERSION: u8 = b'2';

pub const TYPE_WINDOW: u8 = b'W';
pub const TYPE_JSON: u8 = b'J';
pub const TYPE_COMPRESSED: u8 = b'C';
pub const TYPE_ACK: u8 = b'A';

/// Default ceiling for any single frame's payload, in bytes.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Window(u32),
    Json { seq: u32, data: Bytes },
    /// On the wire this carries compressed bytes; in memory we always hold the
    /// *decompressed* inner bytes (a concatenation of `J` frames). The encoder
    /// compresses on the way out, the decoder decompresses on the way in.
    Compressed(Bytes),
    Ack(u32),
}

impl Frame {
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            Frame::Window(n) => {
                dst.reserve(2 + 4);
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_WINDOW);
                dst.put_u32(*n);
            }
            Frame::Json { seq, data } => {
                dst.reserve(2 + 4 + 4 + data.len());
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_JSON);
                dst.put_u32(*seq);
                dst.put_u32(data.len() as u32);
                dst.extend_from_slice(data);
            }
            Frame::Ack(seq) => {
                dst.reserve(2 + 4);
                dst.put_u8(PROTOCOL_VERSION);
                dst.put_u8(TYPE_ACK);
                dst.put_u32(*seq);
            }
            Frame::Compressed(inner) => {
                #[cfg(feature = "compression")]
                {
                    let compressed = zlib_compress(inner);
                    dst.reserve(2 + 4 + compressed.len());
                    dst.put_u8(PROTOCOL_VERSION);
                    dst.put_u8(TYPE_COMPRESSED);
                    dst.put_u32(compressed.len() as u32);
                    dst.extend_from_slice(&compressed);
                }
                #[cfg(not(feature = "compression"))]
                {
                    let _ = inner;
                    panic!("compression feature disabled");
                }
            }
        }
    }

    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>> {
        Self::decode_with_limit(src, DEFAULT_MAX_FRAME_SIZE)
    }

    pub fn decode_with_limit(src: &mut BytesMut, max_frame_size: usize) -> Result<Option<Frame>> {
        if src.len() < 2 {
            return Ok(None);
        }
        let version = src[0];
        if version != PROTOCOL_VERSION {
            return Err(Error::InvalidFrame("unsupported protocol version"));
        }
        let ty = src[1];
        match ty {
            TYPE_WINDOW => {
                if src.len() < 2 + 4 {
                    return Ok(None);
                }
                src.advance(2);
                let n = src.get_u32();
                Ok(Some(Frame::Window(n)))
            }
            TYPE_JSON => {
                if src.len() < 2 + 4 + 4 {
                    return Ok(None);
                }
                let len = u32::from_be_bytes(src[6..10].try_into().unwrap()) as usize;
                if len > max_frame_size {
                    return Err(Error::FrameTooLarge {
                        size: len,
                        max: max_frame_size,
                    });
                }
                if src.len() < 2 + 4 + 4 + len {
                    return Ok(None);
                }
                src.advance(2);
                let seq = src.get_u32();
                let _len = src.get_u32();
                let data = src.split_to(len).freeze();
                Ok(Some(Frame::Json { seq, data }))
            }
            TYPE_ACK => {
                if src.len() < 2 + 4 {
                    return Ok(None);
                }
                src.advance(2);
                let seq = src.get_u32();
                Ok(Some(Frame::Ack(seq)))
            }
            TYPE_COMPRESSED => {
                #[cfg(not(feature = "compression"))]
                {
                    let _ = max_frame_size;
                    return Err(Error::InvalidFrame("compression feature disabled"));
                }
                #[cfg(feature = "compression")]
                {
                    if src.len() < 2 + 4 {
                        return Ok(None);
                    }
                    let len = u32::from_be_bytes(src[2..6].try_into().unwrap()) as usize;
                    if len > max_frame_size {
                        return Err(Error::FrameTooLarge {
                            size: len,
                            max: max_frame_size,
                        });
                    }
                    if src.len() < 2 + 4 + len {
                        return Ok(None);
                    }
                    src.advance(2 + 4);
                    let payload = src.split_to(len);
                    let inflated = zlib_decompress(&payload, max_frame_size)?;
                    Ok(Some(Frame::Compressed(inflated)))
                }
            }
            _ => Err(Error::InvalidFrame("unknown frame type")),
        }
    }
}

#[cfg(feature = "compression")]
fn zlib_compress(input: &[u8]) -> Vec<u8> {
    use flate2::{write::ZlibEncoder, Compression};
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(3));
    enc.write_all(input).expect("writing to Vec never fails");
    enc.finish().expect("finishing in-memory zlib never fails")
}

#[cfg(feature = "compression")]
fn zlib_decompress(input: &[u8], max: usize) -> Result<Bytes> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;
    let mut dec = ZlibDecoder::new(input);
    let mut out = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = dec
            .read(&mut chunk)
            .map_err(|e| Error::Compression(e.to_string()))?;
        if n == 0 {
            break;
        }
        if out.len() + n > max {
            return Err(Error::FrameTooLarge {
                size: out.len() + n,
                max,
            });
        }
        out.extend_from_slice(&chunk[..n]);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Window(42).encode(&mut buf);
        assert_eq!(&buf[..], &[b'2', b'W', 0, 0, 0, 42]);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Window(42));
        assert!(buf.is_empty());
    }

    #[test]
    fn window_partial_returns_none() {
        let mut buf = BytesMut::from(&[b'2', b'W', 0, 0][..]);
        assert!(Frame::decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 4, "decode must not consume bytes when incomplete");
    }

    #[test]
    fn invalid_version_errors() {
        let mut buf = BytesMut::from(&[b'1', b'W', 0, 0, 0, 1][..]);
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(Error::InvalidFrame("unsupported protocol version"))
        ));
    }

    #[test]
    fn unknown_type_errors() {
        let mut buf = BytesMut::from(&[b'2', b'X'][..]);
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(Error::InvalidFrame("unknown frame type"))
        ));
    }

    #[test]
    fn json_round_trip() {
        let payload = Bytes::from_static(b"{\"hello\":\"world\"}");
        let frame = Frame::Json {
            seq: 7,
            data: payload.clone(),
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        assert_eq!(&buf[..2], b"2J");
        assert_eq!(buf.len(), 2 + 4 + 4 + payload.len());

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            decoded,
            Frame::Json {
                seq: 7,
                data: payload
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn json_partial_returns_none() {
        let frame = Frame::Json {
            seq: 1,
            data: Bytes::from_static(b"abc"),
        };
        let mut full = BytesMut::new();
        frame.encode(&mut full);

        for take in 0..full.len() {
            let mut partial = BytesMut::from(&full[..take]);
            assert!(
                Frame::decode(&mut partial).unwrap().is_none(),
                "len={take} should be incomplete"
            );
            assert_eq!(partial.len(), take, "must not consume on incomplete");
        }
    }

    #[test]
    fn json_oversize_errors() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"2J");
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&10u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 10]);

        let res = Frame::decode_with_limit(&mut buf, 5);
        assert!(matches!(
            res,
            Err(Error::FrameTooLarge { size: 10, max: 5 })
        ));
    }

    #[test]
    fn ack_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Ack(99).encode(&mut buf);
        assert_eq!(&buf[..], &[b'2', b'A', 0, 0, 0, 99]);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Ack(99));
    }

    #[test]
    fn ack_zero_round_trip() {
        let mut buf = BytesMut::new();
        Frame::Ack(0).encode(&mut buf);
        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::Ack(0));
    }

    #[test]
    fn ack_partial_returns_none() {
        let mut buf = BytesMut::from(&[b'2', b'A', 0, 0][..]);
        assert!(Frame::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "compression")]
    fn compressed_round_trip() {
        let inner_a = Frame::Json {
            seq: 1,
            data: Bytes::from_static(b"{\"a\":1}"),
        };
        let inner_b = Frame::Json {
            seq: 2,
            data: Bytes::from_static(b"{\"b\":2}"),
        };
        let mut inner = BytesMut::new();
        inner_a.encode(&mut inner);
        inner_b.encode(&mut inner);
        let inner_bytes = inner.freeze();

        let mut buf = BytesMut::new();
        Frame::Compressed(inner_bytes.clone()).encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        match decoded {
            Frame::Compressed(b) => assert_eq!(b, inner_bytes),
            other => panic!("expected Compressed, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "compression")]
    fn compressed_corrupted_payload_errors() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"2C");
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(matches!(Frame::decode(&mut buf), Err(Error::Compression(_))));
    }

    #[test]
    #[cfg(feature = "compression")]
    fn compressed_partial_returns_none() {
        let mut full = BytesMut::new();
        Frame::Compressed(Bytes::from_static(
            b"hello world hello world hello world hello world",
        ))
        .encode(&mut full);
        for take in 0..full.len() {
            let mut partial = BytesMut::from(&full[..take]);
            assert!(Frame::decode(&mut partial).unwrap().is_none());
        }
    }
}
