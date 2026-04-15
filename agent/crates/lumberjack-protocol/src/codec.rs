use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{Error, Result};
use crate::frame::{Frame, DEFAULT_MAX_FRAME_SIZE};

#[derive(Debug, Clone)]
pub struct LumberjackCodec {
    pub max_frame_size: usize,
}

impl Default for LumberjackCodec {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl LumberjackCodec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for LumberjackCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        Frame::decode_with_limit(src, self.max_frame_size)
    }
}

impl Encoder<Frame> for LumberjackCodec {
    type Error = Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn encode_then_decode_window() {
        let mut codec = LumberjackCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Frame::Window(5), &mut buf).unwrap();
        let out = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(out, Frame::Window(5));
    }

    #[test]
    fn decode_streams_back_to_back_frames() {
        let mut codec = LumberjackCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Frame::Window(2), &mut buf).unwrap();
        codec
            .encode(
                Frame::Json {
                    seq: 1,
                    data: Bytes::from_static(b"{}"),
                },
                &mut buf,
            )
            .unwrap();
        codec
            .encode(
                Frame::Json {
                    seq: 2,
                    data: Bytes::from_static(b"[]"),
                },
                &mut buf,
            )
            .unwrap();

        let f1 = codec.decode(&mut buf).unwrap().unwrap();
        let f2 = codec.decode(&mut buf).unwrap().unwrap();
        let f3 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1, Frame::Window(2));
        assert_eq!(
            f2,
            Frame::Json {
                seq: 1,
                data: Bytes::from_static(b"{}")
            }
        );
        assert_eq!(
            f3,
            Frame::Json {
                seq: 2,
                data: Bytes::from_static(b"[]")
            }
        );
        assert!(buf.is_empty());
    }
}
