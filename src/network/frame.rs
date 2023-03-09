use crate::error::KvError;
use crate::pb::{CommandRequest, CommandResponse};
use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

// frame 头
pub const LEN_LEN: usize = 4;

// frame大小

const MAX_FRAME: usize = 2 * 1024 * 1024; // 2G = 2 * 1MB * 1024

//超过 1436字节就压缩

const COMPRESSION_LIMIT: usize = 1436;

// 代表压缩的bit
const COMPRESSION_BIT: usize = 1 << 31; // 1 * 2 ^ 31

pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    // 默认实现
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        // 拿到长度
        let size = self.encoded_len();

        if size >= MAX_FRAME {
            return Err(KvError::FrameError);
        }

        // 压缩
        buf.put_u32(size as _);

        if size > COMPRESSION_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;

            let payload = buf.split_off(LEN_LEN); //只剩下后半部分了
                                                  // 处理压缩

            // 压缩后写入缓存
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            let payload = encoder.finish()?.into_inner();

            debug!("Encoded a frame: size {}({})", size, payload.len());

            buf.put_f32((payload.len() | COMPRESSION_BIT) as _);

            buf.unsplit(payload);
            Ok(())

            // 压缩完成后
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_f32() as usize;

        let (len, compressed) = decode_header(header);

        debug!("Got a frame:msg len {},compressed {}", len, compressed);

        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);

            decoder.read_to_end(&mut buf1)?;

            buf.advance(len);

            Ok(Self::decode(&buf1[..buf1.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}
