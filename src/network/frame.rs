use crate::error::KvError;
use crate::pb::Value;
use crate::pb::{CommandRequest, CommandResponse};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;
use tracing::info;

// frame 头
pub const LEN_LEN: usize = 4;

// frame大小 2G

const MAX_FRAME: usize = 2 * 1024 * 1024; // 2G = 2 * 1MB * 1024

// 超过 1436字节就压缩，因为以太网的MTU是1500,
// 除去 IP 头 20 字节、TCP 头 20 字节，还剩 1460
// 一般 TCP 包会包含一些 Option（比如 timestamp），IP 包也可能包含，所以我们预留 20 字节
// 再减去 4 字节的长度，就是 1436
// 不用分片的最大消息长度。如果大于这个，很可能会导致分片，我们就干脆压缩一下。
const COMPRESSION_LIMIT: usize = 1436;

// 代表压缩的bit（4字节的最高位）
const COMPRESSION_BIT: usize = 1 << 31; // 1 * 2 ^ 31

pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    // 编码步骤：

    // 1.获取长度
    // 2.是否需压迫编码
    // 3.重组长度和编码后的内容

    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        // 拿到编码前的长度
        let size = self.encoded_len();

        // 如果太长，直接报错
        if size >= MAX_FRAME {
            return Err(KvError::FrameError);
        }

        // 把编码前的长度先写到buf中
        buf.put_u32(size as _);

        // 判断是否需要压缩
        if size > COMPRESSION_LIMIT {
            // 创建一个新buf
            let mut buf1 = Vec::with_capacity(size);
            // 把信息编码进buf中
            self.encode(&mut buf1)?;

            // 拿到buf的后半部分
            let payload = buf.split_off(LEN_LEN); //只剩下后半部分了（空的）
            buf.clear(); //清空

            // 创建编码器
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            // 把整个buf1的内容都写入
            encoder.write_all(&buf1[..])?;

            // 把处理好的 BytesMut拿回来
            let payload = encoder.finish()?.into_inner();

            debug!("Encoded a frame: size {}({})", size, payload.len());

            // 重写长度
            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);

            // 拼接
            buf.unsplit(payload);
            Ok(())

            // 压缩完成后
        } else {
            self.encode(buf)?;

            Ok(())
        }
    }

    // 把一个完整的 frame decode成一个Message
    // 1. 解头
    // 2. 解内容
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        // 先拿到头 4 字节
        let header = buf.get_u32() as usize;

        // 解码头
        let (len, compressed) = decode_header(header);

        println!("Got a frame: msg len {},compressed {}", len, compressed);

        // 根据是否压缩继续解码
        if compressed {
            // 解码
            let mut decoder = GzDecoder::new(&buf[..len]);
            // 新建空buf
            let mut buf1 = Vec::with_capacity(len * 2);

            // 全部读到空 buf中
            decoder.read_to_end(&mut buf1)?;

            // 取长度
            buf.advance(len);

            // 拿到消息
            Ok(Self::decode(&buf1[..buf1.len()])?)
        } else {
            // 直接解码
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    //
    let len = header & !COMPRESSION_BIT; // 如果头长度和!COMPRESSION_BIT不相等，返回 0
    println!("len {:?}", len);
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    // 拿到头
    let header = stream.read_u32().await? as usize;

    // 检查是否压缩
    let (len, _compressed) = decode_header(header);

    // 分配内存
    buf.reserve(LEN_LEN + len);

    // 放入头
    buf.put_u32(header as _);
    // advance_mut 是 unsafe 的原因是，从当前位置 pos 到 pos + len，
    // 这段内存目前没有初始化。我们就是为了 reserve 这段内存，然后从 stream
    // 里读取，读取完，它就是初始化的。所以，我们这么用是安全的

    unsafe { buf.advance_mut(len) };

    // 从头开始读
    stream.read_exact(&mut buf[LEN_LEN..]).await?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn command_request_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let cmd = CommandRequest::new_hdel("t1", "k1");

        cmd.encode_frame(&mut buf).unwrap();

        println!("buf {:?}", buf);

        assert_eq!(is_compressed(&buf), false);
        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();

        assert_eq!(cmd, cmd1)
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let values: Vec<Value> = vec![1.into(), "hello".into(), b"data".into()];
        let res: CommandResponse = values.into();

        res.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);
        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();

        assert_eq!(res, res1)
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let res: CommandResponse = value.into();

        res.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), true);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();

        assert_eq!(res, res1)
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}
