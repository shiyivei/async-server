use futures::FutureExt;

// 处理 db server 中 frame 中的 stream
use super::*;
use std::{marker::PhantomData, task::Context};

use crate::FrameCoder;

pub struct ProstStream<S, In, Out> {
    stream: S,
    rbuf: BytesMut,
    wbuf: BytesMut,
    written: usize,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

// 为其实现 Stream

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Out: Unpin + Send,
    In: Unpin + Send + FrameCoder,
{
    type Item = Result<In, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 上次调用结束后，rbuf应为空
        assert!(self.rbuf.len() == 0);

        // 分离 rbuf，摆脱 self引用
        let mut rest = self.rbuf.split_off(0);

        let fut = read_frame(&mut self.stream, &mut rest);
        ready!(Box::pin(fut).poll_unpin(cx))?;

        //拿到frame 数据,把buffer合并回去
        self.rbuf.unsplit(rest);

        //调用 decode_frame 获取解包后的数据
        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}

impl<S, In, Out> Sink<&Out> for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: &Out) -> Result<(), Self::Error> {
        let pr_stream = self.get_mut();

        item.encode_frame(&mut pr_stream.wbuf)?;

        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pr_stream = self.get_mut();

        while pr_stream.written != pr_stream.wbuf.len() {
            let n = ready!(Pin::new(&mut pr_stream.stream)
                .poll_write(cx, &pr_stream.wbuf[pr_stream.written..]))?;

            pr_stream.written += n
        }

        pr_stream.wbuf.clear();
        pr_stream.written = 0;

        ready!(Pin::new(&mut pr_stream.stream).poll_flush(cx)?);

        Poll::Ready(Ok(()))
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        ready!(Pin::new(&mut self.stream).poll_shutdown(cx))?;

        Poll::Ready(Ok(()))
    }
}

impl<S, In, Out> ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            written: 0,
            wbuf: BytesMut::new(),
            rbuf: BytesMut::new(),
            _in: PhantomData::default(),
            _out: PhantomData::default(),
        }
    }
}

// 一般来说，为异步操作而创建的数据结构，如果使用了泛型参数，那么只要内部没有自引用数据，就应该实现 Unpin。

impl<S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::BufMut;
    use futures::StreamExt;

    use super::*;

    // 编写测试数据结构

    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn prost_stream_should_work() -> Result<()> {
        let buf = BytesMut::new();

        let stream = DummyStream { buf };

        let mut stream = ProstStream::<_, CommandRequest, CommandRequest>::new(stream);

        let cmd = CommandRequest::new_hdel("t1", "k1");

        stream.send(&cmd.clone()).await?;

        if let Some(Ok(s)) = stream.next().await {
            assert_eq!(s, cmd);
        } else {
            assert!(false);
        }

        Ok(())
    }
}
