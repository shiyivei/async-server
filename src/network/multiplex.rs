// 使用 yamux 做多路复用

use crate::ProstClientStream;
use futures::{future, Future, TryStreamExt};
use std::marker::PhantomData;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing::info;
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};
// yamux 的控制结构
pub struct YamuxCtrl<S> {
    ctrl: Control, // 用于创建新的 stream
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    // 生成客户端
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    // 生成服务器
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,

        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    // 生成一个控制结构
    pub fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        // 创建配置

        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        let conn = Connection::new(stream.compat(), config, mode);

        let ctrl = conn.control();

        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData::default(),
        }
    }

    // 辅助函数，打开tcp 六
    pub async fn open_stream(
        &mut self,
    ) -> Result<ProstClientStream<Compat<yamux::Stream>>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;

        Ok(ProstClientStream::new(stream.compat()))
    }
}
