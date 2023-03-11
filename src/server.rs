use anyhow::Result;
use async_prost::AsyncProstStream;
use db_server::{CommandRequest, CommandResponse, MemTable, Service, ServiceInner};
use db_server::{ProstServerStream, TlsServerAcceptor};
use futures::prelude::*;
use tokio::net::TcpListener;
use tracing::info;
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 监听地址
    let addr = "127.0.0.1:9527";

    // 读取配置

    let server_cert: &str = include_str!("../fixtures/server.cert");
    let server_key: &str = include_str!("../fixtures/server.key");

    // 验证安全(接收器，也是一个监听者)
    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;

    // 建表（初始化db）
    let service: Service = ServiceInner::new(MemTable::default()).into();

    // 监听请求
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {:?}", addr);

    loop {
        let tls = acceptor.clone();
        // 拿到tcp数据流
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);

        //    let svc = service.clone();

        //    tokio::spawn(async move {
        //        let mut async_stream =
        //            AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();

        //        while let Some(Ok(cmd)) = async_stream.next().await {
        //            info!("Got a new command: {:?}", cmd);
        //            let res = svc.execute(cmd);

        //            async_stream.send(res).await.unwrap();
        //        }
        //        info!("Client {:?} disconnected", addr)
        //    });

        // 处理数据流（验证客户端）
        let stream = tls.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move {
            {
                stream.process().await
            }
        });
    }
}
