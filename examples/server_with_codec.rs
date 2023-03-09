use anyhow::Result;
use db_server::{CommandRequest, MemTable, Service, ServiceInner, SledDb};
use futures::prelude::*;
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let service: Service = ServiceInner::new(MemTable::new()).into();

    let listener = TcpListener::bind(addr).await?;

    info!("Start listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;

        info!("Client {:?} connected", addr);
        info!("Got stream: {:#?}", stream);

        let svc = service.clone();

        tokio::spawn(async move {
            // 使用Framed直接处理
            let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
            info!("Cut stream: {:#?}", stream);

            while let Some(Ok(mut buf)) = stream.next().await {
                let cmd = CommandRequest::decode(&buf[..]).unwrap(); // 解码
                info!("Got a new command {:?}", cmd);

                let res = svc.execute(cmd);
                buf.clear();
                res.encode(&mut buf).unwrap();

                stream.send(buf.freeze()).await.unwrap();
            }
            info!("Client {:?} disconnected", addr)
        });
    }
}
