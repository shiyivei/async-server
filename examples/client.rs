use anyhow::Result;
use async_prost::AsyncProstStream;
use db_server::{CommandRequest, CommandResponse};
use futures::*; //有些方法怎么感觉要显式引进
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let stream = TcpStream::connect(addr).await?;

    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());

    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got response {:?}", data);
    }
    Ok(())
}
