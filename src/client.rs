use anyhow::Result;
use async_prost::AsyncProstStream;
use db_server::CommandRequest;
use db_server::ProstClientStream;
use db_server::TlsClientConnector;
use futures::*; //有些方法怎么感觉要显式引进
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";

    // 读取配置文件
    let ca_cert = include_str!("../fixtures/ca.cert");

    // 提供证书
    let connector = TlsClientConnector::new("dbserver.acme.inc", None, Some(ca_cert))?;

    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    // let mut client =
    //         AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    let mut client = ProstClientStream::new(stream);

    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());

    //     client.send(cmd).await?;

    let data = client.execute_unary(&cmd).await?;

    info!("Got response {:?}", data);

    //     if let Some(Ok(data)) = client.next().await {
    //         info!("Got response {:?}", data);
    //     }

    Ok(())
}
