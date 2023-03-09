use anyhow::Result;
use async_prost::AsyncProstStream;
use db_server::{CommandRequest, CommandResponse};
use futures::prelude::*;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let listener = TcpListener::bind(addr).await?;

    info!("Start listening on {:?}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;

        info!("Client {:?} connected", addr);

        tokio::spawn(async move {
            let mut async_stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();

            while let Some(Ok(msg)) = async_stream.next().await {
                info!("Got a new command: {:?}", msg);

                let mut response = CommandResponse::default();
                response.status = 404;
                response.message = "Not Found".to_string();
                async_stream.send(response).await.unwrap();
            }
            info!("Client {:?} disconnected", addr)
        });
    }
}
