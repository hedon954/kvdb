use async_prost::AsyncProstStream;
use futures::prelude::*;
use kvdb::{CommandRequest, CommandResponse};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(socket).for_async();

            while let Some(Ok(msg)) = stream.next().await {
                info!("Got a new command: {:?}", msg);

                // create a 404 response for client
                let resp = CommandResponse {
                    status: 404,
                    message: "Not found".to_string(),
                    ..Default::default()
                };

                // send the response back to client
                stream.send(resp).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
