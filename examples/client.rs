use async_prost::AsyncProstStream;
use futures::prelude::*;
use kvdb::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;

    // use AsyncProstStream to handle tcp frame
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // generate a hset command request
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    // send the command request to the server
    client.send(cmd).await?;

    // receive the response from the server
    if let Some(resp) = client.next().await {
        info!("Got response: {:?}", resp);
    }

    Ok(())
}
