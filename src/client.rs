use std::time::Duration;

use futures::StreamExt;
use kvdb::{CommandRequest, KvError, ProstClientStream, TlsClientConnector, YamuxCtrl};
use tokio::{net::TcpStream, time};
use tokio_util::compat::Compat;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ca_cert = include_str!("../fixtures/ca.cert");

    // connect to server
    let addr = "127.0.0.1:9527";

    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca_cert))?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    // create a yamux client
    let mut ctrl = YamuxCtrl::new_client(stream, None);
    let channel = "lobby";

    let stream = ctrl.open_stream().await?;
    start_publishing(stream, channel)?;

    // create client stream
    let stream = ctrl.open_stream().await?;
    let mut client = ProstClientStream::new(stream);

    // send unary command
    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
    let resp = client.execute_unary(&cmd).await?;
    info!("Got response: {:?}", resp);

    // subscribe to the channel
    let cmd = CommandRequest::new_subscribe(channel);
    let mut stream = client.execute_stream(&cmd).await?;
    let id = stream.id;
    start_unsubscribes(ctrl.open_stream().await?, channel, id)?;

    while let Some(Ok(data)) = stream.next().await {
        info!("Got published data: {:?}", data);
    }

    info!("Done!");
    Ok(())
}

fn start_publishing(stream: Compat<yamux::Stream>, name: &str) -> Result<(), KvError> {
    let cmd = CommandRequest::new_publish(name, vec![1.into(), 2.into(), "hello".into()]);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(1000)).await;
        let mut client = ProstClientStream::new(stream);
        let res = client.execute_unary(&cmd).await;
        match res {
            Ok(resp) => info!("Finished publishing {:?}", resp),
            Err(e) => error!("Failed to publish: {:?}", e),
        }
    });
    Ok(())
}

fn start_unsubscribes(stream: Compat<yamux::Stream>, name: &str, id: u32) -> Result<(), KvError> {
    let cmd = CommandRequest::new_unsubscribe(name, id);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(2000)).await;
        let mut client = ProstClientStream::new(stream);
        let res = client.execute_unary(&cmd).await;
        match res {
            Ok(resp) => info!("Finished unsubscribing {:?}", resp),
            Err(e) => error!("Failed to unsubscribe: {:?}", e),
        }
    });
    Ok(())
}
