use async_prost::AsyncProstStream;
use futures::prelude::*;
use kvdb::{CommandRequest, CommandResponse, Service, ServiceInner, SledDb};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let service: Service<SledDb> = ServiceInner::new(SledDb::new("/tmp/kvserver/sled"))
        .fn_before_send(|res| match res.message.as_ref() {
            "" => res.message = "altered. Original message is empty".into(),
            s => res.message = format!("altered: {}", s),
        })
        .into();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let svc = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(socket).for_async();

            while let Some(Ok(msg)) = stream.next().await {
                info!("Got a new command: {:?}", msg);

                // execute the command
                let mut resp = svc.execute(msg);
                while let Some(v) = resp.next().await {
                    stream.send((*v).clone()).await.unwrap();
                }
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
