use kvdb::{MemTable, ProstServerStream, Service, ServiceInner};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let service: Service = ServiceInner::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("start server at {}", addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("accept connection from {}", addr);
        let stream = ProstServerStream::new(socket, service.clone());
        tokio::spawn(async move {
            if let Err(e) = stream.process().await {
                error!("failed to process connection: {}", e);
            }
        });
    }
}
