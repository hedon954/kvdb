use kvdb::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");

    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;
    let service: Service = ServiceInner::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("start server at {}", addr);

    loop {
        let tls = acceptor.clone();
        let (socket, addr) = listener.accept().await?;
        info!("accept connection from {}", addr);
        let stream = match tls.accept(socket).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("failed to accept connection: {}", e);
                continue;
            }
        };
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move {
            if let Err(e) = stream.process().await {
                error!("failed to process connection: {}", e);
            }
        });
    }
}
