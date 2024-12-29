use kvdb::{CommandRequest, ProstClientStream};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // connect to server
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;

    // create client stream
    let mut client = ProstClientStream::new(stream);

    // send command
    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
    let resp = client.execute(cmd).await?;

    // print response
    println!("resp: {:?}", resp);
    Ok(())
}
