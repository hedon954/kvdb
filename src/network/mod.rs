mod frame;

use bytes::BytesMut;
pub use frame::{read_frame, FrameCoder};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError, Service};

/// A stream used to handle the read and write of a socket accepted by the server
pub struct ProstServerStream<S> {
    inner: S,
    service: Service,
}

/// A stream used to handle the read and write of a socket connected to the server
pub struct ProstClientStream<S> {
    inner: S,
}

impl<S> ProstServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self {
            inner: stream,
            service,
        }
    }

    /// Process the client connection
    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(cmd) = self.recv().await {
            info!("Got a new command: {:?}", cmd);
            let resp = self.service.execute(cmd);
            self.send(resp).await?;
        }
        info!("The client has closed the connection");
        Ok(())
    }

    /// Read a command from the client
    async fn recv(&mut self) -> Result<CommandRequest, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandRequest::decode_frame(&mut buf)
    }

    /// Send a response to the client
    async fn send(&mut self, resp: CommandResponse) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        resp.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self { inner: stream }
    }

    /// Send a command to the server and wait for the response
    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        self.send(cmd).await?;
        self.recv().await
    }

    /// Send a command to the server
    async fn send(&mut self, cmd: CommandRequest) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        cmd.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }

    /// Read a response from the server
    async fn recv(&mut self) -> Result<CommandResponse, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandResponse::decode_frame(&mut buf)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};

    use crate::{assert_res_ok, MemTable, ServiceInner, Value};

    use super::*;

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        // hset
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let resp = client.execute(cmd).await?;

        // first time should return none
        assert_res_ok(resp, &[Value::default()], &[]);

        // hset again
        let cmd = CommandRequest::new_hset("t1", "k1", "v2".into());
        let resp = client.execute(cmd).await?;

        // should return the old value
        assert_res_ok(resp, &["v1".into()], &[]);
        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t1", "k1", v.clone());
        let resp = client.execute(cmd).await?;

        assert_res_ok(resp, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t1", "k1");
        let resp = client.execute(cmd).await?;
        assert_res_ok(resp, &[v], &[]);

        Ok(())
    }

    async fn start_server() -> anyhow::Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let service: Service = ServiceInner::new(MemTable::new()).into();
                let server = ProstServerStream::new(socket, service);
                tokio::spawn(server.process());
            }
        });

        Ok(addr)
    }
}
