mod frame;
mod multiplex;
mod stream;
mod tls;

use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError, Service};

pub use frame::{read_frame, FrameCoder};
pub use multiplex::YamuxCtrl;
pub use stream::ProstStream;
pub use tls::{TlsClientConnector, TlsServerAcceptor};

/// A stream used to handle the read and write of a socket accepted by the server
pub struct ProstServerStream<S> {
    inner: ProstStream<S, CommandRequest, CommandResponse>,
    service: Service,
}

/// A stream used to handle the read and write of a socket connected to the server
pub struct ProstClientStream<S> {
    inner: ProstStream<S, CommandResponse, CommandRequest>,
}

impl<S> ProstServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self {
            inner: ProstStream::new(stream),
            service,
        }
    }

    /// Process the client connection
    pub async fn process(mut self) -> Result<(), KvError> {
        let stream = &mut self.inner;
        while let Some(Ok(cmd)) = stream.next().await {
            info!("Got a new command: {:?}", cmd);
            let mut resp = self.service.execute(cmd);
            while let Some(v) = resp.next().await {
                stream.send(&v).await?;
            }
        }
        info!("The client has closed the connection");
        Ok(())
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: ProstStream::new(stream),
        }
    }

    /// Send a command to the server and wait for the response
    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        let stream = &mut self.inner;
        stream.send(&cmd).await?;

        match stream.next().await {
            Some(v) => v,
            None => Err(KvError::Internal("Didn't get any response".into())),
        }
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
        assert_res_ok(&resp, &[Value::default()], &[]);

        // hset again
        let cmd = CommandRequest::new_hset("t1", "k1", "v2".into());
        let resp = client.execute(cmd).await?;

        // should return the old value
        assert_res_ok(&resp, &["v1".into()], &[]);
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

        assert_res_ok(&resp, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t1", "k1");
        let resp = client.execute(cmd).await?;
        assert_res_ok(&resp, &[v], &[]);

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

#[cfg(test)]
pub mod utils {
    use std::task::Poll;

    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite};

    use crate::CommandRequest;

    use super::{read_frame, FrameCoder};

    #[derive(Default)]
    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let len = std::cmp::min(buf.capacity(), this.buf.len());
            let data = this.buf.split_to(len);
            buf.put_slice(&data);
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();

        let mut stream = DummyStream { buf };

        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1);
    }
}
