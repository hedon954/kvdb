use std::marker::PhantomData;

use futures::{future, Future, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};

/// A multiplexed connection
pub struct YamuxCtrl<S> {
    /// The control of the multiplexed connection, used to create streams
    ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Create a new multiplexed client connection
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    /// Create a new multiplexed server connection
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    /// Create a new multiplexed connection
    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        let conn = Connection::new(stream.compat(), config, mode);

        let ctrl = conn.control();

        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData,
        }
    }

    /// Open a new stream
    pub async fn open_stream(&mut self) -> Result<Compat<yamux::Stream>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(stream.compat())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;
    use tracing::warn;

    use crate::{
        assert_res_ok,
        network::tls::tls_utils::{tls_acceptor, tls_connector},
        utils::DummyStream,
        CommandRequest, KvError, MemTable, ProstClientStream, ProstServerStream, Service,
        ServiceInner, Storage, TlsServerAcceptor,
    };

    use super::*;

    pub async fn start_server_with<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => match tls.accept(stream).await {
                        Ok(stream) => {
                            f(stream, service.clone());
                        }
                        Err(e) => {
                            warn!("failed to accept tls stream: {}", e);
                        }
                    },
                    Err(e) => {
                        warn!("failed to accept tcp stream: {}", e);
                    }
                }
            }
        });

        Ok(addr)
    }

    pub async fn start_yamux_server<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let f = |stream, service: Service| {
            YamuxCtrl::new_server(stream, None, move |s| {
                let svc = service.clone();
                async move {
                    let stream = ProstServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };

        start_server_with(addr, tls, store, f).await
    }

    #[tokio::test]
    async fn yamux_ctrl_creation_should_work() -> anyhow::Result<()> {
        let s = DummyStream::default();
        let mut ctrl = YamuxCtrl::new_client(s, None);
        let stream = ctrl.open_stream().await;

        assert!(stream.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> anyhow::Result<()> {
        // create a yamux server
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;

        // create a client stream
        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(&addr).await?;
        let stream = connector.connect(stream).await?;

        // create a yamux client
        let mut ctrl = YamuxCtrl::new_client(stream, None);
        let stream = ctrl.open_stream().await?;

        // wrap into a prost stream
        let mut client = ProstClientStream::new(stream);

        // send a command
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        client.execute(cmd).await.unwrap();

        // send again
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(cmd).await.unwrap();
        assert_res_ok(&res, &["v1".into()], &[]);
        Ok(())
    }
}
