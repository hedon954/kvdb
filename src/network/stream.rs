use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::{ready, FutureExt, Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{read_frame, KvError};

use super::FrameCoder;

// A stream used to handle the stream of kv server prost frame.
pub struct ProstStream<S, In, Out> {
    stream: S,

    /// The number of bytes written to the stream.
    written: usize,

    /// The buffer used to write data to the stream.
    wbuf: BytesMut,

    /// The buffer used to read data from the stream.
    rbuf: BytesMut,

    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send + FrameCoder,
    Out: Unpin + Send,
{
    type Item = Result<In, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(self.rbuf.is_empty());

        let mut rest = self.rbuf.split_off(0);

        let fut = read_frame(&mut self.stream, &mut rest);
        ready!(Box::pin(fut).poll_unpin(cx))?;

        self.rbuf.unsplit(rest);

        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}

impl<S, In, Out> Sink<Out> for ProstStream<S, In, Out>
where
    S: AsyncWrite + AsyncRead + Unpin,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.wbuf)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        while this.written != this.wbuf.len() {
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &this.wbuf[this.written..]))?;
            this.written += n;
        }

        this.wbuf.clear();
        this.written = 0;

        ready!(Pin::new(&mut this.stream).poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        ready!(Pin::new(&mut self.stream).poll_shutdown(cx))?;

        Poll::Ready(Ok(()))
    }
}

impl<S, In, Out> ProstStream<S, In, Out> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            written: 0,
            wbuf: BytesMut::new(),
            rbuf: BytesMut::new(),
            _in: PhantomData,
            _out: PhantomData,
        }
    }
}

/// In most cases, the stream is Unpin, so we implement it for ProstStream.
/// NOTE: in most cases, if the stream has generic type,
/// and it dose not have self reference data, we should do this.
impl<S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}

#[cfg(test)]
mod tests {
    use crate::{utils::DummyStream, CommandRequest};

    use super::*;
    use futures::prelude::*;

    #[tokio::test]
    async fn prost_stream_should_work() -> anyhow::Result<()> {
        let buf = BytesMut::new();
        let stream = DummyStream { buf };
        let mut stream = ProstStream::<_, CommandRequest, CommandRequest>::new(stream);
        let cmd = CommandRequest::new_hget("t1", "k1");
        stream.send(cmd.clone()).await?;
        if let Some(Ok(cmd)) = stream.next().await {
            assert_eq!(cmd, cmd);
        } else {
            unreachable!()
        }
        Ok(())
    }
}
