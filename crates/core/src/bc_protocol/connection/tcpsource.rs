use crate::bc::model::*;
use crate::Result;
use crate::{bc::codex::BcCodex, Credentials};
use delegate::delegate;
use futures::{sink::Sink, stream::Stream};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::{timeout, Duration};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub(crate) struct TcpSource {
    inner: Framed<TcpStream, BcCodex>,
}

impl TcpSource {
    pub(crate) async fn new<T: Into<String>, U: Into<String>>(
        addr: SocketAddr,
        username: T,
        password: Option<U>,
        debug: bool,
    ) -> Result<TcpSource> {
        let stream = connect_to(addr).await?;

        let codex = if debug {
            BcCodex::new_with_debug(Credentials::new(username, password))
        } else {
            BcCodex::new(Credentials::new(username, password))
        };
        Ok(Self {
            inner: Framed::new(stream, codex),
        })
    }
}

impl Stream for TcpSource {
    type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<Bc> for TcpSource {
    type Error = <BcCodex as Encoder<Bc>>::Error;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
        }
    }
}

/// How long a camera has to accept a connection. Left to the OS, a camera
/// that is off held up the connection attempt for about two minutes.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a connection may sit idle before TCP checks that the camera is
/// still there, and how often it checks again. A camera that lost power
/// without closing the connection was otherwise never noticed (#229).
const KEEPALIVE_TIME: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Helper to create a TcpStream with a connect timeout
async fn connect_to(addr: SocketAddr) -> Result<TcpStream> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    let stream = timeout(CONNECT_TIMEOUT, socket.connect(addr)).await??;
    if let Err(e) = keep_alive(&stream) {
        log::debug!("Could not set TCP keepalive: {e}");
    }
    Ok(stream)
}

fn keep_alive(stream: &TcpStream) -> std::io::Result<()> {
    let keepalive = socket2::TcpKeepalive::new().with_time(KEEPALIVE_TIME);
    #[cfg(any(
        target_os = "android",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
    ))]
    let keepalive = keepalive.with_interval(KEEPALIVE_INTERVAL);
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connections_keep_alive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = connect_to(listener.local_addr().unwrap()).await.unwrap();
        assert!(socket2::SockRef::from(&stream).keepalive().unwrap());
    }
}
