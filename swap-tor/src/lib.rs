use std::sync::Arc;

use tor_rtcompat::tokio::TokioRustlsRuntime;

use arti_client::TorClient;
use libp2p::core::multiaddr::Protocol;
use libp2p::core::transport::{ListenerId, TransportEvent};
use libp2p::{Multiaddr, Transport, TransportError};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_socks::tcp::Socks5Stream;
use tokio_socks::TargetAddr;

pub enum TcpOrUnixStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}
impl AsyncRead for TcpOrUnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrUnixStream::Tcp(tsock) => AsyncRead::poll_read(Pin::new(tsock), cx, buf),
            TcpOrUnixStream::Unix(sock) => AsyncRead::poll_read(Pin::new(sock), cx, buf),
        }
    }
}
impl AsyncWrite for TcpOrUnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TcpOrUnixStream::Tcp(tsock) => AsyncWrite::poll_write(Pin::new(tsock), cx, buf),
            TcpOrUnixStream::Unix(sock) => AsyncWrite::poll_write(Pin::new(sock), cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TcpOrUnixStream::Tcp(tsock) => {
                AsyncWrite::poll_write_vectored(Pin::new(tsock), cx, bufs)
            }
            TcpOrUnixStream::Unix(sock) => {
                AsyncWrite::poll_write_vectored(Pin::new(sock), cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrUnixStream::Tcp(tsock) => AsyncWrite::poll_flush(Pin::new(tsock), cx),
            TcpOrUnixStream::Unix(sock) => AsyncWrite::poll_flush(Pin::new(sock), cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrUnixStream::Tcp(tsock) => AsyncWrite::poll_shutdown(Pin::new(tsock), cx),
            TcpOrUnixStream::Unix(sock) => AsyncWrite::poll_shutdown(Pin::new(sock), cx),
        }
    }
}

fn multi_to_socks(addr: &Multiaddr) -> Option<TargetAddr<'static>> {
    let mut addr = addr.iter();
    match (addr.next()?, addr.next()) {
        (
            Protocol::Dns(domain) | Protocol::Dns4(domain) | Protocol::Dns6(domain),
            Some(Protocol::Tcp(port)),
        ) => Some(TargetAddr::Domain(domain.into_owned().into(), port)),
        (Protocol::Onion3(service), _) => {
            let mut domain = data_encoding::BASE32.encode(service.hash()).to_lowercase();
            domain.push_str(".onion");
            Some(TargetAddr::Domain(domain.into(), service.port()))
        }
        (Protocol::Ip4(ip), Some(Protocol::Tcp(port))) => {
            Some(TargetAddr::Ip(SocketAddr::from((ip, port))))
        }
        (Protocol::Ip6(ip), Some(Protocol::Tcp(port))) => {
            Some(TargetAddr::Ip(SocketAddr::from((ip, port))))
        }
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio_socks::TargetAddr;

    #[test]
    fn multi_to_socks() {
        assert_eq!(
            [
                "/dns/ip.tld/tcp/10",
                "/dns4/dns.ip4.tld/tcp/11",
                "/dns6/dns.ip6.tld/tcp/12",
                "/onion3/cebulka7uxchnbpvmqapg5pfos4ngaxglsktzvha7a5rigndghvadeyd:13",
                "/ip4/127.0.0.1/tcp/10",
                "/ip6/::1/tcp/10",
            ]
            .map(|ma| super::multi_to_socks(&ma.parse().unwrap()).unwrap()),
            [
                TargetAddr::Domain("ip.tld".into(), 10),
                TargetAddr::Domain("dns.ip4.tld".into(), 11),
                TargetAddr::Domain("dns.ip6.tld".into(), 12),
                TargetAddr::Domain(
                    "cebulka7uxchnbpvmqapg5pfos4ngaxglsktzvha7a5rigndghvadeyd.onion".into(),
                    13,
                ),
                TargetAddr::Ip(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 10)),
                TargetAddr::Ip(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 10)),
            ],
        );
    }
}

pub struct Socks5Transport(Arc<SocksServerAddress>);
impl Transport for Socks5Transport {
    type Output = tokio_util::compat::Compat<TcpOrUnixStream>;
    type Error = tokio_socks::Error;
    type ListenerUpgrade = std::future::Pending<Result<Self::Output, Self::Error>>;
    type Dial = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send + 'static>>;

    fn listen_on(
        &mut self,
        _: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        Err(TransportError::MultiaddrNotSupported(addr))
    }

    fn remove_listener(&mut self, _: ListenerId) -> bool {
        false
    }

    fn dial(&mut self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        let target = multi_to_socks(&addr).ok_or(TransportError::MultiaddrNotSupported(addr))?;
        let proxy = self.0.clone();

        Ok(Box::pin(async move {
            Ok(tokio_util::compat::TokioAsyncReadCompatExt::compat(
                Socks5Stream::connect_with_socket(proxy.connect().await?, target)
                    .await?
                    .into_inner(),
            ))
        }))
    }

    fn dial_as_listener(
        &mut self,
        addr: Multiaddr,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.dial(addr)
    }

    fn poll(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        Poll::Pending
    }

    fn address_translation(&self, _: &Multiaddr, _: &Multiaddr) -> Option<Multiaddr> {
        None
    }
}

#[derive(Debug)]
pub enum SocksServerAddress {
    Ip(SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl SocksServerAddress {
    pub fn transport(self: Arc<Self>) -> Socks5Transport {
        tracing::debug!("Using SOCKS5 proxy at {self:?}");
        Socks5Transport(self.clone())
    }

    pub async fn connect(&self) -> std::io::Result<TcpOrUnixStream> {
        match self {
            SocksServerAddress::Ip(tcp) => TcpStream::connect(tcp).await.map(TcpOrUnixStream::Tcp),
            #[cfg(unix)]
            SocksServerAddress::Unix(unix) => {
                UnixStream::connect(unix).await.map(TcpOrUnixStream::Unix)
            }
        }
    }

    /// Consult `$TOR_SOCKS_{IPC_PATH,HOST+PORT}`
    ///
    /// `$TOR_SOCKS_IPC_PATH` is ignored if `cfg(not(unix))`, and takes precedence if `cfg(unix)`.
    pub fn from_tor_environment() -> anyhow::Result<Option<Self>> {
        #[cfg(unix)]
        if let Some(p) = std::env::var_os("TOR_SOCKS_IPC_PATH") {
            return Ok(Some(SocksServerAddress::Unix(p.into())));
        }

        use std::env::var;
        match (var("TOR_SOCKS_HOST"), var("TOR_SOCKS_PORT")) {
            (Ok(h), Ok(p)) => Ok(Some(SocksServerAddress::Ip(SocketAddr::new(
                h.parse()?,
                p.parse()?,
            )))),
            _ => Ok(None),
        }
    }
}

#[derive(Clone)]
pub enum TorBackend {
    Arti(Arc<TorClient<TokioRustlsRuntime>>),
    Socks(Arc<SocksServerAddress>),
    None,
}
