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

fn onion3_to_dotonion(service: &[u8; 35]) -> String {
    let mut domain = data_encoding::BASE32.encode(service).to_lowercase();
    domain.push_str(".onion");
    domain
}
fn multi_to_socks(addr: &Multiaddr) -> Option<TargetAddr<'static>> {
    let mut addr = addr.iter();
    match (addr.next()?, addr.next()) {
        (
            Protocol::Dns(domain) | Protocol::Dns4(domain) | Protocol::Dns6(domain),
            Some(Protocol::Tcp(port)),
        ) => Some(TargetAddr::Domain(domain.into_owned().into(), port)),
        (Protocol::Onion3(service), _) => Some(TargetAddr::Domain(
            onion3_to_dotonion(service.hash()).into(),
            service.port(),
        )),
        (Protocol::Ip4(ip), Some(Protocol::Tcp(port))) => {
            Some(TargetAddr::Ip(SocketAddr::from((ip, port))))
        }
        (Protocol::Ip6(ip), Some(Protocol::Tcp(port))) => {
            Some(TargetAddr::Ip(SocketAddr::from((ip, port))))
        }
        _ => None,
    }
}
fn multi_to_torsocksmulti(addr: Multiaddr) -> Result<Multiaddr, Multiaddr> {
    let Some(Protocol::Onion3(service)) = addr.iter().next() else {
        return Err(addr);
    };

    let mut new_addr = Multiaddr::with_capacity(addr.len() + 1);
    new_addr.push(Protocol::Dns(onion3_to_dotonion(service.hash()).into()));
    new_addr.push(Protocol::Tcp(service.port()));
    addr.iter().skip(1).for_each(|p| new_addr.push(p));
    Ok(new_addr)
}
#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio_socks::TargetAddr;

    const MULTIS: [&str; 6] = [
        "/dns/ip.tld/tcp/10",
        "/dns4/dns.ip4.tld/tcp/11",
        "/dns6/dns.ip6.tld/tcp/12",
        "/onion3/cebulka7uxchnbpvmqapg5pfos4ngaxglsktzvha7a5rigndghvadeyd:13",
        "/ip4/127.0.0.1/tcp/10",
        "/ip6/::1/tcp/10",
    ];

    #[test]
    fn multi_to_socks() {
        assert_eq!(
            MULTIS.map(|ma| super::multi_to_socks(&ma.parse().unwrap()).unwrap()),
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

    #[test]
    fn multi_to_torsocksmulti() {
        assert_eq!(
            MULTIS.map(|ma| super::multi_to_torsocksmulti(ma.parse().unwrap()).ok()),
            [
                None,
                None,
                None,
                Some(
                    "/dns/cebulka7uxchnbpvmqapg5pfos4ngaxglsktzvha7a5rigndghvadeyd.onion/tcp/13"
                        .parse()
                        .unwrap()
                ),
                None,
                None,
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
                proxy.proxy(target).await?,
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

    pub async fn proxy(
        &self,
        target: TargetAddr<'_>,
    ) -> Result<TcpOrUnixStream, tokio_socks::Error> {
        Socks5Stream::connect_with_socket(self.connect().await?, target)
            .await
            .map(Socks5Stream::into_inner)
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

pub type TcpTransport = libp2p::dns::tokio::Transport<libp2p::tcp::tokio::Transport>;
pub struct TorsocksTransport(pub TcpTransport);
impl Transport for TorsocksTransport {
    type Output = <TcpTransport as Transport>::Output;
    type Error = <TcpTransport as Transport>::Error;
    type ListenerUpgrade = std::future::Pending<Result<Self::Output, Self::Error>>;
    type Dial = <TcpTransport as Transport>::Dial;

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
        let addr = multi_to_torsocksmulti(addr).map_err(TransportError::MultiaddrNotSupported)?;
        self.0.dial(addr)
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

    fn address_translation(&self, a: &Multiaddr, b: &Multiaddr) -> Option<Multiaddr> {
        self.0.address_translation(a, b)
    }
}

#[derive(Clone)]
pub enum TorBackend {
    /// Private Tor client
    Arti(Arc<TorClient<TokioRustlsRuntime>>),
    /// Talking through a Tor SOCKS5 proxy
    Socks(Arc<SocksServerAddress>),
    /// In an environment where standard TCP calls go over Tor and TCP can resolve .onion addresses
    Torsocks,
    /// No Tor at all
    None,
}

impl std::fmt::Debug for TorBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(match self {
            TorBackend::Arti(..) => "Arti",
            TorBackend::Socks(..) => "Socks",
            TorBackend::Torsocks => "Torsocks",
            TorBackend::None => "None",
        })
    }
}
