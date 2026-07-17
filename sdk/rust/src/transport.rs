use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::{ConnectionOptions, Error, Result};

pub(crate) enum Socket {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Socket {
    pub(crate) fn open(options: &ConnectionOptions) -> Result<Self> {
        let address = (options.host.as_str(), options.port)
            .to_socket_addrs()
            .map_err(|error| Error::Connection(error.to_string()))?
            .next()
            .ok_or_else(|| Error::Connection("host resolved to no addresses".into()))?;
        let tcp = TcpStream::connect_timeout(&address, options.connect_timeout)
            .map_err(|error| Error::Connection(error.to_string()))?;
        tcp.set_nodelay(true)
            .map_err(|error| Error::Connection(error.to_string()))?;
        let Some(tls) = &options.tls else {
            return Ok(Self::Plain(tcp));
        };
        let loaded = rustls_native_certs::load_native_certs();
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(loaded.certs);
        if let Some(path) = &tls.ca_file {
            let mut reader = BufReader::new(
                File::open(path).map_err(|error| Error::Connection(error.to_string()))?,
            );
            for certificate in rustls_pemfile::certs(&mut reader) {
                roots
                    .add(certificate.map_err(|error| Error::Connection(error.to_string()))?)
                    .map_err(|error| Error::Connection(error.to_string()))?;
            }
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_name = ServerName::try_from(options.host.clone())
            .map_err(|error| Error::Connection(error.to_string()))?;
        let connection = ClientConnection::new(Arc::new(config), server_name)
            .map_err(|error| Error::Connection(error.to_string()))?;
        Ok(Self::Tls(Box::new(StreamOwned::new(connection, tcp))))
    }

    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref(),
        }
    }
}

impl Read for Socket {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}
