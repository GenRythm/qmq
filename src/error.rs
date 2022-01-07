use std::{net::AddrParseError, io};

use bincode::ErrorKind;
use quinn::{
    ConnectionError, ConnectError, WriteError, ReadToEndError,
};
use rcgen::RcgenError;

pub type Result<T> = std::result::Result<T, QmqError>;

#[derive(Debug)]
pub enum QmqError {
    AddrParseError(AddrParseError),
    ConnectionError(ConnectionError),
    ConnectError(ConnectError),
    WriteError(WriteError),
    ReadToEndError(ReadToEndError),
    BincodeError(Box<ErrorKind>),
    IoError(io::Error),
    RcgenError(RcgenError),
    Rustls(rustls::Error),
    VarIntBoundExceed,
    CertificationError,
    ConnectionLost,
    SubscribeError,
    CommonError,
}
impl From<AddrParseError> for QmqError {
    fn from(e: AddrParseError) -> Self {
        Self::AddrParseError(e)
    }
}
impl From<ConnectionError> for QmqError {
    fn from(e: ConnectionError) -> Self {
        Self::ConnectionError(e)
    }
}
impl From<ConnectError> for QmqError {
    fn from(e: ConnectError) -> Self {
        Self::ConnectError(e)
    }
}
impl From<Box<bincode::ErrorKind>> for QmqError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        Self::BincodeError(e)
    }
}
impl From<WriteError> for QmqError {
    fn from(e: WriteError) -> Self {
        Self::WriteError(e)
    }
}
impl From<ReadToEndError> for QmqError {
    fn from(e: ReadToEndError) -> Self {
        Self::ReadToEndError(e)
    }
}
impl From<RcgenError> for QmqError {
    fn from(e: RcgenError) -> Self {
        Self::RcgenError(e)
    }
}
impl From<rustls::Error> for QmqError {
    fn from(e: rustls::Error) -> Self {
        Self::Rustls(e)
    }
}
impl From<io::Error> for QmqError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}
