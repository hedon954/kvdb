use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not found {0}")]
    NotFound(String),

    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {0:?} to {1}")]
    ConvertCommand(String, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}. Error: {3}")]
    StorageError(&'static str, String, String, String),

    #[error("Failed to encode protobuf message: {0}")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Sled error: {0}")]
    SledError(#[from] sled::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Frame is large than max size")]
    FrameTooLarge,

    #[error("Failed to parse certificate: {0} {1}")]
    CertificateParseError(&'static str, &'static str),

    #[error("TLS error")]
    TlsError(#[from] tokio_rustls::rustls::TLSError),
}
