use bytes::Bytes;

use crate::codecs::CodecError;

#[derive(Debug)]
pub enum Error {
    ConfusedFrame,
    IncompleteErrorFrame,
    UnmatchedReply,
    RecvError,
    SendError,
    Codec(CodecError),
    Serde(amp_serde::Error),
    Remote { code: Bytes, description: Bytes },
    IO(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<CodecError> for Error {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_error: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::RecvError
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_error: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::SendError
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::IO(error)
    }
}

impl From<amp_serde::Error> for Error {
    fn from(error: amp_serde::Error) -> Self {
        Self::Serde(error)
    }
}
