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
    Remote { code: Bytes, description: Bytes },
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
