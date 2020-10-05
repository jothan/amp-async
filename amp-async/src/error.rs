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
    Remote(RemoteError),
    IO(std::io::Error),
    InvalidUtf8(std::str::Utf8Error),
}

#[derive(Clone, Debug)]
pub struct RemoteError {
    pub(crate) code: String,
    pub(crate) description: String,
}

impl RemoteError {
    pub fn new<C, D>(code: Option<C>, description: Option<D>) -> RemoteError
    where
        C: Into<String>,
        D: Into<String>,
    {
        RemoteError {
            code: code.map(Into::into).unwrap_or_else(|| "UNKNOWN".into()),
            description: description.map(Into::into).unwrap_or_else(|| "".into()),
        }
    }
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

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(error)
    }
}
