use crate::codecs::CodecError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Ambiguous frame type")]
    ConfusedFrame,
    #[error("This error frame is missing the code or description")]
    IncompleteErrorFrame,
    #[error("Received a reply to a non-existent request")]
    UnmatchedReply,
    #[error("Internal channel error")]
    InternalError,
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("Serde error: {0}")]
    Serde(#[from] amp_serde::Error),
    #[error("Remote error: {0}")]
    Remote(RemoteError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

#[derive(thiserror::Error, Clone, Debug)]
#[error("{code:?}: {description:?}")]
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

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_error: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::InternalError
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_error: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::InternalError
    }
}
