#[derive(Debug, Clone)]
pub enum Error {
    ConfusedFrame,
    IncompleteErrorFrame,
    UnmatchedReply,
    SendError,
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Error {}
