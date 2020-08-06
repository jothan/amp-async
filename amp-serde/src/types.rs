use std::fmt::Display;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Request<Q> {
    #[serde(rename = "_ask", skip_serializing_if = "Option::is_none")]
    pub tag: Option<Bytes>,
    #[serde(rename = "_command")]
    pub command: String,
    #[serde(flatten)]
    pub fields: Q,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OkResponse<R> {
    #[serde(rename = "_answer")]
    pub tag: Bytes,
    #[serde(flatten)]
    pub fields: R,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ErrorResponse {
    #[serde(rename = "_error")]
    pub tag: Bytes,
    #[serde(rename = "_error_code")]
    pub code: String,
    #[serde(rename = "_error_description")]
    pub description: String,
}

impl<R> From<Response<R>> for Result<OkResponse<R>, ErrorResponse> {
    fn from(value: Response<R>) -> Result<OkResponse<R>, ErrorResponse> {
        match value {
            Response::Ok(v) => Ok(v),
            Response::Err(e) => Err(e),
        }
    }
}

impl<R> From<Result<OkResponse<R>, ErrorResponse>> for Response<R> {
    fn from(value: Result<OkResponse<R>, ErrorResponse>) -> Response<R> {
        match value {
            Ok(v) => Response::Ok(v),
            Err(e) => Response::Err(e),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum Response<F> {
    Ok(OkResponse<F>),
    Err(ErrorResponse),
}

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    KeyTooLong,
    EmptyKey,
    ValueTooLong,
    Serde(String),
    Unsupported,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Error
    where
        T: Display,
    {
        Error::Serde(msg.to_string())
    }
}

impl std::error::Error for Error {}
