use std::fmt::Display;

use bytes::Bytes;
use serde::{ser::SerializeTupleVariant, Deserialize, Serialize, Serializer};

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
pub enum Response<R> {
    Ok(OkResponse<R>),
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

pub struct AmpList<L>(pub L);

impl<L, I> Serialize for AmpList<L>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    I: Serialize + ?Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_tuple_variant(
            crate::AMP_LIST_COOKIE,
            0,
            "shaken, not stirred",
            0,
        )?;

        for item in &self.0 {
            s.serialize_field(&item)?;
        }
        s.end()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn amp_list() {
        #[derive(serde::Serialize)]
        struct AB {
            a: u32,
            b: u64,
        }
        let list = crate::AmpList(vec![
            AB { a: 1, b: 2 },
            AB { a: 3, b: 4 },
            AB { a: 5, b: 6 },
        ]);
        let bytes = crate::to_bytes(list).unwrap();
        assert_eq!(
            bytes,
            [
                0, 1, 97, 0, 1, 49, 0, 1, 98, 0, 1, 50, 0, 0, 0, 1, 97, 0, 1, 51, 0, 1, 98, 0, 1,
                52, 0, 0, 0, 1, 97, 0, 1, 53, 0, 1, 98, 0, 1, 54, 0, 0
            ]
            .as_ref()
        );
    }
}
