use std::fmt::{self, Display};
use std::marker::PhantomData;

use bytes::Bytes;
use serde::{
    de::{SeqAccess, Visitor},
    ser::SerializeTupleVariant,
    Deserialize, Deserializer, Serialize, Serializer,
};

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

impl<R> From<Response<R>> for std::result::Result<OkResponse<R>, ErrorResponse> {
    fn from(value: Response<R>) -> std::result::Result<OkResponse<R>, ErrorResponse> {
        match value {
            Response::Ok(v) => Ok(v),
            Response::Err(e) => Err(e),
        }
    }
}

impl<R> From<std::result::Result<OkResponse<R>, ErrorResponse>> for Response<R> {
    fn from(value: std::result::Result<OkResponse<R>, ErrorResponse>) -> Response<R> {
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
    // Serialization errors
    IO(std::io::Error),
    KeyTooLong,
    EmptyKey,
    ValueTooLong,

    // Deserialization errors
    ExpectedBool,
    RemainingBytes,
    ExpectedInteger,
    ExpectedFloat,
    ExpectedUtf8,
    ExpectedChar,
    ExpectedMapKey,
    ExpectedMapValue,
    ExpectedSeqLength,
    ExpectedSeqValue,

    Custom(String),
    Unsupported,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Error
    where
        T: Display,
    {
        Error::Custom(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Error
    where
        T: Display,
    {
        Error::Custom(msg.to_string())
    }
}

impl std::error::Error for Error {}

pub struct AmpList<I>(pub Vec<I>);

impl<I> Serialize for AmpList<I>
where
    I: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
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
            s.serialize_field(item)?;
        }
        s.end()
    }
}

impl<'de, I> Deserialize<'de> for AmpList<I>
where
    I: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ListVisitor<I>(PhantomData<I>);
        impl<'de, I> Visitor<'de> for ListVisitor<I>
        where
            I: Deserialize<'de>,
        {
            type Value = Vec<I>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a list of dictionaries")
            }

            fn visit_seq<A>(self, mut access: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut out = Vec::new();

                while let Some(item) = access.next_element::<I>()? {
                    out.push(item)
                }

                Ok(out)
            }
        }
        Ok(AmpList(deserializer.deserialize_tuple_struct(
            crate::AMP_LIST_COOKIE,
            0,
            ListVisitor(PhantomData),
        )?))
    }
}

#[cfg(test)]
mod test {
    use crate::{from_bytes, to_bytes, AmpList, Error};
    use serde::{Deserialize, Serialize};

    const LIST_ENC: [u8; 42] = [
        0, 1, 97, 0, 1, 49, 0, 1, 98, 0, 1, 50, 0, 0, 0, 1, 97, 0, 1, 51, 0, 1, 98, 0, 1, 52, 0, 0,
        0, 1, 97, 0, 1, 53, 0, 1, 98, 0, 1, 54, 0, 0,
    ];

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct AB {
        a: u32,
        b: u64,
    }

    #[test]
    fn amp_list_enc() {
        let list = AmpList(vec![
            AB { a: 1, b: 2 },
            AB { a: 3, b: 4 },
            AB { a: 5, b: 6 },
        ]);
        let bytes = to_bytes(list).unwrap();
        assert_eq!(bytes, LIST_ENC.as_ref());
    }

    #[test]
    fn amp_list_dec() {
        let list: AmpList<AB> = from_bytes(&LIST_ENC).unwrap();

        assert_eq!(
            list.0,
            vec![AB { a: 1, b: 2 }, AB { a: 3, b: 4 }, AB { a: 5, b: 6 }]
        );
    }

    #[test]
    fn trailling_dicts() {
        match from_bytes::<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>(&LIST_ENC) {
            Err(Error::RemainingBytes) => (),
            _ => unreachable!(),
        }
    }
}
