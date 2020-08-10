use std::convert::TryInto;
use std::fmt::Display;
use std::str::FromStr;

use crate::AMP_LENGTH_SIZE;

use serde::{
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize,
};

#[derive(Debug)]
pub enum Error {
    ExpectedBool,
    Custom(String),
    RemainingBytes,
    ExpectedInteger,
    ExpectedFloat,
    ExpectedUtf8,
    ExpectedChar,
    ExpectedMapKey,
    ExpectedMapValue,
    ExpectedSeqLength,
    ExpectedSeqValue,
    Unsupported,
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

impl Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    fn parse_int<I: FromStr>(&self) -> Result<I, Error> {
        std::str::from_utf8(self.input)
            .ok()
            .and_then(|v| v.parse().ok())
            .ok_or(Error::ExpectedInteger)
    }
}

impl<'de, 'a> serde::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Sadly, only possible sane behavior.
        self.deserialize_bytes(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::Unsupported)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.eq_ignore_ascii_case(b"true") {
            visitor.visit_bool(true)
        } else if self.input.eq_ignore_ascii_case(b"false") {
            visitor.visit_bool(false)
        } else {
            Err(Error::ExpectedBool)
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.parse_int()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_int()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_int()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_int()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_int()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_int()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_int()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_int()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_f64(visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.eq_ignore_ascii_case(b"nan") {
            visitor.visit_f64(f64::NAN)
        } else if self.input.eq_ignore_ascii_case(b"inf") {
            visitor.visit_f64(f64::INFINITY)
        } else if self.input.eq_ignore_ascii_case(b"-inf") {
            visitor.visit_f64(f64::NEG_INFINITY)
        } else {
            visitor.visit_f64(
                std::str::from_utf8(self.input)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or(Error::ExpectedFloat)?,
            )
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor
            .visit_borrowed_str(std::str::from_utf8(self.input).map_err(|_| Error::ExpectedUtf8)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor
            .visit_borrowed_str(std::str::from_utf8(self.input).map_err(|_| Error::ExpectedUtf8)?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_bytes(self.input)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_bytes(self.input)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.is_empty() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let s = std::str::from_utf8(self.input).map_err(|_| Error::ExpectedUtf8)?;

        let mut i = s.chars();
        let c = match i.next() {
            Some(c) => c,
            None => return Err(Error::ExpectedChar),
        };

        if i.next().is_none() {
            visitor.visit_char(c)
        } else {
            Err(Error::ExpectedChar)
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }
}

impl<'de, 'a> SeqAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedSeqLength);
        }
        let length: usize =
            u16::from_be_bytes(self.input[0..AMP_LENGTH_SIZE].try_into().unwrap()).into();

        self.input = &self.input[AMP_LENGTH_SIZE..];

        if self.input == b"" {
            Ok(None)
        } else if self.input.len() >= length {
            let (value, rest) = self.input.split_at(length);
            self.input = rest;

            let mut sub = Deserializer { input: value };
            seed.deserialize(&mut sub).map(Some)
        } else {
            Err(Error::ExpectedSeqValue)
        }
    }
}

impl<'de, 'a> MapAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.input == [0, 0] {
            self.input = b"";
            return Ok(None);
        } else if self.input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedMapKey);
        }

        let length: usize =
            u16::from_be_bytes(self.input[0..AMP_LENGTH_SIZE].try_into().unwrap()).into();

        if length > crate::AMP_KEY_LIMIT {
            return Err(Error::ExpectedMapKey);
        }
        self.input = &self.input[AMP_LENGTH_SIZE..];

        if self.input.len() >= length {
            let (key, rest) = self.input.split_at(length);
            self.input = rest;

            let mut sub = Deserializer { input: key };
            seed.deserialize(&mut sub).map(Some)
        } else {
            Err(Error::ExpectedMapKey)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if self.input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedMapValue);
        }
        let length: usize =
            u16::from_be_bytes(self.input[0..AMP_LENGTH_SIZE].try_into().unwrap()).into();

        self.input = &self.input[AMP_LENGTH_SIZE..];

        if self.input.len() >= length {
            let (value, rest) = self.input.split_at(length);
            self.input = rest;

            let mut sub = Deserializer { input: value };
            seed.deserialize(&mut sub)
        } else {
            Err(Error::ExpectedMapValue)
        }
    }
}

pub fn from_bytes<'a, T>(s: &'a [u8]) -> Result<T, Error>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        // FIXME
        Err(Error::RemainingBytes)
    }
}