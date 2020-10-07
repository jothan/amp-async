use std::marker::PhantomData;
use std::str::FromStr;

use bytes::{Buf, Bytes};
use serde::{
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize,
};

use crate::{Error, Result, AMP_LENGTH_SIZE, AMP_VALUE_LIMIT, V1, V2};

struct AmpListHandler<'a, V>(&'a mut Deserializer<V>);

pub struct Deserializer<V> {
    input: Bytes,
    marker: PhantomData<V>,
}

pub trait AmpDecoder {
    fn read_map_value(input: &mut Bytes) -> Result<Bytes>;
}

impl AmpDecoder for V1 {
    fn read_map_value(input: &mut Bytes) -> Result<Bytes> {
        if input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedMapValue);
        }
        let length: usize = input.get_u16().into();

        if input.len() < length {
            return Err(Error::ExpectedMapValue);
        }

        Ok(input.split_to(length))
    }
}
impl AmpDecoder for V2 {
    fn read_map_value(input: &mut Bytes) -> Result<Bytes> {
        let mut done = false;
        let mut value = Vec::new();

        while !done {
            let segment = V1::read_map_value(&mut *input)?;
            value.extend_from_slice(&segment);
            done = segment.len() != AMP_VALUE_LIMIT;
        }

        Ok(value.into())
    }
}

impl<'de, V> Deserializer<V> {
    pub fn from_bytes(input: Bytes) -> Self {
        Deserializer {
            input,
            marker: PhantomData,
        }
    }

    fn parse_int<I: FromStr>(&mut self) -> Result<I> {
        std::str::from_utf8(&self.input)
            .ok()
            .and_then(|v| v.parse().ok())
            .ok_or(Error::ExpectedInteger)
            .map(|v| {
                self.input.clear();
                v
            })
    }
}

impl<'de, 'a, V: AmpDecoder> serde::Deserializer<'de> for &'a mut Deserializer<V> {
    type Error = Error;

    fn deserialize_any<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        // Sadly, only possible sane behavior.
        self.deserialize_bytes(visitor)
    }

    fn deserialize_ignored_any<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<T>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: T,
    ) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        Err(Error::Unsupported)
    }

    fn deserialize_newtype_struct<T>(self, _name: &'static str, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_unit_struct<T>(self, _name: &'static str, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_bool<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        if self.input.eq_ignore_ascii_case(b"true") {
            self.input.clear();
            visitor.visit_bool(true)
        } else if self.input.eq_ignore_ascii_case(b"false") {
            self.input.clear();
            visitor.visit_bool(false)
        } else {
            Err(Error::ExpectedBool)
        }
    }

    fn deserialize_i8<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_i8(self.parse_int()?)
    }

    fn deserialize_i16<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_int()?)
    }

    fn deserialize_i32<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_int()?)
    }

    fn deserialize_i64<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_int()?)
    }

    fn deserialize_u8<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_int()?)
    }

    fn deserialize_u16<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_int()?)
    }

    fn deserialize_u32<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_int()?)
    }

    fn deserialize_u64<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_int()?)
    }

    fn deserialize_f32<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        self.deserialize_f64(visitor)
    }

    fn deserialize_f64<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        let res = if self.input.eq_ignore_ascii_case(b"nan") {
            visitor.visit_f64(f64::NAN)
        } else if self.input.eq_ignore_ascii_case(b"inf") {
            visitor.visit_f64(f64::INFINITY)
        } else if self.input.eq_ignore_ascii_case(b"-inf") {
            visitor.visit_f64(f64::NEG_INFINITY)
        } else {
            visitor.visit_f64::<Error>(
                std::str::from_utf8(&self.input)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or(Error::ExpectedFloat)?,
            )
        }?;

        self.input.clear();
        Ok(res)
    }

    fn deserialize_str<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor
            .visit_str(std::str::from_utf8(&self.input).map_err(|_| Error::ExpectedUtf8)?)
            .map(|v| {
                self.input.clear();
                v
            })
    }

    fn deserialize_string<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_bytes(&self.input).map(|v| {
            self.input.clear();
            v
        })
    }

    fn deserialize_byte_buf<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_unit<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_option<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        if self.input.is_empty() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_seq<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<T>(self, _len: usize, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<T>(
        self,
        name: &'static str,
        _len: usize,
        visitor: T,
    ) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        // Ugly hack for AmpList
        if name == crate::AMP_LIST_COOKIE {
            visitor.visit_seq(AmpListHandler(self))
        } else {
            visitor.visit_seq(self)
        }
    }

    fn deserialize_map<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<T>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: T,
    ) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_char<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        let s = std::str::from_utf8(&self.input).map_err(|_| Error::ExpectedUtf8)?;

        let mut i = s.chars();
        let c = match i.next() {
            Some(c) => c,
            None => return Err(Error::ExpectedChar),
        };

        if i.next().is_none() {
            visitor.visit_char(c).map(|v| {
                self.input.clear();
                v
            })
        } else {
            Err(Error::ExpectedChar)
        }
    }

    fn deserialize_identifier<T>(self, visitor: T) -> Result<T::Value>
    where
        T: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }
}

impl<'de, 'a, V: AmpDecoder> SeqAccess<'de> for &'a mut Deserializer<V> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedSeqLength);
        }
        let length: usize = self.input.get_u16().into();

        if self.input.is_empty() {
            Ok(None)
        } else if self.input.len() >= length {
            let mut sub = Deserializer::<V>::from_bytes(self.input.split_to(length));
            let res = seed.deserialize(&mut sub).map(Some);
            if !sub.input.is_empty() {
                return Err(Error::RemainingBytes);
            }
            res
        } else {
            Err(Error::ExpectedSeqValue)
        }
    }
}

impl<'de, 'a, V: AmpDecoder> MapAccess<'de> for &'a mut Deserializer<V> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if self.input.starts_with(&[0, 0]) {
            self.input.advance(AMP_LENGTH_SIZE);
            return Ok(None);
        } else if self.input.len() < AMP_LENGTH_SIZE {
            return Err(Error::ExpectedMapKey);
        }

        let length: usize = self.input.get_u16().into();

        if length > crate::AMP_KEY_LIMIT {
            return Err(Error::ExpectedMapKey);
        }

        if self.input.len() >= length {
            let mut sub = Deserializer::<V>::from_bytes(self.input.split_to(length));
            let res = seed.deserialize(&mut sub).map(Some);
            if !sub.input.is_empty() {
                return Err(Error::RemainingBytes);
            }
            res
        } else {
            Err(Error::ExpectedMapKey)
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        let value = V::read_map_value(&mut self.input)?;
        let mut sub = Deserializer::<V>::from_bytes(value);
        let res = seed.deserialize(&mut sub)?;

        if sub.input.is_empty() {
            Ok(res)
        } else {
            Err(Error::RemainingBytes)
        }
    }
}

impl<'de, 'a, V: AmpDecoder> SeqAccess<'de> for AmpListHandler<'a, V> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.0.input.is_empty() {
            Ok(None)
        } else {
            seed.deserialize(&mut *self.0).map(Some)
        }
    }
}

pub fn from_bytes<'a, V: AmpDecoder, B: Into<Bytes>, T>(s: B) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::<V>::from_bytes(s.into());
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::RemainingBytes)
    }
}
