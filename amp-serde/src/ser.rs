use std::convert::TryInto;

use bytes::BufMut;
use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct,
};
use serde::Serialize;

use crate::Error;

pub(crate) const AMP_KEY_LIMIT: usize = 0xff;
pub(crate) const AMP_VALUE_LIMIT: usize = 0xffff;

#[derive(Debug)]
pub struct Serializer;

pub struct BufferSerializer<'a>(&'a Serializer, Vec<u8>);

impl<'a> SerializeSeq for BufferSerializer<'a> {
    type Ok = Vec<u8>;
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        Self::push_value(&value.serialize(self.0)?, &mut self.1)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.1)
    }
}

impl<'a> SerializeTuple for BufferSerializer<'a> {
    type Ok = Vec<u8>;
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        Self::push_value(&value.serialize(self.0)?, &mut self.1)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.1)
    }
}

impl<'a> SerializeTupleStruct for BufferSerializer<'a> {
    type Ok = Vec<u8>;
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        Self::push_value(&value.serialize(self.0)?, &mut self.1)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.1)
    }
}

impl<'a> SerializeMap for BufferSerializer<'a> {
    type Ok = Vec<u8>;
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        Self::push_key(&key.serialize(self.0)?, &mut self.1)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        Self::push_value(&value.serialize(self.0)?, &mut self.1)
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.1.put_u16(0);
        Ok(self.1)
    }
}

impl<'a> SerializeStruct for BufferSerializer<'a> {
    type Ok = Vec<u8>;
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        Self::push_key(&key.serialize(self.0)?, &mut self.1)?;
        Self::push_value(&value.serialize(self.0)?, &mut self.1)?;
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.1.put_u16(0);
        Ok(self.1)
    }
}

impl BufferSerializer<'_> {
    fn push_value(input: &[u8], output: &mut Vec<u8>) -> Result<(), Error> {
        if input.len() > AMP_VALUE_LIMIT {
            return Err(Error::ValueTooLong);
        }

        output.put_u16(input.len().try_into().unwrap());
        output.extend_from_slice(input);
        Ok(())
    }

    fn push_key(input: &[u8], output: &mut Vec<u8>) -> Result<(), Error> {
        if input.is_empty() {
            return Err(Error::EmptyKey);
        }
        if input.len() > AMP_KEY_LIMIT {
            return Err(Error::KeyTooLong);
        }

        output.put_u16(input.len().try_into().unwrap());
        output.extend_from_slice(input);
        Ok(())
    }
}

impl<'a> serde::Serializer for &'a Serializer {
    type Ok = Vec<u8>;
    type Error = Error;

    type SerializeSeq = BufferSerializer<'a>;
    type SerializeTuple = BufferSerializer<'a>;
    type SerializeTupleStruct = BufferSerializer<'a>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = BufferSerializer<'a>;
    type SerializeStruct = BufferSerializer<'a>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        if v {
            Ok(b"True".to_vec())
        } else {
            Ok(b"False".to_vec())
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v).into())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v).into())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v).into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v).into())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        // A char encoded as UTF-8 takes 4 bytes at most.
        let mut buf = [0; 4];
        self.serialize_str(v.encode_utf8(&mut buf))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.as_bytes().into())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(value.into())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_some<T: ?Sized + serde::Serialize>(self, v: &T) -> Result<Self::Ok, Self::Error> {
        v.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _idx: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Ok(BufferSerializer(self, Vec::new()))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Unsupported)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Ok(BufferSerializer(self, Vec::new()))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(BufferSerializer(self, Vec::new()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _id: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Unsupported)
    }
}

pub fn to_bytes<T: Serialize>(value: T) -> Result<Vec<u8>, Error> {
    value.serialize(&Serializer)
}
