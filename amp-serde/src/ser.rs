use std::convert::TryFrom;
use std::io::{self, Write};
use std::marker::PhantomData;

const INITIAL_CAPACITY: usize = 256;

use bytes::BufMut;
use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct,
    SerializeTupleVariant,
};
use serde::Serialize;

use crate::{Error, Result, AMP_KEY_LIMIT, AMP_LENGTH_SIZE, AMP_VALUE_LIMIT, V1, V2};

#[derive(Debug)]
pub struct Serializer<V>(Vec<u8>, PhantomData<V>);

impl<V> Default for Serializer<V> {
    fn default() -> Serializer<V> {
        Serializer(Vec::with_capacity(INITIAL_CAPACITY), PhantomData)
    }
}

#[doc(hidden)]
pub struct Compound<'a, V> {
    ser: &'a mut Serializer<V>,
}

impl<'a, V> Compound<'a, V> {
    fn new(ser: &'a mut Serializer<V>) -> Compound<'a, V> {
        Compound { ser }
    }
}

impl<'a, V: AmpEncoder> SerializeSeq for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.ser.push_value(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, V: AmpEncoder> SerializeTuple for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.ser.push_value(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, V: AmpEncoder> SerializeTupleStruct for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.ser.push_value(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, V: AmpEncoder> SerializeTupleVariant for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        // Encode with no separator
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, V: AmpEncoder> SerializeMap for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.ser.push_key(key)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        V::push_long_value(&mut self.ser, value)
    }

    fn end(self) -> Result<Self::Ok> {
        self.ser.0.put_u16(0);
        Ok(())
    }
}

impl<'a, V: AmpEncoder> SerializeStruct for Compound<'a, V> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.ser.push_key(key)?;
        V::push_long_value(&mut self.ser, value)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        self.ser.0.put_u16(0);
        Ok(())
    }
}

pub trait AmpEncoder: Sized {
    fn push_long_value<T: Serialize + ?Sized>(ser: &mut Serializer<Self>, input: &T) -> Result<()>;
}

impl AmpEncoder for V1 {
    fn push_long_value<T: Serialize + ?Sized>(ser: &mut Serializer<Self>, input: &T) -> Result<()> {
        ser.push_value(input)
    }
}

impl AmpEncoder for V2 {
    fn push_long_value<T: Serialize + ?Sized>(ser: &mut Serializer<Self>, input: &T) -> Result<()> {
        // Allocate a temporary buffer. Somewhat less efficient than
        // recursive position tracking, but easy to get right for now.
        let mut subser = Serializer::<V2>::default();
        input.serialize(&mut subser)?;

        let value = Vec::from(subser);
        if value.is_empty() {
            ser.push_bytes(b"\x00\x00");
            return Ok(());
        }

        for chunk in value.chunks(AMP_VALUE_LIMIT) {
            let length = u16::try_from(chunk.len()).unwrap();
            ser.push_bytes(length.to_be_bytes().as_ref());
            ser.push_bytes(chunk);
        }

        Ok(())
    }
}

impl<V: AmpEncoder> Serializer<V> {
    fn push_bytes(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes)
    }

    fn push_value<T: Serialize + ?Sized>(&mut self, input: &T) -> Result<()> {
        let length_offset = self.prep_len();
        input.serialize(&mut *self)?;
        self.write_len(length_offset, false)
    }

    fn prep_len(&mut self) -> usize {
        let length_offset = self.0.len();

        // Dummy value
        self.0.put_u16(0x55aa);
        length_offset
    }

    fn write_len(&mut self, length_offset: usize, key: bool) -> Result<()> {
        assert!(self.0.len() >= length_offset + AMP_LENGTH_SIZE);
        let length = self.0.len() - length_offset - AMP_LENGTH_SIZE;

        if key {
            if length == 0 {
                return Err(Error::EmptyKey);
            }
            if length > AMP_KEY_LIMIT {
                return Err(Error::KeyTooLong);
            }
        } else if length > AMP_VALUE_LIMIT {
            return Err(Error::ValueTooLong);
        }
        let length = u16::try_from(length).unwrap().to_be_bytes();
        self.0[length_offset..length_offset + AMP_LENGTH_SIZE].copy_from_slice(length.as_ref());

        Ok(())
    }

    fn push_key<T: Serialize + ?Sized>(&mut self, input: &T) -> Result<()> {
        let length_offset = self.prep_len();
        input.serialize(&mut *self)?;
        self.write_len(length_offset, true)
    }
}

impl<V> From<Serializer<V>> for Vec<u8> {
    fn from(input: Serializer<V>) -> Vec<u8> {
        input.0
    }
}

impl<'a, V: AmpEncoder> serde::Serializer for &'a mut Serializer<V> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Compound<'a, V>;
    type SerializeTuple = Compound<'a, V>;
    type SerializeTupleStruct = Compound<'a, V>;
    type SerializeTupleVariant = Compound<'a, V>;
    type SerializeMap = Compound<'a, V>;
    type SerializeStruct = Compound<'a, V>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        if v {
            self.push_bytes(b"True");
        } else {
            self.push_bytes(b"False");
        }
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        write!(self, "{}", v)?;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        write!(self, "{}", v)?;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        self.serialize_f64(v.into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        if v.is_nan() {
            self.push_bytes(b"nan");
        } else if v.is_infinite() {
            if v.is_sign_positive() {
                self.push_bytes(b"inf");
            } else {
                self.push_bytes(b"-inf");
            }
        } else {
            write!(self, "{}", v)?;
        }

        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        // A char encoded as UTF-8 takes 4 bytes at most.
        let mut buf = [0; 4];
        self.serialize_str(v.encode_utf8(&mut buf))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        self.push_bytes(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok> {
        self.push_bytes(value);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + serde::Serialize>(self, v: &T) -> Result<Self::Ok> {
        v.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _idx: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(Compound::new(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        // Ugly hack for the AmpList special case.
        if name == crate::AMP_LIST_COOKIE {
            Ok(Compound::new(self))
        } else {
            Err(Error::Unsupported)
        }
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(Compound::new(self))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(Compound::new(self))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _id: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Error::Unsupported)
    }

    fn is_human_readable(&self) -> bool {
        // Python abuses strings
        true
    }
}

impl<V> Write for Serializer<V>
where
    V: AmpEncoder,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn to_bytes<V: AmpEncoder, T: Serialize>(value: T) -> Result<Vec<u8>> {
    let mut serializer = Serializer::<V>::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.into())
}
