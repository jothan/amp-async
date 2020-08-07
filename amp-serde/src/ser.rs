use std::convert::TryFrom;
use std::io::Write;

use bytes::BufMut;
use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct,
};
use serde::Serialize;

use crate::Error;

pub(crate) const AMP_KEY_LIMIT: usize = 0xff;
pub(crate) const AMP_VALUE_LIMIT: usize = 0xffff;

#[derive(Debug, Default)]
pub struct Serializer(Vec<u8>);

pub struct Compound<'a> {
    ser: &'a mut Serializer,
}

impl Compound<'_> {
    fn new(ser: &mut Serializer) -> Compound<'_> {
        Compound { ser }
    }
}

impl<'a> SerializeSeq for Compound<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push_value(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeTuple for Compound<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push_value(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeTupleStruct for Compound<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push_value(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeMap for Compound<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.push_key(key)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push_value(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.ser.0.put_u16(0);
        Ok(())
    }
}

impl<'a> SerializeStruct for Compound<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.push_key(key)?;
        self.push_value(value)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.ser.0.put_u16(0);
        Ok(())
    }
}

impl Compound<'_> {
    fn push_value<T: Serialize + ?Sized>(&mut self, input: &T) -> Result<(), Error> {
        let length_offset = self.ser.prep_len();
        input.serialize(&mut *self.ser)?;
        self.ser.write_len(length_offset, false)
    }

    fn push_key<T: Serialize + ?Sized>(&mut self, input: &T) -> Result<(), Error> {
        let length_offset = self.ser.prep_len();
        input.serialize(&mut *self.ser)?;
        self.ser.write_len(length_offset, true)
    }
}

impl Serializer {
    fn prep_len(&mut self) -> usize {
        let length_offset = self.0.len();

        // Dummy value
        self.0.put_u16(0x55aa);
        length_offset
    }

    fn write_len(&mut self, length_offset: usize, key: bool) -> Result<(), Error> {
        assert!(self.0.len() >= length_offset + 2);
        let length = self.0.len() - length_offset - 2;

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
        self.0[length_offset..length_offset + 2].copy_from_slice(length.as_ref());

        Ok(())
    }
}

impl From<Serializer> for Vec<u8> {
    fn from(input: Serializer) -> Vec<u8> {
        input.0
    }
}

impl<'a> serde::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        if v {
            self.0.extend_from_slice(b"True");
        } else {
            self.0.extend_from_slice(b"False");
        }
        Ok(())
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
        write!(self.0, "{}", v)?;
        Ok(())
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
        write!(self.0, "{}", v)?;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v.into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_nan() {
            self.0.extend_from_slice(b"nan");
        } else if v.is_infinite() {
            if v.is_sign_positive() {
                self.0.extend_from_slice(b"inf");
            } else {
                self.0.extend_from_slice(b"-inf");
            }
        } else {
            write!(self.0, "{}", v)?;
        }

        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        // A char encoded as UTF-8 takes 4 bytes at most.
        let mut buf = [0; 4];
        self.serialize_str(v.encode_utf8(&mut buf))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.extend_from_slice(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.extend_from_slice(value);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + serde::Serialize>(self, v: &T) -> Result<Self::Ok, Self::Error> {
        v.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
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
        Ok(Compound::new(self))
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
        Ok(Compound::new(self))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(Compound::new(self))
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
    let mut serializer: Serializer = Default::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.into())
}
