use std::convert::TryInto;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::codec::{Decoder, Encoder};

#[derive(Debug, Default, PartialEq)]
pub struct AmpCodec<D = Vec<(Bytes, Bytes)>> {
    state: State,
    key: Bytes,
    frame: D,
}

#[derive(Debug, PartialEq)]
enum State {
    Key,
    Value,
}

impl Default for State {
    fn default() -> Self {
        State::Key
    }
}

impl<D> AmpCodec<D>
where
    D: Default,
{
    pub fn new() -> Self {
        Default::default()
    }

    fn read_key(length: usize, buf: &mut BytesMut) -> Result<Option<Bytes>, AmpError> {
        if length > 255 {
            return Err(AmpError::KeyTooLong);
        }

        Ok(Self::read_delimited(length, buf))
    }

    fn read_delimited(length: usize, buf: &mut BytesMut) -> Option<Bytes> {
        if buf.len() >= length + LENGTH_SIZE {
            buf.split_to(LENGTH_SIZE);
            Some(buf.split_to(length).freeze())
        } else {
            None
        }
    }
}

const LENGTH_SIZE: usize = std::mem::size_of::<u16>();

impl<D> Decoder for AmpCodec<D>
where
    D: Default + Extend<(Bytes, Bytes)>,
{
    type Error = AmpError;
    type Item = D;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if buf.len() < 2 {
                return Ok(None);
            }

            let (length_bytes, _) = buf.split_at(LENGTH_SIZE);
            let length = usize::from(u16::from_be_bytes(length_bytes.try_into().unwrap()));

            match self.state {
                State::Key => {
                    if length == 0 {
                        buf.split_to(LENGTH_SIZE);
                        return Ok(Some(std::mem::replace(&mut self.frame, Default::default())));
                    } else {
                        match Self::read_key(length, buf)? {
                            Some(key) => {
                                self.key = key;
                                self.state = State::Value;
                            }
                            None => {
                                return Ok(None);
                            }
                        }
                    }
                }
                State::Value => match Self::read_delimited(length, buf) {
                    Some(value) => {
                        let key = std::mem::replace(&mut self.key, Bytes::new());
                        self.frame.extend(std::iter::once((key, value)));
                        self.state = State::Key;
                    }
                    None => {
                        return Ok(None);
                    }
                },
            }
        }
    }
}

impl<D, K, V> Encoder for AmpCodec<D>
where
    D: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    type Error = AmpError;
    type Item = D;

    fn encode(&mut self, item: D, dst: &mut BytesMut) -> Result<(), Self::Error> {
        for (key, value) in item {
            let key = key.as_ref();
            let value = value.as_ref();

            if key.is_empty() {
                return Err(AmpError::EmptyKey);
            }
            if key.len() > 255 {
                return Err(AmpError::KeyTooLong);
            }
            if value.len() > 0xffff {
                return Err(AmpError::ValueTooLong);
            }

            dst.reserve(LENGTH_SIZE * 2 + key.len() + value.len());
            dst.put_u16_be(key.len().try_into().unwrap());
            dst.extend(key);
            dst.put_u16_be(value.len().try_into().unwrap());
            dst.extend(value);
        }
        dst.reserve(LENGTH_SIZE);
        dst.put_u16_be(0);

        Ok(())
    }
}

#[derive(Debug)]
pub enum AmpError {
    IO(std::io::Error),
    KeyTooLong,
    EmptyKey,
    ValueTooLong,
}

impl From<std::io::Error> for AmpError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl std::fmt::Display for AmpError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for AmpError {}

#[cfg(test)]
mod test {
    use crate::*;
    use bytes::BytesMut;
    use tokio::codec::{Decoder, Encoder};

    const WWW_EXAMPLE: &[u8] = &[
        0x00, 0x04, 0x5F, 0x61, 0x73, 0x6B, 0x00, 0x02, 0x32, 0x33, 0x00, 0x08, 0x5F, 0x63, 0x6F,
        0x6D, 0x6D, 0x61, 0x6E, 0x64, 0x00, 0x03, 0x53, 0x75, 0x6D, 0x00, 0x01, 0x61, 0x00, 0x02,
        0x31, 0x33, 0x00, 0x01, 0x62, 0x00, 0x02, 0x38, 0x31, 0x00, 0x00,
    ];
    const WWW_EXAMPLE_DEC: &[(&[u8], &[u8])] = &[
        (b"_ask", b"23"),
        (b"_command", b"Sum"),
        (b"a", b"13"),
        (b"b", b"81"),
    ];

    #[test]
    fn decode_example() {
        let mut codec = AmpCodec::<Vec<_>>::new();
        let mut buf = BytesMut::new();
        buf.extend(WWW_EXAMPLE);

        let frame = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(
            frame
                .iter()
                .map(|(k, v)| (k.as_ref(), v.as_ref()))
                .collect::<Vec<_>>(),
            WWW_EXAMPLE_DEC
        );
        assert_eq!(buf.len(), 0);
        assert_eq!(codec, AmpCodec::new());
    }

    #[test]
    fn encode_example() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();

        codec.encode(WWW_EXAMPLE_DEC.to_vec(), &mut buf).unwrap();
        assert_eq!(buf, WWW_EXAMPLE);
    }
}
