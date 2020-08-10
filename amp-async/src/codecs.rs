use std::convert::TryInto;

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

pub(crate) const AMP_KEY_LIMIT: usize = 0xff;
const LENGTH_SIZE: usize = std::mem::size_of::<u16>();

#[derive(Debug, Default, PartialEq)]
pub struct Dec<D = Vec<(Bytes, Bytes)>> {
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

impl<D> Dec<D>
where
    D: Default,
{
    pub fn new() -> Self {
        Default::default()
    }

    fn read_key(length: usize, buf: &mut BytesMut) -> Result<Option<Bytes>, CodecError> {
        if length > AMP_KEY_LIMIT {
            return Err(CodecError::KeyTooLong);
        }

        Ok(Self::read_delimited(length, buf))
    }

    fn read_delimited(length: usize, buf: &mut BytesMut) -> Option<Bytes> {
        if buf.len() >= length + LENGTH_SIZE {
            buf.advance(LENGTH_SIZE);
            Some(buf.split_to(length).freeze())
        } else {
            None
        }
    }
}

impl<D> Decoder for Dec<D>
where
    D: Default + Extend<(Bytes, Bytes)>,
{
    type Error = CodecError;
    type Item = D;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if buf.len() < LENGTH_SIZE {
                return Ok(None);
            }

            let (length_bytes, _) = buf.split_at(LENGTH_SIZE);
            let length = usize::from(u16::from_be_bytes(length_bytes.try_into().unwrap()));

            match self.state {
                State::Key => {
                    if length == 0 {
                        buf.advance(LENGTH_SIZE);
                        return Ok(Some(std::mem::take(&mut self.frame)));
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
                        let key = std::mem::take(&mut self.key);
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

#[derive(Debug)]
pub enum CodecError {
    IO(std::io::Error),
    KeyTooLong,
}

impl From<std::io::Error> for CodecError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod test {
    use amp_serde::Request;
    use bytes::BytesMut;
    use serde::Serialize;
    use tokio_util::codec::Decoder as _;

    use crate::*;

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
        let mut dec = Decoder::<Vec<_>>::new();
        let mut buf = BytesMut::new();
        buf.extend(WWW_EXAMPLE);

        let frame = dec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(
            frame
                .iter()
                .map(|(k, v)| (k.as_ref(), v.as_ref()))
                .collect::<Vec<_>>(),
            WWW_EXAMPLE_DEC
        );
        assert_eq!(buf.len(), 0);
        assert_eq!(dec, Decoder::<Vec<_>>::new());
    }

    #[test]
    fn encode_example() {
        #[derive(Serialize)]
        struct Sum {
            a: u32,
            b: u32,
        }
        let fields = Sum { a: 13, b: 81 };

        let buf = amp_serde::to_bytes(Request {
            command: "Sum".into(),
            tag: Some(b"23".as_ref().into()),
            fields,
        })
        .unwrap();

        assert_eq!(buf, WWW_EXAMPLE);
    }
}
