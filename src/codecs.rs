use std::convert::TryInto;

use bytes::{Bytes, BytesMut};
use tokio::codec::Decoder;

type AmpBox = Vec<(Bytes, Bytes)>;

#[derive(Debug, Default, PartialEq)]
pub struct AmpCodec {
    state: State,
    key: Bytes,
    frame: AmpBox,
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

impl AmpCodec {
    pub fn new() -> Self {
        Default::default()
    }

    fn read_key(length: usize, buf: &mut BytesMut) -> Result<Option<Bytes>, AmpError> {
        if length >= 256 {
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

impl Decoder for AmpCodec {
    type Error = AmpError;
    type Item = AmpBox;

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
                        return Ok(Some(std::mem::replace(&mut self.frame, Vec::new())));
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
                        self.frame.push((key, value));
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
pub enum AmpError {
    IO(std::io::Error),
    KeyTooLong,
}

impl From<std::io::Error> for AmpError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

#[cfg(test)]
mod test {
    use crate::*;
    use bytes::BytesMut;
    use tokio::codec::Decoder;

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
    fn www_example() {
        let mut codec = AmpCodec::new();
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
}
