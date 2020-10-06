use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};

pub(crate) const AMP_KEY_LIMIT: usize = 0xff;
pub(crate) const AMP_VALUE_LIMIT: usize = 0xffff;

#[derive(Debug)]
pub struct Dec<D = Vec<(Bytes, Bytes)>> {
    state: State,
    key: Bytes,
    value: BytesMut,
    frame: D,
    decoder: LengthDelimitedCodec,
    version2: bool,
}

#[derive(Debug, PartialEq)]
enum State {
    Key,
    Value,
    ValueCont,
}

impl Default for State {
    fn default() -> Self {
        State::Key
    }
}

impl<D: Default> Default for Dec<D> {
    fn default() -> Self {
        Dec {
            decoder: LengthDelimitedCodec::builder()
                .big_endian()
                .length_field_length(2)
                .max_frame_length(AMP_KEY_LIMIT)
                .new_codec(),
            key: Default::default(),
            value: Default::default(),
            frame: Default::default(),
            state: Default::default(),
            version2: false,
        }
    }
}

impl<D> Dec<D>
where
    D: Default + Extend<(Bytes, Bytes)>,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_v2() -> Self {
        let mut d = Dec::new();
        d.version2 = true;
        d
    }

    fn handle_value(&mut self, segment: BytesMut) {
        if self.version2 && segment.len() == AMP_VALUE_LIMIT {
            self.value = segment;
            self.state = State::ValueCont;
        } else {
            let key = std::mem::take(&mut self.key);
            self.frame.extend(std::iter::once((key, segment.freeze())));
            self.state = State::Key;
            self.decoder.set_max_frame_length(AMP_KEY_LIMIT);
        }
    }

    fn handle_valuecont(&mut self, segment: BytesMut) {
        self.value.extend_from_slice(&segment);

        if segment.len() != AMP_VALUE_LIMIT {
            let key = std::mem::take(&mut self.key);
            let value = std::mem::take(&mut self.value);
            self.frame.extend(std::iter::once((key, value.freeze())));
            self.state = State::Key;
            self.decoder.set_max_frame_length(AMP_KEY_LIMIT);
        }
    }
}

impl<D> Decoder for Dec<D>
where
    D: Default + Extend<(Bytes, Bytes)>,
{
    type Error = std::io::Error;
    type Item = D;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let segment = match self.decoder.decode(buf)? {
                Some(s) => s,
                None => return Ok(None),
            };

            match self.state {
                State::Key => {
                    if segment.is_empty() {
                        break Ok(Some(std::mem::take(&mut self.frame)));
                    } else {
                        self.key = segment.freeze();
                        self.state = State::Value;
                        self.decoder.set_max_frame_length(AMP_VALUE_LIMIT);
                    }
                }
                State::Value => self.handle_value(segment),
                State::ValueCont => self.handle_valuecont(segment),
            }
        }
    }
}

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
