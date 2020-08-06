use std::collections::HashMap;
use std::convert::TryFrom;

use bytes::Bytes;

use crate::Error;

pub type RawFrame = HashMap<Vec<u8>, Bytes>;
pub(crate) type Response = Result<RawFrame, WireError>;

#[derive(Debug, Clone)]
pub(crate) struct WireError {
    pub(crate) code: Bytes,
    pub(crate) description: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) enum Frame {
    Request {
        command: Bytes,
        tag: Option<Bytes>,
        fields: RawFrame,
    },
    Response {
        tag: Bytes,
        response: Response,
    },
}

impl TryFrom<RawFrame> for Frame {
    type Error = crate::Error;

    fn try_from(mut frame: RawFrame) -> Result<Self, Self::Error> {
        if frame.contains_key(b"_command".as_ref()) {
            if frame.contains_key(b"_error".as_ref()) || frame.contains_key(b"_answer".as_ref()) {
                return Err(Error::ConfusedFrame);
            }
            let command = frame.remove(b"_command".as_ref()).unwrap();
            let tag = frame.remove(b"_ask".as_ref());

            Ok(Frame::Request {
                command,
                tag,
                fields: frame,
            })
        } else if frame.contains_key(b"_answer".as_ref()) {
            if frame.contains_key(b"_error".as_ref()) || frame.contains_key(b"_command".as_ref()) {
                return Err(Error::ConfusedFrame);
            }

            let tag = frame.remove(b"_answer".as_ref()).unwrap();
            Ok(Frame::Response {
                tag,
                response: Ok(frame),
            })
        } else if frame.contains_key(b"_error".as_ref()) {
            if frame.contains_key(b"_answer".as_ref()) || frame.contains_key(b"_command".as_ref()) {
                return Err(Error::ConfusedFrame);
            }
            let tag = frame.remove(b"_error".as_ref()).unwrap();
            let code = frame
                .remove(b"_error_code".as_ref())
                .ok_or(Error::IncompleteErrorFrame)?;
            let description = frame
                .remove(b"_error_description".as_ref())
                .ok_or(Error::IncompleteErrorFrame)?;

            Ok(Frame::Response {
                tag,
                response: Err(WireError { code, description }),
            })
        } else {
            Err(Error::ConfusedFrame)
        }
    }
}
