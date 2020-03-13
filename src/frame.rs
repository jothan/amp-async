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

impl Frame {
    pub fn error(tag: Bytes, code: Option<Bytes>, description: Option<Bytes>) -> Self {
        let code = code.unwrap_or_else(|| "UNKNOWN".into());

        // Twisted absolutely needs this field.
        let description = description.unwrap_or_else(|| "Unknown Error".into());
        Self::Response {
            tag,
            response: Err(WireError { code, description }),
        }
    }
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

impl From<Frame> for RawFrame {
    fn from(frame: Frame) -> RawFrame {
        match frame {
            Frame::Response {
                tag,
                response: Ok(mut fields),
            } => {
                fields.insert(b"_answer".as_ref().into(), tag);
                fields
            }
            Frame::Response {
                tag,
                response: Err(WireError { code, description }),
            } => {
                let mut fields = RawFrame::new();
                fields.insert("_error".into(), tag);
                fields.insert("_error_code".into(), code);
                fields.insert("_error_description".into(), description);
                fields
            }
            Frame::Request {
                command,
                tag,
                mut fields,
            } => {
                fields.insert("_command".into(), command);
                if let Some(tag) = tag {
                    fields.insert("_ask".into(), tag);
                };
                fields
            }
        }
    }
}
