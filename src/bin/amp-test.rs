use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::Write;

use bytes::Bytes;
use futures_util::try_stream::TryStreamExt;
use tokio::codec::{FramedRead, FramedWrite};
use tokio::io::{stdin, stdout};
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use amp_async::AmpCodec;

type RawFrame = HashMap<Bytes, Bytes>;

type Response = Result<RawFrame, WireError>;

#[derive(Debug, Clone)]
struct WireError {
    code: Bytes,
    description: Bytes,
}

#[derive(Debug, Clone)]
enum Frame {
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
    fn error(tag: Bytes, code: Option<Bytes>, description: Option<Bytes>) -> Self {
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
    type Error = Error;

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

#[derive(Debug, Clone)]
enum Error {
    ConfusedFrame,
    IncompleteErrorFrame,
    UnmatchedReply,
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
enum DispatchMsg {
    Frame(Frame),
    Request(Bytes, RawFrame, oneshot::Sender<Response>),
    Reply(Bytes, Response),
    Exit,
}

#[derive(Debug)]
struct ReplyTicket {
    tag: Bytes,
    write_handle: mpsc::Sender<DispatchMsg>,
    sent: bool,
}

impl ReplyTicket {
    pub async fn ok(mut self, reply: RawFrame) -> Result<(), mpsc::error::SendError> {
        self.sent = true;
        self.write_handle
            .send(DispatchMsg::Frame(Frame::Response {
                tag: self.tag.clone(),
                response: Ok(reply),
            }))
            .await?;

        Ok(())
    }

    pub async fn error(
        mut self,
        code: Option<Bytes>,
        description: Option<Bytes>,
    ) -> Result<(), mpsc::error::SendError> {
        self.sent = true;
        let frame = Frame::error(self.tag.clone(), code, description);
        self.write_handle.send(DispatchMsg::Frame(frame)).await?;

        Ok(())
    }
}

#[derive(Clone)]
struct RequestSender(mpsc::Sender<DispatchMsg>);

impl RequestSender {
    async fn call_remote(
        &mut self,
        command: Bytes,
        fields: RawFrame,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(DispatchMsg::Request(command, fields, tx))
            .await?;
        let res = rx.await?;

        Ok(res)
    }
}

impl Drop for ReplyTicket {
    fn drop(&mut self) {
        if !self.sent {
            let mut write_handle = self.write_handle.clone();
            let frame = Frame::error(
                self.tag.clone(),
                None,
                Some("Request dropped without reply".into()),
            );
            tokio::spawn(async move {
                write_handle
                    .send(DispatchMsg::Frame(frame))
                    .await
                    .expect("error on drop")
            });
        }
    }
}

#[derive(Debug)]
struct Request(Bytes, RawFrame, Option<ReplyTicket>);

fn amp_loop<R, W>(input: R, output: W) -> (RequestSender, mpsc::Receiver<Request>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (write_tx, write_rx) = mpsc::channel::<DispatchMsg>(32);
    let write_tx2 = write_tx.clone();
    let (dispatch_tx, dispatch_rx) = mpsc::channel::<Request>(32);
    tokio::spawn(async move {
        read_loop(input, write_tx2, dispatch_tx)
            .await
            .expect("read loop crash")
    });
    tokio::spawn(async move {
        write_loop(output, write_rx)
            .await
            .expect("write loop crash")
    });

    (RequestSender(write_tx), dispatch_rx)
}

async fn read_loop<R>(
    input: R,
    mut write_tx: mpsc::Sender<DispatchMsg>,
    mut dispatch_tx: mpsc::Sender<Request>,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: AsyncRead + Unpin,
{
    let codec_in: AmpCodec<RawFrame> = AmpCodec::new();
    let mut input = FramedRead::new(input, codec_in);

    while let Some(frame) = input.try_next().await? {
        let frame: Frame = frame.try_into()?;
        match frame {
            Frame::Request {
                tag,
                command,
                fields,
            } => {
                let ticket = tag.map(|tag| ReplyTicket {
                    tag,
                    write_handle: write_tx.clone(),
                    sent: false,
                });
                dispatch_tx.send(Request(command, fields, ticket)).await?;
            }

            Frame::Response { tag, response } => {
                write_tx.send(DispatchMsg::Reply(tag, response)).await?;
            }
        }
    }

    write_tx.send(DispatchMsg::Exit).await?;
    Ok(())
}

async fn write_loop<W>(
    output: W,
    mut input: mpsc::Receiver<DispatchMsg>,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: AsyncWrite + Unpin,
{
    let codec_out: AmpCodec<RawFrame> = AmpCodec::new();
    let mut output = FramedWrite::new(output, codec_out);
    let mut seqno: u64 = 1;
    let mut seqno_str = String::with_capacity(10);
    let mut reply_map = HashMap::new();

    while let Some(msg) = input.next().await {
        match msg {
            DispatchMsg::Frame(frame) => {
                let frame = frame.into();
                output.send(frame).await?;
            }
            DispatchMsg::Request(command, fields, reply) => {
                seqno += 1;
                seqno_str.clear();
                write!(seqno_str, "{:x}", seqno).unwrap();

                let seq_str: Bytes = seqno_str.as_bytes().into();
                reply_map.insert(seq_str.clone(), reply);

                let frame = Frame::Request {
                    command,
                    tag: Some(seq_str),
                    fields,
                };
                output.send(frame.into()).await?;
            }
            DispatchMsg::Reply(tag, response) => {
                let reply_tx = reply_map.remove(&tag).ok_or(Error::UnmatchedReply)?;
                reply_tx.send(response).unwrap();
            }
            DispatchMsg::Exit => break,
        }
    }

    Ok(())
}

fn parse_int(input: Option<Bytes>) -> Option<i64> {
    input
        .as_ref()
        .and_then(|i| std::str::from_utf8(i.as_ref()).ok())
        .and_then(|i| str::parse(i).ok())
}

async fn sum_request(
    mut fields: RawFrame,
    tag: Option<ReplyTicket>,
) -> Result<(), Box<dyn std::error::Error>> {
    let a: i64 = parse_int(fields.remove(b"a".as_ref())).unwrap();
    let b: i64 = parse_int(fields.remove(b"b".as_ref())).unwrap();

    if let Some(tag) = tag {
        let sum = a + b;
        let sum_str = format!("{}", sum);
        let mut fields = RawFrame::new();
        fields.insert("total".into(), sum_str.into());
        tag.ok(fields).await?;
    };

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut request, dispatch) = amp_loop(stdin(), stdout());

    let mut fields = RawFrame::new();
    fields.insert("a".into(), "123".into());
    fields.insert("b".into(), "321".into());
    let res = request.call_remote("Sum".into(), fields.clone()).await?;
    eprintln!("res1: {:?}", res);
    let res = request.call_remote("Sum".into(), fields).await?;
    eprintln!("res2: {:?}", res);

    dispatch
        .for_each_concurrent(10, |request| {
            async move {
                eprintln!("got request: {:?} {:?}", request.0, request.1);
                match request.0.as_ref() {
                    b"Sum" => sum_request(request.1, request.2).await.unwrap(),
                    _ => {
                        if let Some(tag) = request.2 {
                            tag.error(Some("UNHANDLED".into()), None).await.unwrap();
                        };
                    }
                }
            }
        })
        .await;

    Ok(())
}
