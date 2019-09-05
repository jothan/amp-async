use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Write;

use bytes::Bytes;
use futures_util::try_stream::TryStreamExt;
use tokio::codec::{FramedRead, FramedWrite};
use tokio::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::frame::Response;
use crate::{AmpCodec, Error, Frame, RawFrame};

#[derive(Debug)]
pub struct Request(pub Bytes, pub RawFrame, pub Option<ReplyTicket>);

#[derive(Debug)]
enum DispatchMsg {
    Frame(Frame),
    Request(Bytes, RawFrame, oneshot::Sender<Response>),
    Reply(Bytes, Response),
    Exit,
}

#[derive(Debug)]
pub struct ReplyTicket {
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

impl Drop for ReplyTicket {
    fn drop(&mut self) {
        if !self.sent {
            let mut write_handle = self.write_handle.clone();
            let frame = Frame::error(
                self.tag.split_off(0),
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

#[derive(Clone)]
pub struct RequestSender(mpsc::Sender<DispatchMsg>);

impl RequestSender {
    pub async fn call_remote(
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

pub fn serve<R, W>(input: R, output: W) -> (RequestSender, mpsc::Receiver<Request>)
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
    let mut seqno: u64 = 0;
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
