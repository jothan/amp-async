use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;

use futures::sink::SinkExt;
use futures::stream::StreamExt;

use tokio::prelude::*;
use tokio::sync::{mpsc, oneshot, Barrier};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use crate::codecs::CodecError;
use crate::frame::Response;
use crate::{Decoder, Error, Frame, RawFrame};

#[derive(Debug)]
pub struct Request(pub Bytes, pub RawFrame, pub Option<ReplyTicket>);

struct ExpectReply {
    tag: u64,
    reply: oneshot::Sender<Response>,
    barrier: Arc<tokio::sync::Barrier>,
}

type _FrameMaker = Box<dyn FnOnce(Option<Bytes>) -> Result<Vec<u8>, CodecError> + Send>;

struct FrameMaker(_FrameMaker);

impl std::fmt::Debug for FrameMaker {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "callback")
    }
}

#[derive(Debug)]
enum WriteCmd {
    Reply(Bytes),
    Request(FrameMaker, Option<oneshot::Sender<Response>>),
}

#[derive(Debug)]
pub struct ReplyTicket {
    tag: Option<Bytes>,
    write_handle: mpsc::Sender<WriteCmd>,
}

impl ReplyTicket {
    pub async fn ok<R: Serialize>(mut self, reply: R) -> Result<(), Error> {
        let tag = self.tag.take().expect("Tag taken out of sequence");

        let reply =
            crate::ser::OkResponse { tag, fields: reply }.serialize(&crate::ser::Serializer)?;

        self.write_handle
            .send(WriteCmd::Reply(reply.into()))
            .await?;

        Ok(())
    }

    pub async fn error(
        mut self,
        code: Option<String>,
        description: Option<String>,
    ) -> Result<(), Error> {
        let tag = self.tag.take().expect("Tag taken out of sequence");

        let reply = crate::ser::ErrorResponse {
            tag,
            code: code.unwrap_or_else(|| "UNKNOWN".into()),
            description: description.unwrap_or_else(|| "".into()),
        }
        .serialize(&crate::ser::Serializer)?;

        self.write_handle
            .send(WriteCmd::Reply(reply.into()))
            .await?;

        Ok(())
    }
}

impl Drop for ReplyTicket {
    fn drop(&mut self) {
        if let Some(tag) = self.tag.take() {
            let mut write_handle = self.write_handle.clone();
            let reply = crate::ser::ErrorResponse {
                tag,
                code: "UNKNOWN".into(),
                description: "Request dropped without reply".into(),
            }
            .serialize(&crate::ser::Serializer)
            .unwrap();

            // Can't wait for poll_drop
            tokio::spawn(async move {
                write_handle
                    .send(WriteCmd::Reply(reply.into()))
                    .await
                    .expect("error on drop")
            });
        }
    }
}

#[derive(Clone)]
pub struct RequestSender(mpsc::Sender<WriteCmd>);

impl RequestSender {
    pub async fn call_remote<Q: Serialize + Send + 'static>(
        &mut self,
        command: String,
        request: Q,
    ) -> Result<RawFrame, Error> {
        let (tx, rx) = oneshot::channel();

        let frame = FrameMaker(Box::new(move |tag| {
            crate::ser::Request {
                tag,
                command,
                fields: request,
            }
            .serialize(&crate::ser::Serializer)
        }));

        self.0.send(WriteCmd::Request(frame, Some(tx))).await?;

        rx.await?.map_err(|err| Error::Remote {
            code: err.code,
            description: err.description,
        })
    }

    pub async fn call_remote_noreply<Q: Serialize + Send + 'static>(
        &mut self,
        command: String,
        request: Q,
    ) -> Result<(), Error> {
        let frame = FrameMaker(Box::new(move |tag| {
            crate::ser::Request {
                tag,
                command,
                fields: request,
            }
            .serialize(&crate::ser::Serializer)
        }));

        self.0.send(WriteCmd::Request(frame, None)).await?;

        Ok(())
    }
}

pub struct Handle {
    write_res: JoinHandle<Result<(), Error>>,
    read_res: JoinHandle<Result<(), Error>>,
    write_loop_handle: mpsc::Sender<WriteCmd>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Handle {
    pub fn shutdown(&mut self) -> Result<(), Error> {
        if let Some(s) = self.shutdown.take() {
            // Read loop might already be shutdown.
            s.send(()).map_err(|_| Error::SendError)?;
        }
        Ok(())
    }

    pub async fn join(mut self) -> Result<(), Error> {
        let _ = (&mut self.write_res).await.unwrap()?;
        let _ = (&mut self.read_res).await.unwrap()?;
        Ok(())
    }

    pub fn request_sender(&self) -> RequestSender {
        RequestSender(self.write_loop_handle.clone())
    }
}

pub fn serve<R, W>(input: R, output: W) -> (Handle, mpsc::Receiver<Request>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(32);
    let (dispatch_tx, dispatch_rx) = mpsc::channel::<Request>(32);
    let (expect_tx, expect_rx) = mpsc::channel::<ExpectReply>(32);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let read_res = tokio::spawn(read_loop(
        input,
        shutdown_rx,
        write_tx.clone(),
        dispatch_tx,
        expect_rx,
    ));
    let write_res = tokio::spawn(write_loop(output, write_rx, expect_tx));

    (
        Handle {
            write_res,
            read_res,
            write_loop_handle: write_tx,
            shutdown: Some(shutdown_tx),
        },
        dispatch_rx,
    )
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

type ReplyMap = HashMap<u64, oneshot::Sender<Response>>;

async fn read_loop<R>(
    input: R,
    mut shutdown: oneshot::Receiver<()>,
    mut write_tx: mpsc::Sender<WriteCmd>,
    mut dispatch_tx: mpsc::Sender<Request>,
    mut expect_rx: mpsc::Receiver<ExpectReply>,
) -> Result<(), Error>
where
    R: AsyncRead + Unpin,
{
    let codec_in: Decoder<RawFrame> = Decoder::new();
    let mut input = FramedRead::new(input, codec_in);
    let mut reply_map = ReplyMap::new();

    loop {
        tokio::select! {
            frame = input.next() => {
                if let Some(frame) = frame {
                    dispatch_frame(frame?, &mut reply_map, &mut write_tx, &mut dispatch_tx).await?;
                } else {
                    break;
                }
            }
            expect = expect_rx.recv() => {
                if let Some(expect) = expect {
                    reply_map.insert(expect.tag, expect.reply);
                    expect.barrier.wait().await;
                }
            }
            _ = &mut shutdown => break
        }
    }

    Ok(())
}

async fn dispatch_frame(
    frame: RawFrame,
    reply_map: &mut ReplyMap,
    write_tx: &mut mpsc::Sender<WriteCmd>,
    dispatch_tx: &mut mpsc::Sender<Request>,
) -> Result<(), Error> {
    match frame.try_into()? {
        Frame::Request {
            tag,
            command,
            fields,
        } => {
            let ticket = tag.map(|tag| ReplyTicket {
                tag: Some(tag),
                write_handle: write_tx.clone(),
            });

            // The application may close its dispatch channel. All
            // incoming requests will generate a "Request dropped
            // without reply" error.
            let _ = dispatch_tx.send(Request(command, fields, ticket)).await;
        }

        Frame::Response { tag, response } => {
            let reply_tx = std::str::from_utf8(&tag)
                .ok()
                .and_then(|tag_str| u64::from_str_radix(tag_str, 16).ok())
                .and_then(|tag_u64| reply_map.remove(&tag_u64))
                .ok_or(Error::UnmatchedReply)?;

            reply_tx.send(response).map_err(|_| Error::SendError)?;
        }
    }

    Ok(())
}

async fn write_loop<W>(
    output: W,
    mut input: mpsc::Receiver<WriteCmd>,
    mut expect_tx: mpsc::Sender<ExpectReply>,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let mut output = FramedWrite::new(output, BytesCodec::new());
    let mut seqno: u64 = 0;

    while let Some(msg) = input.next().await {
        match msg {
            WriteCmd::Reply(frame) => {
                output.send(frame).await?;
            }
            WriteCmd::Request(request, reply) => {
                if let Some(reply) = reply {
                    seqno += 1;

                    let barrier = Arc::new(Barrier::new(2));

                    let expect = ExpectReply {
                        tag: seqno,
                        reply,
                        barrier: barrier.clone(),
                    };

                    expect_tx.send(expect).await?;
                    barrier.wait().await;

                    output
                        .send(request.0(Some(format!("{:x}", seqno).into()))?.into())
                        .await?;
                } else {
                    output.send(request.0(None)?.into()).await?;
                }
            }
        }
    }

    Ok(())
}
