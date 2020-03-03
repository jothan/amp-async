use std::collections::HashMap;
use std::convert::TryInto;

use bytes::Bytes;

use futures_util::future::select;
use futures_util::future::Either;
use futures_util::sink::SinkExt;
use futures_util::stream::{Stream, StreamExt};

use tokio::prelude::*;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::codecs::CodecError;
use crate::frame::Response;
use crate::{Encoder, Decoder, Error, Frame, RawFrame};

#[derive(Debug)]
pub struct Request(pub Bytes, pub RawFrame, pub Option<ReplyTicket>);

#[derive(Debug)]
enum WriteCmd {
    Frame(Frame),
    Request(Bytes, RawFrame, Option<oneshot::Sender<Response>>),
    Reply(Bytes, Response),
    Exit,
}

#[derive(Debug)]
pub struct ReplyTicket {
    tag: Bytes,
    write_handle: mpsc::Sender<WriteCmd>,
    sent: bool,
}

impl ReplyTicket {
    pub async fn ok(mut self, reply: RawFrame) -> Result<(), Error> {
        self.sent = true;
        self.write_handle
            .send(WriteCmd::Frame(Frame::Response {
                tag: self.tag.split_off(0),
                response: Ok(reply),
            }))
            .await?;

        Ok(())
    }

    pub async fn error(
        mut self,
        code: Option<Bytes>,
        description: Option<Bytes>,
    ) -> Result<(), Error> {
        self.sent = true;
        let frame = Frame::error(self.tag.split_off(0), code, description);
        self.write_handle.send(WriteCmd::Frame(frame)).await?;

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
                    .send(WriteCmd::Frame(frame))
                    .await
                    .expect("error on drop")
            });
        }
    }
}

#[derive(Clone)]
pub struct RequestSender(mpsc::Sender<WriteCmd>);

impl RequestSender {
    pub async fn call_remote(
        &mut self,
        command: Bytes,
        fields: RawFrame,
    ) -> Result<RawFrame, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(WriteCmd::Request(command, fields, Some(tx)))
            .await?;

        rx.await?.map_err(|err| Error::Remote {
            code: err.code,
            description: err.description,
        })
    }

    pub async fn call_remote_noreply(
        &mut self,
        command: Bytes,
        fields: RawFrame,
    ) -> Result<(), Error> {
        self.0
            .send(WriteCmd::Request(command, fields, None))
            .await?;

        Ok(())
    }
}

pub struct Handle {
    write_res: oneshot::Receiver<Result<(), Error>>,
    read_res: oneshot::Receiver<Result<(), Error>>,
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
        let _ = (&mut self.write_res).await?;
        let _ = (&mut self.read_res).await?;
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
    let write_tx2 = write_tx.clone();
    let (dispatch_tx, dispatch_rx) = mpsc::channel::<Request>(32);
    let (read_res_tx, read_res_rx) = oneshot::channel();
    let (write_res_tx, write_res_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let res = read_loop(input, shutdown_rx, write_tx2, dispatch_tx).await;
        // Handle may already be dropped.
        let _ = read_res_tx.send(res);
    });
    tokio::spawn(async move {
        let res = write_loop(output, write_rx).await;
        // Handle may already be dropped.
        let _ = write_res_tx.send(res);
    });

    (
        Handle {
            write_res: write_res_rx,
            read_res: read_res_rx,
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

async fn read_or_shutdown<S>(
    stream: &mut S,
    shutdown: &mut oneshot::Receiver<()>,
) -> Option<Result<RawFrame, CodecError>>
where
    S: Unpin + Stream<Item = Result<RawFrame, CodecError>>,
{
    let select_res = select(stream.next(), shutdown).await;
    match select_res {
        Either::Left((None, _)) => None,
        Either::Left((Some(frame), _)) => Some(frame),
        Either::Right((_, _)) => None,
    }
}

async fn read_loop<R>(
    input: R,
    mut shutdown: oneshot::Receiver<()>,
    mut write_tx: mpsc::Sender<WriteCmd>,
    mut dispatch_tx: mpsc::Sender<Request>,
) -> Result<(), Error>
where
    R: AsyncRead + Unpin,
{
    let codec_in: Decoder<RawFrame> = Decoder::new();
    let mut input = FramedRead::new(input, codec_in);

    while let Some(frame) = read_or_shutdown(&mut input, &mut shutdown).await {
        match frame?.try_into()? {
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

                // The application may close its dispatch channel. All
                // incoming requests will generate a "Request dropped
                // without reply" error.
                let _ = dispatch_tx.send(Request(command, fields, ticket)).await;
            }

            Frame::Response { tag, response } => {
                write_tx.send(WriteCmd::Reply(tag, response)).await?;
            }
        }
    }

    write_tx.send(WriteCmd::Exit).await?;
    Ok(())
}

async fn write_loop<W>(output: W, mut input: mpsc::Receiver<WriteCmd>) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let codec_out: Encoder<RawFrame> = Encoder::new();
    let mut output = FramedWrite::new(output, codec_out);
    let mut seqno: u64 = 0;
    let mut reply_map = HashMap::new();

    while let Some(msg) = input.next().await {
        match msg {
            WriteCmd::Frame(frame) => {
                let frame = frame.into();
                output.send(frame).await?;
            }
            WriteCmd::Request(command, fields, reply) => {
                let tag = reply.map(|reply| {
                    seqno += 1;
                    let seq_str = Bytes::from(format!("{:x}", seqno));

                    reply_map.insert(seq_str.clone(), reply);
                    seq_str
                });

                let frame = Frame::Request {
                    command,
                    tag,
                    fields,
                };
                output.send(frame.into()).await?;
            }
            WriteCmd::Reply(tag, response) => {
                let reply_tx = reply_map.remove(&tag).ok_or(Error::UnmatchedReply)?;
                reply_tx.send(response).map_err(|_| Error::SendError)?;
            }
            WriteCmd::Exit => input.close(),
        }
    }

    Ok(())
}
