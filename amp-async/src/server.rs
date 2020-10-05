use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use futures::sink::SinkExt;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use futures::FutureExt;

use async_trait::async_trait;
use tokio::prelude::*;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use amp_serde::{ErrorResponse, OkResponse, Request};

use crate::frame::Response;
use crate::{Decoder, Error, Frame, RawFrame, RemoteError};

const QUEUE_DEPTH: usize = 32;

#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    async fn dispatch(&self, _command: &str, _frame: RawFrame) -> Result<RawFrame, RemoteError> {
        Err(RemoteError::new(Some("UNHANDLED"), Option::<&str>::None))
    }

    async fn dispatch_noreply(&self, _command: &str, _frame: RawFrame) {}
}

pub struct NoopDispatcher;

impl Dispatcher for NoopDispatcher {}

struct ExpectReply {
    tag: u64,
    reply: oneshot::Sender<Response>,
    confirm: oneshot::Sender<()>,
}

#[derive(Default)]
struct LoopState {
    read_done: bool,
    write_done: bool,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum State {
    Connected,
    Closing,
    Closed,
}

type _FrameMaker = Box<dyn FnOnce(Option<Bytes>) -> Result<Vec<u8>, amp_serde::Error> + Send>;

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
    Exit,
}

#[derive(Clone)]
pub struct RequestSender(mpsc::Sender<WriteCmd>);

impl RequestSender {
    pub async fn call_remote<Q: Serialize + Send + 'static, R: DeserializeOwned>(
        &mut self,
        command: String,
        request: Q,
    ) -> Result<R, Error> {
        let (tx, rx) = oneshot::channel();

        let frame = FrameMaker(Box::new(move |tag| {
            amp_serde::to_bytes(Request {
                tag,
                command,
                fields: request,
            })
        }));

        self.0.send(WriteCmd::Request(frame, Some(tx))).await?;

        let raw_frame = rx.await?.map_err(Error::Remote)?;

        // FIXME: do this without an intermediary copy when serde gets
        // good at deserializing untagged enums with flattened structs.
        amp_serde::to_bytes(raw_frame)
            .and_then(|b| amp_serde::from_bytes(&b))
            .map_err(Into::into)
    }

    pub async fn call_remote_noreply<Q: Serialize + Send + 'static>(
        &mut self,
        command: String,
        request: Q,
    ) -> Result<(), Error> {
        let frame = FrameMaker(Box::new(move |tag| {
            amp_serde::to_bytes(Request {
                tag,
                command,
                fields: request,
            })
        }));

        self.0.send(WriteCmd::Request(frame, None)).await?;

        Ok(())
    }
}

pub struct Handle {
    state: Arc<RwLock<LoopState>>,
    write_res: JoinHandle<Result<(), Error>>,
    read_res: JoinHandle<Result<(), Error>>,
    write_loop_handle: Option<mpsc::Sender<WriteCmd>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Handle {
    pub fn shutdown(&mut self) {
        self.write_loop_handle = None;
        if let Some(s) = self.shutdown.take() {
            let _ = s.send(());
        }
    }

    pub async fn join(mut self) -> Result<(), Error> {
        self.write_loop_handle = None;
        self.write_res.await.unwrap()?;
        if let Some(s) = self.shutdown.take() {
            let _ = s.send(());
        }
        self.read_res.await.unwrap()?;

        Ok(())
    }

    pub fn request_sender(&self) -> Option<RequestSender> {
        self.write_loop_handle.as_ref().cloned().map(RequestSender)
    }

    pub fn state(&self) -> State {
        let state = self.state.read().unwrap();
        let read_done = state.read_done;
        let write_done = state.write_done;
        drop(state);

        if read_done && write_done {
            State::Closed
        } else if read_done || write_done {
            State::Closing
        } else {
            State::Connected
        }
    }
}

pub fn serve<R, W, D>(input: R, output: W, dispatcher: D) -> Handle
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    D: Dispatcher,
{
    let state = Arc::new(RwLock::new(LoopState::default()));
    let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(QUEUE_DEPTH);
    let (expect_tx, expect_rx) = mpsc::channel::<ExpectReply>(QUEUE_DEPTH);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let read_state = state.clone();
    let write_tx2 = write_tx.clone();
    let read_res = tokio::spawn(async move {
        let res = read_loop(input, shutdown_rx, write_tx2, dispatcher, expect_rx).await;
        read_state.write().unwrap().read_done = true;
        res
    });

    let write_state = state.clone();
    let write_res = tokio::spawn(async move {
        let res = write_loop(output, write_rx, expect_tx).await;
        write_state.write().unwrap().write_done = true;
        res
    });

    Handle {
        state,
        write_res,
        read_res,
        write_loop_handle: Some(write_tx),
        shutdown: Some(shutdown_tx),
    }
}

type ReplyMap = HashMap<u64, oneshot::Sender<Response>>;

async fn read_loop<R, D>(
    input: R,
    mut shutdown: oneshot::Receiver<()>,
    mut write_tx: mpsc::Sender<WriteCmd>,
    dispatcher: D,
    mut expect_rx: mpsc::Receiver<ExpectReply>,
) -> Result<(), Error>
where
    R: AsyncRead + Unpin,
    D: Dispatcher,
{
    let codec_in: Decoder<RawFrame> = Decoder::new();
    let mut input = FramedRead::new(input, codec_in);
    let mut reply_map = ReplyMap::new();
    let mut dispatched_requests = FuturesUnordered::new();

    loop {
        tokio::select! {
            frame = input.next() => {
                if let Some(frame) = frame {
                    if let Some(dr) = dispatch_frame(frame?, &mut reply_map, &mut write_tx, &dispatcher).await? {
                        dispatched_requests.push(dr);
                    }
                } else {
                    break;
                }
            }
            expect = expect_rx.recv() => {
                if let Some(expect) = expect {
                    reply_map.insert(expect.tag, expect.reply);
                    let _ = expect.confirm.send(());
                } else {
                    break;
                }
            }
            dr = dispatched_requests.try_next(), if !dispatched_requests.is_empty() => {
                dr?;
            }
            _ = &mut shutdown => {
                write_tx.send(WriteCmd::Exit).await?;
                break;
            }
        }
    }

    Ok(())
}

async fn dispatch_frame<'a, D>(
    frame: RawFrame,
    reply_map: &mut ReplyMap,
    write_tx: &mut mpsc::Sender<WriteCmd>,
    dispatcher: &'a D,
) -> Result<Option<impl Future<Output = Result<(), Error>> + 'a>, Error>
where
    D: Dispatcher,
{
    match frame.try_into()? {
        Frame::Request {
            tag,
            command,
            fields,
        } => Ok(Some(match tag {
            None => async move {
                dispatcher
                    .dispatch_noreply(std::str::from_utf8(&command)?, fields)
                    .await;

                Ok(())
            }
            .left_future(),
            Some(tag) => {
                let mut write_tx = write_tx.clone();
                async move {
                    let reply = match dispatcher
                        .dispatch(std::str::from_utf8(&command)?, fields)
                        .await
                    {
                        Ok(reply) => amp_serde::to_bytes(OkResponse { tag, fields: reply })?,
                        Err(e) => amp_serde::to_bytes(ErrorResponse {
                            tag,
                            code: e.code,
                            description: e.description,
                        })?,
                    };
                    write_tx.send(WriteCmd::Reply(reply.into())).await?;
                    Ok(())
                }
                .right_future()
            }
        })),

        Frame::Response { tag, response } => {
            let reply_tx = std::str::from_utf8(&tag)
                .ok()
                .and_then(|tag_str| u64::from_str_radix(tag_str, 16).ok())
                .and_then(|tag_u64| reply_map.remove(&tag_u64))
                .ok_or(Error::UnmatchedReply)?;

            reply_tx.send(response).map_err(|_| Error::InternalError)?;
            Ok(None)
        }
    }
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
                let tag = if let Some(reply) = reply {
                    seqno += 1;

                    let (confirm_tx, confirm_rx) = oneshot::channel();

                    let expect = ExpectReply {
                        tag: seqno,
                        reply,
                        confirm: confirm_tx,
                    };

                    expect_tx.send(expect).await?;
                    let _ = confirm_rx.await;

                    Some(format!("{:x}", seqno).into())
                } else {
                    None
                };

                output.send(request.0(tag)?.into()).await?;
            }
            WriteCmd::Exit => break,
        }
    }

    Ok(())
}
