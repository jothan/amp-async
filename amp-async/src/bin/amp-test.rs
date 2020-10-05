// Clippy does not like using Bytes as keys.
#![allow(clippy::mutable_key_type)]

use async_trait::async_trait;
use bytes::Bytes;

use serde::{Deserialize, Serialize};

use tokio::io::{stdin, stdout};

use amp_async::{serve, AmpList, Dispatcher, RawFrame, RemoteError};

struct SumDispatcher;

#[async_trait]
impl Dispatcher for SumDispatcher {
    async fn dispatch(&self, command: &str, frame: RawFrame) -> Result<RawFrame, RemoteError> {
        eprintln!("got request: {:?} {:?}", command, frame);
        match command {
            "Sum" => Ok(sum_request(frame).await),
            _ => Err(RemoteError::new(Some("UNHANDLED"), Option::<&str>::None)),
        }
    }

    async fn dispatch_noreply(&self, command: &str, frame: RawFrame) {
        eprintln!("got blind request: {:?} {:?}", command, frame);
    }
}

fn parse_int(input: Option<Bytes>) -> Option<i64> {
    input
        .as_ref()
        .and_then(|i| std::str::from_utf8(i.as_ref()).ok())
        .and_then(|i| str::parse(i).ok())
}

#[derive(Serialize, Clone)]
struct SumRequest {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SumResponse {
    total: i64,
}

#[derive(Serialize)]
struct SumManyRequest {
    ops: AmpList<SumRequest>,
}

#[derive(Serialize, Deserialize)]
struct SumManyResponse {
    totals: AmpList<SumResponse>,
}

async fn sum_request(mut fields: RawFrame) -> RawFrame {
    let a: i64 = parse_int(fields.remove(b"a".as_ref())).unwrap();
    let b: i64 = parse_int(fields.remove(b"b".as_ref())).unwrap();

    let total = a + b;
    let mut out = RawFrame::new();
    out.insert(b"total".as_ref().into(), format!("{}", total).into());

    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let handle = serve(stdin(), stdout(), SumDispatcher);

    let mut request = handle.request_sender().unwrap();

    let res: SumResponse = request
        .call_remote("Sum".into(), SumRequest { a: 123, b: 321 })
        .await?;

    eprintln!("res1: {:?}", res);
    let res: SumResponse = request
        .call_remote("Sum".into(), SumRequest { a: 777, b: 777 })
        .await?;
    eprintln!("res2: {:?}", res);

    let req = SumManyRequest {
        ops: AmpList(vec![
            SumRequest { a: 10, b: 1 },
            SumRequest { a: 20, b: 2 },
            SumRequest { a: 30, b: 3 },
        ]),
    };
    let res: SumManyResponse = request.call_remote("SumMany".into(), req).await?;
    eprintln!("res3: {:?}", res.totals.0);

    drop(request);
    handle.join().await?;
    Ok(())
}
