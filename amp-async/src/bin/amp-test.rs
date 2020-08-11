// Clippy does not like using Bytes as keys.
#![allow(clippy::mutable_key_type)]

use bytes::Bytes;

use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};

use tokio::io::{stdin, stdout};

use amp_async::{serve, AmpList, RawFrame, ReplyTicket};

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

async fn sum_request(
    mut fields: RawFrame,
    tag: Option<ReplyTicket>,
) -> Result<(), Box<dyn std::error::Error>> {
    let a: i64 = parse_int(fields.remove(b"a".as_ref())).unwrap();
    let b: i64 = parse_int(fields.remove(b"b".as_ref())).unwrap();

    if let Some(tag) = tag {
        let resp = SumResponse { total: a + b };
        tag.ok(resp).await?;
    };

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (handle, dispatch) = serve(stdin(), stdout());

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

    dispatch
        .for_each_concurrent(10, |request| async move {
            eprintln!("got request: {:?} {:?}", request.0, request.1);
            match request.0.as_ref() {
                b"Sum" => sum_request(request.1, request.2).await.unwrap(),
                _ => {
                    if let Some(tag) = request.2 {
                        tag.error(Some("UNHANDLED".into()), None).await.unwrap();
                    };
                }
            }
        })
        .await;

    drop(request);
    handle.join().await?;

    Ok(())
}
