use bytes::Bytes;

use futures::stream::StreamExt;
use serde::Serialize;

use tokio::io::{stdin, stdout};

use amp_async::{serve, RawFrame, ReplyTicket};

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

#[derive(Serialize, Clone)]
struct SumResponse {
    total: i64,
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

    let res = request
        .call_remote("Sum".into(), SumRequest { a: 123, b: 321 })
        .await?;
    eprintln!("res1: {:?}", res);
    let res = request
        .call_remote("Sum".into(), SumRequest { a: 777, b: 777 })
        .await?;
    eprintln!("res2: {:?}", res);

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
