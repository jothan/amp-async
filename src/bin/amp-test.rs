use bytes::Bytes;

use futures_util::stream::StreamExt;

use tokio::io::{stdin, stdout};

use amp_async::{serve, RawFrame, ReplyTicket};

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
    let (handle, dispatch) = serve(stdin(), stdout());

    let mut request = handle.request_sender();

    let mut fields = RawFrame::new();
    fields.insert("a".into(), "123".into());
    fields.insert("b".into(), "321".into());
    let res = request.call_remote("Sum".into(), fields.clone()).await?;
    eprintln!("res1: {:?}", res);
    let res = request.call_remote("Sum".into(), fields).await?;
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

    handle.join().await?;

    Ok(())
}
