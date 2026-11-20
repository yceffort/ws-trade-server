use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use ws_trade_server::corrections::{Event, Ledger, TOPIC};
use ws_trade_server::{Config, start_managed, stomp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = start_managed(Config { addr: "127.0.0.1:0".into(), ..Config::default() }).await?;
    let mut request = format!("ws://{}/ws", server.addr).into_client_request()?;
    request.headers_mut().insert("sec-websocket-protocol", "v12.stomp".parse()?);
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    socket.send(Message::text(stomp::serialize("CONNECT", &[("connection-id", "synthetic")], ""))).await?;
    socket.next().await.ok_or("CONNECTED 없음")??;
    socket
        .send(Message::text(stomp::serialize(
            "SUBSCRIBE",
            &[("id", "events"), ("destination", TOPIC), ("receipt", "ready")],
            "",
        )))
        .await?;
    socket.next().await.ok_or("RECEIPT 없음")??;
    let events = [
        Event::Create { event_id: "e1".into(), trade_id: "t1".into(), price_minor: 12300, quantity: 10 },
        Event::Correct { event_id: "e2".into(), target_id: "t1".into(), price_minor: 12500, quantity: 7 },
        Event::Cancel { event_id: "e3".into(), target_id: "t1".into() },
    ];
    let mut ledger = Ledger::new(100);
    for event in events {
        server.hub.publish(TOPIC.into(), serde_json::to_string(&event)?.into()).await;
        loop {
            let message =
                tokio::time::timeout(std::time::Duration::from_secs(2), socket.next()).await?.ok_or("연결 종료")??;
            let Some(stomp::Incoming::Frame(frame)) = stomp::parse(message.to_text()?) else { continue };
            if frame.command != "MESSAGE" {
                continue;
            }
            let event: Event = serde_json::from_str(&frame.body)?;
            ledger.apply(event).map_err(|error| format!("적용 실패: {error:?}"))?;
            let (quantity, amount) = ledger.totals();
            println!("quantity={quantity} amount_minor={amount}");
            break;
        }
    }
    server.shutdown().await;
    Ok(())
}
