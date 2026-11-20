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
    receive(&mut socket, "CONNECTED", None).await?;
    socket
        .send(Message::text(stomp::serialize(
            "SUBSCRIBE",
            &[("id", "events"), ("destination", TOPIC), ("receipt", "ready")],
            "",
        )))
        .await?;
    receive(&mut socket, "RECEIPT", Some("ready")).await?;
    let events = [
        Event::Create { event_id: "e1".into(), trade_id: "t1".into(), price_minor: 12300, quantity: 10 },
        Event::Correct { event_id: "e2".into(), target_id: "t1".into(), price_minor: 12500, quantity: 7 },
        Event::Cancel { event_id: "e3".into(), target_id: "t1".into() },
    ];
    let mut ledger = Ledger::new(100);
    for event in events {
        server.hub.publish(TOPIC.into(), serde_json::to_string(&event)?.into()).await;
        let frame = receive(&mut socket, "MESSAGE", None).await?;
        let event: Event = serde_json::from_str(&frame.body)?;
        ledger.apply(event).map_err(|error| format!("적용 실패: {error:?}"))?;
        let (quantity, amount) = ledger.totals();
        println!("quantity={quantity} amount_minor={amount}");
    }
    server.shutdown().await;
    Ok(())
}

type Client = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn receive(
    socket: &mut Client,
    command: &str,
    receipt: Option<&str>,
) -> Result<stomp::Frame, Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let message = socket.next().await.ok_or("연결 종료")??;
            let Some(stomp::Incoming::Frame(frame)) = stomp::parse(message.to_text()?) else { continue };
            if frame.command == "ERROR" {
                return Err("서버 오류".into());
            }
            if frame.command == command && receipt.is_none_or(|id| frame.header("receipt-id") == Some(id)) {
                return Ok(frame);
            }
        }
    })
    .await?
}
