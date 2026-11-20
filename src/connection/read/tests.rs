use crate::{Config, Server, start_managed, stomp};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn setup(idle_timeout: Option<Duration>) -> (Server, Client) {
    let server = start_managed(Config { addr: "127.0.0.1:0".into(), idle_timeout, ..Config::default() }).await.unwrap();
    let mut request = format!("ws://{}/ws", server.addr).into_client_request().unwrap();
    request.headers_mut().insert("sec-websocket-protocol", "v12.stomp".parse().unwrap());
    let (mut client, _) = connect_async(request).await.unwrap();
    client.send(Message::text(stomp::serialize("CONNECT", &[("connection-id", "blocked-hub")], ""))).await.unwrap();
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("CONNECTED\n"));
    (server, client)
}

async fn subscribe(client: &mut Client, id: &str) {
    let text = stomp::serialize("SUBSCRIBE", &[("id", id), ("destination", "topic"), ("receipt", id)], "");
    client.send(Message::text(text)).await.unwrap();
}

async fn closed(client: &mut Client) {
    timeout(Duration::from_secs(3), async {
        while let Some(message) = client.next().await {
            if message.is_err() || matches!(message, Ok(Message::Close(_))) {
                return;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn heartbeat_survives_a_full_hub_channel_and_commands_stay_ordered() {
    let (server, mut client) = setup(Some(Duration::from_millis(500))).await;
    let release = server.hub.block_for_test().await;
    subscribe(&mut client, "first").await;
    client
        .send(Message::text(stomp::serialize("UNSUBSCRIBE", &[("id", "first"), ("receipt", "removed")], "")))
        .await
        .unwrap();
    subscribe(&mut client, "second").await;
    for _ in 0..12 {
        sleep(Duration::from_millis(100)).await;
        client.send(Message::text("\n")).await.unwrap();
    }
    drop(release);
    for id in ["first", "removed", "second"] {
        let text = timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap();
        let Some(stomp::Incoming::Frame(frame)) = stomp::parse(text.to_text().unwrap()) else { panic!() };
        assert_eq!(frame.command, "RECEIPT");
        assert_eq!(frame.header("receipt-id"), Some(id));
    }
    assert_eq!(server.hub.metrics().await.unwrap().subscriptions, 1);
    server.hub.publish("topic".into(), "{}".into()).await;
    let message = timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap();
    let Some(stomp::Incoming::Frame(frame)) = stomp::parse(message.to_text().unwrap()) else { panic!() };
    assert_eq!(frame.command, "MESSAGE");
    assert_eq!(frame.header("subscription"), Some("second"));
    server.shutdown().await;
}

#[tokio::test]
async fn silent_client_closes_while_the_hub_is_blocked() {
    let (server, mut client) = setup(Some(Duration::from_millis(500))).await;
    let release = server.hub.block_for_test().await;
    subscribe(&mut client, "pending").await;
    closed(&mut client).await;
    drop(release);
    server.shutdown().await;
}

#[tokio::test]
async fn command_backlog_is_bounded_even_when_heartbeats_are_disabled() {
    let (server, mut client) = setup(None).await;
    let release = server.hub.block_for_test().await;
    let text = stomp::serialize("SUBSCRIBE", &[("id", "a"), ("destination", "topic")], "");
    for _ in 0..128 {
        if client.send(Message::text(text.clone())).await.is_err() {
            break;
        }
    }
    closed(&mut client).await;
    drop(release);
    server.shutdown().await;
}

#[tokio::test]
async fn a_burst_of_commands_does_not_overflow_an_available_hub() {
    let (server, mut client) = setup(None).await;
    for id in 0..64 {
        subscribe(&mut client, &id.to_string()).await;
    }
    for id in 0..64 {
        let message = timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap();
        let Some(stomp::Incoming::Frame(frame)) = stomp::parse(message.to_text().unwrap()) else { panic!() };
        assert_eq!(frame.header("receipt-id"), Some(id.to_string().as_str()));
    }
    server.shutdown().await;
}
