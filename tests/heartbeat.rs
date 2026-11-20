#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;

use client::*;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use ws_trade_server::{Config, start};

#[tokio::test]
async fn server_heartbeats_do_not_keep_a_silent_client_alive() {
    let config = Config {
        addr: "127.0.0.1:0".into(),
        heartbeat_ms: 10,
        idle_timeout: Some(Duration::from_millis(100)),
        ..Config::default()
    };
    let (addr, _) = start(config).await.unwrap();
    let mut client = connect_stomp(&addr.to_string(), "silent").await;
    subscribe(&mut client, "a", KR).await;
    let mut beats = 0;
    timeout(Duration::from_secs(2), async {
        while let Some(Ok(Message::Text(text))) = client.next().await {
            assert_eq!(text.as_str(), "\n");
            beats += 1;
        }
    })
    .await
    .expect("수신이 없는 연결은 종료되어야 한다");
    assert!(beats > 0);
}

#[tokio::test]
async fn client_heartbeats_and_commands_refresh_the_deadline() {
    let config =
        Config { addr: "127.0.0.1:0".into(), idle_timeout: Some(Duration::from_millis(200)), ..Config::default() };
    let (addr, _) = start(config).await.unwrap();
    let mut client = connect_stomp(&addr.to_string(), "alive").await;
    for _ in 0..6 {
        sleep(Duration::from_millis(50)).await;
        client.send(Message::text("\n")).await.unwrap();
    }
    subscribe(&mut client, "a", KR).await;
    assert!(timeout(Duration::from_secs(2), client.next()).await.unwrap().is_none_or(|m| m.is_err()));
}

#[tokio::test]
async fn idle_timeout_is_disabled_by_default() {
    let (addr, _) = start_server(10).await;
    let mut client = connect_stomp(&addr, "default").await;
    timeout(Duration::from_secs(2), async {
        for _ in 0..25 {
            assert_eq!(recv_text(&mut client).await, "\n");
        }
    })
    .await
    .unwrap();
}
