#![cfg(feature = "ha")]

#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use ws_trade_server::{
    Config,
    ha::{Lifecycle, Phase},
    start_managed,
};

async fn request(addr: std::net::SocketAddr, method: &str, path: &str) -> String {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

async fn probe(addr: std::net::SocketAddr, mode: &'static str) -> bool {
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("sh")
            .args(["deploy/container/probe.sh", mode])
            .env("WTS_HTTP_PORT", addr.port().to_string())
            .output()
            .unwrap()
            .status
            .success()
    })
    .await
    .unwrap()
}

async fn rejected(addr: std::net::SocketAddr) {
    use tokio_tungstenite::tungstenite::{Error, client::IntoClientRequest};
    let mut request = format!("ws://{addr}/ws").into_client_request().unwrap();
    request.headers_mut().insert("sec-websocket-protocol", "v12.stomp".parse().unwrap());
    match tokio_tungstenite::connect_async(request).await {
        Err(Error::Http(response)) => {
            assert_eq!(response.status(), 503);
            assert_eq!(response.headers()["retry-after"], "1");
        }
        _ => panic!("upgrade must be rejected"),
    }
}

#[tokio::test]
async fn readiness_and_upgrade_share_drain_state_while_existing_stream_and_http_survive() {
    let life = Lifecycle::new(Duration::from_secs(5), Duration::from_millis(100), Duration::from_millis(100)).unwrap();
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        ha_lifecycle: Some(life.clone()),
        heartbeat_ms: 60000,
        ..Config::default()
    })
    .await
    .unwrap();
    let addr = server.addr;
    let http = server.http_addr.unwrap();
    assert!(request(http, "GET", "/livez").await.starts_with("HTTP/1.1 200"));
    assert!(request(http, "GET", "/readyz").await.starts_with("HTTP/1.1 503"));
    rejected(addr).await;
    assert!(probe(http, "ha-live").await);
    assert!(!probe(http, "ha-ready").await);
    life.health(true, false);
    assert!(request(http, "GET", "/readyz").await.starts_with("HTTP/1.1 200"));
    assert!(probe(http, "ha-ready").await);
    let mut socket = client::connect_stomp(&addr.to_string(), "drain").await;
    client::subscribe(&mut socket, "sub", client::KR).await;
    let _silent = TcpStream::connect(addr).await.unwrap();
    assert!(request(http, "POST", "/drain").await.contains("DRAINING"));
    let start = life.drain_started();
    assert!(request(http, "POST", "/drain").await.starts_with("HTTP/1.1 200"));
    assert_eq!(life.drain_started(), start);
    assert!(probe(http, "ha-drain").await);
    assert!(!probe(http, "ha-ready").await);
    assert!(probe(http, "ha-live").await);
    rejected(addr).await;
    assert!(request(http, "GET", "/readyz").await.starts_with("HTTP/1.1 503"));
    assert!(request(http, "GET", "/metrics").await.starts_with("HTTP/1.1 200"));
    assert!(request(http, "GET", "/livez").await.starts_with("HTTP/1.1 200"));
    server.hub.publish(client::KR.into(), "during-drain".into()).await;
    assert_eq!(client::recv_frame(&mut socket).await.body, "during-drain");
    let shutdown = tokio::spawn(server.shutdown());
    tokio::time::timeout(Duration::from_secs(10), shutdown).await.unwrap().unwrap();
    let frame = client::recv_frame(&mut socket).await;
    assert_eq!(frame.header("message"), Some("server-shutdown"));
    assert_eq!(life.phase(), Phase::Stopped);
    assert!(TcpListener::bind(addr).await.is_ok());
    assert!(TcpListener::bind(http).await.is_ok());
}

#[tokio::test]
async fn shutdown_cancels_bootstrap_without_extending_the_drain_deadline() {
    let life = Lifecycle::new(Duration::ZERO, Duration::ZERO, Duration::from_millis(20)).unwrap();
    let server =
        start_managed(Config { addr: "127.0.0.1:0".into(), ha_lifecycle: Some(life.clone()), ..Config::default() })
            .await
            .unwrap();
    let silent = TcpStream::connect(server.addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), server.shutdown()).await.unwrap();
    assert_eq!(life.phase(), Phase::Stopped);
    drop(silent);
}

#[allow(dead_code)]
mod ha_support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "초기화된 독립 Kafka 클러스터 필요"]
async fn ha_bootstrap_stale_recovery_and_drain_keep_explicit_states() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    use ws_trade_server::{
        auth::{Auth, Token},
        ha::*,
    };
    let dir = std::path::PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").unwrap());
    let read = |name| std::fs::read(dir.join(name)).unwrap();
    let source: SourceConfig = serde_json::from_slice(&read("source.json")).unwrap();
    let life =
        Lifecycle::new(Duration::from_millis(500), Duration::from_millis(100), Duration::from_millis(200)).unwrap();
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        ha_lifecycle: Some(life.clone()),
        ha: Some(ReplicaInput {
            kafka: serde_json::from_slice(&read("gateway.json")).unwrap(),
            source: source.clone(),
        }),
        ha_protocol: Some(Protocol::new(serde_json::from_slice(&read("cursor-keys.json")).unwrap()).unwrap()),
        auth: Some(Auth::new(vec![Token {
            token: "lifecycle-test".into(),
            principal: Some("test".into()),
            expires_at: ha_support::now() / 1000 + 600,
        }])),
        ..Config::default()
    })
    .await
    .unwrap();
    let http = server.http_addr.unwrap();
    assert!(request(http, "GET", "/livez").await.starts_with("HTTP/1.1 200"));
    let feed = ha_support::feed(serde_json::from_slice(&read("feed.json")).unwrap(), source.clone());
    server.wait_ready(Duration::from_secs(30)).await.unwrap();
    let connect = || async {
        let mut req = format!("ws://{}/experimental/ha/ws", server.addr).into_client_request().unwrap();
        req.headers_mut().insert("sec-websocket-protocol", "wts.ha.v1".parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        socket.send(Message::text(r#"{"type":"connect","token":"Bearer lifecycle-test"}"#)).await.unwrap();
        let connected = socket.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(serde_json::from_str::<serde_json::Value>(&connected).unwrap()["type"], "connected");
        socket
            .send(Message::text(
                serde_json::json!({"type":"resume","request_id":"state","topics":source.topics,"mode":"state"})
                    .to_string(),
            ))
            .await
            .unwrap();
        socket
    };
    let mut socket = connect().await;
    feed.pause.store(true, std::sync::atomic::Ordering::Relaxed);
    let stale = tokio::time::timeout(Duration::from_secs(6),async {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = heartbeat.tick() => { socket.send(Message::text(r#"{"type":"heartbeat"}"#)).await.unwrap(); },
                message = socket.next() => {
                    let frame:serde_json::Value = serde_json::from_str(&message.unwrap().unwrap().into_text().unwrap()).unwrap();
                    if frame["type"] == "error" { break frame; }
                }
            }
        }
    }).await.unwrap();
    assert_eq!(stale["code"], "source-stale");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !request(http, "GET", "/readyz").await.starts_with("HTTP/1.1 503") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(request(http, "GET", "/livez").await.starts_with("HTTP/1.1 200"));
    feed.pause.store(false, std::sync::atomic::Ordering::Relaxed);
    server.wait_ready(Duration::from_secs(5)).await.unwrap();
    let mut fresh = connect().await;
    assert!(request(http, "POST", "/drain").await.starts_with("HTTP/1.1 200"));
    let started = life.drain_started().unwrap();
    rejected(server.addr).await;
    let drained = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame: serde_json::Value =
                serde_json::from_str(&fresh.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
            if frame["type"] == "draining" {
                break frame;
            }
            assert_ne!(frame["type"], "error", "{frame}");
        }
    })
    .await
    .unwrap();
    assert_eq!(drained["type"], "draining");
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert!(request(http, "GET", "/livez").await.starts_with("HTTP/1.1 200"));
    server.shutdown().await;
    assert_eq!(life.phase(), Phase::Stopped);
}
