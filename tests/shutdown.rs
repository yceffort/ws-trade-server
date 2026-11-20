#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;

use client::*;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use ws_trade_server::{Config, start_managed};

#[tokio::test]
async fn shutdown_notifies_clients_closes_hub_and_releases_both_ports() {
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        ..Config::default()
    })
    .await
    .unwrap();
    let (addr, http, hub) = (server.addr, server.http_addr.unwrap(), server.hub.clone());
    let mut client = connect_stomp(&addr.to_string(), "shutdown").await;
    subscribe(&mut client, "a", KR).await;
    let shutdown = tokio::spawn(server.shutdown());
    let frame = recv_frame(&mut client).await;
    assert_eq!(frame.command, "ERROR");
    assert_eq!(frame.header("message"), Some("server-shutdown"));
    tokio::time::timeout(Duration::from_secs(2), shutdown).await.unwrap().unwrap();
    assert!(hub.snapshot().await.is_none());
    assert!(TcpListener::bind(addr).await.is_ok());
    assert!(TcpListener::bind(http).await.is_ok());
}

#[tokio::test]
async fn shutdown_interrupts_pending_handshake_and_missing_ring_retry() {
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        ring: Some(std::env::temp_dir().join(format!("absent-{}-ring", std::process::id()))),
        shutdown_timeout: Duration::from_millis(100),
        ..Config::default()
    })
    .await
    .unwrap();
    let _silent = TcpStream::connect(server.addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.shutdown()).await.unwrap();
}
