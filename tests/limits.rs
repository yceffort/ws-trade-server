#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;

use client::*;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Error;
use ws_trade_server::limits::Limits;
use ws_trade_server::{Config, start};

async fn server(limits: Limits) -> String {
    start(Config { addr: "127.0.0.1:0".into(), limits, ..Config::default() }).await.unwrap().0.to_string()
}

#[tokio::test]
async fn replacing_and_removing_subscriptions_releases_capacity() {
    let addr = server(Limits { subscriptions: 1, ..Limits::default() }).await;
    let mut client = connect_stomp(&addr, "quota").await;
    subscribe(&mut client, "a", KR).await;
    subscribe(&mut client, "a", US).await;
    send(&mut client, "UNSUBSCRIBE", &[("id", "a"), ("receipt", "removed")]).await;
    assert_eq!(recv_frame(&mut client).await.command, "RECEIPT");
    subscribe(&mut client, "b", KR).await;
    send(&mut client, "SUBSCRIBE", &[("id", "c"), ("destination", US)]).await;
    let error = recv_frame(&mut client).await;
    assert_eq!(error.command, "ERROR");
    assert_eq!(error.header("message"), Some("too-many-subscriptions"));
    assert!(timeout(Duration::from_secs(2), client.next()).await.unwrap().is_none_or(|m| m.is_err()));
}

#[tokio::test]
async fn unknown_topic_and_excess_commands_have_specific_errors() {
    for (limits, code) in [
        (Limits { topics: vec![KR.into()], ..Limits::default() }, "unknown-topic"),
        (Limits { commands_per_second: 1, ..Limits::default() }, "rate-limit-exceeded"),
    ] {
        let addr = server(limits).await;
        let mut client = connect_stomp(&addr, "policy").await;
        subscribe(&mut client, "a", KR).await;
        send(&mut client, "SUBSCRIBE", &[("id", "b"), ("destination", US)]).await;
        assert_eq!(recv_frame(&mut client).await.header("message"), Some(code));
    }
}

#[tokio::test]
async fn excess_connection_is_rejected_without_dropping_the_first() {
    let addr = server(Limits { connections: 1, ..Limits::default() }).await;
    let mut first = connect_stomp(&addr, "first").await;
    let Err(Error::Http(response)) = open(&addr, "/ws", Some("v12.stomp")).await else { panic!("HTTP 거절 필요") };
    assert_eq!(response.status().as_u16(), 503);
    subscribe(&mut first, "a", KR).await;
    send(&mut first, "DISCONNECT", &[]).await;
    let _ = timeout(Duration::from_secs(2), first.next()).await.unwrap();
    let mut next = connect_stomp(&addr, "next").await;
    subscribe(&mut next, "a", KR).await;
}
