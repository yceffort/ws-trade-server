#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;
#[cfg(unix)]
mod temp_file;

use client::*;
use std::time::{SystemTime, UNIX_EPOCH};
use ws_trade_server::auth::{Auth, Token};
use ws_trade_server::hub::Hub;
use ws_trade_server::limits::Limits;
use ws_trade_server::stomp::{self, Incoming};
use ws_trade_server::{Config, Server, start_managed};

const ALICE: &str = "/experimental/personal/acct-alice/orders";

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn token(token: &str, principal: &str) -> Token {
    Token { token: token.into(), expires_at: now() + 60, principal: Some(principal.into()) }
}

async fn server(auth: Option<Auth>, limits: Limits) -> Server {
    start_managed(Config { addr: "127.0.0.1:0".into(), auth, limits, ..Config::default() }).await.unwrap()
}

async fn connect(server: &Server, token: &str) -> Client {
    let (mut client, _) = open(&server.addr.to_string(), "/ws", Some("v12.stomp")).await.unwrap();
    let authorization = format!("Bearer {token}");
    send(&mut client, "CONNECT", &[("connection-id", "personal-test"), ("authorization", &authorization)]).await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
    client
}

async fn subscribe_personal(client: &mut Client, destination: &str) {
    send(client, "SUBSCRIBE", &[("id", "personal"), ("receipt", "r"), ("destination", destination)]).await;
}

fn headers(frame: &stomp::Frame) -> Vec<(&str, &str)> {
    frame.headers.iter().map(|(key, value)| (key.as_str(), value.as_str())).collect()
}

// 거절 프레임 전체에 토큰(tok-)과 주체(acct-)가 들어 있지 않아야 한다.
async fn expect_denied(client: &mut Client) {
    let text = recv_text(client).await;
    assert!(!text.contains("tok-") && !text.contains("acct-"), "{text:?}");
    let Some(Incoming::Frame(frame)) = stomp::parse(&text) else { panic!("STOMP 프레임이 와야 한다") };
    assert_eq!(frame.command, "RECEIPT");
    assert_eq!(headers(&frame), [("receipt-id", "r"), ("message", "forbidden-topic"), ("response-code", "403")]);
}

// hub는 명령을 순서대로 처리한다. 개인 topic에 먼저 발행했는데 다음 프레임이 공개 topic의 MESSAGE라면
// 거절된 구독으로는 아무것도 오지 않았고 연결과 다른 구독은 살아 있다.
async fn only_public_arrives(client: &mut Client, hub: &Hub) {
    hub.publish(ALICE.into(), "secret".into()).await;
    hub.publish(KR.into(), "public".into()).await;
    let frame = recv_frame(client).await;
    assert_eq!(
        (frame.command.as_str(), frame.header("subscription"), frame.body.as_str()),
        ("MESSAGE", Some("a"), "public")
    );
}

#[tokio::test]
async fn owner_receives_own_topic_until_the_token_is_revoked() {
    let auth = Auth::new(vec![token("tok-alice", "acct-alice")]);
    let server = server(Some(auth.clone()), Limits::default()).await;
    let mut alice = connect(&server, "tok-alice").await;
    subscribe_personal(&mut alice, ALICE).await;
    let receipt = recv_frame(&mut alice).await;
    assert_eq!(headers(&receipt), [("receipt-id", "r"), ("message", "subscribe success"), ("response-code", "200")]);
    server.hub.publish(ALICE.into(), "order".into()).await;
    let message = recv_frame(&mut alice).await;
    assert_eq!((message.header("subscription"), message.body.as_str()), (Some("personal"), "order"));
    auth.replace(Vec::new());
    assert_eq!(recv_frame(&mut alice).await.header("message"), Some("auth-revoked"));
    server.shutdown().await;
}

#[tokio::test]
async fn changing_a_tokens_principal_revokes_the_old_session() {
    let auth = Auth::new(vec![token("tok-shared", "acct-alice")]);
    let server = server(Some(auth.clone()), Limits::default()).await;
    let mut alice = connect(&server, "tok-shared").await;
    subscribe_personal(&mut alice, ALICE).await;
    assert_eq!(recv_frame(&mut alice).await.header("response-code"), Some("200"));
    auth.replace(vec![token("tok-shared", "acct-bob")]);
    assert_eq!(recv_frame(&mut alice).await.header("message"), Some("auth-revoked"));
    let mut bob = connect(&server, "tok-shared").await;
    subscribe(&mut bob, "a", KR).await;
    subscribe_personal(&mut bob, ALICE).await;
    expect_denied(&mut bob).await;
    only_public_arrives(&mut bob, &server.hub).await;
    server.shutdown().await;
}

#[tokio::test]
async fn expiring_a_personal_token_closes_the_session() {
    let mut entry = token("tok-alice", "acct-alice");
    let auth = Auth::new(vec![entry.clone()]);
    let server = server(Some(auth.clone()), Limits::default()).await;
    let mut alice = connect(&server, "tok-alice").await;
    subscribe_personal(&mut alice, ALICE).await;
    assert_eq!(recv_frame(&mut alice).await.header("response-code"), Some("200"));
    entry.expires_at = now() - 1;
    auth.replace(vec![entry]);
    assert_eq!(recv_frame(&mut alice).await.header("message"), Some("auth-expired"));
    server.shutdown().await;
}

#[tokio::test]
async fn other_principal_is_denied_only_that_subscription() {
    let auth = Auth::new(vec![token("tok-alice", "acct-alice"), token("tok-bob", "acct-bob")]);
    let server = server(Some(auth), Limits::default()).await;
    let mut bob = connect(&server, "tok-bob").await;
    subscribe(&mut bob, "a", KR).await;
    subscribe_personal(&mut bob, ALICE).await;
    expect_denied(&mut bob).await;
    only_public_arrives(&mut bob, &server.hub).await;
    server.shutdown().await;
}

#[tokio::test]
async fn denied_subscribe_without_receipt_sends_no_frame() {
    let server = server(Some(Auth::new(vec![token("tok-bob", "acct-bob")])), Limits::default()).await;
    let mut bob = connect(&server, "tok-bob").await;
    send(&mut bob, "SUBSCRIBE", &[("id", "personal"), ("destination", ALICE)]).await;
    // 거절이 무엇이든 프레임을 보냈다면 이 RECEIPT보다 먼저 왔을 것이다.
    subscribe(&mut bob, "a", KR).await;
    only_public_arrives(&mut bob, &server.hub).await;
    server.shutdown().await;
}

#[tokio::test]
async fn personal_topic_is_denied_when_auth_is_off() {
    let server = server(None, Limits::default()).await;
    let mut client = connect_stomp(&server.addr.to_string(), "no-auth").await;
    subscribe(&mut client, "a", KR).await;
    subscribe_personal(&mut client, ALICE).await;
    expect_denied(&mut client).await;
    only_public_arrives(&mut client, &server.hub).await;
    server.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn tokens_without_a_principal_still_load_but_own_no_personal_topic() {
    let file = temp_file::TempFile::new("personal-legacy");
    let entries = format!(
        r#"[{{"token":"tok-legacy","expires_at":{0}}},{{"token":"tok-empty","expires_at":{0},"principal":""}}]"#,
        now() + 60
    );
    std::fs::write(file.path(), entries).unwrap();
    let server = server(Some(Auth::from_file(file.path().to_path_buf()).unwrap()), Limits::default()).await;
    for token in ["tok-legacy", "tok-empty"] {
        let mut client = connect(&server, token).await;
        subscribe(&mut client, "a", KR).await;
        // 소유자 구간이 비어 있는 topic도 주체 없는 연결과 맞지 않는다.
        for destination in [ALICE, "/experimental/personal//orders", "/experimental/personal/"] {
            subscribe_personal(&mut client, destination).await;
            expect_denied(&mut client).await;
        }
        only_public_arrives(&mut client, &server.hub).await;
    }
    server.shutdown().await;
}

#[tokio::test]
async fn denied_subscription_does_not_count_toward_the_limit() {
    let limits = Limits { subscriptions: 1, ..Limits::default() };
    let server = server(Some(Auth::new(vec![token("tok-bob", "acct-bob")])), limits).await;
    let mut bob = connect(&server, "tok-bob").await;
    subscribe_personal(&mut bob, ALICE).await;
    expect_denied(&mut bob).await;
    subscribe(&mut bob, "a", KR).await;
    assert_eq!(server.hub.metrics().await.unwrap().subscriptions, 1);
    server.shutdown().await;
}

#[tokio::test]
async fn owner_subscribes_own_topic_outside_allowed_topics() {
    let limits = Limits { topics: vec![KR.into()], ..Limits::default() };
    let server = server(Some(Auth::new(vec![token("tok-alice", "acct-alice")])), limits).await;
    let mut alice = connect(&server, "tok-alice").await;
    subscribe_personal(&mut alice, ALICE).await;
    assert_eq!(recv_frame(&mut alice).await.header("response-code"), Some("200"));
    server.hub.publish(ALICE.into(), "order".into()).await;
    assert_eq!(recv_frame(&mut alice).await.body, "order");
    server.shutdown().await;
}

#[tokio::test]
async fn denied_resubscribe_keeps_the_existing_subscription_of_that_id() {
    let server = server(Some(Auth::new(vec![token("tok-bob", "acct-bob")])), Limits::default()).await;
    let mut bob = connect(&server, "tok-bob").await;
    subscribe(&mut bob, "a", KR).await;
    send(&mut bob, "SUBSCRIBE", &[("id", "a"), ("receipt", "r"), ("destination", ALICE)]).await;
    expect_denied(&mut bob).await;
    only_public_arrives(&mut bob, &server.hub).await;
    server.shutdown().await;
}
