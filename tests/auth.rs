#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;
#[cfg(unix)]
mod temp_file;
#[cfg(unix)]
use temp_file::TempFile;

use client::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ws_trade_server::{
    Config,
    auth::{Auth, Token},
    start_managed,
};

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

async fn connect(addr: &str, token: &str) -> Client {
    let (mut client, _) = open(addr, "/ws", Some("v12.stomp")).await.unwrap();
    send(&mut client, "CONNECT", &[("connection-id", "auth-test"), ("authorization", token)]).await;
    client
}

#[tokio::test]
async fn invalid_expired_and_revoked_tokens_are_distinguished() {
    let auth = Auth::new(vec![
        Token { token: "demo-valid".into(), expires_at: now() + 60 },
        Token { token: "demo-expired".into(), expires_at: now() - 1 },
    ]);
    let server = start_managed(Config { addr: "127.0.0.1:0".into(), auth: Some(auth.clone()), ..Config::default() })
        .await
        .unwrap();
    let addr = server.addr.to_string();
    for (token, expected) in
        [("Bearer demo-wrong", "auth-invalid"), ("Bearer demo-expired", "auth-expired"), ("", "auth-invalid")]
    {
        let mut client = connect(&addr, token).await;
        let frame = recv_frame(&mut client).await;
        assert_eq!(frame.command, "ERROR");
        assert_eq!(frame.header("message"), Some(expected));
        assert!(!frame.body.contains("demo-"));
    }
    let mut client = connect(&addr, "Bearer demo-valid").await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
    subscribe(&mut client, "a", KR).await;
    auth.replace(Vec::new());
    let frame = recv_frame(&mut client).await;
    assert_eq!(frame.header("message"), Some("auth-revoked"));
    server.shutdown().await;
}

#[tokio::test]
async fn an_active_session_expires_without_another_request() {
    let auth = Auth::new(vec![Token { token: "demo-short".into(), expires_at: now() + 2 }]);
    let server =
        start_managed(Config { addr: "127.0.0.1:0".into(), auth: Some(auth), ..Config::default() }).await.unwrap();
    let mut client = connect(&server.addr.to_string(), "Bearer demo-short").await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
    let frame = tokio::time::timeout(Duration::from_secs(3), recv_frame(&mut client)).await.unwrap();
    assert_eq!(frame.header("message"), Some("auth-expired"));
    server.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unreadable_token_file_revokes_existing_sessions() {
    let file = TempFile::new("auth");
    let path = file.path().to_path_buf();
    std::fs::write(&path, format!(r#"[{{"token":"demo-file","expires_at":{}}}]"#, now() + 60)).unwrap();
    let auth = Auth::from_file(path.clone()).unwrap();
    let server =
        start_managed(Config { addr: "127.0.0.1:0".into(), auth: Some(auth), ..Config::default() }).await.unwrap();
    let mut client = connect(&server.addr.to_string(), "Bearer demo-file").await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
    std::fs::remove_file(path).unwrap();
    assert_eq!(recv_frame(&mut client).await.header("message"), Some("auth-revoked"));
    server.shutdown().await;
}

#[cfg(unix)]
#[test]
fn token_file_rejects_a_fifo_without_a_writer() {
    let file = TempFile::new("auth-fifo");
    assert!(std::process::Command::new("mkfifo").arg(file.path()).status().unwrap().success());
    let (done, result) = std::sync::mpsc::channel();
    let path = file.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = done.send(Auth::from_file(path).is_err());
    });
    assert!(result.recv_timeout(Duration::from_secs(2)).unwrap());
}
