#![cfg(feature = "ha")]

#[allow(dead_code)]
mod ha_support;
use ha_support::{feed, now};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::sync::oneshot;
use ws_trade_server::{
    Config,
    auth::{Auth, Token},
    ha::*,
    start_managed,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "합성 checkpoint와 Node ws 의존성을 준비한 독립 Kafka 시험 클러스터 필요"]
async fn durable_client_recovers_across_gateways_and_replay_limits() {
    let directory = PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").unwrap());
    let kafka: Kafka = serde_json::from_slice(&std::fs::read(directory.join("feed.json")).unwrap()).unwrap();
    let reader: Kafka = serde_json::from_slice(&std::fs::read(directory.join("gateway.json")).unwrap()).unwrap();
    let source: SourceConfig = serde_json::from_slice(&std::fs::read(directory.join("source.json")).unwrap()).unwrap();
    let feed = feed(kafka, source.clone());
    let read = reader.clone();
    let start = tokio::task::spawn_blocking(move || read.reader(EVENTS, None).unwrap().initial_high()).await.unwrap();
    for count in (0..100).map(|_| 100).chain([40]) {
        let (done, committed) = oneshot::channel();
        feed.commands.send((count, done)).unwrap();
        tokio::time::timeout(Duration::from_secs(10), committed).await.unwrap().unwrap();
    }
    let read = reader.clone();
    let source_read = source.clone();
    let expected = tokio::task::spawn_blocking(move || {
        let mut log = read.reader(EVENTS, Some(start)).unwrap();
        let end = log.initial_high();
        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while log.next_offset() < end {
            assert!(std::time::Instant::now() < deadline);
            if let Some(stored) = log.poll(Duration::from_millis(50)).unwrap()
                && let LogRecord::Trade(event) = LogRecord::from_json(&stored.payload, &source_read).unwrap() {
                let (topic, body) = ws_trade_server::trade::to_message(event.trade()).unwrap();
                events.push(json!({"id":event.event_id(), "offset":stored.offset.to_string(), "topic":topic, "body":serde_json::from_str::<Value>(&body).unwrap()}));
            }
        }
        events
    }).await.unwrap();
    assert_eq!(expected.len(), 10040);
    let config = || Config {
        addr: "127.0.0.1:0".into(),
        ha: Some(ReplicaInput { kafka: reader.clone(), source: source.clone() }),
        ha_protocol: Some(
            Protocol::new(serde_json::from_slice(&std::fs::read(directory.join("cursor-keys.json")).unwrap()).unwrap())
                .unwrap(),
        ),
        auth: Some(Auth::new(vec![Token {
            token: "synthetic-protocol-test".into(),
            principal: Some("test-account".into()),
            expires_at: now() / 1000 + 600,
        }])),
        ..Config::default()
    };
    let (a, b, c) = tokio::join!(start_managed(config()), start_managed(config()), start_managed(config()));
    let (a, b, c) = (a.unwrap(), b.unwrap(), c.unwrap());
    let urls: Vec<_> = [&a, &b, &c].iter().map(|server| format!("ws://{}/experimental/ha/ws", server.addr)).collect();
    let fixture = directory.join("protocol-oracle.json");
    std::fs::write(&fixture,serde_json::to_vec(&json!({"urls":urls,"topics":source.topics,"log_id":source.log_id,"start":expected[0]["offset"],"events":expected})).unwrap()).unwrap();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("node")
            .args(["tests/ha-client-oracle.mjs", fixture.to_str().unwrap()])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(output.status.success(), "oracle failed: {}", String::from_utf8_lossy(&output.stderr));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["applied"], 10040);
    assert_eq!(report["crash_recovered"], true);
    assert_eq!(report["snapshot_recovered"], true);
    assert!(report["replay_limits"].as_u64().unwrap() >= 1);
    println!("{report}");
    tokio::join!(a.shutdown(), b.shutdown(), c.shutdown());
}
