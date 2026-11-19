mod client;
// 테스트 파일마다 일부만 쓰므로 안 쓰는 함수 경고를 막는다.
#[allow(dead_code)]
mod common;

use client::*;
use common::contract;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use ws_trade_server::feed::Market;

#[tokio::test]
async fn every_contract_unsubscribe_gets_contract_shaped_receipt() {
    let contract = contract();
    let (addr, _hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, &contract_connection_id(&contract)).await;

    let mut expected = Vec::new();
    for frame in contract["clientFrames"].as_array().unwrap() {
        let command = frame["command"].as_str().unwrap_or_default();
        let (kind, shape) = match command {
            "SUBSCRIBE" => ("sub", &contract["server"]["receipt"][0]),
            "UNSUBSCRIBE" => ("unsub", &contract["server"]["unsubscribeReceipt"][0]),
            _ => continue,
        };
        let headers = expected_pairs(&frame["headers"]);
        let receipt_id = headers.iter().find(|(k, _)| k == "receipt").unwrap().1.clone();
        assert!(receipt_id.ends_with(&format!("{kind}_receipt")));
        let refs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        send(&mut client, command, &refs).await;
        let pairs = expected_pairs(shape);
        expected.push(pairs.into_iter().map(|(k, v)| (k, v.replace("{receipt}", &receipt_id))).collect::<Vec<_>>());
    }
    assert!(expected.iter().any(|pairs| pairs.iter().any(|(_, v)| v == "unsubscribe success")));
    for pairs in expected {
        let receipt = recv_frame(&mut client).await;
        assert_eq!((receipt.command.as_str(), receipt.headers), ("RECEIPT", pairs));
    }
}

#[tokio::test]
async fn no_message_after_unsubscribe_receipt() {
    let (addr, hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, "unsub").await;
    subscribe(&mut client, "a", KR).await;
    subscribe(&mut client, "b", US).await;
    send(&mut client, "UNSUBSCRIBE", &[("receipt", "a-unsub_receipt"), ("id", "a")]).await;
    let receipt = recv_frame(&mut client).await;
    assert_eq!(receipt.header("receipt-id"), Some("a-unsub_receipt"));

    hub.publish(KR.to_string(), sample_trade(Market::Kr, "A005930").1).await;
    hub.publish(US.to_string(), sample_trade(Market::Us, "US19990122001").1).await;
    assert_eq!(recv_frame(&mut client).await.header("subscription"), Some("b"));
}

#[tokio::test]
async fn kicked_client_loses_its_backlog_and_is_closed() {
    let (addr, hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, "stuck").await;
    subscribe(&mut client, "0", KR).await;
    let body: Arc<str> = Arc::from("x".repeat(16 * 1024));
    for _ in 0..3000 {
        hub.publish(KR.to_string(), body.clone()).await;
    }
    let mut received = 0;
    let drained = timeout(Duration::from_secs(5), async {
        while let Some(Ok(message)) = client.next().await {
            if message.to_text().is_ok_and(|text| text.starts_with("MESSAGE")) {
                received += 1;
            }
        }
    })
    .await;
    assert!(drained.is_ok(), "서버가 소켓을 닫아야 한다");
    assert!(received < 1024, "끊긴 연결의 밀린 큐는 버려져야 한다: {received}");
}
