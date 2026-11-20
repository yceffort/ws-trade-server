// 테스트 파일마다 일부만 쓰므로 안 쓰는 함수 경고를 막는다.
#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;

use client::*;
use std::io::ErrorKind;
use ws_trade_server::stomp::Frame;
use ws_trade_server::{Config, start, start_managed};

// 소켓 버퍼를 넉넉히 넘기도록 큰 본문을 보낸다. 합계 32MB다.
const TRADES: usize = 4000;
const BODY_BYTES: usize = 8192;

fn body(n: usize) -> String {
    format!("{n:>BODY_BYTES$}")
}

fn latest_first_config(depth: usize) -> Config {
    let topics = vec![KR.into()];
    Config {
        addr: "127.0.0.1:0".into(),
        heartbeat_ms: 60_000,
        latest_first_topics: topics,
        latest_first_depth: depth,
        ..Config::default()
    }
}

fn seq(frame: &Frame) -> u64 {
    frame.header("message-id").and_then(|id| id.rsplit_once('-')).unwrap().1.parse().unwrap()
}

#[tokio::test]
async fn stalled_client_on_a_latest_first_topic_resumes_with_the_latest_trade() {
    let config = latest_first_config(8);
    let (addr, hub) = start(config).await.unwrap();
    let addr = addr.to_string();
    let mut stalled = connect_stomp(&addr, "stalled").await;
    let mut reader = connect_stomp(&addr, "reader").await;
    subscribe(&mut stalled, "0", KR).await;
    subscribe(&mut reader, "0", KR).await;

    // 읽는 연결은 한 건씩 받고 나서 다음 건을 발행한다. 밀리지 않는 연결은 하나도 잃지 않아야 한다.
    for n in 0..TRADES {
        hub.publish(KR.into(), body(n).into()).await;
        let frame = recv_frame(&mut reader).await;
        assert_eq!((seq(&frame), frame.body.as_str()), (n as u64 + 1, body(n).as_str()));
    }
    let metrics = hub.metrics().await.unwrap();
    assert!(metrics.latest_skipped_total > 0, "멈춘 연결에서 건너뛴 체결이 있어야 한다");
    assert_eq!((metrics.slow_disconnections_total, metrics.stomp_connections), (0, 2));

    // 다시 읽으면 순번은 1부터 빠짐없이 이어지고, 체결은 앞으로만 가며, 마지막은 가장 최근 발행분이다.
    let (mut received, mut previous) = (0, None);
    loop {
        let frame = recv_frame(&mut stalled).await;
        received += 1;
        assert_eq!(seq(&frame), received);
        let n: usize = frame.body.trim_start().parse().unwrap();
        assert!(previous.is_none_or(|p| p < n), "체결 순서가 거꾸로 왔다");
        previous = Some(n);
        if n == TRADES - 1 {
            break;
        }
    }
    assert!(received < TRADES as u64, "건너뛴 체결이 있으므로 다 받을 수 없다: {received}");
    hub.publish(KR.into(), "tail".into()).await;
    let frame = recv_frame(&mut stalled).await;
    assert_eq!((seq(&frame), frame.body.as_str()), (received + 1, "tail"));
}

// 큐에 K건 넘게 쌓일 수 없는 묶음은 읽는 쪽이 늦게 읽어도 하나도 합쳐지지 않는다.
#[tokio::test]
async fn a_burst_smaller_than_the_depth_arrives_in_full() {
    let (addr, hub) = start(latest_first_config(64)).await.unwrap();
    let mut client = connect_stomp(&addr.to_string(), "burst").await;
    subscribe(&mut client, "0", KR).await;
    for n in 0..63 {
        hub.publish(KR.into(), body(n).into()).await;
    }
    for n in 0..63 {
        let frame = recv_frame(&mut client).await;
        assert_eq!((seq(&frame), frame.body.as_str()), (n as u64 + 1, body(n).as_str()));
    }
    assert_eq!(hub.metrics().await.unwrap().latest_skipped_total, 0);
}

// 깊이가 송신 큐 용량 이상이면 접기 전에 큐가 차서 끊기므로 시작하지 않는다. 최신 우선 topic이 없으면 깊이는 쓰이지 않는다.
#[tokio::test]
async fn depth_not_below_the_queue_capacity_is_rejected() {
    let config = Config { queue_capacity: 64, ..latest_first_config(64) };
    let error = start_managed(config.clone()).await.err().expect("시작하지 않아야 한다");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let server = start_managed(Config { latest_first_topics: Vec::new(), ..config }).await.unwrap();
    server.shutdown().await;
}
