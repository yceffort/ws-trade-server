use super::*;
use crate::{limits::Limits, stomp};
use futures_util::{SinkExt, StreamExt};
use tokio::io::duplex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

#[tokio::test]
async fn rejection_ends_a_blocked_write_and_releases_connection_resources() {
    rejected_write(false).await;
}

#[tokio::test]
async fn rejection_reason_survives_a_full_queue_that_drains_before_the_deadline() {
    rejected_write(true).await;
}

#[tokio::test]
async fn slow_consumer_drop_reports_its_reason_without_the_error_grace_period() {
    let hub = Hub::spawn();
    let (server_io, client_io) = duplex(1024);
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let task_hub = hub.clone();
    let (_shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        let (writer, reader) = server.split();
        let config = Config { queue_capacity: 1, ..Config::default() };
        run((reader, writer), &task_hub, &config, 1, "slow", None, receiver).await
    });
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("CONNECTED\n"));
    let subscribe = stomp::serialize("SUBSCRIBE", &[("id", "a"), ("destination", "topic"), ("receipt", "ready")], "");
    client.send(Message::text(subscribe)).await.unwrap();
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("RECEIPT\n"));
    // 쓰는 쪽이 큰 본문에 막힌 뒤 두 건을 더 보내면 크기 1인 큐가 넘친다.
    hub.publish("topic".into(), "x".repeat(64 * 1024).into()).await;
    while hub.metrics().await.unwrap().queued_frames != 0 {
        sleep(Duration::from_millis(1)).await;
    }
    let dropped = Instant::now();
    for body in ["a", "b"] {
        hub.publish("topic".into(), body.into()).await;
    }
    assert_eq!(timeout(Duration::from_secs(3), task).await.unwrap().unwrap(), SLOW_CONSUMER);
    assert!(dropped.elapsed() < Duration::from_millis(900));
    assert_eq!(hub.metrics().await.unwrap().slow_disconnections_total, 1);
}

type Session = (tokio::task::JoinHandle<&'static str>, WebSocketStream<tokio::io::DuplexStream>, watch::Sender<bool>);

// 큐는 넉넉하고 소켓 버퍼만 작은 연결에 구독 하나를 붙인다.
async fn subscribed(hub: &Hub, write_timeout: Option<Duration>) -> Session {
    let (server_io, client_io) = duplex(1024);
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let task_hub = hub.clone();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        let (writer, reader) = server.split();
        let config = Config { write_timeout, ..Config::default() };
        run((reader, writer), &task_hub, &config, 1, "timed", None, receiver).await
    });
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("CONNECTED\n"));
    let subscribe = stomp::serialize("SUBSCRIBE", &[("id", "a"), ("destination", "topic"), ("receipt", "ready")], "");
    client.send(Message::text(subscribe)).await.unwrap();
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("RECEIPT\n"));
    (task, client, shutdown)
}

#[tokio::test]
async fn write_timeout_closes_a_blocked_socket_while_the_queue_still_has_room() {
    let hub = Hub::spawn();
    let (task, _client, _shutdown) = subscribed(&hub, Some(Duration::from_millis(100))).await;
    let blocked = Instant::now();
    hub.publish("topic".into(), "x".repeat(64 * 1024).into()).await;
    assert_eq!(timeout(Duration::from_secs(3), task).await.unwrap().unwrap(), "write_timeout");
    assert!(blocked.elapsed() >= Duration::from_millis(100));
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.stomp_connections, metrics.subscriptions, metrics.slow_disconnections_total), (0, 0, 0));
}

#[tokio::test]
async fn write_timeout_keeps_a_reading_client_through_idle_periods() {
    let hub = Hub::spawn();
    let (task, mut client, _shutdown) = subscribed(&hub, Some(Duration::from_millis(100))).await;
    for n in 0..50 {
        hub.publish("topic".into(), format!("{n:>1024}").into()).await;
        assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("MESSAGE\n"));
    }
    // 쓸 것이 없는 동안은 기한을 재지 않는다.
    sleep(Duration::from_millis(300)).await;
    assert!(!task.is_finished());
    hub.publish("topic".into(), "after-idle".into()).await;
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().ends_with("after-idle\0"));
}

async fn rejected_write(drain: bool) {
    let hub = Hub::with_limits(Limits { subscriptions: 1, ..Limits::default() });
    // OS 송신 버퍼 크기와 무관하게 MESSAGE를 끝까지 쓰지 못하도록 작은 양방향 파이프를 쓴다.
    let (server_io, client_io) = duplex(1024);
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let task_hub = hub.clone();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        let (writer, reader) = server.split();
        run(
            (reader, writer),
            &task_hub,
            &Config { queue_capacity: 1, ..Config::default() },
            1,
            "blocked-write",
            None,
            receiver,
        )
        .await
    });
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("CONNECTED\n"));
    let first = stomp::serialize("SUBSCRIBE", &[("id", "a"), ("destination", "topic"), ("receipt", "ready")], "");
    client.send(Message::text(first)).await.unwrap();
    assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("RECEIPT\n"));
    hub.publish("topic".into(), "x".repeat(64 * 1024).into()).await;
    timeout(Duration::from_secs(2), async {
        while hub.metrics().await.unwrap().queued_frames != 0 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    if drain {
        hub.publish("topic".into(), "tail".into()).await;
        assert_eq!(hub.metrics().await.unwrap().queued_frames, 1);
    }
    let excess = stomp::serialize("SUBSCRIBE", &[("id", "b"), ("destination", "topic")], "");
    client.send(Message::text(excess)).await.unwrap();
    if drain {
        timeout(Duration::from_secs(2), async {
            while hub.metrics().await.unwrap().stomp_connections != 0 {
                sleep(Duration::from_millis(1)).await;
            }
            for _ in 0..2 {
                assert!(client.next().await.unwrap().unwrap().to_text().unwrap().starts_with("MESSAGE\n"));
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(timeout(Duration::from_secs(3), task).await.unwrap().unwrap(), "too-many-subscriptions");
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.stomp_connections, metrics.subscriptions, metrics.queued_frames), (0, 0, 0));
    drop(shutdown);
}
