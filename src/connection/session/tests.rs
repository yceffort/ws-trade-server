use super::*;
use crate::{limits::Limits, stomp};
use futures_util::{SinkExt, StreamExt};
use tokio::io::duplex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

#[tokio::test]
async fn rejection_ends_a_blocked_write_and_releases_connection_resources() {
    let hub = Hub::with_limits(Limits { subscriptions: 1, ..Limits::default() });
    // OS 송신 버퍼 크기와 무관하게 MESSAGE를 끝까지 쓰지 못하도록 작은 양방향 파이프를 쓴다.
    let (server_io, client_io) = duplex(1024);
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let task_hub = hub.clone();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        let (writer, reader) = server.split();
        run((reader, writer), &task_hub, &Config::default(), 1, "blocked-write", None, receiver).await
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
    let excess = stomp::serialize("SUBSCRIBE", &[("id", "b"), ("destination", "topic")], "");
    client.send(Message::text(excess)).await.unwrap();
    assert_eq!(timeout(Duration::from_secs(3), task).await.unwrap().unwrap(), "too-many-subscriptions");
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.stomp_connections, metrics.subscriptions, metrics.queued_frames), (0, 0, 0));
    drop(shutdown);
}
