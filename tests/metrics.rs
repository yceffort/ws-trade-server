use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ws_trade_server::{Config, hub::Hub, start_managed};

#[tokio::test]
async fn queues_and_slow_drops_have_separate_counts_and_cleanup() {
    let hub = Hub::spawn();
    let (_slow, _kick) = hub.connect(1, 1).await;
    let (_healthy, _kick2) = hub.connect(2, 8).await;
    for conn in [1, 2] {
        hub.subscribe(conn, "a".into(), "/t".into(), None).await;
    }
    hub.publish("/t".into(), "first".into()).await;
    let metrics = hub.metrics().await.unwrap();
    assert_eq!(
        (metrics.stomp_connections, metrics.subscriptions, metrics.queued_frames, metrics.queue_capacity),
        (2, 2, 2, 9)
    );
    hub.publish("/t".into(), "second".into()).await;
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.stomp_connections, metrics.subscriptions, metrics.queued_frames), (1, 1, 2));
    assert_eq!((metrics.published_total, metrics.enqueued_frames_total, metrics.slow_disconnections_total), (2, 3, 1));
    hub.disconnect(2).await;
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.stomp_connections, metrics.subscriptions, metrics.queued_frames), (0, 0, 0));
}

#[tokio::test]
async fn metrics_are_available_over_the_optional_local_http_listener() {
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        ..Config::default()
    })
    .await
    .unwrap();
    server.hub.publish("/unsubscribed".into(), "body".into()).await;
    let mut stream = tokio::net::TcpStream::connect(server.http_addr.unwrap()).await.unwrap();
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    let body: serde_json::Value = serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body["published_total"], 1);
    assert_eq!(body["stomp_connections"], 0);
    server.shutdown().await;
}
