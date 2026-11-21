#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ws_trade_server::{Config, hub::Hub, limits::Limits, start_managed};

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    (head.into(), body.into())
}

// 표본 줄만 남긴다. 이름은 wts_ 접두사와 소문자, label은 reason이나 histogram 경계(le) 하나뿐이어야 한다.
fn samples(text: &str) -> Vec<(&str, &str)> {
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (series, value) = line.split_once(' ').unwrap();
            let name = series.split_once('{').map_or(series, |(name, label)| {
                match label.strip_suffix("\"}").and_then(|label| label.split_once("=\"")) {
                    Some(("reason", reason)) => {
                        assert!(reason.bytes().all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'-'), "{line}")
                    }
                    Some(("le", bound)) => assert!(bound == "+Inf" || bound.parse::<f64>().is_ok(), "{line}"),
                    _ => panic!("{line}"),
                }
                name
            });
            assert!(name.starts_with("wts_") && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'), "{line}");
            assert!(value.parse::<f64>().is_ok(), "{line}");
            (series, value)
        })
        .collect()
}

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
async fn latest_first_skips_are_counted_apart_from_slow_disconnections() {
    let hub = Hub::with_latest_first(Limits::default(), Vec::new(), vec!["/latest".into()], 0);
    let (_queue, _kick) = hub.connect(1, 1).await;
    hub.subscribe(1, "a".into(), "/latest".into(), None).await;
    for body in ["first", "second", "third"] {
        hub.publish("/latest".into(), body.into()).await;
    }
    let metrics = hub.metrics().await.unwrap();
    // 표지 하나가 큐 한 칸을 차지하고, 뒤의 두 건은 그 칸의 값만 바꾼다.
    assert_eq!((metrics.stomp_connections, metrics.queued_frames, metrics.enqueued_frames_total), (1, 1, 1));
    assert_eq!((metrics.latest_skipped_total, metrics.slow_disconnections_total), (2, 0));
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
    assert_eq!(body["latest_skipped_total"], 0);
    assert_eq!(body["stomp_connections"], 0);
    server.shutdown().await;
}

#[tokio::test]
async fn prometheus_counts_fixed_close_and_rejection_reasons_without_growing_series() {
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        limits: Limits { subscriptions: 1, ..Limits::default() },
        ..Config::default()
    })
    .await
    .unwrap();
    let (http, addr) = (server.http_addr.unwrap(), server.addr.to_string());
    let (head, before) = get(http, "/prometheus").await;
    assert!(head.starts_with("HTTP/1.1 200") && head.contains("Content-Type: text/plain; version=0.0.4"));
    let started: f64 =
        samples(&before).iter().find(|(name, _)| *name == "wts_start_time_seconds").unwrap().1.parse().unwrap();
    assert!(started > 0.0);

    let mut rejected = client::connect_stomp(&addr, "rejected").await;
    client::subscribe(&mut rejected, "0", client::KR).await;
    client::send(&mut rejected, "SUBSCRIBE", &[("id", "1"), ("destination", client::US)]).await;
    assert_eq!(client::recv_frame(&mut rejected).await.command, "ERROR");
    let mut leaving = client::connect_stomp(&addr, "leaving").await;
    client::send(&mut leaving, "DISCONNECT", &[]).await;
    server.hub.publish("/unsubscribed".into(), "body".into()).await;

    let expected = [
        ("wts_published_total", "1"),
        ("wts_stomp_connections", "0"),
        ("wts_subscription_rejections_total{reason=\"too-many-subscriptions\"}", "1"),
        ("wts_connections_closed_total{reason=\"too-many-subscriptions\"}", "1"),
        ("wts_connections_closed_total{reason=\"client_disconnect\"}", "1"),
        ("wts_connections_closed_total{reason=\"other\"}", "0"),
    ];
    let after = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, text) = get(http, "/prometheus").await;
            if expected.iter().all(|pair| samples(&text).contains(pair)) {
                break text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(samples(&after).len(), samples(&before).len());
    let families: Vec<&str> = after.lines().filter_map(|line| line.strip_prefix("# TYPE ")).collect();
    let histogram = |base: &str| families.iter().any(|family| *family == format!("{base} histogram"));
    for (series, _) in samples(&after) {
        let name = series.split('{').next().unwrap();
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
            .filter(|base| histogram(base));
        let family = family.unwrap_or(name);
        assert_eq!(families.iter().filter(|line| line.split(' ').next() == Some(family)).count(), 1, "{name}");
    }
    let (_, json) = get(http, "/metrics").await;
    for key in serde_json::from_str::<serde_json::Value>(&json).unwrap().as_object().unwrap().keys() {
        assert!(samples(&after).iter().any(|(series, _)| *series == format!("wts_{key}")), "{key}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn delivered_messages_fill_cumulative_wait_and_write_histograms() {
    let server = start_managed(Config {
        addr: "127.0.0.1:0".into(),
        http_addr: Some("127.0.0.1:0".into()),
        ..Config::default()
    })
    .await
    .unwrap();
    let (http, addr) = (server.http_addr.unwrap(), server.addr.to_string());
    let mut reader = client::connect_stomp(&addr, "reader").await;
    client::subscribe(&mut reader, "0", client::KR).await;
    for n in 0..3 {
        server.hub.publish(client::KR.into(), format!("{n}").into()).await;
        assert_eq!(client::recv_frame(&mut reader).await.command, "MESSAGE");
    }
    let (_, text) = get(http, "/prometheus").await;
    for name in ["wts_queue_wait_seconds", "wts_socket_write_seconds"] {
        let buckets: Vec<u64> = samples(&text)
            .iter()
            .filter(|(series, _)| series.starts_with(&format!("{name}_bucket{{")))
            .map(|(_, value)| value.parse().unwrap())
            .collect();
        assert!(buckets.windows(2).all(|pair| pair[0] <= pair[1]), "{name} {buckets:?}");
        let count = samples(&text).iter().find(|(series, _)| *series == format!("{name}_count")).unwrap().1;
        assert_eq!(buckets.last().unwrap().to_string(), count, "{name}");
    }
    // MESSAGE 세 건이 각각 한 번씩 큐 대기로 잡힌다. 소켓 쓰기는 RECEIPT를 포함한 묶음 수라 세 번 이상이다.
    assert!(samples(&text).contains(&("wts_queue_wait_seconds_count", "3")));
    let writes: u64 = samples(&text)
        .iter()
        .find(|(series, _)| *series == "wts_socket_write_seconds_count")
        .unwrap()
        .1
        .parse()
        .unwrap();
    assert!(writes >= 3);
    server.shutdown().await;
}
