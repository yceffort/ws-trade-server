use super::*;
use std::time::Duration;
use tokio::time::timeout;

async fn recv(queue: &mut mpsc::Receiver<Outgoing>) -> Option<Outgoing> {
    timeout(Duration::from_secs(2), queue.recv()).await.expect("2초 안에 와야 한다")
}

#[tokio::test]
async fn full_queue_drops_only_that_connection() {
    let hub = Hub::spawn();
    let (mut slow, slow_kicked) = hub.connect(1, 1).await;
    let (mut healthy, _healthy_kicked) = hub.connect(2, 8).await;
    for conn in [1, 2] {
        hub.subscribe(conn, "0".into(), "/topic".into(), None).await;
    }
    for n in 0..3 {
        hub.publish("/topic".into(), Arc::from(n.to_string())).await;
    }

    for n in 0..3 {
        let Some(Outgoing::Message { body, .. }) = recv(&mut healthy).await else {
            panic!("메시지가 와야 한다")
        };
        assert_eq!(&*body, n.to_string());
    }
    // 큐 용량 1인 연결은 첫 메시지만 받고 끊긴다.
    assert!(timeout(Duration::from_secs(2), slow_kicked).await.unwrap().is_err());
    assert!(matches!(recv(&mut slow).await, Some(Outgoing::Message { .. })));
    assert!(recv(&mut slow).await.is_none());
}

#[tokio::test]
async fn receipt_is_queued_before_first_message_of_that_subscription() {
    let hub = Hub::spawn();
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "0".into(), "/t".into(), Some("r".into())).await;
    hub.publish("/t".into(), Arc::from("x")).await;
    assert!(matches!(recv(&mut queue).await, Some(Outgoing::Receipt { .. })));
    assert!(matches!(recv(&mut queue).await, Some(Outgoing::Message { .. })));
}

#[tokio::test]
async fn nothing_for_a_subscription_follows_its_unsubscribe_receipt() {
    let hub = Hub::spawn();
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "a".into(), "/t".into(), None).await;
    hub.subscribe(1, "b".into(), "/u".into(), None).await;
    hub.publish("/t".into(), Arc::from("before")).await;
    hub.unsubscribe(1, "a".into(), Some("r".into())).await;
    hub.publish("/t".into(), Arc::from("after")).await;
    hub.publish("/u".into(), Arc::from("other")).await;

    assert!(matches!(recv(&mut queue).await, Some(Outgoing::Message { body, .. }) if &*body == "before"));
    assert!(matches!(recv(&mut queue).await, Some(Outgoing::Receipt { message: "unsubscribe success", .. })));
    let Some(Outgoing::Message { subscription, .. }) = recv(&mut queue).await else {
        panic!("메시지가 와야 한다")
    };
    assert_eq!(&*subscription, "b");
}

#[test]
fn personal_owner_is_the_first_segment_after_the_prefix() {
    assert_eq!(personal_owner("/experimental/personal/acct-alice/orders"), Some("acct-alice"));
    assert_eq!(personal_owner("/experimental/personal/acct-alice"), Some("acct-alice"));
    assert_eq!(personal_owner("/experimental/personal//orders"), Some(""));
    assert_eq!(personal_owner("/experimental/personal/"), Some(""));
    assert_eq!(personal_owner("/experimental/personal"), Some(""));
    assert_eq!(personal_owner("/topic/v1/kr/stock/trade/A005930"), None);
}

#[tokio::test]
async fn personal_topic_without_an_owner_is_denied_even_to_a_principal() {
    let hub = Hub::spawn();
    let (mut queue, _kicked) = hub.connect_as(1, 8, Some("acct-alice".into())).await;
    for destination in ["/experimental/personal", "/experimental/personal/", "/experimental/personal//orders"] {
        hub.subscribe(1, "0".into(), destination.into(), Some("r".into())).await;
        assert!(matches!(recv(&mut queue).await, Some(Outgoing::Denied { code: "forbidden-topic", .. })));
    }
}

#[tokio::test]
async fn personal_topics_never_enter_the_snapshot() {
    let personal = "/experimental/personal/acct-alice/orders";
    let root = "/experimental/personal";
    let hub = Hub::configured(Limits::default(), vec!["/t".into(), personal.into(), root.into()]);
    hub.publish(personal.into(), "order".into()).await;
    hub.publish(root.into(), "private".into()).await;
    hub.publish("/t".into(), "x".into()).await;
    let snapshot: serde_json::Value = serde_json::from_str(&hub.snapshot().await.unwrap()).unwrap();
    assert_eq!(snapshot.as_object().unwrap().keys().collect::<Vec<_>>(), ["/t"]);
}

#[tokio::test]
async fn rate_limit_applies_before_the_personal_topic_check() {
    let hub = Hub::with_limits(Limits { commands_per_second: 1, ..Limits::default() });
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "a".into(), "/t".into(), None).await;
    hub.subscribe(1, "b".into(), "/experimental/personal/acct-alice/orders".into(), Some("r".into())).await;
    assert!(matches!(recv(&mut queue).await, Some(Outgoing::Error("rate-limit-exceeded"))));
}

#[tokio::test]
async fn empty_principal_cannot_subscribe_an_empty_owner_topic() {
    let hub = Hub::spawn();
    let (mut queue, _kicked) = hub.connect_as(1, 8, Some(String::new())).await;
    for destination in ["/experimental/personal/", "/experimental/personal//orders"] {
        hub.subscribe(1, "0".into(), destination.into(), Some("r".into())).await;
        assert!(matches!(recv(&mut queue).await, Some(Outgoing::Denied { code: "forbidden-topic", .. })));
    }
}

// depth는 일반 MESSAGE로 쌓아 두는 큐 깊이다. 0이면 처음부터 칸으로 접는다.
fn latest_first_hub(topics: &[&str], depth: usize) -> Hub {
    Hub::with_latest_first(Limits::default(), Vec::new(), topics.iter().map(|topic| topic.to_string()).collect(), depth)
}

// 큐에 쌓인 것을 꺼내 순서대로 적는다. 최신 우선 표지는 꺼내는 순간의 칸 값으로 읽는다.
async fn drain(hub: &Hub, queue: &mut mpsc::Receiver<Outgoing>) -> Vec<String> {
    hub.metrics().await;
    let mut seen = Vec::new();
    while let Ok(item) = queue.try_recv() {
        seen.push(match item {
            Outgoing::Message { subscription, body } => format!("{subscription}:{body}"),
            Outgoing::Latest { subscription, slot } => {
                format!("{subscription}:{}", slot.take().as_deref().unwrap_or("빈 칸"))
            }
            Outgoing::Receipt { receipt_id, .. } => format!("receipt:{receipt_id}"),
            Outgoing::Denied { receipt_id, .. } => format!("denied:{receipt_id}"),
            Outgoing::Error(code) => format!("error:{code}"),
        });
    }
    seen
}

#[tokio::test]
async fn stalled_latest_first_connection_stays_and_gets_the_latest_of_each_subscription() {
    let hub = latest_first_hub(&["/hot", "/quiet"], 0);
    let (mut queue, mut kicked) = hub.connect(1, 2).await;
    hub.subscribe(1, "h".into(), "/hot".into(), None).await;
    hub.subscribe(1, "q".into(), "/quiet".into(), None).await;
    hub.publish("/quiet".into(), "q0".into()).await;
    for n in 0..100 {
        hub.publish("/hot".into(), n.to_string().into()).await;
    }
    let metrics = hub.metrics().await.unwrap();
    assert_eq!(
        (metrics.latest_skipped_total, metrics.slow_disconnections_total, metrics.stomp_connections),
        (99, 0, 1)
    );
    // 한 번만 발행한 quiet도 hot에 밀려나지 않는다.
    assert_eq!(drain(&hub, &mut queue).await, ["q:q0", "h:99"]);
    hub.publish("/hot".into(), "100".into()).await;
    assert_eq!(drain(&hub, &mut queue).await, ["h:100"]);
    assert!(matches!(kicked.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
}

#[tokio::test]
async fn topics_outside_the_latest_first_list_still_drop_a_full_connection() {
    let hub = latest_first_hub(&["/hot"], 0);
    let (_queue, kicked) = hub.connect(1, 1).await;
    hub.subscribe(1, "s".into(), "/strict".into(), None).await;
    for n in 0..2 {
        hub.publish("/strict".into(), n.to_string().into()).await;
    }
    assert!(timeout(Duration::from_secs(2), kicked).await.unwrap().is_err());
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.slow_disconnections_total, metrics.latest_skipped_total), (1, 0));
}

#[tokio::test]
async fn markers_alone_can_fill_the_queue_and_drop_the_connection() {
    let hub = latest_first_hub(&["/a", "/b"], 0);
    let (_queue, kicked) = hub.connect(1, 1).await;
    hub.subscribe(1, "a".into(), "/a".into(), None).await;
    hub.subscribe(1, "b".into(), "/b".into(), None).await;
    hub.publish("/a".into(), "a".into()).await;
    hub.publish("/b".into(), "b".into()).await;
    assert!(timeout(Duration::from_secs(2), kicked).await.unwrap().is_err());
    assert_eq!(hub.metrics().await.unwrap().slow_disconnections_total, 1);
}

#[tokio::test]
async fn a_receipt_without_room_behind_markers_drops_the_connection() {
    let hub = latest_first_hub(&["/a"], 0);
    let (_queue, kicked) = hub.connect(1, 1).await;
    hub.subscribe(1, "a".into(), "/a".into(), None).await;
    hub.publish("/a".into(), "a".into()).await;
    hub.unsubscribe(1, "a".into(), Some("r".into())).await;
    assert!(timeout(Duration::from_secs(2), kicked).await.unwrap().is_err());
    assert_eq!(hub.metrics().await.unwrap().slow_disconnections_total, 1);
}

#[tokio::test]
async fn latest_first_receipt_comes_before_the_first_message() {
    let hub = latest_first_hub(&["/t"], 0);
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "0".into(), "/t".into(), Some("r".into())).await;
    hub.publish("/t".into(), "x".into()).await;
    assert_eq!(drain(&hub, &mut queue).await, ["receipt:r", "0:x"]);
}

#[tokio::test]
async fn pending_latest_value_goes_out_before_the_unsubscribe_receipt() {
    let hub = latest_first_hub(&["/t"], 0);
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "a".into(), "/t".into(), None).await;
    hub.publish("/t".into(), "old".into()).await;
    hub.publish("/t".into(), "new".into()).await;
    hub.unsubscribe(1, "a".into(), Some("r".into())).await;
    hub.publish("/t".into(), "after".into()).await;
    assert_eq!(drain(&hub, &mut queue).await, ["a:new", "receipt:r"]);
}

#[tokio::test]
async fn resubscribing_with_the_same_id_starts_a_fresh_slot() {
    let hub = latest_first_hub(&["/x", "/y"], 0);
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "0".into(), "/x".into(), None).await;
    hub.publish("/x".into(), "x1".into()).await;
    hub.subscribe(1, "0".into(), "/y".into(), Some("r".into())).await;
    hub.publish("/y".into(), "y1".into()).await;
    // 칸을 같이 쓰면 앞의 표지가 y1을 꺼내서 새 구독의 MESSAGE가 RECEIPT보다 먼저 나간다.
    assert_eq!(drain(&hub, &mut queue).await, ["0:x1", "receipt:r", "0:y1"]);
}

#[tokio::test]
async fn nothing_is_skipped_while_the_queue_is_below_the_depth() {
    let hub = latest_first_hub(&["/t"], 8);
    let (mut queue, _kicked) = hub.connect(1, 16).await;
    hub.subscribe(1, "a".into(), "/t".into(), None).await;
    for n in 0..8 {
        hub.publish("/t".into(), n.to_string().into()).await;
    }
    let expected: Vec<String> = (0..8).map(|n| format!("a:{n}")).collect();
    assert_eq!(drain(&hub, &mut queue).await, expected);
    assert_eq!(hub.metrics().await.unwrap().latest_skipped_total, 0);
}

#[tokio::test]
async fn stalled_connection_gets_at_most_depth_old_frames_then_the_latest() {
    let hub = latest_first_hub(&["/hot", "/quiet"], 3);
    let (mut queue, mut kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "h".into(), "/hot".into(), None).await;
    hub.subscribe(1, "q".into(), "/quiet".into(), None).await;
    hub.publish("/quiet".into(), "q0".into()).await;
    for n in 0..100 {
        hub.publish("/hot".into(), n.to_string().into()).await;
    }
    let metrics = hub.metrics().await.unwrap();
    assert_eq!((metrics.queued_frames, metrics.latest_skipped_total, metrics.slow_disconnections_total), (4, 97, 0));
    assert_eq!(drain(&hub, &mut queue).await, ["q:q0", "h:0", "h:1", "h:99"]);
    assert!(matches!(kicked.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
}

// 표지가 큐에 있는 동안 깊이가 K 아래로 내려가도 새 발행은 칸의 값만 바꾼다.
// 일반 MESSAGE로 넣으면 표지가 더 새 값을 먼저 내보내고, 그 뒤에 더 오래된 MESSAGE가 나간다.
#[tokio::test]
async fn a_pending_marker_takes_new_values_even_below_the_depth() {
    let hub = latest_first_hub(&["/t"], 2);
    let (mut queue, _kicked) = hub.connect(1, 8).await;
    hub.subscribe(1, "a".into(), "/t".into(), None).await;
    for body in ["m0", "m1", "m2"] {
        hub.publish("/t".into(), body.into()).await;
    }
    hub.metrics().await;
    for expected in ["a:m0", "a:m1"] {
        let Some(Outgoing::Message { subscription, body }) = queue.try_recv().ok() else {
            panic!("MESSAGE가 와야 한다")
        };
        assert_eq!(format!("{subscription}:{body}"), expected);
    }
    for body in ["m3", "m4"] {
        hub.publish("/t".into(), body.into()).await;
    }
    assert_eq!(drain(&hub, &mut queue).await, ["a:m4"]);
    assert_eq!(hub.metrics().await.unwrap().latest_skipped_total, 2);
}
