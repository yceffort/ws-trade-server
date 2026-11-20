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
