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
