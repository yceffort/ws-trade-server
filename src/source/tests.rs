use super::*;
use crate::feed::{Generator, Market, Symbol};
use crate::hub::Outgoing;
use crate::limits::Limits;
use std::time::Duration;

fn source(provider: &str, stream: &str) -> SourceId {
    SourceId::new(provider.into(), stream.into()).unwrap()
}

fn trade() -> Trade {
    Generator::new(Symbol { market: Market::Kr, code: "DEMO".into() }).next_trade(7)
}

fn event(provider: &str) -> Arc<Envelope> {
    Arc::new(
        Envelope::new(
            "log-v1".into(),
            source(provider, "trades"),
            "epoch-1".into(),
            SourceSeq(u64::MAX),
            trade(),
            ReceivedAtMs(1),
        )
        .unwrap(),
    )
}

fn broker(hub: &Hub) -> BrokerAdapter {
    BrokerAdapter::new(hub.input(), "log-v1".into(), vec![source("demo", "trades")]).unwrap()
}

#[test]
fn stable_identity_is_independent_of_local_sequence_and_broker_offset() {
    let original = event("demo");
    let decoded = Envelope::from_json(&original.to_json().unwrap()).unwrap();
    assert_eq!(original.event_id(), decoded.event_id());
    assert_eq!(decoded.trade(), original.trade());
    assert_eq!(decoded.trade().seq, 7);
    assert_eq!(decoded.source_seq(), u64::MAX);
    assert_ne!(event("other-provider").event_id(), original.event_id());
    let encoded: serde_json::Value = serde_json::from_slice(&original.to_json().unwrap()).unwrap();
    assert_eq!(encoded["source_seq"], u64::MAX.to_string());
    assert_eq!(encoded["received_at_ms"], "1");
    let position = Position::new("log-v1".into(), 0, i64::MAX as u64).unwrap();
    let json = serde_json::to_string(&position).unwrap();
    assert_eq!(serde_json::from_str::<Position>(&json).unwrap(), position);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&json).unwrap()["offset"], i64::MAX.to_string());
}

#[test]
fn decoding_preserves_original_protobuf_bytes_including_unknown_fields() {
    let original = event("demo");
    let mut wire: serde_json::Value = serde_json::from_slice(&original.to_json().unwrap()).unwrap();
    let bytes = wire["trade"].as_array_mut().unwrap();
    bytes.extend([0xa0, 0x06, 0x01].map(serde_json::Value::from));
    let decoded = Envelope::from_json(&serde_json::to_vec(&wire).unwrap()).unwrap();
    let restored: serde_json::Value = serde_json::from_slice(&decoded.to_json().unwrap()).unwrap();
    assert_eq!(restored["trade"], wire["trade"]);
    assert_eq!(decoded.trade(), original.trade());
    assert_eq!(decoded.event_id(), original.event_id());
}

#[test]
fn trade_codes_and_currencies_keep_the_legacy_acceptance_rules() {
    for code in ["BRK/B".to_owned(), "합성코드".to_owned(), "X".repeat(97)] {
        let mut trade = trade();
        trade.code = code;
        trade.currency = "USD/test".into();
        let expected = crate::trade::to_message(&trade).unwrap();
        let ring = RingAdapter::prepare(&trade).unwrap();
        assert_eq!((ring.topic, ring.body), expected);
        let event = Envelope::new(
            "log-v1".into(),
            source("demo", "trades"),
            "epoch-1".into(),
            SourceSeq(1),
            trade,
            ReceivedAtMs(1),
        )
        .unwrap();
        assert_eq!(Envelope::from_json(&event.to_json().unwrap()).unwrap().message(), expected);
    }
}

#[test]
fn untrusted_envelopes_cannot_change_identity_schema_or_numeric_encoding() {
    let original: serde_json::Value = serde_json::from_slice(&event("demo").to_json().unwrap()).unwrap();
    for (field, value) in [
        ("schema_version", serde_json::json!(2)),
        ("source_seq", serde_json::json!(5)),
        ("source_seq", serde_json::json!("+5")),
        ("source_seq", serde_json::json!("01")),
        ("source_seq", serde_json::json!("18446744073709551616")),
        ("event_id", serde_json::json!("forged")),
        ("source_epoch", serde_json::json!("a/b")),
        ("trade", serde_json::json!([255])),
        ("extra", serde_json::json!(true)),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        assert!(Envelope::from_json(&serde_json::to_vec(&changed).unwrap()).is_err(), "{field}");
    }
    assert!(matches!(Envelope::from_json(&vec![b' '; MAX_ENVELOPE_BYTES + 1]), Err(InputError::TooLarge)));
    for provider in ["", "a/b", "a\n", "a b"] {
        assert!(SourceId::new(provider.into(), "stream".into()).is_err());
    }
    assert!(serde_json::from_str::<Position>(r#"{"log_id":"log-v1","partition":0,"offset":42}"#).is_err());
    assert!(Position::new("log-v1".into(), 0, u64::MAX).is_err());
}

#[test]
fn invalid_trade_sizes_are_rejected_before_decimal_rendering() {
    let mut invalid = trade();
    invalid.price.as_mut().unwrap().scale = u32::MAX;
    assert!(matches!(RingAdapter::prepare(&invalid), Err(InputError::InvalidTrade)));
    invalid = trade();
    invalid.code = "x".repeat(16 * 1024);
    assert!(matches!(RingAdapter::prepare(&invalid), Err(InputError::TooLarge)));
    invalid = trade();
    invalid.traded_at_ms = i64::MAX;
    assert!(matches!(RingAdapter::prepare(&invalid), Err(InputError::InvalidTrade)));
}

#[tokio::test]
async fn ring_and_broker_share_the_hub_and_preserve_the_legacy_body() {
    let hub = Hub::configured(Limits::default(), vec!["/topic/v1/kr/stock/trade/DEMO".into()]);
    let (mut outgoing, _kick) = hub.connect(1, 8).await;
    hub.subscribe(1, "s".into(), "/topic/v1/kr/stock/trade/DEMO".into(), Some("r".into())).await;
    assert!(matches!(outgoing.recv().await, Some(Outgoing::Receipt { .. })));
    let event = event("demo");
    RingAdapter::new(hub.input()).publish(event.trade()).await.unwrap();
    let position = Position::new("log-v1".into(), 0, 9_007_199_254_740_993).unwrap();
    let applied = broker(&hub).publish(&event, position.clone()).await.unwrap();
    assert!(Arc::ptr_eq(&applied.envelope, &event));
    assert_eq!(applied.position, position);
    let expected = crate::trade::to_message(event.trade()).unwrap().1;
    for _ in 0..2 {
        let Some(Outgoing::Message { subscription, body }) = outgoing.recv().await else { panic!("message expected") };
        assert_eq!(&*subscription, "s");
        assert_eq!(body, expected);
    }
    assert_eq!(hub.metrics().await.unwrap().published_total, 2);
    hub.stop().await;
}

#[tokio::test]
async fn broker_rejects_mixed_logs_and_unconfigured_sources_before_publishing() {
    let hub = Hub::spawn();
    let broker = broker(&hub);
    assert_eq!(
        broker.publish(&event("other"), Position::new("log-v1".into(), 0, 1).unwrap()).await.unwrap_err(),
        InputError::UnconfiguredSource
    );
    assert_eq!(
        broker.publish(&event("demo"), Position::new("other-log".into(), 0, 1).unwrap()).await.unwrap_err(),
        InputError::InvalidPosition
    );
    assert!(matches!(
        BrokerAdapter::new(hub.input(), "log-v1".into(), vec![source("demo", "a"), source("demo", "b")]),
        Err(InputError::DuplicateProvider)
    ));
    assert_eq!(hub.metrics().await.unwrap().published_total, 0);
    hub.stop().await;
}

#[tokio::test]
async fn a_full_input_is_bounded_and_cancelled_admission_can_be_retried() {
    let hub = Hub::spawn();
    let release = hub.block_for_test().await;
    let ring = RingAdapter::new(hub.input());
    let event = event("demo");
    let broker = broker(&hub);
    let position = Position::new("log-v1".into(), 0, 1).unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(20), ring.publish(event.trade())).await.is_err());
    assert!(tokio::time::timeout(Duration::from_millis(20), broker.publish(&event, position.clone())).await.is_err());
    release.send(()).unwrap();
    assert_eq!(hub.metrics().await.unwrap().published_total, 1024);
    broker.publish(&event, position).await.unwrap();
    assert_eq!(hub.metrics().await.unwrap().published_total, 1025);
    hub.stop().await;
}

#[tokio::test]
async fn cancellation_after_enqueue_does_not_undo_an_admitted_event() {
    let hub = Hub::spawn();
    let release = hub.pause_for_test().await;
    let event = event("demo");
    let broker = broker(&hub);
    let position = Position::new("log-v1".into(), 0, 1).unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(20), broker.publish(&event, position)).await.is_err());
    release.send(()).unwrap();
    assert_eq!(hub.metrics().await.unwrap().published_total, 1);
    assert_eq!(event.source_seq(), u64::MAX);
    hub.stop().await;
}

#[tokio::test]
async fn a_closed_input_returns_errors_to_both_adapters_and_blocking_producers() {
    let hub = Hub::spawn();
    hub.stop().await;
    let ring = RingAdapter::new(hub.input());
    assert_eq!(ring.publish(&trade()).await.unwrap_err(), InputError::Closed);
    assert_eq!(
        broker(&hub).publish(&event("demo"), Position::new("log-v1".into(), 0, 1).unwrap()).await.unwrap_err(),
        InputError::Closed
    );
    let joined = tokio::task::spawn_blocking(move || ring.enqueue_blocking(&trade()));
    assert_eq!(tokio::time::timeout(Duration::from_secs(1), joined).await.unwrap().unwrap(), Err(InputError::Closed));
}
