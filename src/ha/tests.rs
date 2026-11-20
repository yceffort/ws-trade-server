use super::*;
use crate::source::{Envelope, ReceivedAtMs, SourceId, SourceSeq};

fn config() -> SourceConfig {
    SourceConfig {
        log_id: "test-log".into(),
        source_id: SourceId::new("demo".into(), "trades".into()).unwrap(),
        epoch: "epoch-1".into(),
        start_seq: Number(1),
        topics: vec!["/topic/v1/kr/stock/trade/DEMO".into(), "/topic/v1/us/stock/trade/DEMO".into()],
    }
}

#[test]
#[ignore = "외부 입력 시험 뒤의 실제 broker 필요: deploy/ha/smoke.sh"]
fn unclear_commit_result_restores_committed_input() {
    let directory = std::path::PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").unwrap());
    let kafka: Kafka = serde_json::from_slice(&std::fs::read(directory.join("feed.json")).unwrap()).unwrap();
    let config: SourceConfig = serde_json::from_slice(&std::fs::read(directory.join("source.json")).unwrap()).unwrap();
    let mut guard = || Ok(());
    let mut publisher = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 2000, &mut guard).unwrap();
    let before = publisher.checkpoint().to_json().unwrap();
    let next = publisher.checkpoint().next_seq().unwrap();
    let offset = publisher.checkpoint().snapshot.through.unwrap().0 + 1;
    let mut generator = Synthetic::new(&config, 5, 1000).unwrap();
    let input = (next..next + 5).map(|seq| generator.next(&config, seq, 2000).unwrap()).collect::<Vec<_>>();
    publisher.writer.lose_commit_result = true;
    assert!(matches!(publisher.external(&input, 2000, true, &mut guard), Err(Error::Deadline)));
    assert_eq!(publisher.checkpoint().to_json().unwrap(), before);
    assert!(matches!(publisher.external(&input, 2000, true, &mut guard), Err(Error::RestartRequired)));
    drop(publisher);
    let mut restored = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 3000, &mut guard).unwrap();
    assert_eq!(restored.checkpoint().last_seq(), Some(next + 4));
    let replay = restored.external(&input, 3000, true, &mut guard).unwrap();
    assert_eq!((replay.new_events, replay.replays), (0, 5));
    assert_eq!(audit(&kafka, &config).unwrap().source_seq, Some(Number(next + 4)));
    let mut reader = kafka.reader(EVENTS, Some(offset)).unwrap();
    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if let Some(record) = reader.poll(std::time::Duration::from_millis(50)).unwrap()
            && let LogRecord::Trade(event) = LogRecord::from_json(&record.payload, &config).unwrap()
        {
            records.push(event);
        }
        if reader.at_end() {
            break;
        }
    }
    assert!(reader.at_end());
    assert_eq!(records.len(), input.len());
    for (record, expected) in records.iter().zip(input) {
        assert_eq!(record.event_id(), expected.event_id());
        assert_eq!(record.to_json().unwrap(), expected.to_json().unwrap());
    }
}

#[tokio::test]
async fn replica_restores_no_data_and_applies_cache_and_position_once() {
    let config = config();
    let mut snapshot = Snapshot::empty(&config).unwrap();
    snapshot.apply(&LogRecord::Status(SourceStatus::new(&config, replica::now_ms(), false)), 0, &config).unwrap();
    let hub = crate::hub::Hub::spawn();
    hub.restore_replica(config.clone(), snapshot).await.unwrap();
    let initial = hub.replica_view().await.unwrap();
    assert!(!initial.ready(true));
    assert!(initial.snapshot.topics.values().all(Option::is_none));
    let (mut queue, _kick) = hub.connect(1, 8).await;
    hub.subscribe(1, "sub".into(), config.topics[0].clone(), Some("r".into())).await;
    assert!(matches!(queue.recv().await, Some(crate::hub::Outgoing::Receipt { .. })));
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let event = generator.next(&config, 1, 2000).unwrap();
    let record = LogRecord::Trade(Box::new(event.clone()));
    assert!(hub.apply_replica(3, record.clone()).await.unwrap());
    assert!(!hub.apply_replica(3, record).await.unwrap());
    assert!(matches!(queue.recv().await, Some(crate::hub::Outgoing::Message { .. })));
    assert!(queue.try_recv().is_err());
    let view = hub.replica_view().await.unwrap();
    assert_eq!(view.next, 4);
    assert_eq!(view.snapshot.through, Some(Number(3)));
    assert!(view.snapshot.topics[&config.topics[1]].is_none());
    let cache: serde_json::Value = serde_json::from_str(&hub.snapshot().await.unwrap()).unwrap();
    assert_eq!(cache[&config.topics[0]]["updated_at_ms"], 2000);
    assert_eq!(cache[&config.topics[0]]["body"].as_str().unwrap(), event.message().1.as_ref());
    hub.stop().await;
}

#[test]
fn replica_readiness_uses_source_status_policy_and_catchup_not_trade_time() {
    let config = config();
    let mut snapshot = Snapshot::empty(&config).unwrap();
    snapshot.apply(&LogRecord::Status(SourceStatus::new(&config, replica::now_ms(), false)), 0, &config).unwrap();
    let mut replica = Replica::restore(config.clone(), snapshot).unwrap();
    let status = |at| LogRecord::Status(SourceStatus::new(&config, at, false));
    replica.apply(2, &status(replica::now_ms())).unwrap();
    assert!(!replica.view().ready(true));
    replica.boundary(4, true).unwrap();
    assert!(replica.view().ready(true));
    assert!(!replica.view().ready(false));
    assert!(replica.view().snapshot.topics.values().all(Option::is_none));
    replica.apply(4, &status(1000)).unwrap();
    assert!(!replica.view().ready(true));
    replica.apply(5, &status(replica::now_ms() + 10000)).unwrap();
    assert!(!replica.view().ready(true));
    replica.apply(6, &status(replica::now_ms())).unwrap();
    assert!(replica.view().ready(true));
    replica.fail();
    assert!(!replica.view().ready(true));
}

#[tokio::test]
async fn replica_rejects_gap_without_advancing_or_publishing_partial_state() {
    let config = config();
    let mut snapshot = Snapshot::empty(&config).unwrap();
    snapshot.apply(&LogRecord::Status(SourceStatus::new(&config, replica::now_ms(), true)), 0, &config).unwrap();
    let hub = crate::hub::Hub::spawn();
    hub.restore_replica(config.clone(), snapshot).await.unwrap();
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let event = generator.next(&config, 2, 2000).unwrap();
    assert!(matches!(hub.apply_replica(3, LogRecord::Trade(Box::new(event))).await, Err(Error::SourceGap)));
    let view = hub.replica_view().await.unwrap();
    assert!(view.failed);
    assert_eq!(view.next, 1);
    assert!(view.snapshot.topics.values().all(Option::is_none));
    assert_eq!(hub.snapshot().await.unwrap(), "{}");
    hub.stop().await;
}

fn applied(state: &mut Checkpoint, event: &Envelope, offset: u64) {
    assert_eq!(state.stage(event).unwrap(), Disposition::New);
    state.snapshot.apply(&LogRecord::Trade(Box::new(event.clone())), offset, &state.config).unwrap();
    state
        .snapshot
        .apply(&LogRecord::Status(SourceStatus::new(&state.config, 1000, true)), offset + 1, &state.config)
        .unwrap();
}

#[test]
fn input_window_survives_restore_and_rejects_conflicts_and_old_replay() {
    let config = config();
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let mut checkpoint = Checkpoint::new(config.clone(), None).unwrap();
    let mut input = Vec::new();
    for seq in 1..=1025 {
        let event = generator.next(&config, seq, 2000).unwrap();
        applied(&mut checkpoint, &event, seq * 3);
        input.push(event);
    }
    let bytes = checkpoint.to_json().unwrap();
    assert!(bytes.len() <= MAX_STATE_BYTES);
    let mut restored = Checkpoint::from_json(&bytes, &config).unwrap();
    assert!(matches!(restored.stage(&input[0]), Err(Error::DedupeWindowExpired)));
    assert_eq!(restored.stage(&input[1]).unwrap(), Disposition::Replay);
    let original = &input[1024];
    let changed_clock = Envelope::new(
        config.log_id.clone(),
        config.source_id.clone(),
        config.epoch.clone(),
        SourceSeq(1025),
        original.trade().clone(),
        ReceivedAtMs(9999),
    )
    .unwrap();
    assert_eq!(restored.stage(&changed_clock).unwrap(), Disposition::Replay);
    let mut changed_trade = original.trade().clone();
    changed_trade.volume += 1;
    let changed = Envelope::new(
        config.log_id.clone(),
        config.source_id.clone(),
        config.epoch.clone(),
        SourceSeq(1025),
        changed_trade,
        ReceivedAtMs(2000),
    )
    .unwrap();
    assert!(matches!(restored.stage(&changed), Err(Error::ConflictingInput)));
    assert_eq!(restored.last_seq(), Some(1025));
}

#[test]
fn gaps_and_epoch_changes_leave_the_committed_input_position_unchanged() {
    let config = config();
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let mut checkpoint = Checkpoint::new(config.clone(), None).unwrap();
    let first = generator.next(&config, 1, 2000).unwrap();
    applied(&mut checkpoint, &first, 0);
    let before = checkpoint.to_json().unwrap();
    let gap = generator.next(&config, 3, 2000).unwrap();
    assert!(matches!(checkpoint.stage(&gap), Err(Error::SourceGap)));
    let changed = Envelope::new(
        config.log_id.clone(),
        config.source_id.clone(),
        "epoch-2".into(),
        SourceSeq(2),
        first.trade().clone(),
        ReceivedAtMs(2000),
    )
    .unwrap();
    assert!(matches!(checkpoint.stage(&changed), Err(Error::SourceGap)));
    assert_eq!(checkpoint.to_json().unwrap(), before);
}

#[test]
fn generator_checkpoint_replays_every_uncommitted_draw_and_cumulative_value() {
    let config = config();
    let mut state = Checkpoint::new(config.clone(), Some(Synthetic::new(&config, 42, 1000).unwrap())).unwrap();
    for seq in 1..=20 {
        let event = state.generator.as_mut().unwrap().next(&config, seq, 2000).unwrap();
        applied(&mut state, &event, seq * 3);
    }
    let mut restored = Checkpoint::from_json(&state.to_json().unwrap(), &config).unwrap();
    for seq in 21..=120 {
        let original = state.generator.as_mut().unwrap().next(&config, seq, 2000).unwrap();
        let replay = restored.generator.as_mut().unwrap().next(&config, seq, 2000).unwrap();
        assert_eq!(original.to_json().unwrap(), replay.to_json().unwrap());
    }
}

#[test]
fn checkpoint_rejects_identity_change_corruption_and_oversize_before_start() {
    let config = config();
    let mut state = Checkpoint::new(config.clone(), None).unwrap();
    state.snapshot.apply(&LogRecord::Status(SourceStatus::new(&config, 1000, false)), 0, &config).unwrap();
    let encoded = state.to_json().unwrap();
    let mut renamed = config.clone();
    renamed.source_id = SourceId::new("renamed".into(), "trades".into()).unwrap();
    assert!(matches!(Checkpoint::from_json(&encoded, &renamed), Err(Error::IdentityMismatch)));
    for (field, value) in [
        ("hash_schema", serde_json::json!("future")),
        ("schema_version", serde_json::json!(2)),
        ("last_seq", serde_json::json!("100")),
    ] {
        let mut wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        wire[field] = value;
        assert!(Checkpoint::from_json(&serde_json::to_vec(&wire).unwrap(), &config).is_err());
    }
    assert!(matches!(Checkpoint::from_json(&vec![b' '; MAX_STATE_BYTES + 1], &config), Err(Error::TooLarge)));
}

#[test]
fn snapshot_preserves_no_data_and_accepts_broker_control_record_gaps() {
    let config = config();
    let mut snapshot = Snapshot::empty(&config).unwrap();
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let event = generator.next(&config, 1, 2000).unwrap();
    snapshot.apply(&LogRecord::Trade(Box::new(event)), 4, &config).unwrap();
    snapshot.apply(&LogRecord::Status(SourceStatus::new(&config, 3000, false)), 9, &config).unwrap();
    let restored = Snapshot::from_json(&snapshot.to_json().unwrap(), &config).unwrap();
    assert!(restored.topics[&config.topics[1]].is_none());
    assert_eq!(restored.topics[&config.topics[0]].as_ref().unwrap().offset, Number(4));
    assert_eq!(restored.through, Some(Number(9)));
    assert!(!restored.status.unwrap().market_open);
}

#[test]
fn sequence_json_keeps_64_bit_precision() {
    let value = Number(u64::MAX);
    assert_eq!(serde_json::to_string(&value).unwrap(), format!("\"{}\"", u64::MAX));
    for invalid in ["18446744073709551615", "\"01\"", "\"+1\"", "\"18446744073709551616\""] {
        assert!(serde_json::from_str::<Number>(invalid).is_err());
    }
}
