#![cfg(feature = "ha")]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use ws_trade_server::ha::*;
use ws_trade_server::source::Envelope;

fn settings() -> (Kafka, SourceConfig, PathBuf) {
    let directory = PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").expect("HA 시험용 설정 경로 필요"));
    let kafka = serde_json::from_slice(&std::fs::read(directory.join("feed.json")).unwrap()).unwrap();
    let config = serde_json::from_slice(&std::fs::read(directory.join("source.json")).unwrap()).unwrap();
    (kafka, config, directory)
}

fn events(kafka: &Kafka, config: &SourceConfig) -> Vec<Envelope> {
    let mut reader = kafka.reader(EVENTS, None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        if let Some(record) = reader.poll(Duration::from_millis(100)).unwrap()
            && let LogRecord::Trade(event) = LogRecord::from_json(&record.payload, config).unwrap()
        {
            events.push(*event);
        }
        if reader.at_end() {
            return events;
        }
    }
    panic!("committed 로그 읽기 deadline");
}

#[test]
#[ignore = "독립된 3-broker 시험 클러스터 필요: deploy/ha/smoke.sh"]
fn synthetic_checkpoint_fencing_and_snapshot_are_atomic() {
    let (kafka, config, directory) = settings();
    let mut guard = || Ok(());
    let missing = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 1000, &mut guard)
        .err()
        .expect("checkpoint가 없어야 한다");
    assert!(matches!(missing, Error::MissingCheckpoint), "{missing:?}");
    let generator = Synthetic::new(&config, 42, 1000).unwrap();
    let mut first =
        Publisher::open(&kafka, config.clone(), Bootstrap::Initialize(Some(generator.clone())), 2000, &mut guard)
            .unwrap();
    let initial = Snapshot::from_json(&kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap(), &config).unwrap();
    assert!(initial.topics.values().all(Option::is_none));
    assert!(!initial.status.unwrap().market_open);
    first.synthetic(20, 3000, true, &mut guard).unwrap();
    let saved = first.checkpoint().to_json().unwrap();
    let mut second = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 4000, &mut guard).unwrap();
    assert_eq!(second.checkpoint().to_json().unwrap(), saved);
    assert!(first.synthetic(10, 4000, true, &mut guard).is_err());
    assert!(matches!(first.synthetic(10, 4000, true, &mut guard), Err(Error::RestartRequired)));
    second.synthetic(10, 4000, true, &mut guard).unwrap();
    let before = second.checkpoint().to_json().unwrap();
    let mut calls = 0;
    let mut lost = || {
        calls += 1;
        if calls == 2 { Err(Error::LeaseLost) } else { Ok(()) }
    };
    assert!(matches!(second.synthetic(5, 5000, true, &mut lost), Err(Error::LeaseLost)));
    assert_eq!(second.checkpoint().to_json().unwrap(), before);
    drop(second);
    let mut resumed = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 5000, &mut guard).unwrap();
    assert_eq!(resumed.checkpoint().to_json().unwrap(), before);
    resumed.synthetic(10, 5000, true, &mut guard).unwrap();
    resumed.synthetic(0, 6000, false, &mut guard).unwrap();
    let recorded = events(&kafka, &config);
    assert_eq!(recorded.len(), 40);
    let mut oracle = generator;
    for (index, event) in recorded.iter().enumerate() {
        assert_eq!(event.source_seq(), index as u64 + 1);
        assert_eq!(event.trade(), oracle.next(&config, event.source_seq(), 0).unwrap().trade());
    }
    assert_eq!(recorded.iter().map(|event| event.event_id()).collect::<BTreeSet<_>>().len(), recorded.len());
    let snapshot = Snapshot::from_json(&kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap(), &config).unwrap();
    assert_eq!(snapshot.through, resumed.checkpoint().snapshot.through);
    assert!(!snapshot.status.unwrap().market_open);
    assert!(snapshot.topics.values().all(Option::is_some));
    let gateway: Kafka = serde_json::from_slice(&std::fs::read(directory.join("gateway.json")).unwrap()).unwrap();
    assert_eq!(events(&gateway, &config).len(), 40);
    assert!(gateway.writer(&config.log_id).is_err());
    let error = gateway.latest(CHECKPOINT, SHARD.as_bytes()).unwrap_err();
    assert!(
        matches!(error, Error::Broker(ref error) if error.rdkafka_error_code() == Some(rdkafka::error::RDKafkaErrorCode::TopicAuthorizationFailed)),
        "{error:?}"
    );
    assert_topic_write_denied(&gateway);
    let saved = kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap();
    let writer = kafka.writer(&config.log_id).unwrap();
    let records = vec![(SHARD.as_bytes().to_vec(), saved.clone()); 64];
    for _ in 0..65 {
        writer.begin().unwrap();
        writer.send(SNAPSHOT, &records).unwrap();
        writer.commit().unwrap();
    }
    assert_eq!(kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap(), saved);
    let stable = kafka.reader(EVENTS, None).unwrap().initial_high();
    writer.begin().unwrap();
    writer
        .send(
            EVENTS,
            &[(
                b"source-status".to_vec(),
                LogRecord::Status(SourceStatus::new(&config, 7000, false)).to_json().unwrap(),
            )],
        )
        .unwrap();
    let mut tail = kafka.reader(EVENTS, Some(stable)).unwrap();
    assert_eq!(tail.initial_high(), stable);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !tail.at_end() && Instant::now() < deadline {
        assert!(tail.poll(Duration::from_millis(50)).unwrap().is_none());
    }
    assert!(tail.at_end());
    assert_eq!(tail.next_offset(), stable);
    writer.abort().unwrap();
}

#[test]
#[ignore = "초기화된 독립 Kafka 시험 클러스터 필요"]
fn recovery_survives_more_than_4096_aborted_positions() {
    let (kafka, config, _) = settings();
    let saved = kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap();
    let checkpoint = kafka.latest(CHECKPOINT, SHARD.as_bytes()).unwrap().unwrap();
    let writer = kafka.writer(&config.log_id).unwrap();
    let records = vec![(SHARD.as_bytes().to_vec(), saved.clone()); 64];
    for _ in 0..65 {
        writer.begin().unwrap();
        writer.send(SNAPSHOT, &records).unwrap();
        writer.abort().unwrap();
    }
    assert_eq!(kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap(), saved);
    let mut large = saved.clone();
    large.resize(128 * 1024, b' ');
    for _ in 0..200 {
        writer.begin().unwrap();
        writer.send(SNAPSHOT, &[(SHARD.as_bytes().to_vec(), large.clone())]).unwrap();
        writer.commit().unwrap();
    }
    for _ in 0..15 {
        writer.begin().unwrap();
        writer.send(SNAPSHOT, &records).unwrap();
        writer.abort().unwrap();
    }
    assert_eq!(kafka.latest(SNAPSHOT, SHARD.as_bytes()).unwrap().unwrap(), large);
    drop(writer);
    let restored = Publisher::open(&kafka, config, Bootstrap::Existing, 8000, &mut || Ok(())).unwrap();
    assert_eq!(restored.checkpoint().to_json().unwrap(), checkpoint);
}

fn assert_topic_write_denied(kafka: &Kafka) {
    use rdkafka::client::ClientContext;
    use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
    struct Probe(std::sync::mpsc::SyncSender<Result<(), rdkafka::error::KafkaError>>);
    impl ClientContext for Probe {
        fn log(&self, _: rdkafka::config::RDKafkaLogLevel, _: &str, _: &str) {}
        fn error(&self, _: rdkafka::error::KafkaError, _: &str) {}
    }
    impl ProducerContext for Probe {
        type DeliveryOpaque = ();
        fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
            let _ = self.0.try_send(result.as_ref().map(|_| ()).map_err(|(error, _)| error.clone()));
        }
    }
    let mut settings = rdkafka::ClientConfig::new();
    for (key, value) in &kafka.properties {
        settings.set(key, value);
    }
    settings.set("message.timeout.ms", "5000").set("enable.idempotence", "false");
    let (reply, received) = std::sync::mpsc::sync_channel(1);
    let writer: ThreadedProducer<Probe> = settings.create_with_context(Probe(reply)).unwrap();
    writer.send(BaseRecord::to(EVENTS).partition(0).key("acl-probe").payload("denied")).unwrap();
    let error = received.recv_timeout(Duration::from_secs(10)).unwrap().unwrap_err();
    assert_eq!(error.rdkafka_error_code(), Some(rdkafka::error::RDKafkaErrorCode::TopicAuthorizationFailed));
}

struct Replay {
    batch: Vec<Envelope>,
    acknowledged: Vec<u64>,
    first_gap: bool,
    lose_ack: bool,
}

impl ExternalSource for Replay {
    fn read_from(&mut self, _: u64, max: usize, _: Instant) -> Result<Vec<Envelope>, Error> {
        if self.first_gap {
            self.first_gap = false;
            return Ok(vec![self.batch[0].clone(), self.batch[2].clone()]);
        }
        Ok(self.batch.iter().take(max).cloned().collect())
    }
    fn acknowledge(&mut self, through: u64) -> Result<(), Error> {
        if self.lose_ack {
            self.lose_ack = false;
            return Err(Error::Deadline);
        }
        self.acknowledged.push(through);
        Ok(())
    }
}

#[test]
#[ignore = "독립된 새 3-broker 시험 클러스터 필요: deploy/ha/smoke.sh"]
fn external_replay_gap_and_lost_ack_use_committed_input_window() {
    let (kafka, config, _) = settings();
    let mut guard = || Ok(());
    let mut publisher = Publisher::open(&kafka, config.clone(), Bootstrap::Initialize(None), 1000, &mut guard).unwrap();
    let mut generator = Synthetic::new(&config, 1, 1000).unwrap();
    let batch = (1..=1025).map(|seq| generator.next(&config, seq, 1000).unwrap()).collect::<Vec<_>>();
    struct Gap {
        records: Vec<Envelope>,
        empty: bool,
        reads: usize,
        acks: usize,
    }
    impl ExternalSource for Gap {
        fn read_from(&mut self, _: u64, _: usize, _: Instant) -> Result<Vec<Envelope>, Error> {
            self.reads += 1;
            if self.empty && self.reads > 1 { Ok(Vec::new()) } else { Ok(self.records.clone()) }
        }
        fn acknowledge(&mut self, _: u64) -> Result<(), Error> {
            self.acks += 1;
            Ok(())
        }
    }
    for empty in [false, true] {
        let before = publisher.checkpoint().to_json().unwrap();
        let mut gap = Gap { records: vec![batch[0].clone(), batch[2].clone()], empty, reads: 0, acks: 0 };
        let result =
            pump_external(&mut publisher, &mut gap, 2000, true, Instant::now() + Duration::from_millis(50), &mut guard);
        assert!(matches!(result, Err(Error::SourceGap)));
        assert!(gap.reads > 1);
        assert_eq!(gap.acks, 0);
        assert_eq!(publisher.checkpoint().to_json().unwrap(), before);
        assert_eq!(kafka.latest(CHECKPOINT, SHARD.as_bytes()).unwrap().unwrap(), before);
    }
    let mut source = Replay { batch: batch[..3].to_vec(), acknowledged: Vec::new(), first_gap: true, lose_ack: true };
    assert!(matches!(
        pump_external(&mut publisher, &mut source, 2000, true, Instant::now() + Duration::from_secs(15), &mut guard),
        Err(Error::Deadline)
    ));
    assert!(source.acknowledged.is_empty());
    assert_eq!(publisher.checkpoint().last_seq(), Some(3));
    drop(publisher);
    let mut publisher = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 3000, &mut guard).unwrap();
    let committed =
        pump_external(&mut publisher, &mut source, 3000, true, Instant::now() + Duration::from_secs(15), &mut guard)
            .unwrap();
    assert_eq!(committed.new_events, 0);
    assert_eq!(committed.replays, 3);
    assert_eq!(source.acknowledged, vec![3]);
    for chunk in batch[3..].chunks(100) {
        publisher.external(chunk, 4000, true, &mut guard).unwrap();
    }
    assert!(matches!(publisher.external(&batch[..1], 5000, true, &mut guard), Err(Error::DedupeWindowExpired)));
    let mut changed: serde_json::Value = serde_json::to_value(&batch[1024]).unwrap();
    changed["trade"].as_array_mut().unwrap().extend([0xa0, 0x06, 0x01].map(serde_json::Value::from));
    let conflicting = Envelope::from_json(&serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(matches!(publisher.external(&[conflicting], 5000, true, &mut guard), Err(Error::ConflictingInput)));
    let recorded = events(&kafka, &config);
    assert_eq!(recorded.len(), 1025);
    for (expected, actual) in batch.iter().zip(recorded) {
        assert_eq!(expected.event_id(), actual.event_id());
        assert_eq!(expected.trade(), actual.trade());
    }
}

struct Faults {
    project: String,
    disconnected: Option<(String, String, String)>,
}

impl Faults {
    fn compose(&self, args: &[&str]) -> std::process::Output {
        std::process::Command::new("docker")
            .args(["compose", "-f", "deploy/ha/compose.yaml", "-p", &self.project])
            .args(args)
            .output()
            .unwrap()
    }
    fn run(&self, args: &[&str]) {
        let output = self.compose(args);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }
    fn restore(&mut self) {
        if let Some((container, service, ip)) = &self.disconnected {
            let status = std::process::Command::new("docker")
                .args([
                    "network",
                    "connect",
                    "--alias",
                    service,
                    "--ip",
                    ip,
                    &format!("{}_default", self.project),
                    container,
                ])
                .status()
                .unwrap();
            assert!(status.success());
            self.disconnected = None;
        }
        self.run(&["up", "-d", "--wait", "--wait-timeout", "120"]);
    }
}

impl Drop for Faults {
    fn drop(&mut self) {
        if let Some((container, service, ip)) = self.disconnected.take() {
            let _ = std::process::Command::new("docker")
                .args([
                    "network",
                    "connect",
                    "--alias",
                    &service,
                    "--ip",
                    &ip,
                    &format!("{}_default", self.project),
                    &container,
                ])
                .output();
        }
        let _ = self.compose(&["start"]);
    }
}

#[test]
#[ignore = "시험 전용 Docker 프로젝트를 중단/분할한다: deploy/ha/smoke.sh"]
fn replicated_log_survives_broker_loss_and_rejects_no_quorum() {
    assert_eq!(std::env::var("WTS_HA_ALLOW_FAULTS").unwrap(), "owned-test-cluster");
    let project = std::env::var("COMPOSE_PROJECT_NAME").unwrap();
    assert!(project.starts_with("wts-ha") && project.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
    let mut faults = Faults { project, disconnected: None };
    let (kafka, config, _) = settings();
    let mut guard = || Ok(());
    faults.run(&["stop", "-t", "1", "kafka3"]);
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut publisher = loop {
        assert!(Instant::now() < deadline, "한 broker 손실 후 복구 deadline");
        if let Ok(mut publisher) = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 7000, &mut guard)
            && publisher.synthetic(5, 7000, true, &mut guard).is_ok()
        {
            break publisher;
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    let before = publisher.checkpoint().last_seq();
    let container = String::from_utf8(faults.compose(&["ps", "-q", "kafka2"]).stdout).unwrap().trim().to_owned();
    assert!(!container.is_empty());
    let network = format!("{}_default", faults.project);
    let inspected = std::process::Command::new("docker")
        .args(["inspect", "--format", "{{json .NetworkSettings.Networks}}", &container])
        .output()
        .unwrap();
    assert!(inspected.status.success());
    let networks: serde_json::Value = serde_json::from_slice(&inspected.stdout).unwrap();
    let ip = networks[&network]["IPAddress"].as_str().unwrap().to_owned();
    assert!(ip.parse::<std::net::Ipv4Addr>().is_ok());
    assert!(
        std::process::Command::new("docker")
            .args(["network", "disconnect", &network, &container])
            .status()
            .unwrap()
            .success()
    );
    // 같은 IP로 복원해 다른 broker의 DNS cache가 다른 인증서를 가진 피어를 가리키지 않게 한다.
    faults.disconnected = Some((container, "kafka2".into(), ip));
    assert!(publisher.synthetic(5, 8000, true, &mut guard).is_err());
    assert_eq!(publisher.checkpoint().last_seq(), before);
    drop(publisher);
    faults.restore();
    let mut resumed = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 9000, &mut guard).unwrap();
    assert_eq!(resumed.checkpoint().last_seq(), before);
    resumed.synthetic(5, 9000, true, &mut guard).unwrap();
    assert_eq!(resumed.checkpoint().last_seq(), before.map(|seq| seq + 5));
    let before = resumed.checkpoint().last_seq();
    drop(resumed);
    assert_eq!(audit(&kafka, &config).unwrap().source_seq, before.map(Number));
    faults.run(&["stop", "-t", "3"]);
    // 수동 network reconnect의 IP 지정은 지우고, 같은 volume으로 broker 컨테이너를 새로 시작한다.
    faults.run(&["up", "-d", "--force-recreate", "--wait", "--wait-timeout", "120"]);
    let restored = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 10000, &mut guard).unwrap();
    assert_eq!(restored.checkpoint().last_seq(), before);
    assert_eq!(audit(&kafka, &config).unwrap().source_seq, before.map(Number));
}
