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
    assert!(gateway.latest(CHECKPOINT, SHARD.as_bytes()).is_err());
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
    disconnected: Option<String>,
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
        if let Some(container) = self.disconnected.take() {
            let status = std::process::Command::new("docker")
                .args(["network", "connect", &format!("{}_default", self.project), &container])
                .status()
                .unwrap();
            assert!(status.success());
        }
        self.run(&["up", "-d", "--wait", "--wait-timeout", "120"]);
    }
}

impl Drop for Faults {
    fn drop(&mut self) {
        if let Some(container) = self.disconnected.take() {
            let _ = std::process::Command::new("docker")
                .args(["network", "connect", &format!("{}_default", self.project), &container])
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
    assert!(
        std::process::Command::new("docker")
            .args(["network", "disconnect", &network, &container])
            .status()
            .unwrap()
            .success()
    );
    faults.disconnected = Some(container);
    assert!(publisher.synthetic(5, 8000, true, &mut guard).is_err());
    assert_eq!(publisher.checkpoint().last_seq(), before);
    drop(publisher);
    faults.restore();
    let resumed = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 9000, &mut guard).unwrap();
    assert_eq!(resumed.checkpoint().last_seq(), before);
    drop(resumed);
    assert_eq!(audit(&kafka, &config).unwrap().source_seq, before.map(Number));
    faults.run(&["stop", "-t", "3"]);
    faults.restore();
    let restored = Publisher::open(&kafka, config.clone(), Bootstrap::Existing, 10000, &mut guard).unwrap();
    assert_eq!(restored.checkpoint().last_seq(), before);
    assert_eq!(audit(&kafka, &config).unwrap().source_seq, before.map(Number));
}
