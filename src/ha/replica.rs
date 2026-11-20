use super::{EVENTS, Error, Kafka, LogRecord, Number, SHARD, SNAPSHOT, Snapshot, SourceConfig, digest};
use crate::hub::Hub;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

pub const FRESHNESS: Duration = Duration::from_secs(3);

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Clone)]
pub struct ReplicaInput {
    pub kafka: Kafka,
    pub source: SourceConfig,
}

#[derive(Clone, Debug)]
pub struct ReplicaView {
    pub snapshot: Arc<Snapshot>,
    pub next: u64,
    pub caught_up: bool,
    pub failed: bool,
    broker_seen: Option<Instant>,
    source_seen: Instant,
}

impl ReplicaView {
    pub fn ready(&self, policy_fresh: bool) -> bool {
        let now = now_ms();
        !self.failed
            && self.caught_up
            && policy_fresh
            && self.broker_seen.is_some_and(|seen| seen.elapsed() < FRESHNESS)
            && self.source_seen.elapsed() < FRESHNESS
            && self.snapshot.status.as_ref().is_some_and(|status| {
                status.connected
                    && status.at_ms.0 <= now.saturating_add(1000)
                    && now.saturating_sub(status.at_ms.0) < FRESHNESS.as_millis() as u64
            })
    }
}

pub(crate) struct Replica {
    config: SourceConfig,
    view: ReplicaView,
    last_seq: Option<u64>,
    seen: VecDeque<(u64, String)>,
}

impl Replica {
    pub fn restore(config: SourceConfig, snapshot: Snapshot) -> Result<Self, Error> {
        config.validate()?;
        snapshot.validate(&config)?;
        let next = snapshot.through.ok_or(Error::InvalidState)?.0.checked_add(1).ok_or(Error::Exhausted)?;
        if snapshot.status.is_none() {
            return Err(Error::InvalidState);
        }
        let last_seq = snapshot.topics.values().flatten().map(|latest| latest.event.source_seq()).max();
        let mut seen = VecDeque::new();
        for latest in snapshot.topics.values().flatten() {
            seen.push_back((latest.offset.0, digest(&LogRecord::Trade(Box::new(latest.event.clone())).to_json()?)));
        }
        Ok(Self {
            config,
            last_seq,
            seen,
            view: ReplicaView {
                snapshot: Arc::new(snapshot),
                next,
                caught_up: false,
                failed: false,
                broker_seen: None,
                source_seen: Instant::now(),
            },
        })
    }

    pub fn view(&self) -> ReplicaView {
        self.view.clone()
    }

    pub fn apply(&mut self, offset: u64, record: &LogRecord) -> Result<bool, Error> {
        if self.view.failed {
            return Err(Error::ReplicaFailed);
        }
        let hash = digest(&record.to_json()?);
        if offset < self.view.next {
            return if self.seen.iter().any(|(old, value)| *old == offset && *value == hash) {
                Ok(false)
            } else {
                Err(Error::ConflictingInput)
            };
        }
        if let LogRecord::Trade(event) = record {
            let expected =
                self.last_seq.map_or(Ok(self.config.start_seq.0), |seq| seq.checked_add(1).ok_or(Error::Exhausted))?;
            if event.source_seq() != expected {
                return Err(Error::SourceGap);
            }
        }
        // 검증 실패로 부분 상태를 남기지 않고, 공개 map과 위치를 하나의 hub 명령에서 교체한다.
        let mut snapshot = self.view.snapshot.as_ref().clone();
        snapshot.apply(record, offset, &self.config)?;
        snapshot.validate(&self.config)?;
        if let LogRecord::Trade(event) = record {
            self.last_seq = Some(event.source_seq());
        }
        if matches!(record, LogRecord::Status(_)) {
            self.view.source_seen = Instant::now();
        }
        self.view.snapshot = Arc::new(snapshot);
        self.view.next = offset.checked_add(1).ok_or(Error::Exhausted)?;
        self.view.broker_seen = Some(Instant::now());
        self.seen.push_back((offset, hash));
        if self.seen.len() > 1024 {
            self.seen.pop_front();
        }
        Ok(true)
    }

    pub fn boundary(&mut self, next: u64, caught_up: bool) -> Result<(), Error> {
        if next < self.view.next || next > i64::MAX as u64 {
            return Err(Error::InvalidState);
        }
        if next > self.view.next {
            Arc::make_mut(&mut self.view.snapshot).through = Some(Number(next - 1));
            self.view.next = next;
        }
        self.view.caught_up |= caught_up;
        Ok(())
    }

    pub fn fail(&mut self) {
        self.view.failed = true;
    }
}

pub struct ReplicaTask {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), Error>>>,
}

impl ReplicaTask {
    pub fn spawn(input: ReplicaInput, hub: Hub) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let runtime = tokio::runtime::Handle::current();
        let worker = tokio::task::spawn_blocking(move || {
            let result = (|| {
                let snapshot = input.kafka.latest(SNAPSHOT, SHARD.as_bytes())?.ok_or(Error::MissingCheckpoint)?;
                let snapshot = Snapshot::from_json(&snapshot, &input.source)?;
                let next = snapshot.through.ok_or(Error::InvalidState)?.0.checked_add(1).ok_or(Error::Exhausted)?;
                let mut reader = input.kafka.reader(EVENTS, Some(next))?;
                let barrier = reader.initial_high();
                runtime.block_on(hub.restore_replica(input.source.clone(), snapshot))?;
                while !stopped.load(Ordering::Relaxed) {
                    if let Some(stored) = reader.poll(Duration::from_millis(50))? {
                        let record = LogRecord::from_json(&stored.payload, &input.source)?;
                        runtime.block_on(hub.apply_replica(stored.offset, record))?;
                    }
                    if reader.at_end() || reader.next_offset() >= barrier {
                        runtime.block_on(hub.replica_boundary(reader.next_offset(), true))?;
                    }
                }
                Ok(())
            })();
            if result.is_err() {
                runtime.block_on(hub.fail_replica());
            }
            result
        });
        Self { stop, worker: Some(worker) }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub async fn join(mut self) -> Result<(), Error> {
        self.stop();
        self.worker.take().ok_or(Error::ReplicaFailed)?.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub async fn wait_ready(&self, hub: &Hub, timeout: Duration) -> Result<(), Error> {
        tokio::time::timeout(timeout, async {
            loop {
                if self.worker.as_ref().is_none_or(JoinHandle::is_finished) {
                    return Err(Error::ReplicaFailed);
                }
                if hub.replica_view().await.is_ok_and(|view| view.ready(true)) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}

impl Drop for ReplicaTask {
    fn drop(&mut self) {
        self.stop();
    }
}
