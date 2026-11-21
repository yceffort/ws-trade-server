use super::{EVENTS, Error, Kafka, LogRecord, Number, SHARD, SNAPSHOT, Snapshot, SourceConfig, digest};
use crate::hub::Hub;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

pub const MAX_LAG: u64 = 100;

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
    pub log_end: Option<u64>,
    pub failed: bool,
    broker_seen: Option<Instant>,
    log_seen: Option<Instant>,
    source_seen: Instant,
}

impl ReplicaView {
    pub fn ready(&self, policy_fresh: bool) -> bool {
        let now = now_ms();
        !self.failed
            && self.caught_up
            && policy_fresh
            && self.broker_seen.is_some_and(|seen| seen.elapsed() < FRESHNESS)
            && self.log_seen.is_some_and(|seen| seen.elapsed() < FRESHNESS)
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
    initialized: bool,
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
            initialized: false,
            seen,
            view: ReplicaView {
                snapshot: Arc::new(snapshot),
                next,
                caught_up: false,
                log_end: None,
                failed: false,
                broker_seen: None,
                log_seen: None,
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
        self.view.caught_up =
            self.initialized && self.view.log_end.is_some_and(|end| end.saturating_sub(self.view.next) <= MAX_LAG);
        self.view.broker_seen = Some(Instant::now());
        self.seen.push_back((offset, hash));
        if self.seen.len() > 1024 {
            self.seen.pop_front();
        }
        Ok(true)
    }

    pub fn boundary(&mut self, next: u64, log_end: u64) -> Result<(), Error> {
        if next < self.view.next || next > i64::MAX as u64 {
            return Err(Error::InvalidState);
        }
        if next > self.view.next {
            Arc::make_mut(&mut self.view.snapshot).through = Some(Number(next - 1));
            self.view.next = next;
        }
        self.view.log_end = Some(log_end);
        self.initialized |= next >= log_end;
        self.view.caught_up = self.initialized && log_end.saturating_sub(next) <= MAX_LAG;
        Ok(())
    }

    pub fn observed(&mut self, at: Instant) {
        self.view.broker_seen = Some(at);
        self.view.log_seen = Some(at);
    }

    pub fn fail(&mut self) {
        self.view.failed = true;
    }
}

pub struct ReplicaTask {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), Error>>>,
    failed: Arc<AtomicBool>,
}

impl ReplicaTask {
    pub fn spawn(input: ReplicaInput, hub: Hub) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let failure = failed.clone();
        let runtime = tokio::runtime::Handle::current();
        let worker = tokio::task::spawn_blocking(move || {
            loop {
                if stopped.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let result = (|| {
                    let snapshot = input.kafka.latest(SNAPSHOT, SHARD.as_bytes())?.ok_or(Error::MissingCheckpoint)?;
                    if stopped.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    let snapshot = Snapshot::from_json(&snapshot, &input.source)?;
                    let next = snapshot.through.ok_or(Error::InvalidState)?.0.checked_add(1).ok_or(Error::Exhausted)?;
                    let mut reader = input.kafka.reader_catchup(EVENTS, next)?;
                    if stopped.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    let mut log_end = reader.initial_high();
                    let mut sampled = Instant::now();
                    runtime.block_on(hub.restore_replica(input.source.clone(), snapshot))?;
                    failure.store(false, Ordering::Relaxed);
                    runtime.block_on(hub.replica_observation(next, log_end, sampled))?;
                    while !stopped.load(Ordering::Relaxed) {
                        if sampled.elapsed() >= Duration::from_millis(250) {
                            match reader.refresh_end() {
                                Ok(end) => {
                                    sampled = Instant::now();
                                    log_end = end;
                                    runtime.block_on(hub.replica_observation(
                                        reader.next_offset(),
                                        log_end,
                                        Instant::now(),
                                    ))?;
                                }
                                Err(Error::Deadline) => {
                                    std::thread::sleep(Duration::from_millis(50));
                                    continue;
                                }
                                Err(Error::Broker(error)) if super::broker::transient(&error) => {
                                    std::thread::sleep(Duration::from_millis(50));
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        if let Some(stored) = reader.poll(Duration::from_millis(50))? {
                            let record = LogRecord::from_json(&stored.payload, &input.source)?;
                            runtime.block_on(hub.apply_replica(stored.offset, record))?;
                        }
                        runtime.block_on(hub.replica_boundary(reader.next_offset(), log_end))?;
                    }
                    Ok(())
                })();
                match result {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        failure.store(true, Ordering::Relaxed);
                        runtime.block_on(hub.fail_replica());
                        if !matches!(error, Error::Deadline | Error::Broker(_)) {
                            return Err(error);
                        }
                        for _ in 0..20 {
                            if stopped.load(Ordering::Relaxed) {
                                return Ok(());
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
        });
        Self { stop, worker: Some(worker), failed }
    }

    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
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
