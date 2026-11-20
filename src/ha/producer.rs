use super::{
    CHECKPOINT, Checkpoint, Disposition, EVENTS, Error, Kafka, LogRecord, SHARD, SNAPSHOT, SourceConfig, SourceStatus,
    Synthetic, Writer,
};
use crate::source::Envelope;

pub enum Bootstrap {
    Existing,
    Initialize(Option<Synthetic>),
}

pub struct Committed {
    pub source_seq: Option<u64>,
    pub through: u64,
    pub new_events: usize,
    pub replays: usize,
}

pub struct Publisher {
    writer: Writer,
    state: Checkpoint,
    poisoned: bool,
}

impl Publisher {
    pub fn open(
        kafka: &Kafka,
        config: SourceConfig,
        bootstrap: Bootstrap,
        now_ms: u64,
        guard: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Self, Error> {
        config.validate()?;
        guard()?;
        // 잘못 실행한 초기화 명령이 정상 writer를 fence하지 않도록 먼저 기존 상태를 확인한다.
        if matches!(bootstrap, Bootstrap::Initialize(_)) && kafka.latest(CHECKPOINT, SHARD.as_bytes())?.is_some() {
            return Err(Error::InvalidConfig);
        }
        let writer = kafka.writer(&config.log_id)?;
        guard()?;
        let restored = kafka.latest(CHECKPOINT, SHARD.as_bytes())?;
        let (state, initialize) = match (restored, bootstrap) {
            (Some(bytes), Bootstrap::Existing) => (Checkpoint::from_json(&bytes, &config)?, false),
            (Some(_), Bootstrap::Initialize(_)) => return Err(Error::InvalidConfig),
            (None, Bootstrap::Existing) => return Err(Error::MissingCheckpoint),
            (None, Bootstrap::Initialize(generator)) => (Checkpoint::new(config, generator)?, true),
        };
        guard()?;
        let mut publisher = Self { writer, state, poisoned: false };
        if initialize {
            publisher.synthetic(0, now_ms, false, guard)?;
        }
        Ok(publisher)
    }

    pub fn checkpoint(&self) -> &Checkpoint {
        &self.state
    }

    pub fn external(
        &mut self,
        events: &[Envelope],
        now_ms: u64,
        market_open: bool,
        guard: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        self.usable()?;
        if self.state.generator.is_some() || events.len() > 100 {
            return Err(Error::InvalidConfig);
        }
        let mut candidate = self.state.clone();
        let mut records = Vec::new();
        let mut replays = 0;
        for event in events {
            match candidate.stage(event)? {
                Disposition::New => records.push(LogRecord::Trade(Box::new(event.clone()))),
                Disposition::Replay => replays += 1,
            }
        }
        self.commit(candidate, records, replays, now_ms, market_open, guard)
    }

    pub fn synthetic(
        &mut self,
        count: usize,
        now_ms: u64,
        market_open: bool,
        guard: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        self.usable()?;
        if count > 100 || (count > 0 && !market_open) {
            return Err(Error::InvalidConfig);
        }
        let mut candidate = self.state.clone();
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let seq = candidate.next_seq()?;
            let event =
                candidate.generator.as_mut().ok_or(Error::InvalidConfig)?.next(&candidate.config, seq, now_ms)?;
            candidate.stage(&event)?;
            records.push(LogRecord::Trade(Box::new(event)));
        }
        self.commit(candidate, records, 0, now_ms, market_open, guard)
    }

    fn usable(&self) -> Result<(), Error> {
        if self.poisoned { Err(Error::RestartRequired) } else { Ok(()) }
    }

    fn commit(
        &mut self,
        mut candidate: Checkpoint,
        mut records: Vec<LogRecord>,
        replays: usize,
        now_ms: u64,
        market_open: bool,
        guard: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        guard()?;
        let new_events = records.len();
        records.push(LogRecord::Status(SourceStatus::new(&candidate.config, now_ms, market_open)));
        let encoded = records
            .iter()
            .map(|record| {
                let key = match record {
                    LogRecord::Trade(event) => event.event_id().as_bytes(),
                    LogRecord::Status(_) => b"source-status",
                };
                Ok((key.to_vec(), record.to_json()?))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut committing = false;
        let result = (|| {
            self.writer.begin()?;
            let offsets = self.writer.send(EVENTS, &encoded)?;
            for (record, offset) in records.iter().zip(offsets) {
                candidate.snapshot.apply(record, offset, &candidate.config)?;
            }
            candidate.snapshot.validate(&candidate.config)?;
            let checkpoint = candidate.to_json()?;
            self.writer.send(SNAPSHOT, &[(SHARD.as_bytes().to_vec(), candidate.snapshot.to_json()?)])?;
            self.writer.send(CHECKPOINT, &[(SHARD.as_bytes().to_vec(), checkpoint)])?;
            guard()?;
            committing = true;
            self.writer.commit()?;
            Ok::<(), Error>(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            // commit 응답 유실은 abort로 추측하지 않는다. 새 writer의 fencing과 committed checkpoint로 판정한다.
            if !committing && self.writer.abort().is_err() {
                tracing::warn!(event = "producer_abort_failed");
            }
            return Err(error);
        }
        let through = candidate.snapshot.through.ok_or(Error::InvalidState)?.0;
        self.state = candidate;
        Ok(Committed { source_seq: self.state.last_seq(), through, new_events, replays })
    }
}
