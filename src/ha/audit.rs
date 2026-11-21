use super::{CHECKPOINT, Checkpoint, EVENTS, Error, Kafka, LogRecord, Number, SHARD, Snapshot, SourceConfig};
use serde::Serialize;
use std::time::{Duration, Instant};

#[derive(Serialize)]
pub struct Audit {
    pub events: usize,
    pub source_seq: Option<Number>,
    pub through: Number,
    pub market_open: bool,
}

pub fn audit(kafka: &Kafka, config: &SourceConfig) -> Result<Audit, Error> {
    let bytes = kafka.latest(CHECKPOINT, SHARD.as_bytes())?.ok_or(Error::MissingCheckpoint)?;
    let checkpoint = Checkpoint::from_json(&bytes, config)?;
    let through = checkpoint.snapshot.through.ok_or(Error::InvalidState)?;
    let mut snapshot = Snapshot::empty(config)?;
    let mut reader = kafka.reader(EVENTS, None)?;
    let mut last = None;
    let mut count = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(stored) = reader.poll(Duration::from_millis(100))? {
            if stored.offset > through.0 {
                return Err(Error::InvalidState);
            }
            let record = LogRecord::from_json(&stored.payload, config)?;
            if let LogRecord::Trade(event) = &record {
                let expected = match last {
                    Some(last) => u64::checked_add(last, 1).ok_or(Error::Exhausted)?,
                    None => config.start_seq.0,
                };
                if event.source_seq() != expected {
                    return Err(Error::SourceGap);
                }
                last = Some(expected);
                count += 1;
                if count > 100_000 {
                    return Err(Error::TooLarge);
                }
            }
            snapshot.apply(&record, stored.offset, config)?;
            if stored.offset == through.0 {
                if last != checkpoint.last_seq() || snapshot.to_json()? != checkpoint.snapshot.to_json()? {
                    return Err(Error::InvalidState);
                }
                return Ok(Audit {
                    events: count,
                    source_seq: last.map(Number),
                    through,
                    market_open: snapshot.status.ok_or(Error::InvalidState)?.market_open,
                });
            }
        }
    }
    Err(Error::Deadline)
}
