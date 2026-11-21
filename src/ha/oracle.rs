use super::{EVENTS, Error, Kafka, LogRecord, Number, SourceConfig};
use serde::Serialize;
use std::time::{Duration, Instant};

#[derive(Serialize)]
pub struct OracleEvent {
    pub offset: Number,
    pub id: String,
    pub topic: String,
    pub body: serde_json::Value,
}
#[derive(Serialize)]
pub struct Oracle {
    pub log_id: String,
    pub start: Number,
    pub through: Number,
    pub events: Vec<OracleEvent>,
}

pub fn oracle(kafka: &Kafka, config: &SourceConfig, start: u64, through: u64) -> Result<Oracle, Error> {
    if start > through {
        return Err(Error::InvalidConfig);
    }
    let mut reader = kafka.reader(EVENTS, Some(start))?;
    let (low, high) = reader.bounds();
    if start < low || through >= high {
        return Err(Error::InvalidState);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut events = Vec::new();
    let mut bytes = 0;
    while reader.next_offset() <= through {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        if let Some(stored) = reader.poll(Duration::from_millis(50))? {
            if stored.offset > through {
                break;
            }
            bytes += stored.payload.len();
            if bytes > 128 * 1024 * 1024 {
                return Err(Error::TooLarge);
            }
            if let LogRecord::Trade(event) = LogRecord::from_json(&stored.payload, config)? {
                if events.len() >= 100_000 {
                    return Err(Error::TooLarge);
                }
                let (topic, body) = event.message();
                let body: serde_json::Value = serde_json::from_str(&body).map_err(|_| Error::InvalidState)?;
                events.push(OracleEvent {
                    offset: Number(stored.offset),
                    id: event.event_id().to_owned(),
                    topic,
                    body,
                });
            }
        }
    }
    Ok(Oracle { log_id: config.log_id.clone(), start: Number(start), through: Number(through), events })
}
