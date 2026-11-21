use super::{Error, INPUT_WINDOW, Number, Snapshot, SourceConfig, Synthetic, decode, digest, encode};
use crate::source::Envelope;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

const HASH_SCHEMA: &str = "sha256-event-id-protobuf-bytes-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seen {
    seq: Number,
    event_id: String,
    payload_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    New,
    Replay,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    schema_version: u32,
    hash_schema: String,
    pub config: SourceConfig,
    last_seq: Option<Number>,
    window_start: Option<Number>,
    window: VecDeque<Seen>,
    pub generator: Option<Synthetic>,
    pub snapshot: Snapshot,
}

impl Checkpoint {
    pub fn new(config: SourceConfig, generator: Option<Synthetic>) -> Result<Self, Error> {
        config.validate()?;
        if let Some(generator) = &generator {
            generator.validate(&config)?;
        }
        let snapshot = Snapshot::empty(&config)?;
        Ok(Self {
            schema_version: 1,
            hash_schema: HASH_SCHEMA.into(),
            config,
            last_seq: None,
            window_start: None,
            window: VecDeque::new(),
            generator,
            snapshot,
        })
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq.map(|n| n.0)
    }

    pub fn next_seq(&self) -> Result<u64, Error> {
        match self.last_seq {
            Some(last) => last.0.checked_add(1).ok_or(Error::Exhausted),
            None => Ok(self.config.start_seq.0),
        }
    }

    pub fn stage(&mut self, event: &Envelope) -> Result<Disposition, Error> {
        self.config.check_event(event)?;
        // JSON 공백과 수신 시각은 제외하고 event ID와 원본 protobuf byte 배열을 정규 JSON 튜플로 해시한다.
        let payload_hash = digest(&encode(&(event.event_id(), event.payload_bytes()))?);
        let seq = event.source_seq();
        if self.last_seq.is_some_and(|last| seq <= last.0) {
            let seen = self.window.iter().find(|seen| seen.seq.0 == seq).ok_or(Error::DedupeWindowExpired)?;
            if seen.event_id != event.event_id() || seen.payload_hash != payload_hash {
                return Err(Error::ConflictingInput);
            }
            return Ok(Disposition::Replay);
        }
        if seq != self.next_seq()? {
            return Err(Error::SourceGap);
        }
        self.window.push_back(Seen { seq: Number(seq), event_id: event.event_id().into(), payload_hash });
        if self.window.len() > INPUT_WINDOW {
            self.window.pop_front();
        }
        self.last_seq = Some(Number(seq));
        self.window_start = self.window.front().map(|seen| seen.seq);
        Ok(Disposition::New)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        encode(self)
    }

    pub fn from_json(bytes: &[u8], config: &SourceConfig) -> Result<Self, Error> {
        let state: Self = decode(bytes)?;
        if &state.config != config {
            return Err(Error::IdentityMismatch);
        }
        if state.schema_version != 1
            || state.hash_schema != HASH_SCHEMA
            || state.window.len() > INPUT_WINDOW
            || state.window_start != state.window.front().map(|seen| seen.seq)
            || state.last_seq != state.window.back().map(|seen| seen.seq)
        {
            return Err(Error::InvalidState);
        }
        if let Some(last) = state.last_seq {
            let start = last.0.saturating_sub(INPUT_WINDOW as u64 - 1).max(config.start_seq.0);
            if last.0 < config.start_seq.0
                || state.window_start != Some(Number(start))
                || last.0 - start + 1 != state.window.len() as u64
            {
                return Err(Error::InvalidState);
            }
            for (index, seen) in state.window.iter().enumerate() {
                let seq = start + index as u64;
                let id =
                    format!("{}/{}/{}/{seq}", config.source_id.provider(), config.source_id.stream(), config.epoch);
                if seen.seq.0 != seq
                    || seen.event_id != id
                    || seen.payload_hash.len() != 64
                    || !seen.payload_hash.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(Error::InvalidState);
                }
            }
        }
        state.snapshot.validate(config)?;
        if state.snapshot.through.is_none()
            || state.snapshot.status.is_none()
            || state.snapshot.topics.values().flatten().map(|latest| latest.event.source_seq()).max()
                != state.last_seq()
        {
            return Err(Error::InvalidState);
        }
        if let Some(generator) = &state.generator {
            generator.validate(config)?;
        }
        Ok(state)
    }
}
