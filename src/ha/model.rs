use super::{Error, decode, digest, encode};
use crate::source::{Envelope, SourceId, identifier};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Number(#[serde(with = "crate::source::decimal")] pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub log_id: String,
    pub source_id: SourceId,
    pub epoch: String,
    pub start_seq: Number,
    pub topics: Vec<String>,
}

impl SourceConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if !identifier(&self.log_id)
            || !identifier(&self.epoch)
            || self.topics.is_empty()
            || self.topics.len() > 256
            || self.topics.windows(2).any(|pair| pair[0] >= pair[1])
            || self.topics.iter().any(|topic| {
                !topic.starts_with("/topic/v1/") || topic.len() > 256 || topic.chars().any(char::is_control)
            })
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(digest(&encode(self)?))
    }

    pub fn check_event(&self, event: &Envelope) -> Result<(), Error> {
        if event.log_id() != self.log_id || event.source_id() != &self.source_id {
            return Err(Error::IdentityMismatch);
        }
        if event.source_epoch() != self.epoch {
            return Err(Error::SourceGap);
        }
        let (topic, _) = crate::trade::to_message(event.trade()).ok_or(Error::InvalidState)?;
        if self.topics.binary_search(&topic).is_err() {
            return Err(Error::InvalidState);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceStatus {
    pub schema_version: u32,
    pub log_id: String,
    pub source_id: SourceId,
    pub at_ms: Number,
    pub connected: bool,
    pub market_open: bool,
}

impl SourceStatus {
    pub fn new(config: &SourceConfig, at_ms: u64, market_open: bool) -> Self {
        Self {
            schema_version: 1,
            log_id: config.log_id.clone(),
            source_id: config.source_id.clone(),
            at_ms: Number(at_ms),
            connected: true,
            market_open,
        }
    }

    pub fn validate(&self, config: &SourceConfig) -> Result<(), Error> {
        if self.schema_version != 1 || self.log_id != config.log_id || self.source_id != config.source_id {
            return Err(Error::IdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", deny_unknown_fields)]
pub enum LogRecord {
    Trade(Box<Envelope>),
    Status(SourceStatus),
}

impl LogRecord {
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        encode(self)
    }
    pub fn from_json(bytes: &[u8], config: &SourceConfig) -> Result<Self, Error> {
        let record: Self = decode(bytes)?;
        match &record {
            Self::Trade(event) => config.check_event(event)?,
            Self::Status(status) => status.validate(config)?,
        }
        Ok(record)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Latest {
    pub offset: Number,
    pub event: Envelope,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema_version: u32,
    pub log_id: String,
    pub config_hash: String,
    pub through: Option<Number>,
    pub topics: BTreeMap<String, Option<Latest>>,
    pub status: Option<SourceStatus>,
}

impl Snapshot {
    pub fn empty(config: &SourceConfig) -> Result<Self, Error> {
        Ok(Self {
            schema_version: 1,
            log_id: config.log_id.clone(),
            config_hash: config.hash()?,
            through: None,
            topics: config.topics.iter().map(|topic| (topic.clone(), None)).collect(),
            status: None,
        })
    }

    pub fn validate(&self, config: &SourceConfig) -> Result<(), Error> {
        if self.schema_version != 1
            || self.log_id != config.log_id
            || self.config_hash != config.hash()?
            || !self.topics.keys().eq(config.topics.iter())
            || self.through.is_some_and(|n| n.0 > i64::MAX as u64)
        {
            return Err(Error::InvalidState);
        }
        for (topic, latest) in &self.topics {
            if let Some(latest) = latest {
                config.check_event(&latest.event)?;
                if Some(latest.offset) > self.through
                    || crate::trade::to_message(latest.event.trade()).ok_or(Error::InvalidState)?.0 != *topic
                {
                    return Err(Error::InvalidState);
                }
            }
        }
        if let Some(status) = &self.status {
            status.validate(config)?;
        }
        encode(self)?;
        Ok(())
    }

    pub fn apply(&mut self, record: &LogRecord, offset: u64, config: &SourceConfig) -> Result<(), Error> {
        if offset > i64::MAX as u64 || self.through.is_some_and(|n| offset <= n.0) {
            return Err(Error::InvalidState);
        }
        match record {
            LogRecord::Trade(event) => {
                config.check_event(event)?;
                let (topic, _) = crate::trade::to_message(event.trade()).ok_or(Error::InvalidState)?;
                *self.topics.get_mut(&topic).ok_or(Error::InvalidState)? =
                    Some(Latest { offset: Number(offset), event: event.as_ref().clone() });
            }
            LogRecord::Status(status) => {
                status.validate(config)?;
                self.status = Some(status.clone());
            }
        }
        self.through = Some(Number(offset));
        Ok(())
    }

    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        encode(self)
    }
    pub fn from_json(bytes: &[u8], config: &SourceConfig) -> Result<Self, Error> {
        let state: Self = decode(bytes)?;
        state.validate(config)?;
        Ok(state)
    }
}
