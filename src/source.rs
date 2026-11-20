mod envelope;
use crate::{hub::Hub, pb::Trade};
pub use envelope::{Envelope, MAX_ENVELOPE_BYTES, Position, ReceivedAtMs, SourceId, SourceSeq};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputError {
    InvalidIdentity,
    InvalidPosition,
    InvalidEnvelope,
    UnsupportedSchema,
    InvalidTrade,
    TooLarge,
    DuplicateProvider,
    UnconfiguredSource,
    Closed,
    CompletionLost,
}

impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            Self::InvalidIdentity => "invalid source identity",
            Self::InvalidPosition => "invalid broker position",
            Self::InvalidEnvelope => "invalid envelope",
            Self::UnsupportedSchema => "unsupported envelope schema",
            Self::InvalidTrade => "invalid trade",
            Self::TooLarge => "input too large",
            Self::DuplicateProvider => "duplicate provider namespace",
            Self::UnconfiguredSource => "unconfigured source",
            Self::Closed => "input closed",
            Self::CompletionLost => "input completion unknown",
        };
        f.write_str(code)
    }
}
impl std::error::Error for InputError {}

#[derive(Clone, Debug)]
pub struct AppliedRecord {
    pub envelope: Arc<Envelope>,
    pub position: Position,
}

#[derive(Clone)]
pub(crate) struct Publication {
    pub topic: String,
    pub body: Arc<str>,
    pub record: Option<Arc<AppliedRecord>>,
}

impl Publication {
    pub(crate) fn local(topic: String, body: Arc<str>) -> Self {
        Self { topic, body, record: None }
    }
}

#[derive(Clone)]
pub struct Input {
    pub(crate) hub: Hub,
}

impl Input {
    // 성공은 hub 상태 반영까지이며 클라이언트 적용이나 broker commit의 ACK가 아니다.
    async fn publish(&self, publication: &Publication) -> Result<Option<Arc<AppliedRecord>>, InputError> {
        self.hub.publish_input(publication).await
    }

    pub(crate) fn enqueue_blocking(&self, publication: Publication) -> Result<(), InputError> {
        self.hub.enqueue_input_blocking(publication)
    }
}

#[derive(Clone)]
pub struct RingAdapter {
    input: Input,
}

impl RingAdapter {
    pub fn new(input: Input) -> Self {
        Self { input }
    }

    pub(crate) fn prepare(trade: &Trade) -> Result<Publication, InputError> {
        let (topic, body) = envelope::prepare_trade(trade)?;
        Ok(Publication::local(topic, body))
    }

    pub async fn publish(&self, trade: &Trade) -> Result<(), InputError> {
        self.input.publish(&Self::prepare(trade)?).await.map(|_| ())
    }

    pub fn enqueue_blocking(&self, trade: &Trade) -> Result<(), InputError> {
        self.input.enqueue_blocking(Self::prepare(trade)?)
    }
}

pub struct BrokerAdapter {
    input: Input,
    log_id: String,
    sources: Vec<SourceId>,
}

impl BrokerAdapter {
    pub fn new(input: Input, log_id: String, sources: Vec<SourceId>) -> Result<Self, InputError> {
        if !envelope::identifier(&log_id) || sources.is_empty() {
            return Err(InputError::InvalidIdentity);
        }
        // 최초 구현은 제공자별 stream 하나만 등록하며 namespace 중복 할당을 시작 전에 거절한다.
        let mut providers = std::collections::HashSet::new();
        for source in &sources {
            if !providers.insert(source.provider()) {
                return Err(InputError::DuplicateProvider);
            }
        }
        Ok(Self { input, log_id, sources })
    }

    pub async fn publish(&self, event: &Arc<Envelope>, position: Position) -> Result<Arc<AppliedRecord>, InputError> {
        if event.log_id() != self.log_id || position.log_id() != self.log_id {
            return Err(InputError::InvalidPosition);
        }
        if !self.sources.contains(event.source_id()) {
            return Err(InputError::UnconfiguredSource);
        }
        let (topic, body) = event.message();
        let publication =
            Publication { topic, body, record: Some(Arc::new(AppliedRecord { envelope: event.clone(), position })) };
        self.input.publish(&publication).await?.ok_or(InputError::CompletionLost)
    }
}

#[cfg(test)]
mod tests;
