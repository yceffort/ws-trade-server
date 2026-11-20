use crate::pb::Trade;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::InputError;

pub const MAX_ENVELOPE_BYTES: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceSeq(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceivedAtMs(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawSource")]
pub struct SourceId {
    provider: String,
    stream: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    provider: String,
    stream: String,
}

impl SourceId {
    pub fn new(provider: String, stream: String) -> Result<Self, InputError> {
        if !identifier(&provider) || !identifier(&stream) {
            return Err(InputError::InvalidIdentity);
        }
        Ok(Self { provider, stream })
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

impl TryFrom<RawSource> for SourceId {
    type Error = InputError;

    fn try_from(raw: RawSource) -> Result<Self, Self::Error> {
        Self::new(raw.provider, raw.stream)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawPosition")]
pub struct Position {
    log_id: String,
    partition: u32,
    #[serde(with = "decimal")]
    offset: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPosition {
    log_id: String,
    partition: u32,
    #[serde(with = "decimal")]
    offset: u64,
}

impl Position {
    pub fn new(log_id: String, partition: u32, offset: u64) -> Result<Self, InputError> {
        if !identifier(&log_id) || partition > i32::MAX as u32 || offset > i64::MAX as u64 {
            return Err(InputError::InvalidPosition);
        }
        Ok(Self { log_id, partition, offset })
    }

    pub fn log_id(&self) -> &str {
        &self.log_id
    }
    pub fn partition(&self) -> u32 {
        self.partition
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl TryFrom<RawPosition> for Position {
    type Error = InputError;

    fn try_from(raw: RawPosition) -> Result<Self, Self::Error> {
        Self::new(raw.log_id, raw.partition, raw.offset)
    }
}

#[derive(Clone, Debug)]
pub struct Envelope {
    wire: WireEnvelope,
    trade: Trade,
    message: (String, Arc<str>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelope {
    schema_version: u32,
    log_id: String,
    source_id: SourceId,
    source_epoch: String,
    #[serde(with = "decimal")]
    source_seq: u64,
    event_id: String,
    #[serde(with = "decimal")]
    received_at_ms: u64,
    // 기존 Trade 스키마를 복제하지 않고 protobuf bytes로 보존한다.
    trade: Vec<u8>,
}

impl Envelope {
    pub fn new(
        log_id: String,
        source_id: SourceId,
        source_epoch: String,
        source_seq: SourceSeq,
        trade: Trade,
        received_at_ms: ReceivedAtMs,
    ) -> Result<Self, InputError> {
        if !identifier(&log_id) || !identifier(&source_epoch) {
            return Err(InputError::InvalidIdentity);
        }
        let SourceSeq(source_seq) = source_seq;
        let ReceivedAtMs(received_at_ms) = received_at_ms;
        let message = prepare_trade(&trade)?;
        // 식별자에는 '/'를 허용하지 않으므로 튜플 경계가 모호해지지 않는다.
        let event_id = format!("{}/{}/{}/{source_seq}", source_id.provider, source_id.stream, source_epoch);
        let wire = WireEnvelope {
            schema_version: 1,
            log_id,
            source_id,
            source_epoch,
            source_seq,
            event_id,
            received_at_ms,
            trade: trade.encode_to_vec(),
        };
        Ok(Self { wire, trade, message })
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, InputError> {
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(InputError::TooLarge);
        }
        let wire: WireEnvelope = serde_json::from_slice(bytes).map_err(|_| InputError::InvalidEnvelope)?;
        if wire.schema_version != 1 {
            return Err(InputError::UnsupportedSchema);
        }
        if wire.trade.len() > 16 * 1024 {
            return Err(InputError::TooLarge);
        }
        let trade = Trade::decode(wire.trade.as_slice()).map_err(|_| InputError::InvalidTrade)?;
        let raw_trade = wire.trade;
        let mut envelope = Self::new(
            wire.log_id,
            wire.source_id,
            wire.source_epoch,
            SourceSeq(wire.source_seq),
            trade,
            ReceivedAtMs(wire.received_at_ms),
        )?;
        // prost가 알 수 없는 필드를 버려도 원본 레코드의 bytes는 재인코딩으로 바꾸지 않는다.
        envelope.wire.trade = raw_trade;
        if wire.event_id != envelope.event_id() {
            return Err(InputError::InvalidIdentity);
        }
        Ok(envelope)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, InputError> {
        let bytes = serde_json::to_vec(&self.wire).map_err(|_| InputError::InvalidEnvelope)?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(InputError::TooLarge);
        }
        Ok(bytes)
    }

    pub fn event_id(&self) -> &str {
        &self.wire.event_id
    }
    pub fn log_id(&self) -> &str {
        &self.wire.log_id
    }
    pub fn source_id(&self) -> &SourceId {
        &self.wire.source_id
    }
    pub fn source_seq(&self) -> u64 {
        self.wire.source_seq
    }
    pub fn trade(&self) -> &Trade {
        &self.trade
    }
    pub(super) fn message(&self) -> (String, Arc<str>) {
        self.message.clone()
    }
}

pub(super) fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 96 && value.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}

pub(super) fn prepare_trade(trade: &Trade) -> Result<(String, Arc<str>), InputError> {
    if trade.encoded_len() > 16 * 1024 {
        return Err(InputError::TooLarge);
    }
    let krw = trade.krw.as_ref();
    let decimals = [
        trade.price.as_ref(),
        trade.base_price.as_ref(),
        trade.trading_strength.as_ref(),
        trade.cumulative_amount.as_ref(),
        krw.and_then(|k| k.price.as_ref()),
        krw.and_then(|k| k.base_price.as_ref()),
        krw.and_then(|k| k.cumulative_amount.as_ref()),
    ];
    if decimals.into_iter().flatten().any(|d| d.scale > 18) {
        return Err(InputError::InvalidTrade);
    }
    crate::trade::to_message(trade).ok_or(InputError::InvalidTrade)
}

mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() || (value.len() > 1 && value.starts_with('0')) || !value.bytes().all(|c| c.is_ascii_digit())
        {
            return Err(serde::de::Error::custom("invalid decimal string"));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}
