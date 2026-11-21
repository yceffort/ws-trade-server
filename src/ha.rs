mod audit;
mod broker;
mod checkpoint;
mod cursor;
mod external;
mod lease;
mod live;
mod model;
mod producer;
mod protocol;
mod replica;
mod socket;
mod synthetic;
pub use audit::{Audit, audit};
pub use broker::{Kafka, LogReader, StoredRecord, Writer};
pub use checkpoint::{Checkpoint, Disposition};
pub use cursor::CursorKeys;
pub use external::{ExternalSource, pump_external};
pub use lease::{Lease, LeaseConfig, LeaseGuard};
pub(crate) use live::LiveSubscribers;
pub use live::{LiveItem, LiveStream};
pub use model::{Latest, LogRecord, Number, Snapshot, SourceConfig, SourceStatus};
pub use producer::{Bootstrap, Committed, Publisher};
pub use protocol::Protocol;
pub(crate) use replica::Replica;
pub use replica::{ReplicaInput, ReplicaTask, ReplicaView};
pub(crate) use socket::{negotiate, serve_socket};
pub use synthetic::Synthetic;

pub const MAX_STATE_BYTES: usize = 512 * 1024;
pub const INPUT_WINDOW: usize = 1024;
pub const EVENTS: &str = "wts.events.v1";
pub const CHECKPOINT: &str = "wts.checkpoint.v1";
pub const SNAPSHOT: &str = "wts.snapshot.v1";
pub const SHARD: &str = "trade-0";

#[derive(Debug)]
pub enum Error {
    InvalidConfig,
    InvalidState,
    TooLarge,
    IdentityMismatch,
    SourceGap,
    ConflictingInput,
    DedupeWindowExpired,
    Exhausted,
    MissingCheckpoint,
    Deadline,
    Fenced,
    LeaseLost,
    RestartRequired,
    ReplicaFailed,
    Broker(rdkafka::error::KafkaError),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid-ha-config",
            Self::InvalidState => "invalid-ha-state",
            Self::TooLarge => "ha-state-too-large",
            Self::IdentityMismatch => "source-identity-mismatch",
            Self::SourceGap => "source-gap",
            Self::ConflictingInput => "conflicting-source-payload",
            Self::DedupeWindowExpired => "dedupe-window-expired",
            Self::Exhausted => "source-sequence-exhausted",
            Self::MissingCheckpoint => "checkpoint-missing-initialize-explicitly",
            Self::Deadline => "ha-deadline",
            Self::Fenced => "producer-fenced",
            Self::LeaseLost => "lease-lost",
            Self::RestartRequired => "producer-restart-required",
            Self::ReplicaFailed => "replica-failed",
            Self::Broker(_) => "broker-error",
            Self::Io(_) => "ha-io-error",
        })
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Broker(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rdkafka::error::KafkaError> for Error {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        use rdkafka::error::RDKafkaErrorCode;
        if matches!(
            error.rdkafka_error_code(),
            Some(
                RDKafkaErrorCode::Fenced
                    | RDKafkaErrorCode::ProducerFenced
                    | RDKafkaErrorCode::InvalidProducerEpoch
                    | RDKafkaErrorCode::TransactionCoordinatorFenced
            )
        ) {
            Self::Fenced
        } else {
            Self::Broker(error)
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::InvalidState)?;
    if bytes.len() > MAX_STATE_BYTES {
        return Err(Error::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_STATE_BYTES {
        return Err(Error::TooLarge);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::InvalidState)
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests;
