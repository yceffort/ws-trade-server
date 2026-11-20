use super::{Error, MAX_STATE_BYTES, SHARD};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::{Duration, Instant};

fn transient(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::OperationTimedOut
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::RequestTimedOut
                | RDKafkaErrorCode::BrokerNotAvailable
                | RDKafkaErrorCode::NetworkException
        )
    )
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Kafka {
    pub properties: BTreeMap<String, String>,
    pub timeout_ms: u64,
}

impl Kafka {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    fn client(&self) -> Result<ClientConfig, Error> {
        const ALLOWED: &[&str] = &[
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.username",
            "sasl.password",
            "ssl.ca.location",
            "ssl.certificate.location",
            "ssl.key.location",
            "ssl.key.password",
        ];
        if !(100..=30_000).contains(&self.timeout_ms)
            || self.properties.keys().any(|key| !ALLOWED.contains(&key.as_str()))
            || !matches!(self.properties.get("security.protocol").map(String::as_str), Some("SASL_SSL" | "SSL"))
            || self.properties.get("bootstrap.servers").is_none_or(String::is_empty)
        {
            return Err(Error::InvalidConfig);
        }
        let mut config = ClientConfig::new();
        for (key, value) in &self.properties {
            config.set(key, value);
        }
        config
            .set("enable.ssl.certificate.verification", "true")
            .set("ssl.endpoint.identification.algorithm", "https")
            .set("socket.timeout.ms", self.timeout_ms.to_string())
            .set("socket.connection.setup.timeout.ms", self.timeout_ms.to_string());
        Ok(config)
    }

    pub fn writer(&self, log_id: &str) -> Result<Writer, Error> {
        if !crate::source::identifier(log_id) {
            return Err(Error::InvalidConfig);
        }
        let producer: ThreadedProducer<Quiet> = self
            .client()?
            .set("transactional.id", format!("wts-{log_id}-{SHARD}"))
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("transaction.timeout.ms", "30000")
            .set("message.timeout.ms", self.timeout_ms.to_string())
            .set("queue.buffering.max.messages", "128")
            .set("queue.buffering.max.kbytes", "1024")
            .set("message.max.bytes", "1048576")
            .set("linger.ms", "0")
            .create_with_context(Quiet)?;
        producer.init_transactions(self.timeout())?;
        Ok(Writer {
            producer,
            timeout: self.timeout(),
            #[cfg(test)]
            lose_commit_result: false,
        })
    }

    pub fn reader(&self, topic: &str, start: Option<u64>) -> Result<LogReader, Error> {
        let consumer: BaseConsumer<Quiet> = self
            .client()?
            .set("group.id", format!("wts-reader-{SHARD}"))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "error")
            .set("enable.partition.eof", "true")
            .set("queued.max.messages.kbytes", "1024")
            .set("fetch.message.max.bytes", "1048576")
            .set("fetch.max.bytes", "1048576")
            .set("receive.message.max.bytes", "2097152")
            .create_with_context(Quiet)?;
        let deadline = Instant::now() + self.timeout();
        let (low, high) = loop {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            match consumer.fetch_watermarks(
                topic,
                0,
                deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(2)),
            ) {
                Ok(bounds) => break bounds,
                Err(error) if transient(&error) => std::thread::sleep(Duration::from_millis(50)),
                Err(error) => return Err(error.into()),
            }
        };
        let start = match start {
            Some(n) => i64::try_from(n).map_err(|_| Error::InvalidState)?,
            None => low,
        };
        if start < low || start > high {
            return Err(Error::InvalidState);
        }
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(topic, 0, Offset::Offset(start))?;
        consumer.assign(&assignment)?;
        Ok(LogReader {
            consumer,
            topic: topic.into(),
            next: start as u64,
            initial_high: high as u64,
            scan_end: high as u64,
            timeout: self.timeout(),
            end: false,
        })
    }

    pub fn latest(&self, topic: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let deadline = Instant::now() + self.timeout();
        // commit 응답 직후에도 marker 전파로 LSO가 늦을 수 있다. 물리적 끝을 먼저 고정하고 가시화를 기다린다.
        let probe: BaseConsumer<Quiet> =
            self.client()?.set("isolation.level", "read_uncommitted").create_with_context(Quiet)?;
        let metadata = probe.fetch_metadata(Some(topic), self.timeout())?;
        let topic_metadata = metadata.topics().iter().find(|item| item.name() == topic).ok_or(Error::InvalidState)?;
        if let Some(error) = topic_metadata.error() {
            return Err(KafkaError::MetadataFetch(error.into()).into());
        }
        if topic_metadata.partitions().len() != 1 {
            return Err(Error::InvalidConfig);
        }
        let (_, high) = probe.fetch_watermarks(topic, 0, self.timeout())?;
        let boundary = u64::try_from(high).map_err(|_| Error::InvalidState)?;
        let mut reader = self.reader(topic, None)?;
        let low = reader.next_offset();
        if low >= boundary {
            return Ok(None);
        }
        // 단일 shard의 최신 값부터 찾는다. 오래된 compact 이력과 무관하게 tail 4,096개 위치만 검사한다.
        let mut window = 8;
        let mut bytes = 0;
        loop {
            let start = boundary.saturating_sub(window).max(low);
            reader.rewind(start)?;
            let mut latest = None;
            loop {
                if Instant::now() >= deadline {
                    return Err(Error::Deadline);
                }
                if let Some(record) = reader.poll(Duration::from_millis(50))? {
                    bytes += record.payload.len();
                    if bytes > 16 * 1024 * 1024 {
                        return Err(Error::TooLarge);
                    }
                    if record.offset < boundary && record.key == key {
                        latest = Some(record.payload);
                    }
                }
                if reader.next_offset() >= boundary {
                    break;
                }
                if reader.at_end() {
                    // 이전 EOF보다 뒤의 LSO를 바로 적용하지 않는다. 새 assign 경계부터 다시 읽어 확인한다.
                    reader.rewind(start)?;
                    latest = None;
                }
            }
            if latest.is_some() || start == low {
                return Ok(latest);
            }
            if window >= 4096 {
                return Err(Error::Deadline);
            }
            window *= 2;
        }
    }
}

struct Quiet;
impl ClientContext for Quiet {
    fn log(&self, level: RDKafkaLogLevel, _: &str, _: &str) {
        // 외부 라이브러리의 원문 로그에 인증 설정이나 payload가 섞이지 않게 오류 종류만 남긴다.
        if matches!(
            level,
            RDKafkaLogLevel::Emerg | RDKafkaLogLevel::Alert | RDKafkaLogLevel::Critical | RDKafkaLogLevel::Error
        ) {
            tracing::warn!(event = "broker_client_error");
        }
    }
    fn error(&self, _: KafkaError, _: &str) {
        tracing::warn!(event = "broker_client_error");
    }
}
impl ConsumerContext for Quiet {}
type Delivery = (usize, Result<i64, KafkaError>);
impl ProducerContext for Quiet {
    type DeliveryOpaque = Box<(usize, SyncSender<Delivery>)>;
    fn delivery(&self, result: &DeliveryResult<'_>, opaque: Self::DeliveryOpaque) {
        let (index, reply) = *opaque;
        let result = result.as_ref().map(|message| message.offset()).map_err(|(error, _)| error.clone());
        // deadline이 지난 호출자는 수신기를 버리므로 callback에서 다시 기다리지 않는다.
        let _ = reply.try_send((index, result));
    }
}

pub struct Writer {
    producer: ThreadedProducer<Quiet>,
    timeout: Duration,
    #[cfg(test)]
    pub(super) lose_commit_result: bool,
}

impl Writer {
    pub fn begin(&self) -> Result<(), Error> {
        self.producer.begin_transaction().map_err(Error::from)
    }
    pub fn commit(&self) -> Result<(), Error> {
        self.producer.commit_transaction(self.timeout).map_err(Error::from)?;
        // 라이브러리 시험에서만 commit 성공 응답을 잃어 Publisher가 결과를 모르는 경계를 만든다.
        #[cfg(test)]
        if self.lose_commit_result {
            return Err(Error::Deadline);
        }
        Ok(())
    }
    pub fn abort(&self) -> Result<(), Error> {
        self.producer.abort_transaction(self.timeout).map_err(Error::from)
    }

    pub fn send(&self, topic: &str, records: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u64>, Error> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if records.len() > 128
            || records.iter().any(|(_, payload)| payload.len() > MAX_STATE_BYTES)
            || records.iter().map(|(key, payload)| key.len() + payload.len()).sum::<usize>() > 1024 * 1024
        {
            return Err(Error::TooLarge);
        }
        let (reply, results) = sync_channel(records.len());
        let deadline = Instant::now() + self.timeout;
        for (index, (key, payload)) in records.iter().enumerate() {
            self.producer
                .send(
                    BaseRecord::with_opaque_to(topic, Box::new((index, reply.clone())))
                        .partition(0)
                        .key(key)
                        .payload(payload),
                )
                .map_err(|(error, _)| Error::from(error))?;
        }
        let mut offsets = vec![0; records.len()];
        for _ in records {
            let (index, offset) = results
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|_| Error::Deadline)?;
            offsets[index] = u64::try_from(offset?).map_err(|_| Error::InvalidState)?;
        }
        Ok(offsets)
    }
}

pub struct StoredRecord {
    pub offset: u64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

pub struct LogReader {
    consumer: BaseConsumer<Quiet>,
    topic: String,
    next: u64,
    initial_high: u64,
    scan_end: u64,
    timeout: Duration,
    end: bool,
}

impl LogReader {
    pub fn initial_high(&self) -> u64 {
        self.initial_high
    }

    fn rewind(&mut self, offset: u64) -> Result<(), Error> {
        let (_, stable) = self.consumer.fetch_watermarks(&self.topic, 0, self.timeout)?;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(
            &self.topic,
            0,
            Offset::Offset(i64::try_from(offset).map_err(|_| Error::InvalidState)?),
        )?;
        self.consumer.assign(&assignment)?;
        self.next = offset;
        self.scan_end = u64::try_from(stable).map_err(|_| Error::InvalidState)?;
        self.end = false;
        Ok(())
    }
    pub fn at_end(&self) -> bool {
        self.end
    }
    pub fn next_offset(&self) -> u64 {
        self.next
    }

    pub fn poll(&mut self, timeout: Duration) -> Result<Option<StoredRecord>, Error> {
        match self.consumer.poll(timeout) {
            Some(Ok(message)) => {
                let offset = u64::try_from(message.offset()).map_err(|_| Error::InvalidState)?;
                let payload = message.payload().ok_or(Error::InvalidState)?;
                let key = message.key().unwrap_or_default();
                if payload.len() > MAX_STATE_BYTES || key.len() > 1024 {
                    return Err(Error::TooLarge);
                }
                self.next = offset.checked_add(1).ok_or(Error::Exhausted)?;
                self.end = false;
                Ok(Some(StoredRecord { offset, key: key.to_vec(), payload: payload.to_vec() }))
            }
            Some(Err(KafkaError::PartitionEOF(0))) => {
                if let Some(position) = self.consumer.position()?.find_partition(&self.topic, 0)
                    && let Offset::Offset(offset) = position.offset()
                {
                    self.next = self.next.max(u64::try_from(offset).map_err(|_| Error::InvalidState)?);
                }
                // Rust client는 EOF 위치를 반환하지 않는다. assign 전에 조회한 안정 경계까지만 전진한다.
                self.next = self.next.max(self.scan_end);
                self.end = true;
                Ok(None)
            }
            Some(Err(error)) if transient(&error) => {
                self.end = false;
                Ok(None)
            }
            Some(Err(error)) => Err(error.into()),
            None => Ok(None),
        }
    }
}
