use super::LogRecord;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

const BYTES: usize = 1 << 20;

pub struct LiveItem {
    pub offset: u64,
    pub sequence: u64,
    pub record: Arc<LogRecord>,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Default)]
pub struct LiveProgress {
    offset: Option<u64>,
    sequence: u64,
}
impl LiveProgress {
    pub fn through(&self, applied: u64) -> Option<u64> {
        (self.sequence == applied).then_some(self.offset).flatten()
    }
}

pub struct LiveStream {
    pub records: mpsc::Receiver<LiveItem>,
    pub progress: watch::Receiver<LiveProgress>,
    pub failed: oneshot::Receiver<&'static str>,
}

struct Subscriber {
    records: mpsc::Sender<LiveItem>,
    failed: Option<oneshot::Sender<&'static str>>,
    bytes: Arc<Semaphore>,
    topics: Vec<String>,
    progress: watch::Sender<LiveProgress>,
    sequence: u64,
}

#[derive(Default)]
pub(crate) struct LiveSubscribers(HashMap<u64, Subscriber>);

impl LiveSubscribers {
    pub fn attach(&mut self, id: u64, topics: Vec<String>) -> Result<LiveStream, &'static str> {
        self.0.retain(|_, sub| !sub.records.is_closed());
        if self.0.contains_key(&id)
            || self.0.len() >= 10000
            || topics.is_empty()
            || topics.len() > 256
            || topics.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err("retry-later");
        }
        let (records, receiver) = mpsc::channel(1024);
        let (failed, failure) = oneshot::channel();
        let (progress, observed) = watch::channel(LiveProgress::default());
        self.0.insert(
            id,
            Subscriber {
                records,
                failed: Some(failed),
                bytes: Arc::new(Semaphore::new(BYTES)),
                topics,
                progress,
                sequence: 0,
            },
        );
        Ok(LiveStream { records: receiver, failed: failure, progress: observed })
    }

    pub fn detach(&mut self, id: u64) {
        self.0.remove(&id);
    }

    pub fn publish(&mut self, offset: u64, record: LogRecord) {
        let size = record.to_json().map_or(BYTES + 1, |bytes| bytes.len() + 128);
        let topic = match &record {
            LogRecord::Trade(event) => Some(event.message().0),
            _ => None,
        };
        let record = Arc::new(record);
        self.0.retain(|_, sub| {
            if sub.records.is_closed() {
                return false;
            }
            if topic.as_ref().is_some_and(|topic| sub.topics.binary_search(topic).is_err()) {
                sub.progress.send_replace(LiveProgress { offset: Some(offset), sequence: sub.sequence });
                return true;
            }
            sub.sequence = match sub.sequence.checked_add(1) {
                Some(next) => next,
                None => return false,
            };
            let sent = sub.bytes.clone().try_acquire_many_owned(size.min(u32::MAX as usize) as u32).ok().is_some_and(
                |bytes| {
                    sub.records
                        .try_send(LiveItem { offset, sequence: sub.sequence, record: record.clone(), _bytes: bytes })
                        .is_ok()
                },
            );
            if sent {
                sub.progress.send_replace(LiveProgress { offset: Some(offset), sequence: sub.sequence });
            }
            if !sent && let Some(failed) = sub.failed.take() {
                let _ = failed.send("replay-limit");
            }
            sent
        });
    }
}
