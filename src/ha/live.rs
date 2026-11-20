use super::LogRecord;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

const BYTES: usize = 1 << 20;

pub struct LiveItem {
    pub offset: u64,
    pub record: Arc<LogRecord>,
    _bytes: OwnedSemaphorePermit,
}

pub struct LiveStream {
    pub records: mpsc::Receiver<LiveItem>,
    pub failed: oneshot::Receiver<&'static str>,
}

struct Subscriber {
    records: mpsc::Sender<LiveItem>,
    failed: Option<oneshot::Sender<&'static str>>,
    bytes: Arc<Semaphore>,
}

#[derive(Default)]
pub(crate) struct LiveSubscribers(HashMap<u64, Subscriber>);

impl LiveSubscribers {
    pub fn attach(&mut self, id: u64) -> Result<LiveStream, &'static str> {
        self.0.retain(|_, sub| !sub.records.is_closed());
        if self.0.contains_key(&id) || self.0.len() >= 10000 {
            return Err("retry-later");
        }
        let (records, receiver) = mpsc::channel(1024);
        let (failed, failure) = oneshot::channel();
        self.0.insert(id, Subscriber { records, failed: Some(failed), bytes: Arc::new(Semaphore::new(BYTES)) });
        Ok(LiveStream { records: receiver, failed: failure })
    }

    pub fn detach(&mut self, id: u64) {
        self.0.remove(&id);
    }

    pub fn publish(&mut self, offset: u64, record: LogRecord) {
        let size = record.to_json().map_or(BYTES + 1, |bytes| bytes.len() + 128);
        let record = Arc::new(record);
        self.0.retain(|_, sub| {
            if sub.records.is_closed() {
                return false;
            }
            let sent = sub.bytes.clone().try_acquire_many_owned(size.min(u32::MAX as usize) as u32).ok().is_some_and(
                |bytes| sub.records.try_send(LiveItem { offset, record: record.clone(), _bytes: bytes }).is_ok(),
            );
            if !sent && let Some(failed) = sub.failed.take() {
                let _ = failed.send("replay-limit");
            }
            sent
        });
    }
}
