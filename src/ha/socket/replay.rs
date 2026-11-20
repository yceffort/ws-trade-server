use super::super::{Error, LogReader, LogRecord, SourceConfig};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio::time::Instant;

pub(super) enum Item {
    Record(u64, LogRecord),
    End(u64),
    Failed(&'static str),
}

pub(super) struct Replay {
    pub inbox: mpsc::Receiver<Item>,
    stop: Arc<AtomicBool>,
}

impl Replay {
    pub fn spawn(
        mut reader: LogReader,
        source: SourceConfig,
        end: u64,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        let (send, inbox) = mpsc::channel(2);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = (|| {
                let mut count = 0;
                while reader.next_offset() <= end {
                    if stopped.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    if Instant::now() >= deadline || count >= 10000 {
                        return Err("replay-limit");
                    }
                    if reader.at_end() && reader.next_offset() <= end {
                        reader.refresh_end().map_err(error)?;
                    }
                    if let Some(stored) = reader.poll(Duration::from_millis(50)).map_err(error)? {
                        if stored.offset > end {
                            break;
                        }
                        let record = LogRecord::from_json(&stored.payload, &source).map_err(error)?;
                        count += 1;
                        if send.blocking_send(Item::Record(stored.offset, record)).is_err() {
                            return Ok(());
                        }
                    }
                }
                let _ = send.blocking_send(Item::End(end));
                Ok(())
            })();
            if let Err(code) = result {
                let _ = send.blocking_send(Item::Failed(code));
            }
        });
        Self { inbox, stop }
    }
}

impl Drop for Replay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

pub(super) fn error(error: Error) -> &'static str {
    match error {
        Error::InvalidState => "cursor-expired",
        Error::Deadline => "retry-later",
        Error::Broker(ref error)
            if error.rdkafka_error_code() == Some(rdkafka::error::RDKafkaErrorCode::OffsetOutOfRange) =>
        {
            "cursor-expired"
        }
        _ => "source-error",
    }
}
