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

struct Request {
    source: SourceConfig,
    end: u64,
    deadline: Instant,
    send: mpsc::Sender<Item>,
    stopped: Arc<AtomicBool>,
}

pub(super) struct Reader {
    bounds: (u64, u64),
    command: tokio::sync::oneshot::Sender<Request>,
}
impl Reader {
    pub async fn open(
        kafka: super::super::Kafka,
        start: Option<u64>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Self, Error> {
        let (ready, opened) = tokio::sync::oneshot::channel();
        let (command, request) = tokio::sync::oneshot::channel::<Request>();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut reader = match kafka.reader(super::super::EVENTS, start) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
            if ready.send(Ok(reader.bounds())).is_ok()
                && let Ok(request) = request.blocking_recv()
            {
                let result = replay(&mut reader, &request);
                if let Err(code) = result {
                    let _ = request.send.blocking_send(Item::Failed(code));
                }
            }
            // 생성부터 취소와 종료까지 같은 blocking 작업이 reader를 소유한다. permit은 종료 뒤 반환한다.
            drop(reader);
        });
        let bounds = opened.await.map_err(|_| Error::ReplicaFailed)??;
        Ok(Self { bounds, command })
    }
    pub fn bounds(&self) -> (u64, u64) {
        self.bounds
    }
}

fn replay(reader: &mut LogReader, request: &Request) -> Result<(), &'static str> {
    let mut count = 0;
    while reader.next_offset() <= request.end {
        if request.stopped.load(Ordering::Relaxed) {
            return Ok(());
        }
        if Instant::now() >= request.deadline || count >= 10000 {
            return Err("replay-limit");
        }
        if reader.at_end() && reader.next_offset() <= request.end {
            reader.refresh_end().map_err(error)?;
        }
        if let Some(stored) = reader.poll(Duration::from_millis(50)).map_err(error)? {
            if stored.offset > request.end {
                break;
            }
            let record = LogRecord::from_json(&stored.payload, &request.source).map_err(error)?;
            count += 1;
            if request.send.blocking_send(Item::Record(stored.offset, record)).is_err() {
                return Ok(());
            }
        }
    }
    let _ = request.send.blocking_send(Item::End(request.end));
    Ok(())
}

impl Replay {
    pub fn spawn(reader: Reader, source: SourceConfig, end: u64, deadline: Instant) -> Self {
        let (send, inbox) = mpsc::channel(2);
        let stop = Arc::new(AtomicBool::new(false));
        let _ = reader.command.send(Request { source, end, deadline, send, stopped: stop.clone() });
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "실제 broker 필요: deploy/ha/smoke.sh"]
    async fn cancelled_reader_releases_its_permit_after_native_cleanup() {
        let directory = std::path::PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").unwrap());
        let kafka = serde_json::from_slice(&std::fs::read(directory.join("gateway.json")).unwrap()).unwrap();
        let workers = Arc::new(tokio::sync::Semaphore::new(1));
        let reader = Reader::open(kafka, None, workers.clone().acquire_owned().await.unwrap()).await.unwrap();
        assert_eq!(workers.available_permits(), 0);
        let started = Instant::now();
        drop(reader);
        assert!(started.elapsed() < Duration::from_millis(100));
        let timer = tokio::time::timeout(Duration::from_millis(100), tokio::task::yield_now()).await;
        assert!(timer.is_ok());
        let _permit = tokio::time::timeout(Duration::from_secs(5), workers.acquire()).await.unwrap().unwrap();
    }
}
