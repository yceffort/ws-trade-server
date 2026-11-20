use crate::hub::Hub;
use crate::pb::Trade;
use crate::ring::{Read, Reader};
use crate::source::{Publication, RingAdapter};
use prost::Message;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

pub struct Bridge {
    stopped: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Bridge {
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
    }

    pub async fn join(mut self) {
        self.stop();
        if let Some(thread) = self.thread.take() {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.stop();
    }
}

// hub 종료로 채널도 닫아야 blocking_send에서 기다리는 브리지까지 풀린다.
pub fn spawn(path: PathBuf, poll: Duration, hub: Hub) -> Bridge {
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    let input = hub.input();
    let thread = thread::spawn(move || {
        tracing::info!(event = "ring_waiting");
        let Some(mut reader) = open(&path, &stop) else { return };
        let mut last_seq = None;
        while !stop.load(Ordering::Relaxed) {
            let read = reader.read();
            if matches!(read, Read::Empty) {
                thread::park_timeout(poll);
            } else if let Some(publication) = process(read, &mut last_seq, &hub.bridge)
                && input.enqueue_blocking(publication).is_err()
            {
                break;
            }
        }
    });
    Bridge { stopped, thread: Some(thread) }
}

fn process(read: Read, last_seq: &mut Option<u64>, counters: &crate::metrics::BridgeCounters) -> Option<Publication> {
    let payload = match read {
        Read::Empty => return None,
        Read::Lost => {
            counters.ring_overruns.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Read::Record(payload) => payload,
    };
    let trade = match Trade::decode(payload.as_slice()) {
        Ok(trade) => trade,
        Err(_) => {
            counters.decode_failures.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    if let Some(last) = *last_seq {
        counters.observe_seq(last, trade.seq);
    }
    *last_seq = Some(trade.seq);
    let message = RingAdapter::prepare(&trade).ok();
    if message.is_none() {
        counters.invalid_trades.fetch_add(1, Ordering::Relaxed);
    }
    message
}

// 링 파일이 아직 없거나 만드는 중이면 0.5초마다 다시 연다. feed와 gateway를 어느 순서로 띄워도 된다.
fn open(path: &Path, stopped: &AtomicBool) -> Option<Reader> {
    while !stopped.load(Ordering::Relaxed) {
        match Reader::open(path) {
            Ok(reader) => {
                tracing::info!(event = "ring_opened");
                return Some(reader);
            }
            Err(_) => thread::park_timeout(Duration::from_millis(500)),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{Generator, Market, Symbol};
    #[test]
    fn bridge_counts_loss_events_decode_errors_and_missing_sequences_separately() {
        let counters = crate::metrics::BridgeCounters::default();
        let mut last = None;
        let mut generator = Generator::new(Symbol { market: Market::Kr, code: "DEMO".into() });
        for seq in [10, 15, 1] {
            let trade = generator.next_trade(seq);
            assert!(process(Read::Record(trade.encode_to_vec()), &mut last, &counters).is_some());
        }
        process(Read::Lost, &mut last, &counters);
        process(Read::Record(vec![255]), &mut last, &counters);
        process(Read::Record(Trade { seq: 2, ..Trade::default() }.encode_to_vec()), &mut last, &counters);
        assert_eq!(counters.ring_overruns.load(Ordering::Relaxed), 1);
        assert_eq!(counters.sequence_missing.load(Ordering::Relaxed), 4);
        assert_eq!(counters.sequence_resets.load(Ordering::Relaxed), 1);
        assert_eq!(counters.decode_failures.load(Ordering::Relaxed), 1);
        assert_eq!(counters.invalid_trades.load(Ordering::Relaxed), 1);
    }
}
