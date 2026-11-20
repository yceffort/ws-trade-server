use crate::hub::Hub;
use crate::pb::Trade;
use crate::ring::{Read, Reader};
use crate::trade;
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
    let thread = thread::spawn(move || {
        let Some(mut reader) = open(&path, &stop) else { return };
        let mut last_seq = None;
        while !stop.load(Ordering::Relaxed) {
            let read = reader.read();
            if matches!(read, Read::Empty) {
                thread::park_timeout(poll);
            } else if let Some((topic, body)) = process(read, &mut last_seq, &hub.bridge)
                && !hub.publish_blocking(topic, body)
            {
                break;
            }
        }
    });
    Bridge { stopped, thread: Some(thread) }
}

fn process(
    read: Read,
    last_seq: &mut Option<u64>,
    counters: &crate::metrics::BridgeCounters,
) -> Option<(String, Arc<str>)> {
    let payload = match read {
        Read::Empty => return None,
        Read::Lost => {
            counters.ring_overruns.fetch_add(1, Ordering::Relaxed);
            eprintln!("링에서 뒤처져 덮어쓰인 체결을 건너뛰었다");
            return None;
        }
        Read::Record(payload) => payload,
    };
    let trade = match Trade::decode(payload.as_slice()) {
        Ok(trade) => trade,
        Err(_) => {
            counters.decode_failures.fetch_add(1, Ordering::Relaxed);
            eprintln!("Trade로 디코딩하지 못한 레코드를 버렸다");
            return None;
        }
    };
    if let Some(last) = *last_seq {
        counters.observe_seq(last, trade.seq);
        report_gap(last, trade.seq);
    }
    *last_seq = Some(trade.seq);
    let message = trade::to_message(&trade);
    if message.is_none() {
        counters.invalid_trades.fetch_add(1, Ordering::Relaxed);
        eprintln!("본문을 만들 수 없는 체결을 버렸다: seq {}", trade.seq);
    }
    message
}

// 링 파일이 아직 없거나 만드는 중이면 0.5초마다 다시 연다. feed와 gateway를 어느 순서로 띄워도 된다.
fn open(path: &Path, stopped: &AtomicBool) -> Option<Reader> {
    while !stopped.load(Ordering::Relaxed) {
        match Reader::open(path) {
            Ok(reader) => {
                eprintln!("링 열림: {}", path.display());
                return Some(reader);
            }
            Err(_) => thread::park_timeout(Duration::from_millis(500)),
        }
    }
    None
}

fn report_gap(last: u64, seq: u64) {
    if seq < last {
        eprintln!("seq가 {last}에서 {seq}로 작아졌다. feed가 다시 뜬 것으로 본다");
    } else if seq.saturating_sub(last) > 1 {
        eprintln!("유실 {}건: seq {last} 다음에 {seq}", seq - last - 1);
    }
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
