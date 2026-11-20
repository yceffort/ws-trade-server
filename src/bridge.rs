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
    thread: thread::JoinHandle<()>,
}

impl Bridge {
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.thread.thread().unpark();
    }

    pub async fn join(self) {
        let _ = tokio::task::spawn_blocking(move || self.thread.join()).await;
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
            match reader.read() {
                Read::Empty => thread::park_timeout(poll),
                Read::Lost => eprintln!("링에서 뒤처져 덮어쓰인 체결을 건너뛰었다"),
                Read::Record(payload) => {
                    let Ok(trade) = Trade::decode(payload.as_slice()) else {
                        eprintln!("Trade로 디코딩하지 못한 레코드를 버렸다");
                        continue;
                    };
                    if let Some(last) = last_seq {
                        report_gap(last, trade.seq);
                    }
                    last_seq = Some(trade.seq);
                    match trade::to_message(&trade) {
                        Some((topic, body)) => {
                            if !hub.publish_blocking(topic, body) {
                                break;
                            }
                        }
                        None => eprintln!("본문을 만들 수 없는 체결을 버렸다: seq {}", trade.seq),
                    }
                }
            }
        }
    });
    Bridge { stopped, thread }
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
