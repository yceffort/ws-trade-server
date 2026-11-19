use crate::hub::Hub;
use crate::pb::Trade;
use crate::ring::{Read, Reader};
use crate::trade;
use prost::Message;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

// 링을 읽어 hub에 발행하는 전용 OS 스레드. 비동기 런타임 밖이라 블로킹 전송을 쓴다.
// hub의 명령 채널이 차면 여기서 막히고, 그동안 링에서 뒤처져 유실이 생긴다. 따로 버리지 않는 것은 의도한 동작이다.
pub fn spawn(path: PathBuf, poll: Duration, hub: Hub) {
    thread::spawn(move || {
        let mut reader = open(&path);
        let mut last_seq = None;
        loop {
            match reader.read() {
                Read::Empty => thread::sleep(poll),
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
                        Some((topic, body)) => hub.publish_blocking(topic, body),
                        None => eprintln!("본문을 만들 수 없는 체결을 버렸다: seq {}", trade.seq),
                    }
                }
            }
        }
    });
}

// 링 파일이 아직 없거나 만드는 중이면 0.5초마다 다시 연다. feed와 gateway를 어느 순서로 띄워도 된다.
fn open(path: &Path) -> Reader {
    loop {
        match Reader::open(path) {
            Ok(reader) => {
                eprintln!("링 열림: {}", path.display());
                return reader;
            }
            Err(_) => thread::sleep(Duration::from_millis(500)),
        }
    }
}

fn report_gap(last: u64, seq: u64) {
    if seq < last {
        eprintln!("seq가 {last}에서 {seq}로 작아졌다. feed가 다시 뜬 것으로 본다");
    } else if seq > last + 1 {
        eprintln!("유실 {}건: seq {last} 다음에 {seq}", seq - last - 1);
    }
}
