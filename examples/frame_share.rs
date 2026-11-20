// 체결 한 건을 구독자 N명에게 보낼 프레임으로 만드는 비용을 잰다. 연결마다 조립하는 지금 방식과,
// 연결별 필드가 없는 프레임을 한 번만 조립해 공유하는 가상의 방식을 견준다. 사용: cargo run --release --example frame_share
// 전역 할당자로 할당을 세야 하는데 GlobalAlloc 구현은 unsafe를 피할 수 없어서 이 파일에서만 허용한다.
#![allow(unsafe_code)]
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use ws_trade_server::feed::{Generator, default_symbols};
use ws_trade_server::{stomp, trade};

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

// 할당과 해제는 System에 그대로 넘기고 세기만 한다. 시간을 잴 때는 COUNTING을 꺼서 세는 비용이 섞이지 않게 한다.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        // SAFETY: layout은 호출자가 GlobalAlloc 규약대로 준 값이고 System에 그대로 넘긴다.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr과 layout은 이 할당자의 alloc이 System에서 받아 준 것이므로 System에 돌려주는 것이 맞다.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(new_size as u64, Relaxed);
        }
        // SAFETY: ptr은 System이 layout으로 할당한 것이고 new_size의 유효성은 호출자가 보장한다.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const CONNECTION_ID: &str = "00000000-0000-4000-8000-000000000000";

// batch.rs가 구독자마다 하는 일 그대로다. seq를 올리고 message-id를 만들어 조립한 뒤 Message로 감싼다.
fn per_subscriber(subscribers: usize, body: &str, frames: &mut Vec<Message>) {
    let mut seq = 0u64;
    for _ in 0..subscribers {
        seq += 1;
        let text = stomp::message(&format!("{CONNECTION_ID}-{seq}"), "0", body);
        frames.push(Message::text(text));
    }
}

// message-id와 subscription이 없는 프레임을 한 번만 조립하고 구독자마다 복제한다. 비용을 재려는 가상의 모양이고 STOMP 1.2에서는 MESSAGE에 두 헤더가 필수다.
fn shared(subscribers: usize, topic: &str, body: &str, frames: &mut Vec<Message>) {
    let content_length = body.len().to_string();
    let headers =
        [("destination", topic), ("content-length", content_length.as_str()), ("content-type", "application/json")];
    let frame = Message::text(stomp::serialize("MESSAGE", &headers, body));
    for _ in 0..subscribers {
        frames.push(frame.clone());
    }
}

// 체결 한 건 분의 프레임을 만들어 black_box에 넘기고 버리는 데 드는 시간(ns)과 할당을 잰다.
fn measure(subscribers: usize, build: impl Fn(&mut Vec<Message>)) -> (f64, u64, u64) {
    let mut frames = Vec::with_capacity(subscribers);
    let reps = 2_000_000 / subscribers;
    let start = Instant::now();
    for _ in 0..reps {
        build(&mut frames);
        black_box(&frames);
        frames.clear();
    }
    let nanos = start.elapsed().as_nanos() as f64 / reps as f64;
    ALLOCS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    COUNTING.store(true, Relaxed);
    build(&mut frames);
    COUNTING.store(false, Relaxed);
    black_box(&frames);
    (nanos, ALLOCS.load(Relaxed), BYTES.load(Relaxed))
}

fn main() -> std::io::Result<()> {
    let trade = Generator::new(default_symbols().remove(0)).next_trade(1);
    let (topic, body) = trade::to_message(&trade).expect("기본 종목은 topic이 있다");
    let frame_size = stomp::message(CONNECTION_ID, "0", &body).len();
    println!("본문 {}바이트, 구독자마다 조립한 프레임 {frame_size}바이트", body.len());
    println!("구독자 | 방식        | 한 건 조립 us | 구독자당 ns | 할당 횟수/건 | 할당 바이트/건");
    for subscribers in [100, 1_000, 10_000] {
        let rows = [
            ("구독자마다", measure(subscribers, |f| per_subscriber(subscribers, &body, f))),
            ("한 번 조립", measure(subscribers, |f| shared(subscribers, &topic, &body, f))),
        ];
        for (name, (nanos, allocs, bytes)) in rows {
            println!(
                "{subscribers:>6} | {name:<11} | {:>13.1} | {:>11.1} | {allocs:>12} | {bytes:>14}",
                nanos / 1e3,
                nanos / subscribers as f64
            );
        }
    }

    // boundary_cost와 같은 조건이다. 루프백 TCP에 프레임을 쓰고 읽는 스레드가 계속 비운다.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let reader = std::thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut buffer = vec![0; 64 << 10];
        while stream.read(&mut buffer)? > 0 {}
        Ok(())
    });
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    let frame = stomp::message(CONNECTION_ID, "0", &body);
    let count = 1_000_000;
    let start = Instant::now();
    for _ in 0..count {
        stream.write_all(black_box(frame.as_bytes()))?;
    }
    println!("TCP 소켓에 같은 크기 프레임 send: {:.1}ns", start.elapsed().as_nanos() as f64 / count as f64);
    drop(stream);
    reader.join().expect("읽는 스레드는 패닉하지 않는다")
}
