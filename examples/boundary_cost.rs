// 체결 한 건이 넘는 경계마다 드는 비용을 같은 기계에서 잰다. 사용: cargo run --release --example boundary_cost
use prost::Message;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Instant;
use ws_trade_server::feed::{Generator, default_symbols};
use ws_trade_server::ring::Writer;
use ws_trade_server::{stomp, trade};

#[inline(never)]
fn add(a: u64, b: u64) -> u64 {
    a.wrapping_add(b)
}

fn report(label: &str, count: u64, start: Instant) {
    println!("{label}: {:.1}ns", start.elapsed().as_nanos() as f64 / count as f64);
}

fn main() -> std::io::Result<()> {
    let trade = Generator::new(default_symbols().remove(0)).next_trade(1);
    let record = trade.encode_to_vec();
    let (_, body) = trade::to_message(&trade).expect("기본 종목은 topic이 있다");
    let frame = stomp::message("00000000-0000-4000-8000-000000000000-1", "0", &body);
    println!("링 레코드 {}바이트, STOMP 프레임 {}바이트", record.len(), frame.len());

    let count = 200_000_000;
    let start = Instant::now();
    let mut sum = 0;
    for i in 0..count {
        sum = add(black_box(sum), black_box(i));
    }
    black_box(sum);
    report("함수 호출", count, start);

    let path = std::env::temp_dir().join(format!("boundary-cost-{}.ring", std::process::id()));
    let mut writer = Writer::open(&path, 1 << 20)?;

    // 열린 파일의 정보를 읽는 fstat은 커널이 하는 일이 적어서 커널에 다녀오는 비용에 가깝다.
    let file = std::fs::File::open(&path)?;
    let count = 5_000_000;
    let start = Instant::now();
    for _ in 0..count {
        black_box(file.metadata()?);
    }
    report("시스템 콜 fstat", count, start);

    let start = Instant::now();
    for _ in 0..count {
        writer.write(black_box(&record))?;
    }
    report("공유 메모리 링에 레코드 쓰기", count, start);
    drop(writer);
    std::fs::remove_file(&path)?;

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
    let count = 1_000_000;
    let start = Instant::now();
    for _ in 0..count {
        stream.write_all(black_box(frame.as_bytes()))?;
    }
    report("TCP 소켓에 프레임 send", count, start);
    drop(stream);
    reader.join().expect("읽는 스레드는 패닉하지 않는다")
}
