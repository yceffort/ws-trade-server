// 체결 한 건이 형식마다 어떤 바이트가 되는지 찍어 본다. 사용: cargo run --example trade_bytes
use prost::Message;
use ws_trade_server::feed::{Generator, default_symbols};
use ws_trade_server::pb::Decimal;
use ws_trade_server::{stomp, trade};

fn hex(bytes: &[u8]) -> String {
    bytes
        .chunks(16)
        .map(|line| line.iter().map(|byte| format!("{byte:02x}")).collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn main() {
    let trade = Generator::new(default_symbols().remove(0)).next_trade(1);
    let record = trade.encode_to_vec();
    let (topic, body) = trade::to_message(&trade).expect("기본 종목은 topic이 있다");
    let frame = stomp::message("00000000-0000-4000-8000-000000000000-1", "0", &body);
    println!("protobuf 레코드 {}바이트\n{}\n", record.len(), hex(&record));
    println!("JSON 본문 {}바이트 ({topic})\n{body}\n", body.len());
    println!("STOMP 프레임 {}바이트\n", frame.len());

    // 유효 숫자 19자리인 값을 f64로 한 번 거치면 자릿수를 잃는다.
    let strength = Decimal { units: 1_042_857_142_857_142_857, scale: 16 };
    let through_f64 = strength.units as f64 / 10f64.powi(strength.scale as i32);
    println!("정수와 자릿수 그대로: {}.{}", strength.units / 10i64.pow(16), strength.units % 10i64.pow(16));
    println!("f64를 거친 값: {through_f64}");
}
