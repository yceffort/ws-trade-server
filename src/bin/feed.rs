use std::path::PathBuf;
use ws_trade_server::default_ring_path;
use ws_trade_server::feed::{self, Symbol, default_symbols, parse_symbols};
use ws_trade_server::ring::Writer;

struct Args {
    ring: PathBuf,
    symbols: Vec<Symbol>,
    rate: f64,
    capacity: u64,
}

fn parse_args() -> Option<Args> {
    let mut parsed = Args { ring: default_ring_path(), symbols: default_symbols(), rate: 5.0, capacity: 1 << 20 };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            "--ring" => parsed.ring = value.into(),
            "--symbols" => parsed.symbols = parse_symbols(&value)?,
            "--rate" => parsed.rate = value.parse().ok().filter(|rate: &f64| rate.is_finite() && *rate > 0.0)?,
            "--capacity" => parsed.capacity = value.parse().ok()?,
            _ => return None,
        }
    }
    Some(parsed)
}

fn main() {
    let Some(args) = parse_args() else {
        eprintln!("사용법: feed [--ring 경로] [--symbols kr:A005930,us:US19990122001] [--rate 5] [--capacity 1048576]");
        std::process::exit(2);
    };
    let mut writer = match Writer::open(&args.ring, args.capacity) {
        Ok(writer) => writer,
        Err(error) => {
            eprintln!("링 열기 실패: {error}");
            std::process::exit(1);
        }
    };
    eprintln!("링에 쓰는 중: {}", args.ring.display());
    if let Err(error) = feed::run(&mut writer, &args.symbols, args.rate) {
        eprintln!("링 쓰기 실패: {error}");
        std::process::exit(1);
    }
}
