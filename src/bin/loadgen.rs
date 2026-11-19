use std::path::PathBuf;
use ws_trade_server::feed::parse_symbols;
use ws_trade_server::loadgen::{Args, run};

const USAGE: &str = "사용법: loadgen [--url ws://127.0.0.1:8080/ws] [--ring 경로] [--connections 100] [--symbols kr:A005930,us:US19990122001] [--rate 5] [--stalled 0] [--probes 50] [--warmup 3] [--seconds 20] [--out 파일]";

fn parse_args() -> Option<(Args, Option<PathBuf>)> {
    let (mut parsed, mut out) = (Args::default(), None);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            "--url" => parsed.url = value,
            "--ring" => parsed.ring = value.into(),
            "--connections" => parsed.connections = value.parse().ok()?,
            "--symbols" => parsed.symbols = parse_symbols(&value)?,
            "--rate" => parsed.rate = value.parse().ok().filter(|rate: &f64| rate.is_finite() && *rate > 0.0)?,
            "--stalled" => parsed.stalled = value.parse().ok()?,
            "--probes" => parsed.probes = value.parse().ok()?,
            "--warmup" => parsed.warmup = value.parse().ok()?,
            "--seconds" => parsed.seconds = value.parse().ok().filter(|seconds| *seconds > 0)?,
            "--out" => out = Some(value.into()),
            _ => return None,
        }
    }
    (parsed.stalled + parsed.probes <= parsed.connections).then_some((parsed, out))
}

#[tokio::main]
async fn main() {
    let Some((args, out)) = parse_args() else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    let report = match run(args).await {
        Ok(report) => serde_json::to_string_pretty(&report).expect("JSON 값은 항상 직렬화된다"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    match out {
        Some(path) => std::fs::write(path, report + "\n").unwrap_or_else(|e| {
            eprintln!("결과 쓰기 실패: {e}");
            std::process::exit(1);
        }),
        None => println!("{report}"),
    }
}
