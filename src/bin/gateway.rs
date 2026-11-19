use ws_trade_server::{Config, default_ring_path, start};

fn parse_args() -> Option<Config> {
    let mut config = Config { ring: Some(default_ring_path()), ..Config::default() };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            "--addr" => config.addr = value,
            "--ring" => config.ring = Some(value.into()),
            "--poll-us" => config.poll_us = value.parse().ok()?,
            _ => return None,
        }
    }
    Some(config)
}

#[tokio::main]
async fn main() {
    let Some(config) = parse_args() else {
        eprintln!("사용법: gateway [--addr 127.0.0.1:8080] [--ring 경로] [--poll-us 200]");
        std::process::exit(2);
    };
    match start(config).await {
        Ok((addr, _hub)) => {
            eprintln!("수신 대기: {addr}");
            std::future::pending::<()>().await;
        }
        Err(error) => {
            eprintln!("시작 실패: {error}");
            std::process::exit(1);
        }
    }
}
