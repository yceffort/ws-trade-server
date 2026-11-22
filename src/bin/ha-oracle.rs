use std::{
    io::Read,
    process::ExitCode,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use ws_trade_server::ha::{EVENTS, Error, Kafka, LogRecord, SHARD, SNAPSHOT, Snapshot, SourceConfig};

fn read<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(Error::TooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InvalidConfig)
}
fn run() -> Result<bool, Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(3..=5).contains(&args.len()) {
        return Err(Error::InvalidConfig);
    }
    let kafka: Kafka = read(&args[1])?;
    let source: SourceConfig = read(&args[2])?;
    let mut met = true;
    let result = match args[0].as_str() {
        "head" if args.len() == 3 => {
            let reader = kafka.reader(EVENTS, None)?;
            let (low, high) = reader.bounds();
            let snapshot = kafka
                .latest(SNAPSHOT, SHARD.as_bytes())?
                .map(|bytes| Snapshot::from_json(&bytes, &source))
                .transpose()?;
            let source_at = snapshot.and_then(|snapshot| snapshot.status.map(|status| status.at_ms));
            serde_json::json!({"log_id":source.log_id,"low":low.to_string(),"next":high.to_string(),"topics":source.topics,"source_status_at_ms":source_at})
        }
        "range" if args.len() == 5 => {
            let start = args[3].parse().map_err(|_| Error::InvalidConfig)?;
            let through = args[4].parse().map_err(|_| Error::InvalidConfig)?;
            serde_json::to_value(ws_trade_server::ha::oracle(&kafka, &source, start, through)?)
                .map_err(|_| Error::InvalidState)?
        }
        "retention" if args.len() == 4 => {
            let target: u64 = args[3].parse().map_err(|_| Error::InvalidConfig)?;
            let mut reader = kafka.reader(EVENTS, None)?;
            let (low, next) = reader.bounds();
            // offset 차이를 시간으로 환산하지 않고, 남아 있는 가장 오래된 committed 레코드의 시각을 읽는다.
            let deadline = Instant::now() + kafka.timeout();
            let mut oldest = None;
            while low < next && oldest.is_none() {
                if Instant::now() >= deadline {
                    return Err(Error::Deadline);
                }
                match reader.poll(Duration::from_millis(100))? {
                    Some(record) => {
                        oldest = Some(match LogRecord::from_json(&record.payload, &source)? {
                            LogRecord::Trade(event) => event.received_at_ms(),
                            LogRecord::Status(status) => status.at_ms.0,
                        });
                    }
                    // high watermark 아래에 abort나 제어 레코드만 있으면 committed 레코드 없이 끝에 닿는다.
                    None if reader.at_end() => break,
                    None => {}
                }
            }
            let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| Error::InvalidState)?.as_millis() as u64;
            let retained = oldest.map(|at| now.saturating_sub(at) / 1000);
            // 한 번도 지워지지 않은 로그는 아직 목표 시간이 지나지 않았을 뿐 잃은 구간이 없다.
            let truncated = low > 0;
            met = !truncated || retained.is_some_and(|seconds| seconds >= target);
            serde_json::json!({"log_id":source.log_id,"start":low.to_string(),"next":next.to_string(),"oldest_at_ms":oldest.map(|at| at.to_string()),
                "retained_seconds":retained,"target_seconds":target,"truncated":truncated,"meets_target":met})
        }
        _ => return Err(Error::InvalidConfig),
    };
    println!("{}", serde_json::to_string(&result).map_err(|_| Error::InvalidState)?);
    Ok(met)
}
fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        // 보관 범위가 목표보다 짧다. 설정 오류나 조회 실패(1)와 구분한다.
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
