use std::{io::Read, process::ExitCode};
use ws_trade_server::ha::{EVENTS, Error, Kafka, SHARD, SNAPSHOT, Snapshot, SourceConfig};

fn read<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(Error::TooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InvalidConfig)
}
fn run() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 && args.len() != 5 {
        return Err(Error::InvalidConfig);
    }
    let kafka: Kafka = read(&args[1])?;
    let source: SourceConfig = read(&args[2])?;
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
        _ => return Err(Error::InvalidConfig),
    };
    println!("{}", serde_json::to_string(&result).map_err(|_| Error::InvalidState)?);
    Ok(())
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
