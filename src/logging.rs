mod reporter;
use crate::hub::Hub;
pub use reporter::Reporter;
use std::io;
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::{EnvFilter, LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Logging {
    _worker: WorkerGuard,
    dropped: ErrorCounter,
}

impl Logging {
    pub fn reporter(&self, hub: Hub) -> Reporter {
        Reporter::start(hub, self.dropped.clone())
    }
}

pub fn init() -> Result<Logging, Error> {
    let format = std::env::var("WTS_LOG_FORMAT").unwrap_or_else(|_| "text".into());
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "ws_trade_server=info".into());
    let (writer, worker) = NonBlockingBuilder::default().buffered_lines_limit(1024).lossy(true).finish(io::stderr());
    let dropped = writer.error_counter();
    tracing::dispatcher::set_global_default(subscriber(writer, &format, &filter)?)?;
    Ok(Logging { _worker: worker, dropped })
}

fn subscriber(writer: NonBlocking, format: &str, filter: &str) -> Result<tracing::Dispatch, Error> {
    let filter =
        EnvFilter::try_new(filter).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "RUST_LOG 설정 오류"))?;
    // trace 필터에서도 외부 라이브러리가 원본 프레임을 출력하지 않도록 대상을 고정한다.
    let targets = Targets::new().with_target("ws_trade_server", LevelFilter::TRACE);
    let builder = tracing_subscriber::fmt().with_writer(writer).with_ansi(false).with_env_filter(filter);
    match format {
        "json" => Ok(tracing::Dispatch::new(builder.json().finish().with(targets))),
        "text" => Ok(tracing::Dispatch::new(builder.finish().with(targets))),
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "WTS_LOG_FORMAT은 text 또는 json이어야 한다").into()),
    }
}

#[cfg(test)]
mod tests;
