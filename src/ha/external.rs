use super::{Committed, Error, Publisher};
use crate::source::Envelope;
use std::time::{Duration, Instant};

pub trait ExternalSource {
    fn read_from(&mut self, next_seq: u64, max: usize, deadline: Instant) -> Result<Vec<Envelope>, Error>;
    fn acknowledge(&mut self, through: u64) -> Result<(), Error>;
}

pub fn pump_external(
    publisher: &mut Publisher,
    source: &mut impl ExternalSource,
    now_ms: u64,
    market_open: bool,
    deadline: Instant,
    guard: &mut impl FnMut() -> Result<(), Error>,
) -> Result<Committed, Error> {
    let mut recovering = false;
    while Instant::now() < deadline {
        guard()?;
        let next = publisher.checkpoint().next_seq()?;
        let batch = source.read_from(next, 100, deadline)?;
        if recovering && batch.is_empty() {
            std::thread::sleep(Duration::from_millis(1).min(deadline.saturating_duration_since(Instant::now())));
            continue;
        }
        match publisher.external(&batch, now_ms, market_open, guard) {
            Ok(committed) => {
                if let Some(seq) = committed.source_seq {
                    source.acknowledge(seq)?;
                }
                return Ok(committed);
            }
            Err(Error::SourceGap) => {
                recovering = true;
                // 미확정 prefix도 버리고 마지막 committed 다음부터 다시 요청한다.
                std::thread::sleep(Duration::from_millis(1).min(deadline.saturating_duration_since(Instant::now())));
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::SourceGap)
}
