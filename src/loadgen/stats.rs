use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Instant;

// 탐침이 받은 체결 하나. 받은 시각은 프레임을 읽은 직후에 찍고, 본문 파싱은 그 뒤에 한다.
pub struct Sample {
    pub received: Instant,
    pub symbol: usize,
    pub cumulative_volume: u64,
}

// 링에 쓰기 직전에 찍은 시각.
pub struct Stamp {
    pub symbol: usize,
    pub cumulative_volume: u64,
    pub written: Instant,
}

// nearest-rank 방식이다. 오름차순 n개에서 p백분위는 ceil(p/100 * n)번째 값이라 표본이 적어도 실제로 관측한 값만 나온다.
pub fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len().max(1)) - 1).copied()
}

// 쓴 시각이 [start, stop) 안인 표본만 지연으로 센다. 나머지는 버리지 않고 건수로 남긴다.
pub fn latency_report<'a>(
    stamps: &[Stamp],
    samples: impl Iterator<Item = &'a Sample>,
    unparsed: u64,
    (start, stop): (Instant, Instant),
) -> Value {
    let written: HashMap<(usize, u64), Instant> =
        stamps.iter().map(|stamp| ((stamp.symbol, stamp.cumulative_volume), stamp.written)).collect();
    let (mut unmatched, mut outside_window) = (0, 0);
    let mut micros = Vec::new();
    for sample in samples {
        match written.get(&(sample.symbol, sample.cumulative_volume)) {
            None => unmatched += 1,
            Some(at) if *at < start || *at >= stop => outside_window += 1,
            Some(at) => micros.push(sample.received.saturating_duration_since(*at).as_micros() as u64),
        }
    }
    micros.sort_unstable();
    json!({
        "samples": micros.len(),
        "unmatched": unmatched,
        "unparsed": unparsed,
        "outside_window": outside_window,
        "p50": percentile(&micros, 50.0),
        "p90": percentile(&micros, 90.0),
        "p99": percentile(&micros, 99.0),
        "max": micros.last(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn percentile_is_nearest_rank() {
        let hundred: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&hundred, 50.0), Some(50));
        assert_eq!(percentile(&hundred, 90.0), Some(90));
        assert_eq!(percentile(&hundred, 99.0), Some(99));
        assert_eq!(percentile(&hundred, 100.0), Some(100));
        // 표본이 10개면 p99는 10번째, 곧 최댓값이다. 보간하지 않는다.
        let ten: Vec<u64> = (1..=10).map(|n| n * 10).collect();
        assert_eq!(
            (percentile(&ten, 50.0), percentile(&ten, 90.0), percentile(&ten, 99.0)),
            (Some(50), Some(90), Some(100))
        );
        assert_eq!(percentile(&[7], 50.0), Some(7));
        assert_eq!(percentile(&[7], 99.0), Some(7));
        assert_eq!(percentile(&[], 50.0), None);
    }

    #[test]
    fn latency_counts_what_it_excludes() {
        let base = Instant::now();
        let at = |ms: u64| base + Duration::from_millis(ms);
        let stamp = |cumulative_volume, ms| Stamp { symbol: 0, cumulative_volume, written: at(ms) };
        let sample = |symbol, cumulative_volume, ms| Sample { received: at(ms), symbol, cumulative_volume };
        // 구간은 [100, 200). 1번은 구간 앞, 4번은 구간 끝(제외), 9번은 쓴 기록이 없다.
        let stamps = [stamp(1, 90), stamp(2, 110), stamp(3, 150), stamp(4, 200)];
        let samples = [sample(0, 1, 95), sample(0, 2, 112), sample(0, 3, 160), sample(0, 4, 201), sample(0, 9, 150)];
        let report = latency_report(&stamps, samples.iter(), 1, (at(100), at(200)));
        assert_eq!(
            report,
            json!({"samples": 2, "unmatched": 1, "unparsed": 1, "outside_window": 2,
                   "p50": 2000, "p90": 10000, "p99": 10000, "max": 10000})
        );
    }
}
