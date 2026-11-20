use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct Limits {
    pub connections: usize,
    pub subscriptions: usize,
    pub commands_per_second: usize,
    pub topics: Vec<String>,
}

pub(crate) struct Rate {
    start: Instant,
    count: usize,
}

impl Default for Rate {
    fn default() -> Self {
        Self { start: Instant::now(), count: 0 }
    }
}

impl Rate {
    pub fn allow(&mut self, limit: usize) -> bool {
        self.at(limit, Instant::now())
    }

    fn at(&mut self, limit: usize, now: Instant) -> bool {
        if limit == 0 {
            return true;
        }
        if now.duration_since(self.start) >= Duration::from_secs(1) {
            self.start = now;
            self.count = 0;
        }
        if self.count >= limit {
            return false;
        }
        self.count += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_window_resets_and_zero_disables_it() {
        let mut rate = Rate::default();
        let now = rate.start;
        assert!(rate.at(2, now));
        assert!(rate.at(2, now));
        assert!(!rate.at(2, now));
        assert!(rate.at(2, now + Duration::from_secs(1)));
        assert!(rate.at(0, now + Duration::from_secs(1)));
    }
}
