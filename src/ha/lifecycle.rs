use serde::Serialize;
use std::time::Duration;
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    Bootstrapping,
    Ready,
    Degraded,
    Draining,
    Stopped,
}

#[derive(Clone, Copy)]
struct State {
    phase: Phase,
    generation: u64,
    started: Option<Instant>,
}

#[derive(Clone)]
pub struct Lifecycle {
    state: watch::Sender<State>,
    routing: Duration,
    spread: Duration,
    flush: Duration,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new(Duration::from_secs(20), Duration::from_secs(10), Duration::from_secs(10)).expect("40초 drain 예산")
    }
}

impl Lifecycle {
    pub fn new(routing: Duration, spread: Duration, flush: Duration) -> Result<Self, &'static str> {
        if routing.saturating_add(spread).saturating_add(flush) > Duration::from_secs(40) || flush.is_zero() {
            return Err("invalid-drain-budget");
        }
        let (state, _) = watch::channel(State { phase: Phase::Bootstrapping, generation: 0, started: None });
        Ok(Self { state, routing, spread, flush })
    }

    pub fn phase(&self) -> Phase {
        self.state.borrow().phase
    }
    pub fn ready(&self) -> bool {
        self.phase() == Phase::Ready
    }
    pub fn health(&self, ready: bool, failed: bool) {
        self.state.send_if_modified(|state| {
            if matches!(state.phase, Phase::Draining | Phase::Stopped) {
                return false;
            }
            let next = if ready && !failed {
                Phase::Ready
            } else if state.phase == Phase::Bootstrapping && !failed {
                Phase::Bootstrapping
            } else {
                Phase::Degraded
            };
            if state.phase == next {
                return false;
            }
            if next == Phase::Degraded {
                state.generation += 1;
            }
            state.phase = next;
            tracing::info!(event = "ha_lifecycle_changed", phase = ?next);
            true
        });
    }
    pub fn drain(&self) {
        self.state.send_if_modified(|state| {
            if matches!(state.phase, Phase::Draining | Phase::Stopped) {
                return false;
            }
            state.phase = Phase::Draining;
            state.started = Some(Instant::now());
            tracing::info!(event = "ha_drain_started");
            true
        });
    }
    pub fn drain_started(&self) -> Option<Instant> {
        self.state.borrow().started
    }
    pub fn close_deadline(&self) -> Option<Instant> {
        self.drain_started().map(|start| start + self.routing + self.spread + self.flush)
    }
    pub fn flush_started(&self) -> bool {
        self.drain_started().is_some_and(|start| Instant::now() >= start + self.routing + self.spread)
    }
    pub fn routing_deadline(&self) -> Option<Instant> {
        self.drain_started().map(|start| start + self.routing)
    }
    pub fn stop(&self) {
        self.state.send_modify(|state| state.phase = Phase::Stopped);
        tracing::info!(event = "ha_lifecycle_changed", phase = ?Phase::Stopped);
    }
    pub async fn stopped(&self) {
        let mut state = self.state.subscribe();
        loop {
            if state.borrow_and_update().phase == Phase::Stopped {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    }
    pub async fn close_reason(&self, conn: u64) -> &'static str {
        let mut receiver = self.state.subscribe();
        let generation = receiver.borrow().generation;
        loop {
            let state = *receiver.borrow_and_update();
            if state.generation != generation || matches!(state.phase, Phase::Degraded | Phase::Bootstrapping) {
                return "source-stale";
            }
            if state.phase == Phase::Stopped {
                return "server-shutdown";
            }
            if let Some(started) = state.started {
                let fraction = (conn.wrapping_mul(2654435761) % 10000) as f64 / 10000.0;
                tokio::time::sleep_until(started + self.routing + self.spread.mul_f64(fraction)).await;
                return "draining";
            }
            if receiver.changed().await.is_err() {
                return "server-shutdown";
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn drain_is_monotonic_and_does_not_extend_its_budget() {
        let life = Lifecycle::new(Duration::from_millis(20), Duration::ZERO, Duration::from_millis(20)).unwrap();
        assert!(!life.ready());
        life.health(true, false);
        assert!(life.ready());
        life.drain();
        let deadline = life.close_deadline();
        life.health(true, false);
        life.drain();
        assert_eq!(life.close_deadline(), deadline);
        assert_eq!(life.phase(), Phase::Draining);
        assert!(!life.ready());
        assert_eq!(life.close_reason(1).await, "draining");
        life.stop();
        life.health(true, false);
        assert_eq!(life.phase(), Phase::Stopped);
    }
    #[test]
    fn failed_observation_overrides_stale_ready_view() {
        let life = Lifecycle::default();
        life.health(true, false);
        life.health(true, true);
        assert_eq!(life.phase(), Phase::Degraded);
        life.health(true, false);
        assert!(life.ready());
    }

    #[tokio::test]
    async fn a_coalesced_degradation_invalidates_existing_connections() {
        let life = Lifecycle::default();
        life.health(true, false);
        let copy = life.clone();
        let waiting = tokio::spawn(async move { copy.close_reason(1).await });
        tokio::task::yield_now().await;
        life.health(false, false);
        life.health(true, false);
        assert_eq!(waiting.await.unwrap(), "source-stale");
        assert!(life.ready());
    }
}
