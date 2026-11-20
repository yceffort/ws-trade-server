use super::{Error, Number, SourceConfig};
use crate::pb::{self, ChangeType, Decimal, Exchange, KrwConversion, Session, Side, Trade};
use crate::source::{Envelope, ReceivedAtMs, SourceSeq};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Symbol {
    topic: String,
    close: i64,
    volume: Number,
    amount: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Synthetic {
    schema_version: u32,
    random: Number,
    clock_ms: Number,
    cursor: usize,
    symbols: Vec<Symbol>,
}

fn symbol(topic: &str) -> Result<(bool, &str), Error> {
    for (prefix, us) in [("/topic/v1/kr/stock/trade/", false), ("/topic/v1/us/stock/trade/", true)] {
        if let Some(code) = topic.strip_prefix(prefix).filter(|code| !code.is_empty()) {
            return Ok((us, code));
        }
    }
    Err(Error::InvalidConfig)
}

impl Synthetic {
    pub fn new(config: &SourceConfig, seed: u64, clock_ms: u64) -> Result<Self, Error> {
        config.validate()?;
        let symbols = config
            .topics
            .iter()
            .map(|topic| {
                let (us, _) = symbol(topic)?;
                Ok(Symbol {
                    topic: topic.clone(),
                    close: if us { 15_000 } else { 70_000 },
                    volume: Number(0),
                    amount: 0,
                })
            })
            .collect::<Result<_, Error>>()?;
        let state = Self { schema_version: 1, random: Number(seed), clock_ms: Number(clock_ms), cursor: 0, symbols };
        state.validate(config)?;
        Ok(state)
    }

    pub fn validate(&self, config: &SourceConfig) -> Result<(), Error> {
        if self.schema_version != 1
            || self.symbols.is_empty()
            || self.cursor >= self.symbols.len()
            || !self.symbols.iter().map(|s| &s.topic).eq(config.topics.iter())
            || self.clock_ms.0 > i64::MAX as u64
            || chrono::DateTime::from_timestamp_millis(self.clock_ms.0 as i64).is_none()
        {
            return Err(Error::InvalidState);
        }
        for state in &self.symbols {
            symbol(&state.topic)?;
            if state.close <= 0 || state.amount < 0 {
                return Err(Error::InvalidState);
            }
        }
        Ok(())
    }

    // 합성 시세 전용 SplitMix64다. 보안 난수에 쓰지 않으며 정수 상태와 호출 순서를 checkpoint에 고정한다.
    fn draw(&mut self) -> u64 {
        self.random.0 = self.random.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.random.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    pub fn next(&mut self, config: &SourceConfig, seq: u64, received_at_ms: u64) -> Result<Envelope, Error> {
        let delta = (self.draw() % 3) as i64 - 1;
        let volume = self.draw() % 20 + 1;
        let strength = (self.draw() % 10_000 + 5_000) as i64;
        let buy = self.draw() & 1 == 0;
        let krx = self.draw() & 1 == 0;
        let state = self.symbols.get_mut(self.cursor).ok_or(Error::InvalidState)?;
        let (us, code) = symbol(&state.topic)?;
        let (tick, scale, base) = if us { (5, 2, 15_000) } else { (100, 0, 70_000) };
        state.close = state.close.checked_add(delta * tick).ok_or(Error::Exhausted)?.max(tick);
        state.volume.0 = state.volume.0.checked_add(volume).ok_or(Error::Exhausted)?;
        state.amount = state
            .amount
            .checked_add(state.close.checked_mul(volume as i64).ok_or(Error::Exhausted)?)
            .ok_or(Error::Exhausted)?;
        let mut trade = Trade {
            seq,
            code: code.into(),
            traded_at_ms: i64::try_from(self.clock_ms.0).map_err(|_| Error::Exhausted)?,
            price: Some(Decimal { units: state.close, scale }),
            base_price: Some(Decimal { units: base, scale }),
            volume,
            cumulative_volume: state.volume.0,
            cumulative_amount: Some(Decimal { units: if us { state.amount / 100 } else { state.amount }, scale: 0 }),
            trading_strength: Some(Decimal { units: strength, scale: 2 }),
            currency: if us { "USD" } else { "KRW" }.into(),
            ..Trade::default()
        };
        trade.set_market(if us { pb::Market::Us } else { pb::Market::Kr });
        trade.set_side(if buy { Side::Buy } else { Side::Sell });
        trade.set_change_type(if state.close >= base { ChangeType::Up } else { ChangeType::Down });
        trade.set_session(if us { Session::Day } else { Session::Regular });
        if us {
            let krw = |cents: i64| -> Result<Decimal, Error> {
                Ok(Decimal { units: cents.checked_mul(13_825).ok_or(Error::Exhausted)? / 10, scale: 2 })
            };
            trade.krw = Some(KrwConversion {
                price: Some(krw(state.close)?),
                base_price: Some(krw(base)?),
                cumulative_amount: Some(krw(state.amount)?),
            });
        } else {
            trade.set_exchange(if krx { Exchange::Krx } else { Exchange::Nxt });
        }
        self.cursor = (self.cursor + 1) % self.symbols.len();
        self.clock_ms.0 = self.clock_ms.0.checked_add(10).ok_or(Error::Exhausted)?;
        Envelope::new(
            config.log_id.clone(),
            config.source_id.clone(),
            config.epoch.clone(),
            SourceSeq(seq),
            trade,
            ReceivedAtMs(received_at_ms),
        )
        .map_err(|_| Error::InvalidState)
    }
}
