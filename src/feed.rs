use crate::pb::{self, ChangeType, Decimal, Exchange, KrwConversion, Session, Side, Trade};
use crate::ring::Writer;
use chrono::Utc;
use prost::Message;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub enum Market {
    Kr,
    Us,
}

#[derive(Clone)]
pub struct Symbol {
    pub market: Market,
    pub code: String,
}

impl Symbol {
    pub fn topic(&self) -> String {
        let market = match self.market {
            Market::Kr => "kr",
            Market::Us => "us",
        };
        format!("/topic/v1/{market}/stock/trade/{}", self.code)
    }
}

// feed와 loadgen이 같이 쓰는 --symbols 기본값.
pub fn default_symbols() -> Vec<Symbol> {
    let symbol = |market, code: &str| Symbol { market, code: code.to_string() };
    vec![symbol(Market::Kr, "A005930"), symbol(Market::Kr, "A000660"), symbol(Market::Us, "US19990122001")]
}

// "kr:A005930,us:US19990122001" 꼴을 읽는다.
pub fn parse_symbols(text: &str) -> Option<Vec<Symbol>> {
    text.split(',')
        .map(|item| {
            let (market, code) = item.split_once(':')?;
            let market = match market {
                "kr" => Market::Kr,
                "us" => Market::Us,
                _ => return None,
            };
            Some(Symbol { market, code: code.to_string() })
        })
        .collect()
}

// 가격은 국내는 원, 미국은 센트 정수로 들고 있다. 소수는 Decimal의 자릿수로만 나타낸다.
pub struct Generator {
    symbol: Symbol,
    base: i64,
    close: i64,
    cumulative_volume: u64,
    cumulative_amount: i64,
}

impl Generator {
    pub fn new(symbol: Symbol) -> Generator {
        let base = match symbol.market {
            Market::Kr => 70_000,
            Market::Us => 15_000,
        };
        Generator { symbol, base, close: base, cumulative_volume: 0, cumulative_amount: 0 }
    }

    pub fn next_trade(&mut self, seq: u64) -> Trade {
        // 국내 가격은 원 단위 정수(자릿수 0), 미국 가격은 센트라 자릿수 2다.
        let (tick, scale) = match self.symbol.market {
            Market::Kr => (100, 0),
            Market::Us => (5, 2),
        };
        self.close = (self.close + rand::random_range(-1..=1) * tick).max(tick);
        let volume = rand::random_range(1..=20);
        self.cumulative_volume += volume;
        self.cumulative_amount += volume as i64 * self.close;

        let mut trade = Trade {
            seq,
            code: self.symbol.code.clone(),
            traded_at_ms: Utc::now().timestamp_millis(),
            price: Some(Decimal { units: self.close, scale }),
            base_price: Some(Decimal { units: self.base, scale }),
            volume,
            trading_strength: Some(Decimal { units: rand::random_range(5_000..15_000), scale: 2 }),
            cumulative_volume: self.cumulative_volume,
            ..Trade::default()
        };
        trade.set_side(if rand::random() { Side::Buy } else { Side::Sell });
        trade.set_change_type(if self.close >= self.base { ChangeType::Up } else { ChangeType::Down });
        match self.symbol.market {
            Market::Kr => {
                trade.set_market(pb::Market::Kr);
                trade.set_exchange(if rand::random() { Exchange::Krx } else { Exchange::Nxt });
                trade.set_session(Session::Regular);
                trade.currency = "KRW".to_string();
                trade.cumulative_amount = Some(Decimal { units: self.cumulative_amount, scale: 0 });
            }
            Market::Us => {
                trade.set_market(pb::Market::Us);
                trade.set_session(Session::Day);
                trade.currency = "USD".to_string();
                // 누적 거래대금은 계약상 정수라 달러 아래는 버린다.
                trade.cumulative_amount = Some(Decimal { units: self.cumulative_amount / 100, scale: 0 });
                trade.krw = Some(KrwConversion {
                    price: Some(cents_to_krw(self.close)),
                    base_price: Some(cents_to_krw(self.base)),
                    cumulative_amount: Some(cents_to_krw(self.cumulative_amount)),
                });
            }
        }
        trade
    }
}

// 환율 1382.5원. 센트 × 1382.5는 원의 100배이므로 자릿수 2인 원이 되고, 그 아래는 버린다.
fn cents_to_krw(cents: i64) -> Decimal {
    Decimal { units: cents * 13_825 / 10, scale: 2 }
}

// 종목마다 평균 초당 rate건, 간격은 지수분포로 불규칙하다. 쓰는 쪽은 하나여야 하므로 스레드 하나가 모든 종목의 차례를 돌며 쓴다.
pub fn run(writer: &mut Writer, symbols: &[Symbol], rate: f64) -> io::Result<()> {
    let wait = || Duration::from_secs_f64(-(1.0 - rand::random::<f64>()).ln() / rate);
    let mut schedule: Vec<(Instant, Generator)> =
        symbols.iter().map(|symbol| (Instant::now() + wait(), Generator::new(symbol.clone()))).collect();
    for seq in 1.. {
        let (due, generator) = schedule.iter_mut().min_by_key(|(due, _)| *due).expect("종목은 하나 이상이다");
        thread::sleep(due.saturating_duration_since(Instant::now()));
        *due += wait();
        writer.write(&generator.next_trade(seq).encode_to_vec())?;
    }
    Ok(())
}
