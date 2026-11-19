use crate::pb::{ChangeType, Decimal, Exchange, Market, Session, Side, Trade};
use chrono::{DateTime, SecondsFormat};
use serde::Serialize;
use serde_json::value::RawValue;
use std::sync::Arc;

// 필드 선언 순서가 곧 JSON 키 순서다. 계약의 keyOrders와 같아야 한다.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KrTrade<'a> {
    code: &'a str,
    dt: String,
    currency: &'a str,
    exchange: &'static str,
    session: &'static str,
    base: Box<RawValue>,
    close: Box<RawValue>,
    volume: u64,
    trade_type: &'static str,
    change_type: &'static str,
    trading_strength: Box<RawValue>,
    cumulative_volume: u64,
    cumulative_amount: Box<RawValue>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UsTrade<'a> {
    code: &'a str,
    dt: String,
    session: &'static str,
    currency: &'a str,
    base: Box<RawValue>,
    close: Box<RawValue>,
    base_krw: Box<RawValue>,
    close_krw: Box<RawValue>,
    volume: u64,
    trade_type: &'static str,
    change_type: &'static str,
    trading_strength: Box<RawValue>,
    cumulative_volume: u64,
    cumulative_amount: Box<RawValue>,
    cumulative_amount_krw: Box<RawValue>,
}

// topic과 JSON 본문을 만든다. 필수 필드가 비었거나 enum이 UNSPECIFIED면 None이다.
pub fn to_message(trade: &Trade) -> Option<(String, Arc<str>)> {
    if trade.code.is_empty() || trade.currency.is_empty() {
        return None;
    }
    let dt = DateTime::from_timestamp_millis(trade.traded_at_ms)?.to_rfc3339_opts(SecondsFormat::Secs, true);
    let session = match trade.session() {
        Session::Regular => "REGULAR",
        Session::Day => "DAY",
        Session::Unspecified => return None,
    };
    let trade_type = match trade.side() {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
        Side::Unspecified => return None,
    };
    let change_type = match trade.change_type() {
        ChangeType::Up => "UP",
        ChangeType::Down => "DOWN",
        ChangeType::UpperLimit => "UPPER_LIMIT",
        ChangeType::Unspecified => return None,
    };
    let (code, currency, volume, cumulative_volume) =
        (trade.code.as_str(), trade.currency.as_str(), trade.volume, trade.cumulative_volume);
    let base = number(trade.base_price.as_ref()?);
    let close = number(trade.price.as_ref()?);
    let trading_strength = number(trade.trading_strength.as_ref()?);
    let cumulative_amount = number(trade.cumulative_amount.as_ref()?);
    let (market, body) = match trade.market() {
        Market::Kr => {
            let exchange = match trade.exchange() {
                Exchange::Krx => "krx",
                Exchange::Nxt => "nxt",
                Exchange::Unspecified => return None,
            };
            let body = KrTrade {
                code,
                dt,
                currency,
                exchange,
                session,
                base,
                close,
                volume,
                trade_type,
                change_type,
                trading_strength,
                cumulative_volume,
                cumulative_amount,
            };
            ("kr", serde_json::to_string(&body))
        }
        Market::Us => {
            let krw = trade.krw.as_ref()?;
            let body = UsTrade {
                code,
                dt,
                session,
                currency,
                base,
                close,
                base_krw: number(krw.base_price.as_ref()?),
                close_krw: number(krw.price.as_ref()?),
                volume,
                trade_type,
                change_type,
                trading_strength,
                cumulative_volume,
                cumulative_amount,
                cumulative_amount_krw: number(krw.cumulative_amount.as_ref()?),
            };
            ("us", serde_json::to_string(&body))
        }
        Market::Unspecified => return None,
    };
    let topic = format!("/topic/v1/{market}/stock/trade/{code}");
    Some((topic, Arc::from(body.expect("체결 구조체는 항상 직렬화된다"))))
}

// units / 10^scale을 f64를 거치지 않고 자릿수 그대로 JSON 숫자로 쓴다. units 15096, scale 2는 150.96이다.
fn number(decimal: &Decimal) -> Box<RawValue> {
    let digits = decimal.units.unsigned_abs().to_string();
    let sign = if decimal.units < 0 { "-" } else { "" };
    let scale = decimal.scale as usize;
    let text = if scale == 0 {
        format!("{sign}{digits}")
    } else {
        // 정수부가 한 자리는 남도록 앞을 0으로 채운다. units 5, scale 2는 0.05다.
        let digits = format!("{digits:0>width$}", width = scale + 1);
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        format!("{sign}{whole}.{fraction}")
    };
    RawValue::from_string(text).expect("부호와 숫자, 소수점으로만 만든 글자는 JSON 숫자다")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_keeps_its_digits_in_json() {
        for (units, scale, expected) in [
            (15096, 0, "15096"),
            (15096, 2, "150.96"),
            (15000, 2, "150.00"),
            (1_234_567_890_123_456_789, 18, "1.234567890123456789"),
            (1, 18, "0.000000000000000001"),
            (-15096, 2, "-150.96"),
            (5, 2, "0.05"),
            (-5, 2, "-0.05"),
        ] {
            assert_eq!(serde_json::to_string(&number(&Decimal { units, scale })).unwrap(), expected);
        }
    }
}
