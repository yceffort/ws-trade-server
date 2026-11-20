use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const TOPIC: &str = "/experimental/trade-events";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Event {
    Create { event_id: String, trade_id: String, price_minor: i64, quantity: u64 },
    Correct { event_id: String, target_id: String, price_minor: i64, quantity: u64 },
    Cancel { event_id: String, target_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyError {
    InvalidValue,
    UnknownTrade,
    DuplicateTrade,
    AlreadyCancelled,
    ConflictingEvent,
    Capacity,
    Overflow,
}

#[derive(Clone, Copy)]
struct Trade {
    price: i64,
    quantity: u64,
}

// 정정 대상을 잊지 않도록 자동 퇴출 대신 용량 초과를 호출자에게 돌려준다.
pub struct Ledger {
    capacity: usize,
    events: HashMap<String, Event>,
    trades: HashMap<String, Option<Trade>>,
    quantity: u64,
    amount_minor: i128,
}

impl Ledger {
    pub fn new(capacity: usize) -> Self {
        Self { capacity, events: HashMap::new(), trades: HashMap::new(), quantity: 0, amount_minor: 0 }
    }

    pub fn totals(&self) -> (u64, i128) {
        (self.quantity, self.amount_minor)
    }

    pub fn apply(&mut self, event: Event) -> Result<bool, ApplyError> {
        let (event_id, id, replacement) = match &event {
            Event::Create { event_id, trade_id, price_minor, quantity } => {
                (event_id, trade_id, Some(Trade { price: *price_minor, quantity: *quantity }))
            }
            Event::Correct { event_id, target_id, price_minor, quantity } => {
                (event_id, target_id, Some(Trade { price: *price_minor, quantity: *quantity }))
            }
            Event::Cancel { event_id, target_id } => (event_id, target_id, None),
        };
        if event_id.is_empty() || id.is_empty() || replacement.is_some_and(|t| t.price <= 0 || t.quantity == 0) {
            return Err(ApplyError::InvalidValue);
        }
        if let Some(previous) = self.events.get(event_id) {
            return if previous == &event { Ok(false) } else { Err(ApplyError::ConflictingEvent) };
        }
        if self.events.len() >= self.capacity {
            return Err(ApplyError::Capacity);
        }
        let old = match &event {
            Event::Create { .. } => {
                if self.trades.contains_key(id) {
                    return Err(ApplyError::DuplicateTrade);
                }
                None
            }
            _ => Some(self.trades.get(id).ok_or(ApplyError::UnknownTrade)?.ok_or(ApplyError::AlreadyCancelled)?),
        };
        let amount = |trade: Option<Trade>| trade.map_or(0, |t| i128::from(t.price) * i128::from(t.quantity));
        let quantity = self.quantity - old.map_or(0, |t| t.quantity);
        let quantity = quantity.checked_add(replacement.map_or(0, |t| t.quantity)).ok_or(ApplyError::Overflow)?;
        let amount = (self.amount_minor - amount(old)).checked_add(amount(replacement)).ok_or(ApplyError::Overflow)?;
        self.quantity = quantity;
        self.amount_minor = amount;
        self.trades.insert(id.clone(), replacement);
        self.events.insert(event_id.clone(), event);
        Ok(true)
    }
}
