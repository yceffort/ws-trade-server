use ws_trade_server::corrections::{ApplyError, Event, Ledger};

fn create(id: &str, quantity: u64) -> Event {
    Event::Create { event_id: id.into(), trade_id: id.into(), price_minor: 100, quantity }
}
fn correct(event_id: &str, target_id: &str, quantity: u64) -> Event {
    Event::Correct { event_id: event_id.into(), target_id: target_id.into(), price_minor: 120, quantity }
}
fn cancel(event_id: &str, target_id: &str) -> Event {
    Event::Cancel { event_id: event_id.into(), target_id: target_id.into() }
}

#[test]
fn correction_and_cancellation_adjust_totals_once() {
    let mut ledger = Ledger::new(10);
    ledger.apply(create("a", 10)).unwrap();
    ledger.apply(create("b", 5)).unwrap();
    let correction = correct("c", "a", 3);
    assert_eq!(ledger.apply(correction.clone()), Ok(true));
    assert_eq!(ledger.totals(), (8, 860));
    assert_eq!(ledger.apply(correction), Ok(false));
    let cancellation = cancel("d", "a");
    assert_eq!(ledger.apply(cancellation.clone()), Ok(true));
    assert_eq!(ledger.apply(cancellation), Ok(false));
    assert_eq!(ledger.totals(), (5, 500));
    assert_eq!(ledger.apply(cancel("e", "a")), Err(ApplyError::AlreadyCancelled));
    assert_eq!(ledger.apply(create("a", 7)), Err(ApplyError::ConflictingEvent));
}

#[test]
fn invalid_events_leave_totals_and_history_unchanged() {
    let mut ledger = Ledger::new(2);
    assert_eq!(ledger.apply(correct("x", "missing", 1)), Err(ApplyError::UnknownTrade));
    assert_eq!(ledger.apply(create("a", 0)), Err(ApplyError::InvalidValue));
    ledger.apply(create("a", 1)).unwrap();
    assert_eq!(ledger.apply(correct("x", "a", 0)), Err(ApplyError::InvalidValue));
    ledger.apply(correct("x", "a", 2)).unwrap();
    assert_eq!(ledger.apply(cancel("z", "a")), Err(ApplyError::Capacity));
    assert_eq!(ledger.totals(), (2, 240));
}

#[test]
fn overflow_does_not_partially_apply_an_event() {
    let mut ledger = Ledger::new(10);
    ledger.apply(create("a", u64::MAX)).unwrap();
    assert_eq!(ledger.apply(create("b", 1)), Err(ApplyError::Overflow));
    ledger.apply(cancel("cancel", "a")).unwrap();
    assert_eq!(ledger.apply(create("b", 1)), Ok(true));
    assert_eq!(ledger.totals(), (1, 100));
}
