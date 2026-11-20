// 힙 링으로 도는 테스트. Miri는 파일 mmap을 지원하지 않아서 이 파일만 Miri로 돌린다.
// cargo +nightly miri test --test ring_miri
mod ring_check;

use ring_check::*;
use ws_trade_server::ring::in_memory;

const CAPACITY: u64 = 8192;

#[test]
fn reads_back_in_written_order() {
    let (writer, reader) = in_memory(CAPACITY).unwrap();
    in_order(writer, reader);
}

#[test]
fn follows_many_laps_through_skip_markers() {
    let (writer, reader) = in_memory(CAPACITY).unwrap();
    laps(writer, reader, CAPACITY);
}

#[test]
fn lapped_reader_reports_loss_then_reads_again() {
    let (writer, reader) = in_memory(CAPACITY).unwrap();
    loss(writer, reader, CAPACITY);
}

#[test]
fn two_threads_under_load_never_see_a_torn_record() {
    let (writer, reader) = in_memory(CAPACITY).unwrap();
    // Miri에서는 500번째 레코드 뒤의 강제 덮어쓰기를 유지하며 해석할 레코드 수를 줄인다.
    stress(writer, reader, if cfg!(miri) { 800 } else { 2000 });
}
