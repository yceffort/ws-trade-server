mod ring_check;
mod temp_file;

use ring_check::*;
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use temp_file::TempFile;
use ws_trade_server::ring::{MAX_PAYLOAD, Read, Reader, Writer};

const CAPACITY: u64 = 8192;
const TAIL_INTENT: u64 = 16;
const TAIL: u64 = 24;

// 쓰는 쪽과 읽는 쪽이 같은 파일을 각자 매핑한다.
fn open(file: &TempFile) -> (Writer, Reader) {
    let writer = Writer::open(file.path(), CAPACITY).unwrap();
    (writer, Reader::open(file.path()).unwrap())
}

fn header(file: &TempFile, offset: u64) -> u64 {
    let mut bytes = [0; 8];
    File::open(file.path()).unwrap().read_exact_at(&mut bytes, offset).unwrap();
    u64::from_le_bytes(bytes)
}

fn record_size(payload: &[u8]) -> u64 {
    (4 + payload.len()).next_multiple_of(8) as u64
}

#[test]
fn reads_back_in_written_order() {
    let file = TempFile::new("order");
    let (writer, reader) = open(&file);
    in_order(writer, reader);
}

#[test]
fn follows_many_laps_through_skip_markers() {
    let file = TempFile::new("laps");
    let (writer, reader) = open(&file);
    laps(writer, reader, CAPACITY);
}

#[test]
fn lapped_reader_reports_loss_then_reads_again() {
    let file = TempFile::new("loss");
    let (writer, reader) = open(&file);
    loss(writer, reader, CAPACITY);
}

#[test]
fn two_readers_at_different_speeds_each_get_everything() {
    let file = TempFile::new("two-readers");
    let (mut writer, mut fast) = open(&file);
    let mut slow = Reader::open(file.path()).unwrap();
    for seq in 1..=60 {
        writer.write(&payload(seq)).unwrap();
        assert_eq!(seq_of(&read_record(&mut fast)), seq);
        if seq % 20 == 0 {
            for expected in seq - 19..=seq {
                assert_eq!(seq_of(&read_record(&mut slow)), expected);
            }
        }
    }
    assert!(matches!(fast.read(), Read::Empty));
    assert!(matches!(slow.read(), Read::Empty));
}

#[test]
fn two_threads_under_load_never_see_a_torn_record() {
    let file = TempFile::new("stress");
    let (writer, reader) = open(&file);
    stress(writer, reader, 200_000);
}

#[test]
fn length_running_past_region_end_is_treated_as_loss() {
    let file = TempFile::new("torn-len");
    let (mut writer, mut reader) = open(&file);
    let big = vec![0; MAX_PAYLOAD];
    writer.write(&big).unwrap();
    read_record(&mut reader);
    writer.write(&payload(1)).unwrap();
    // 다음 레코드의 len을 4096 이하지만 데이터 영역 끝을 넘는 값으로 바꿔 덮어쓰인 자리를 흉내 낸다.
    let len_offset = 64 + record_size(&big);
    let bogus = (MAX_PAYLOAD as u32).to_le_bytes();
    File::options().write(true).open(file.path()).unwrap().write_all_at(&bogus, len_offset).unwrap();
    assert!(matches!(reader.read(), Read::Lost));
}

#[test]
fn reopened_writer_continues_from_tail_and_open_reader_keeps_reading() {
    let file = TempFile::new("reopen");
    let (mut writer, mut reader) = open(&file);
    writer.write(&payload(1)).unwrap();
    assert_eq!(seq_of(&read_record(&mut reader)), 1);
    let tail = header(&file, TAIL);
    drop(writer);

    let mut writer = Writer::open(file.path(), CAPACITY).unwrap();
    writer.write(&payload(2)).unwrap();
    assert_eq!(header(&file, TAIL), tail + record_size(&payload(2)));
    assert_eq!(seq_of(&read_record(&mut reader)), 2);
}

#[test]
fn rejects_second_writer_other_capacity_and_oversized_payload() {
    let file = TempFile::new("reject");
    let mut writer = Writer::open(file.path(), CAPACITY).unwrap();
    assert_eq!(Writer::open(file.path(), CAPACITY).err().unwrap().kind(), ErrorKind::WouldBlock);
    assert_eq!(writer.write(&vec![0; MAX_PAYLOAD + 1]).unwrap_err().kind(), ErrorKind::InvalidInput);
    writer.write(&vec![0; MAX_PAYLOAD]).unwrap();
    drop(writer);
    assert_eq!(Writer::open(file.path(), CAPACITY * 2).err().unwrap().kind(), ErrorKind::InvalidData);
}

#[test]
fn reopened_writer_never_lowers_a_tail_intent_left_ahead_of_tail() {
    let file = TempFile::new("intent");
    let mut writer = Writer::open(file.path(), CAPACITY).unwrap();
    writer.write(&payload(1)).unwrap();
    drop(writer);
    // 쓰는 쪽이 tail_intent만 올리고 tail은 올리지 못한 채 죽은 상태를 만든다.
    let tail = header(&file, TAIL);
    let ahead = tail + 4000;
    File::options().write(true).open(file.path()).unwrap().write_all_at(&ahead.to_le_bytes(), TAIL_INTENT).unwrap();

    let mut writer = Writer::open(file.path(), CAPACITY).unwrap();
    writer.write(&payload(2)).unwrap();
    assert_eq!(header(&file, TAIL), tail + record_size(&payload(2)));
    assert_eq!(header(&file, TAIL_INTENT), ahead);
}
