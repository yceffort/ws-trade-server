// 파일 링(tests/ring.rs)과 힙 링(tests/ring_miri.rs)이 같이 쓰는 검사.
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread;
use ws_trade_server::ring::{Read, Reader, Writer};

// 순번 8바이트 뒤에 순번에서 계산한 무늬를 붙인다. 길이도 순번마다 달라서 끝자리에서 건너뛰기 표시가 생긴다.
pub fn payload(seq: u64) -> Vec<u8> {
    let mut bytes = seq.to_le_bytes().to_vec();
    bytes.extend((0..seq % 120).map(|i| seq.wrapping_mul(31).wrapping_add(i) as u8));
    bytes
}

// 무늬가 순번과 맞는지 확인하고 순번을 돌려준다.
pub fn seq_of(bytes: &[u8]) -> u64 {
    assert!(bytes.len() >= 8, "레코드가 너무 짧다: {}바이트", bytes.len());
    let seq = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    assert_eq!(bytes, payload(seq), "무늬가 깨졌다: seq {seq}");
    seq
}

pub fn read_record(reader: &mut Reader) -> Vec<u8> {
    match reader.read() {
        Read::Record(bytes) => bytes,
        Read::Empty => panic!("레코드가 있어야 한다"),
        Read::Lost => panic!("유실이 없어야 한다"),
    }
}

pub fn in_order(mut writer: Writer, mut reader: Reader) {
    let payloads = [b"first".to_vec(), Vec::new(), vec![7; 4096], b"last".to_vec()];
    for payload in &payloads {
        writer.write(payload).unwrap();
    }
    for payload in &payloads {
        assert_eq!(&read_record(&mut reader), payload);
    }
    assert!(matches!(reader.read(), Read::Empty));
}

// 한 건 쓰고 한 건 읽기를 capacity의 세 배가 넘게 되풀이한다.
pub fn laps(mut writer: Writer, mut reader: Reader, capacity: u64) {
    let (mut seq, mut written) = (0, 0);
    while written < 3 * capacity {
        seq += 1;
        let bytes = payload(seq);
        written += bytes.len() as u64;
        writer.write(&bytes).unwrap();
        assert_eq!(seq_of(&read_record(&mut reader)), seq);
    }
    assert!(matches!(reader.read(), Read::Empty));
}

pub fn loss(mut writer: Writer, mut reader: Reader, capacity: u64) {
    let (mut seq, mut written) = (0, 0);
    while written <= capacity {
        seq += 1;
        let bytes = payload(seq);
        written += bytes.len() as u64;
        writer.write(&bytes).unwrap();
    }
    assert!(matches!(reader.read(), Read::Lost));
    let after = seq + 1..=seq + 3;
    for seq in after.clone() {
        writer.write(&payload(seq)).unwrap();
    }
    for seq in after {
        assert_eq!(seq_of(&read_record(&mut reader)), seq);
    }
    assert!(matches!(reader.read(), Read::Empty));
}

// 쓰는 스레드가 records건을 쓰는 동안 읽는 스레드가 무늬와 순번을 확인한다.
// 500의 배수 순번을 받으면 쓰는 쪽이 600건(capacity 8192보다 많은 바이트)을 더 쓸 때까지 멈춰서 일부러 덮어쓰이게 한다.
pub fn stress(mut writer: Writer, mut reader: Reader, records: u64) {
    let written = AtomicU64::new(0);
    thread::scope(|scope| {
        let producer = scope.spawn(|| {
            for seq in 1..=records {
                writer.write(&payload(seq)).unwrap();
                written.store(seq, Release);
            }
        });
        let (mut last, mut gap_allowed, mut losses) = (0, false, 0);
        loop {
            let finished = written.load(Acquire) == records;
            match reader.read() {
                Read::Record(bytes) => {
                    let seq = seq_of(&bytes);
                    assert!(
                        seq == last + 1 || (gap_allowed && seq > last),
                        "유실 알림 없이 {last} 다음에 {seq}가 왔다"
                    );
                    (last, gap_allowed) = (seq, false);
                    if seq.is_multiple_of(500) {
                        while written.load(Acquire) < (seq + 600).min(records) && !producer.is_finished() {
                            thread::yield_now();
                        }
                    }
                }
                Read::Lost => (losses, gap_allowed) = (losses + 1, true),
                Read::Empty if finished => break,
                // 쓰는 스레드가 패닉하면 written이 끝까지 오르지 않으므로 여기서 빠져나가 scope가 패닉을 전한다.
                Read::Empty if producer.is_finished() && written.load(Acquire) < records => break,
                Read::Empty => thread::yield_now(),
            }
        }
        assert!(losses > 0, "덮어쓰기가 한 번은 일어나야 한다");
    });
}
