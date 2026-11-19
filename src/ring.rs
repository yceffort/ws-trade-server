// 공유 메모리를 원자 타입으로 보는 자리가 이 파일에만 있어서 여기서만 unsafe를 허용한다.
#![allow(unsafe_code)]

use crate::region::Region;
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicU64, fence};

pub const MAX_PAYLOAD: usize = 4096;
const MAGIC: &[u8; 8] = b"WSRING01";
const HEADER: u64 = 64;
const TAIL_INTENT: usize = 16;
const TAIL: usize = 24;
const MIN_CAPACITY: u64 = 8192;
const SKIP: u32 = u32::MAX;

// 헤더의 두 원자 값은 이 기계의 바이트 순서로 저장된다. 파일 형식이 리틀 엔디언이라 빅 엔디언에서는 빌드하지 않는다.
const _: () = assert!(cfg!(target_endian = "little"));

pub enum Read {
    Record(Vec<u8>),
    Empty,
    Lost,
}

// 읽기 전용으로 매핑한 링에서는 Relaxed load만 한다. 그 밖의 원자 연산은 읽기 전용 메모리에서 보장되지 않는다.
struct Ring {
    region: Region,
    capacity: u64,
}

impl Ring {
    // 아래 두 unsafe 블록이 기대는 길이와 정렬을 파일과 힙 모두 여기서 확인한다.
    fn new(region: Region, capacity: u64) -> io::Result<Ring> {
        let ring = Ring { region, capacity };
        let (ptr, len) = ring.region.bytes();
        if len as u64 != HEADER + capacity {
            return Err(invalid("링 크기가 헤더의 capacity와 맞지 않는다"));
        }
        assert!(ptr.cast::<AtomicU64>().is_aligned(), "링의 시작 주소는 8바이트 정렬이어야 한다");
        Ok(ring)
    }

    fn tail_intent(&self) -> &AtomicU64 {
        self.header(TAIL_INTENT)
    }

    fn tail(&self) -> &AtomicU64 {
        self.header(TAIL)
    }

    fn header(&self, offset: usize) -> &AtomicU64 {
        let (ptr, _) = self.region.bytes();
        // SAFETY: offset은 16이나 24뿐이고 Ring::new가 영역 길이(64 + capacity)와 시작 주소의 8바이트 정렬을 확인했으므로, ptr + offset부터 8바이트는 영역 안에 있고 AtomicU64로 정렬돼 있다.
        // 참조의 수명은 &self에 묶여 영역을 소유한 self.region보다 오래 살지 않는다.
        // 이 8바이트는 어느 프로세스에서든 AtomicU64로만 접근하므로 원자적이지 않은 접근과 섞이지 않는다.
        // 읽기 전용 매핑에서는 Relaxed load만 하고, std 문서가 aarch64와 x86_64에서 8바이트 이하 Relaxed load는 읽기 전용 메모리에서도 된다고 보장한다.
        // 파일 매핑은 파일이 줄면 SIGBUS가 나지만 이 링은 이미 있는 파일을 줄이지 않는다.
        unsafe { &*ptr.add(offset).cast::<AtomicU64>() }
    }

    fn data(&self) -> &[AtomicU8] {
        let (ptr, _) = self.region.bytes();
        // SAFETY: Ring::new가 영역 길이가 64 + capacity임을 확인했으므로 ptr + 64부터 capacity바이트가 영역 안에 있고, AtomicU8은 u8과 크기와 정렬(1)이 같다.
        // 수명과 읽기 전용 매핑, 파일 크기에 관한 근거는 header와 같다.
        // 데이터 영역은 어느 프로세스에서든 AtomicU8로만 읽고 쓰므로 동시에 덮어써도 데이터 경쟁이 아니다.
        unsafe { std::slice::from_raw_parts(ptr.add(HEADER as usize).cast::<AtomicU8>(), self.capacity as usize) }
    }
}

pub struct Writer {
    ring: Ring,
    // 배타적 잠금은 이 파일을 닫을 때 풀린다. 힙 링에는 파일이 없다.
    _lock: Option<File>,
}

impl Writer {
    pub fn open(path: &Path, capacity: u64) -> io::Result<Writer> {
        check_capacity(capacity)?;
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(io::ErrorKind::WouldBlock, "다른 쓰는 쪽이 이 링 파일을 잠갔다"),
            TryLockError::Error(error) => error,
        })?;
        if file.metadata()?.len() == 0 {
            file.set_len(HEADER + capacity)?;
            file.write_all_at(&capacity.to_le_bytes(), 8)?;
            file.write_all_at(MAGIC, 0)?;
        }
        if read_capacity(&file)? != capacity {
            return Err(invalid("이미 있는 링 파일과 capacity가 다르다"));
        }
        let ring = Ring::new(Region::map(&file)?, capacity)?;
        Ok(Writer { ring, _lock: Some(file) })
    }

    pub fn write(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > MAX_PAYLOAD {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "payload는 4096바이트를 넘을 수 없다"));
        }
        let size = record_size(payload.len());
        let room = self.ring.capacity - self.ring.tail().load(Relaxed) % self.ring.capacity;
        if room < size {
            self.put(room, SKIP, &[]);
        }
        self.put(size, payload.len() as u32, payload);
        Ok(())
    }

    // 레코드 하나를 쓰는 세 단계. 건너뛰기 표시도 같은 순서를 지킨다.
    fn put(&self, size: u64, len: u32, payload: &[u8]) {
        let tail = self.ring.tail().load(Relaxed);
        let end = tail + size;
        // 이전 쓰는 쪽이 tail_intent를 올리고 tail을 올리기 전에 죽었으면 tail_intent가 tail보다 앞서 있다. max는 그 값을 줄이지 않는다.
        self.ring.tail_intent().fetch_max(end, Relaxed);
        // Release 저장만으로는 뒤따르는 바이트 쓰기가 tail_intent보다 먼저 보일 수 있어서 fence를 둔다.
        fence(Release);
        let at = (tail % self.ring.capacity) as usize;
        let bytes = len.to_le_bytes().into_iter().chain(payload.iter().copied());
        for (slot, byte) in self.ring.data()[at..at + 4 + payload.len()].iter().zip(bytes) {
            slot.store(byte, Relaxed);
        }
        self.ring.tail().store(end, Release);
    }
}

pub struct Reader {
    ring: Ring,
    pos: u64,
}

impl Reader {
    pub fn open(path: &Path) -> io::Result<Reader> {
        let file = File::open(path)?;
        let capacity = read_capacity(&file)?;
        let ring = Ring::new(Region::map_read_only(&file)?, capacity)?;
        Ok(Reader::at_tail(ring))
    }

    fn at_tail(ring: Ring) -> Reader {
        let pos = ring.tail().load(Relaxed);
        Reader { ring, pos }
    }

    pub fn read(&mut self) -> Read {
        let capacity = self.ring.capacity;
        loop {
            // 읽기 전용 매핑이라 Acquire load 대신 Relaxed load와 fence(Acquire)를 쓴다. 쓰는 쪽의 tail 저장(Release)과 짝을 이룬다.
            let tail = self.ring.tail().load(Relaxed);
            fence(Acquire);
            if self.pos == tail {
                return Read::Empty;
            }
            let at = (self.pos % capacity) as usize;
            let data = self.ring.data();
            let mut len = [0; 4];
            load(&data[at..at + 4], &mut len);
            let len = u32::from_le_bytes(len);
            let payload = if len == SKIP {
                None
            } else if len as usize > MAX_PAYLOAD || at + 4 + len as usize > capacity as usize {
                // 쓰는 쪽은 이런 len을 쓰지 않으므로 덮어쓰인 자리다. payload를 읽으면 영역 밖으로 나간다.
                return self.lost();
            } else {
                let mut payload = vec![0; len as usize];
                load(&data[at + 4..at + 4 + payload.len()], &mut payload);
                Some(payload)
            };
            // 쓰는 쪽의 fence(Release)와 짝을 이룬다. 복사한 바이트 중 하나라도 새로 덮어쓴 값이면, 쓰는 쪽이 그 전에 올린 tail_intent가 아래 load에 보인다.
            fence(Acquire);
            if self.ring.tail_intent().load(Relaxed) - self.pos > capacity {
                return self.lost();
            }
            match payload {
                None => self.pos += capacity - at as u64,
                Some(payload) => {
                    self.pos += record_size(payload.len());
                    return Read::Record(payload);
                }
            }
        }
    }

    // 덮어쓰인 레코드는 되찾을 수 없으므로 지금 다 쓴 위치로 건너뛴다.
    fn lost(&mut self) -> Read {
        self.pos = self.ring.tail().load(Relaxed);
        Read::Lost
    }
}

/// 파일 대신 힙 메모리에 링을 만들고, 같은 영역을 보는 쓰는 쪽 하나와 읽는 쪽 하나를 돌려준다.
/// 테스트와 Miri 검사용이다. Miri는 파일 mmap을 지원하지 않는다.
pub fn in_memory(capacity: u64) -> io::Result<(Writer, Reader)> {
    check_capacity(capacity)?;
    let (for_writer, for_reader) = Region::heap_pair(HEADER + capacity);
    let writer = Writer { ring: Ring::new(for_writer, capacity)?, _lock: None };
    let reader = Reader::at_tail(Ring::new(for_reader, capacity)?);
    Ok((writer, reader))
}

// 만드는 쪽이 magic을 맨 마지막에 쓰므로, magic이 보이면 그보다 먼저 쓴 capacity도 보인다.
fn read_capacity(file: &File) -> io::Result<u64> {
    let mut magic = [0; 8];
    let mut capacity = [0; 8];
    file.read_exact_at(&mut magic, 0)?;
    file.read_exact_at(&mut capacity, 8)?;
    if &magic != MAGIC {
        return Err(invalid("링 파일이 아니거나 아직 만드는 중이다"));
    }
    check_capacity(u64::from_le_bytes(capacity))
}

// 가장 큰 레코드(4 + 4096바이트를 8의 배수로 올린 4104바이트)가 들어가야 한다.
fn check_capacity(capacity: u64) -> io::Result<u64> {
    if capacity.is_power_of_two() && capacity >= MIN_CAPACITY {
        Ok(capacity)
    } else {
        Err(invalid("capacity는 8192 이상인 2의 거듭제곱이어야 한다"))
    }
}

fn record_size(len: usize) -> u64 {
    (4 + len).next_multiple_of(8) as u64
}

fn load(slots: &[AtomicU8], into: &mut [u8]) {
    for (byte, slot) in into.iter_mut().zip(slots) {
        *byte = slot.load(Relaxed);
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
