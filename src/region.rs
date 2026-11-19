use memmap2::{MmapOptions, MmapRaw};
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// 링이 놓인 메모리. ring.rs는 bytes()의 주소와 길이로만 원자 뷰를 만들므로 이것이 파일인지 힙인지 모른다.
// MmapRaw는 &[u8]을 내주지 않고 원시 포인터만 준다.
pub enum Region {
    File(MmapRaw),
    // AtomicU64 배열이라 시작 주소가 8바이트 정렬이다.
    Heap(Arc<[AtomicU64]>),
}

impl Region {
    pub fn map(file: &File) -> io::Result<Region> {
        Ok(Region::File(MmapRaw::map_raw(file)?))
    }

    pub fn map_read_only(file: &File) -> io::Result<Region> {
        Ok(Region::File(MmapOptions::new().map_raw_read_only(file)?))
    }

    // 0으로 채운 len바이트를 두 핸들이 나눠 갖는다. Miri는 파일 mmap을 지원하지 않아서 테스트는 이것을 쓴다.
    pub fn heap_pair(len: u64) -> (Region, Region) {
        let words: Arc<[AtomicU64]> = (0..len.div_ceil(8)).map(|_| AtomicU64::new(0)).collect();
        (Region::Heap(words.clone()), Region::Heap(words))
    }

    pub fn bytes(&self) -> (*const u8, usize) {
        match self {
            Region::File(map) => (map.as_ptr(), map.len()),
            Region::Heap(words) => (words.as_ptr().cast(), words.len() * 8),
        }
    }
}
