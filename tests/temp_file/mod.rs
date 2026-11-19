use std::path::{Path, PathBuf};

// 테스트마다 다른 임시 링 경로. 테스트가 패닉해도 drop에서 파일을 지운다.
pub struct TempFile(PathBuf);

impl TempFile {
    pub fn new(name: &str) -> TempFile {
        let path = std::env::temp_dir().join(format!("ws-trade-test-{}-{name}.ring", std::process::id()));
        let _ = std::fs::remove_file(&path);
        TempFile(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
