use tokio::net::TcpStream;
use tokio::task::JoinSet;

#[derive(Default)]
pub(crate) struct Rejector(JoinSet<()>);

impl Rejector {
    pub async fn shutdown(&mut self) {
        self.0.shutdown().await;
    }

    pub fn reject(&mut self, stream: TcpStream) {
        while self.0.try_join_next().is_some() {}
        // 거절 대기까지 가득 차면 소켓을 바로 버려 수락 루프와 메모리를 보호한다.
        if self.0.len() < 64 {
            self.0.spawn(super::reject_overloaded(stream));
        }
    }
}
