use super::*;

impl Hub {
    pub(crate) async fn block_for_test(&self) -> oneshot::Sender<()> {
        let (entered, started) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        self.send(Command::Pause { entered, resume }).await;
        started.await.unwrap();
        // 행위자를 멈춘 뒤 채널을 채워 다음 연결 명령이 확실히 대기하게 한다.
        for _ in 0..1024 {
            self.publish("unused".into(), "".into()).await;
        }
        release
    }
}
