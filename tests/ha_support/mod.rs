use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use ws_trade_server::ha::*;

pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub struct Feed {
    stop: Arc<AtomicBool>,
    pub pause: Arc<AtomicBool>,
    pub commands: mpsc::SyncSender<(usize, oneshot::Sender<Committed>)>,
}
impl Drop for Feed {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

pub fn feed(kafka: Kafka, source: SourceConfig) -> Feed {
    let stop = Arc::new(AtomicBool::new(false));
    let pause = Arc::new(AtomicBool::new(false));
    let (commands, inbox) = mpsc::sync_channel::<(usize, oneshot::Sender<Committed>)>(1);
    let (stopped, paused) = (stop.clone(), pause.clone());
    std::thread::spawn(move || {
        let mut guard = || Ok(());
        let mut writer = Publisher::open(&kafka, source, Bootstrap::Existing, now(), &mut guard).unwrap();
        while !stopped.load(Ordering::Relaxed) {
            match inbox.recv_timeout(Duration::from_millis(200)) {
                Ok((count, reply)) => {
                    let _ = reply.send(writer.synthetic(count, now(), count > 0, &mut guard).unwrap());
                }
                Err(mpsc::RecvTimeoutError::Timeout) if !paused.load(Ordering::Relaxed) => {
                    writer.synthetic(0, now(), false, &mut guard).unwrap();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                _ => {}
            }
        }
    });
    Feed { stop, pause, commands }
}
