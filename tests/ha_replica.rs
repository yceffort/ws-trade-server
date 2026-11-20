#![cfg(feature = "ha")]

#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use ws_trade_server::ha::*;
use ws_trade_server::{Config, start_managed};

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

struct Feed {
    stop: Arc<AtomicBool>,
    pause: Arc<AtomicBool>,
    commands: mpsc::SyncSender<(usize, oneshot::Sender<Committed>)>,
}
impl Drop for Feed {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn feed(kafka: Kafka, source: SourceConfig) -> Feed {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "합성 producer checkpoint가 있는 독립 시험 클러스터 필요"]
async fn independent_gateways_restore_and_deliver_the_whole_stream() {
    let directory = std::path::PathBuf::from(std::env::var("WTS_HA_TEST_CONFIG").unwrap());
    let kafka: Kafka = serde_json::from_slice(&std::fs::read(directory.join("feed.json")).unwrap()).unwrap();
    let reader: Kafka = serde_json::from_slice(&std::fs::read(directory.join("gateway.json")).unwrap()).unwrap();
    let source: SourceConfig = serde_json::from_slice(&std::fs::read(directory.join("source.json")).unwrap()).unwrap();
    let feed = feed(kafka, source.clone());
    let config = || Config {
        addr: "127.0.0.1:0".into(),
        ha: Some(ReplicaInput { kafka: reader.clone(), source: source.clone() }),
        ..Config::default()
    };
    let (a, b) = tokio::join!(start_managed(config()), start_managed(config()));
    let (a, b) = (a.unwrap(), b.unwrap());
    let mut ca = client::connect_stomp(&a.addr.to_string(), "replica-a").await;
    let mut cb = client::connect_stomp(&b.addr.to_string(), "replica-b").await;
    for (index, topic) in source.topics.iter().enumerate() {
        client::subscribe(&mut ca, &index.to_string(), topic).await;
        client::subscribe(&mut cb, &index.to_string(), topic).await;
    }
    let (done, committed) = oneshot::channel();
    feed.commands.send((40, done)).unwrap();
    let committed = tokio::time::timeout(Duration::from_secs(10), committed).await.unwrap().unwrap();
    let mut bodies_a = Vec::new();
    let mut bodies_b = Vec::new();
    for _ in 0..40 {
        let (fa, fb) = tokio::join!(client::recv_frame(&mut ca), client::recv_frame(&mut cb));
        assert_eq!(fa.command, "MESSAGE");
        assert_eq!(fb.command, "MESSAGE");
        bodies_a.push(fa.body);
        bodies_b.push(fb.body);
    }
    assert_eq!(bodies_a, bodies_b);
    for hub in [&a.hub, &b.hub] {
        let view = hub.replica_view().await.unwrap();
        assert!(view.next > committed.through);
        assert_eq!(
            view.snapshot.topics.values().flatten().map(|latest| latest.event.source_seq()).max(),
            committed.source_seq
        );
    }
    let c = start_managed(config()).await.unwrap();
    let va = a.hub.replica_view().await.unwrap();
    let vc = c.hub.replica_view().await.unwrap();
    assert_eq!(serde_json::to_vec(&va.snapshot.topics).unwrap(), serde_json::to_vec(&vc.snapshot.topics).unwrap());
    assert!(!vc.snapshot.status.as_ref().unwrap().market_open);
    feed.pause.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(3600)).await;
    assert!(!a.hub.replica_view().await.unwrap().ready(true));
    assert!(!b.hub.replica_view().await.unwrap().ready(true));
    feed.pause.store(false, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !a.hub.replica_view().await.unwrap().ready(true) || !b.hub.replica_view().await.unwrap().ready(true) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    drop(ca);
    drop(cb);
    tokio::join!(a.shutdown(), b.shutdown(), c.shutdown());
}
