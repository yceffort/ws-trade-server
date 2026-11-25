// hub가 체결 한 건을 연결 N개의 큐에 넣는 데 걸리는 시간을 잰다. 사용: cargo run --release --example hub_fanout
use std::sync::Arc;
use std::time::Instant;
use ws_trade_server::hub::Hub;

#[tokio::main]
async fn main() {
    for connections in [100u64, 1_000, 10_000] {
        let hub = Hub::spawn();
        let mut queues = Vec::new();
        for conn in 0..connections {
            let handles = hub.connect(conn, 1024).await;
            hub.subscribe(conn, "0".into(), "/topic".into(), None).await;
            queues.push(handles);
        }
        let body: Arc<str> = Arc::from("x".repeat(248));
        let mut nanos = Vec::new();
        for _ in 0..200 {
            let start = Instant::now();
            hub.publish("/topic".into(), body.clone()).await;
            // 구독한 순서대로 넣으므로 마지막 연결의 큐에 도착하면 전부 넣은 것이다.
            queues.last_mut().expect("연결이 하나 이상 있다").0.recv().await;
            nanos.push(start.elapsed().as_nanos() as u64);
            for (queue, _) in queues.iter_mut().rev().skip(1) {
                let _ = queue.try_recv();
            }
        }
        nanos.sort_unstable();
        let median = nanos[nanos.len() / 2];
        println!("연결 {connections}개: 한 건에 {:.2}ms, 큐 하나에 {}ns", median as f64 / 1e6, median / connections);
    }
}
