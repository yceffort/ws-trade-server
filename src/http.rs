use crate::hub::Hub;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn bind(addr: &str) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "조회 API는 loopback 주소만 허용한다"));
    }
    Ok(listener)
}

pub fn spawn(
    listener: TcpListener,
    hub: Hub,
    #[cfg(feature = "ha")] management: Option<crate::ha::Management>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(error) => {
                    tracing::warn!(event = "http_accept_failed", kind = ?error.kind());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            // 로컬 조회를 순차 처리하고 전체 마감을 두어 느린 요청이 태스크를 쌓지 못하게 한다.
            let _ = tokio::time::timeout(
                Duration::from_secs(2),
                respond(
                    stream,
                    &hub,
                    #[cfg(feature = "ha")]
                    management.as_ref(),
                ),
            )
            .await;
        }
    })
}

async fn respond(
    mut stream: TcpStream,
    hub: &Hub,
    #[cfg(feature = "ha")] management: Option<&crate::ha::Management>,
) -> io::Result<()> {
    let mut bytes = [0; 8192];
    let mut size = 0;
    loop {
        if size == bytes.len() {
            return Ok(());
        }
        let read = stream.read(&mut bytes[size..]).await?;
        if read == 0 {
            return Ok(());
        }
        size += read;
        if bytes[..size].windows(4).any(|part| part == b"\r\n\r\n") {
            break;
        }
    }
    let first = bytes[..size].split(|b| *b == b'\n').next().unwrap_or_default();
    #[cfg(feature = "ha")]
    let managed = management.map(|m| &m.lifecycle).and_then(|life| match first {
        b"GET /livez HTTP/1.1\r" => Some((life.phase() != crate::ha::Phase::Stopped, life.phase())),
        b"GET /readyz HTTP/1.1\r" => Some((life.ready(), life.phase())),
        b"POST /drain HTTP/1.1\r" => {
            life.drain();
            Some((true, life.phase()))
        }
        _ => None,
    });
    #[cfg(not(feature = "ha"))]
    let managed: Option<(bool, &str)> = None;
    #[cfg(feature = "ha")]
    let metrics = if first == b"GET /ha-metrics HTTP/1.1\r" {
        match management {
            Some(m) => Some(m.prometheus(hub).await),
            None => None,
        }
    } else {
        None
    };
    #[cfg(not(feature = "ha"))]
    let metrics: Option<String> = None;
    let content_type = if metrics.is_some() { "text/plain; version=0.0.4" } else { "application/json" };
    let (status, body) = if let Some(metrics) = metrics {
        ("200 OK", metrics)
    } else if let Some((healthy, phase)) = managed {
        (if healthy { "200 OK" } else { "503 Service Unavailable" }, serde_json::json!({"state":phase}).to_string())
    } else if first == b"GET /snapshots HTTP/1.1\r" {
        match hub.snapshot().await {
            Some(body) => ("200 OK", body),
            None => ("503 Service Unavailable", "{}".into()),
        }
    } else if first == b"GET /metrics HTTP/1.1\r" {
        match hub.metrics().await {
            Some(metrics) => ("200 OK", serde_json::to_string(&metrics).expect("정수 필드만 직렬화한다")),
            None => ("503 Service Unavailable", "{}".into()),
        }
    } else {
        ("404 Not Found", "{}".into())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::Limits;

    #[tokio::test]
    async fn snapshot_keeps_latest_without_new_trades_and_bounds_topics() {
        let hub = Hub::configured(Limits::default(), vec!["known".into()]);
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        spawn(
            listener,
            hub.clone(),
            #[cfg(feature = "ha")]
            None,
        );
        assert_eq!(hub.snapshot().await.unwrap(), "{}");
        hub.publish("known".into(), "first".into()).await;
        hub.publish("ignored".into(), "unbounded".into()).await;
        hub.publish("known".into(), "last".into()).await;
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket.write_all(b"GET /snapshots HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
        let mut response = String::new();
        socket.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        let body: serde_json::Value = serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(body["known"]["body"], "last");
        assert!(body["known"]["updated_at_ms"].as_u64().unwrap() > 0);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&hub.snapshot().await.unwrap()).unwrap(), body);
        assert!(bind("0.0.0.0:0").await.is_err());
    }
}
