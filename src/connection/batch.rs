use crate::hub::Outgoing;
use crate::stomp;
use futures_util::{Sink, SinkExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// 한 번에 묶는 프레임 수의 상한. 웹소켓 라이브러리의 쓰기 버퍼는 한번 커지면 줄지 않아서, 묶은 만큼의 메모리가 연결마다 남는다.
// 16으로 두면 한계 부하의 지연은 더 줄지만 연결 1만 개에서 메모리가 30MB 넘게 더 든다(bench/README.md의 "쓰기 묶음").
const MAX_BATCH: usize = 4;

// first와 큐에 이미 와 있는 것을 쓰기 버퍼에 쌓고 flush는 한 번만 한다. 프레임마다 flush하면 프레임 수만큼 소켓에 쓴다.
// 오류 통지를 만나면 그것까지 내보내고 끊을 사유를 돌려준다.
pub(super) async fn write<W>(
    writer: &mut W,
    first: Outgoing,
    outgoing: &mut mpsc::Receiver<Outgoing>,
    connection_id: &str,
    seq: &mut u64,
) -> Result<(), &'static str>
where
    W: Sink<Message> + Unpin,
{
    let mut next = Some(first);
    let mut buffered = 0;
    while let Some(item) = next.take() {
        let text = match item {
            Outgoing::Error(code) => {
                // 앞서 쌓은 프레임을 먼저 내보낸다. 여기서 실패하면 통지를 보낼 수 없으므로 쓰기 실패로 끝낸다.
                writer.flush().await.map_err(|_| "write_error")?;
                let _ = writer.send(Message::text(stomp::error(code))).await;
                return Err(code);
            }
            Outgoing::Receipt { receipt_id, message } => stomp::receipt(&receipt_id, message),
            Outgoing::Message { subscription, body } => {
                *seq += 1;
                stomp::message(&format!("{connection_id}-{seq}"), &subscription, &body)
            }
        };
        writer.feed(Message::text(text)).await.map_err(|_| "write_error")?;
        buffered += 1;
        // 상한에 닿으면 큐에서 더 꺼내지 않는다. 꺼낸 것은 반드시 보내야 하기 때문이다.
        if buffered < MAX_BATCH {
            next = outgoing.try_recv().ok();
        }
    }
    writer.flush().await.map_err(|_| "write_error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // 받은 프레임과 flush 횟수만 적어 두는 가짜 소켓. flush 한 번이 소켓 쓰기 한 번에 해당한다.
    #[derive(Default)]
    struct Recorder {
        frames: Vec<String>,
        flushes: usize,
        broken: bool,
    }

    impl Sink<Message> for Recorder {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
            self.frames.push(item.to_text().unwrap().to_string());
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            self.flushes += 1;
            Poll::Ready(if self.broken { Err(()) } else { Ok(()) })
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn queue_of(items: Vec<Outgoing>) -> mpsc::Receiver<Outgoing> {
        let (queue, outgoing) = mpsc::channel(items.len().max(1));
        for item in items {
            assert!(queue.try_send(item).is_ok());
        }
        outgoing
    }

    fn message(body: &str) -> Outgoing {
        Outgoing::Message { subscription: "0".into(), body: body.into() }
    }

    fn message_ids(socket: &Recorder) -> Vec<&str> {
        socket
            .frames
            .iter()
            .filter_map(|frame| frame.strip_prefix("MESSAGE\nmessage-id:")?.split('\n').next())
            .collect()
    }

    #[tokio::test]
    async fn queued_frames_share_one_flush() {
        let mut outgoing = queue_of(vec![message("a"), message("b"), message("c")]);
        let (mut socket, mut seq) = (Recorder::default(), 0);
        let first = outgoing.recv().await.unwrap();
        write(&mut socket, first, &mut outgoing, "conn", &mut seq).await.unwrap();
        assert_eq!(message_ids(&socket), ["conn-1", "conn-2", "conn-3"]);
        assert_eq!(socket.flushes, 1);
    }

    #[tokio::test]
    async fn frames_over_the_limit_stay_queued() {
        let mut outgoing = queue_of((0..MAX_BATCH + 2).map(|_| message("x")).collect());
        let (mut socket, mut seq) = (Recorder::default(), 0);
        let first = outgoing.recv().await.unwrap();
        write(&mut socket, first, &mut outgoing, "conn", &mut seq).await.unwrap();
        assert_eq!((socket.frames.len(), socket.flushes, outgoing.len()), (MAX_BATCH, 1, 2));
    }

    #[tokio::test]
    async fn error_goes_out_after_the_frames_before_it() {
        let mut outgoing = queue_of(vec![message("a"), Outgoing::Error("too-many-subscriptions"), message("b")]);
        let (mut socket, mut seq) = (Recorder::default(), 0);
        let first = outgoing.recv().await.unwrap();
        let result = write(&mut socket, first, &mut outgoing, "conn", &mut seq).await;
        assert_eq!(result, Err("too-many-subscriptions"));
        assert_eq!(message_ids(&socket), ["conn-1"]);
        assert!(socket.frames[1].starts_with("ERROR\n"), "{:?}", socket.frames);
        assert_eq!(socket.frames.len(), 2);
    }

    // 묶기 전에는 MESSAGE를 쓰다 실패하면 뒤따르는 오류 통지와 상관없이 write_error로 끝났다.
    #[tokio::test]
    async fn write_failure_before_an_error_is_reported_as_write_error() {
        let mut outgoing = queue_of(vec![message("a"), Outgoing::Error("too-many-subscriptions")]);
        let (mut socket, mut seq) = (Recorder { broken: true, ..Recorder::default() }, 0);
        let first = outgoing.recv().await.unwrap();
        let result = write(&mut socket, first, &mut outgoing, "conn", &mut seq).await;
        assert_eq!(result, Err("write_error"));
        assert_eq!(socket.frames.len(), 1, "쓰기가 실패한 소켓에 오류 통지를 더 쌓지 않는다");
    }
}
