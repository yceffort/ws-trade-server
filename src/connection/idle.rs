use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

pub(super) async fn wait(mut activity: watch::Receiver<Instant>, limit: Option<Duration>) {
    let Some(limit) = limit else { return std::future::pending().await };
    loop {
        // 표현할 수 없는 마감은 즉시 종료로 처리해 구독 정리를 건너뛰는 panic을 막는다.
        let Some(deadline) = activity.borrow_and_update().checked_add(limit) else { return };
        match timeout_at(deadline, activity.changed()).await {
            Ok(Ok(())) => {}
            _ => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unrepresentable_deadline_finishes_without_panicking() {
        let (_activity, receiver) = watch::channel(Instant::now());
        tokio::time::timeout(Duration::from_secs(1), wait(receiver, Some(Duration::MAX))).await.unwrap();
    }

    #[tokio::test]
    async fn expires_while_command_handler_is_still_waiting() {
        let (activity, receiver) = watch::channel(Instant::now());
        let idle = wait(receiver, Some(Duration::from_millis(100)));
        tokio::pin!(idle);
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut idle).await.is_err());
        activity.send_replace(Instant::now());
        // 송신자를 살려 둬도 다음 수신이 없으면 마감된다.
        tokio::time::timeout(Duration::from_secs(2), &mut idle).await.unwrap();
    }
}
