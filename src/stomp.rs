pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Frame {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

pub enum Incoming {
    Heartbeat,
    Frame(Frame),
}

pub fn parse(text: &str) -> Option<Incoming> {
    if !text.is_empty() && text.chars().all(|c| c == '\n') {
        return Some(Incoming::Heartbeat);
    }
    let (head, body) = text.split_once("\n\n")?;
    let mut lines = head.split('\n');
    let command = lines.next()?.to_string();
    let headers = lines
        .map(|line| line.split_once(':').map(|(key, value)| (key.to_string(), value.to_string())))
        .collect::<Option<Vec<_>>>()?;
    let body = body.trim_end_matches('\n').strip_suffix('\0')?.to_string();
    Some(Incoming::Frame(Frame { command, headers, body }))
}

pub fn serialize(command: &str, headers: &[(&str, &str)], body: &str) -> String {
    let len = command.len() + headers.iter().map(|(k, v)| k.len() + v.len() + 2).sum::<usize>() + body.len() + 3;
    let mut frame = String::with_capacity(len);
    frame.push_str(command);
    frame.push('\n');
    for (key, value) in headers {
        frame.push_str(key);
        frame.push(':');
        frame.push_str(value);
        frame.push('\n');
    }
    frame.push('\n');
    frame.push_str(body);
    frame.push('\0');
    frame
}

pub fn connected(heart_beat: &str) -> String {
    serialize("CONNECTED", &[("version", "1.2"), ("heart-beat", heart_beat)], "")
}

pub fn receipt(receipt_id: &str, message: &str) -> String {
    let headers = [("receipt-id", receipt_id), ("message", message), ("response-code", "200")];
    serialize("RECEIPT", &headers, "")
}

pub fn message(message_id: &str, subscription: &str, body: &str) -> String {
    let content_length = body.len().to_string();
    let headers = [
        ("message-id", message_id),
        ("subscription", subscription),
        ("content-length", content_length.as_str()),
        ("content-type", "application/json"),
    ];
    serialize("MESSAGE", &headers, body)
}

// 이 오류 코드는 공개 WTS에서 관측한 값이 아니라 선택적 제한 기능의 실험 규약이다.
pub fn error(code: &str) -> String {
    serialize("ERROR", &[("message", code)], "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_without_nul_is_rejected() {
        assert!(parse("CONNECT\nconnection-id:x\n\n").is_none());
        assert!(parse("SUBSCRIBE\nid:0\ndestination:/topic/x\n\n").is_none());
        assert!(parse("SEND\n\nbody\n\n").is_none());
    }

    // STOMP 1.2는 종료 NUL 뒤에 줄바꿈이 더 오는 것을 허용한다. 본문 안의 줄바꿈은 그대로 둔다.
    #[test]
    fn newlines_after_nul_are_accepted() {
        let Some(Incoming::Frame(frame)) = parse("SEND\n\nbody\n\0\n\n") else { panic!() };
        assert_eq!(frame.body, "body\n");
    }

    #[test]
    fn nul_terminated_frame_and_heartbeat_are_accepted() {
        let Some(Incoming::Frame(frame)) = parse(&serialize("SEND", &[("id", "0")], "body")) else { panic!() };
        assert_eq!((frame.command.as_str(), frame.header("id"), frame.body.as_str()), ("SEND", Some("0"), "body"));
        assert!(matches!(parse("\n"), Some(Incoming::Heartbeat)));
    }
}
