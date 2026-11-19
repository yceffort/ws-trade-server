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
    let body = body.strip_suffix('\0').unwrap_or(body).to_string();
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
