use super::*;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn normal_eof_variants_are_silent_and_real_errors_do_not_echo_native_details() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = Buffer(bytes.clone());
    let subscriber =
        tracing_subscriber::fmt().with_ansi(false).without_time().with_writer(move || writer.clone()).finish();
    tracing::subscriber::with_default(subscriber, || {
        Quiet.error(KafkaError::PartitionEOF(0), "synthetic-sensitive-value");
        Quiet.error(KafkaError::Global(RDKafkaErrorCode::PartitionEOF), "synthetic-sensitive-value");
        Quiet.error(KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure), "synthetic-sensitive-value");
    });
    let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert_eq!(text.matches("broker_client_error").count(), 1);
    assert!(text.contains("BrokerTransportFailure"));
    assert!(!text.contains("PartitionEOF"));
    assert!(!text.contains("synthetic-sensitive-value"));
}
