use std::time::Duration;

use crate::connection_io::ensure_frame_size;
use crate::telemetry::{emit, record_failure};
use crate::{ConnectionOptions, Error, MAX_FRAME_SIZE, TelemetryCallback, TelemetryEvent};

#[test]
fn callback_panics_never_escape_into_client_operations() {
    let callback: TelemetryCallback =
        std::sync::Arc::new(|_: TelemetryEvent| panic!("observer failure"));

    emit(Some(&callback), TelemetryEvent::Closed);
}

#[test]
fn timeout_failures_emit_timeout_and_error_events() {
    let mut events = Vec::new();
    record_failure(
        &mut events,
        "command:PULLB",
        Duration::from_millis(25),
        &Error::Timeout("deadline".into()),
    );

    assert!(matches!(
        events.first(),
        Some(TelemetryEvent::Timeout { operation, .. }) if operation == "command:PULLB"
    ));
    assert!(matches!(
        events.get(1),
        Some(TelemetryEvent::Error { operation, .. }) if operation == "command:PULLB"
    ));
}

#[test]
fn frame_limit_accepts_the_boundary_and_rejects_one_byte_more() {
    assert!(ensure_frame_size(MAX_FRAME_SIZE).is_ok());
    assert!(ensure_frame_size(MAX_FRAME_SIZE + 1).is_err());
}

#[test]
fn connection_debug_output_redacts_auth_tokens() {
    let options = ConnectionOptions {
        token: Some("do-not-print-me".into()),
        ..Default::default()
    };
    let debug = format!("{options:?}");

    assert!(!debug.contains("do-not-print-me"));
    assert!(debug.contains("<redacted>"));
}
