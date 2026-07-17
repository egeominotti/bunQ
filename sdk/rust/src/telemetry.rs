use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use crate::Error;

/// Structured lifecycle events emitted by connections and workers.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum TelemetryEvent {
    Connecting {
        host: String,
        port: u16,
    },
    Reconnecting {
        previous_generation: i64,
    },
    Connected {
        generation: i64,
    },
    Authenticated {
        generation: i64,
    },
    CommandStarted {
        command: String,
        request_id: String,
    },
    CommandFinished {
        command: String,
        request_id: String,
        duration: Duration,
        ok: bool,
    },
    Timeout {
        operation: String,
        duration: Duration,
    },
    Error {
        operation: String,
        message: String,
    },
    WorkerRetry {
        queue: String,
        message: String,
        retry_in: Duration,
    },
    Closed,
}

pub type TelemetryCallback = Arc<dyn Fn(TelemetryEvent) + Send + Sync + 'static>;

pub(crate) fn emit(callback: Option<&TelemetryCallback>, event: TelemetryEvent) {
    if let Some(callback) = callback {
        let _ = catch_unwind(AssertUnwindSafe(|| callback(event)));
    }
}

pub(crate) fn emit_all(
    callback: Option<&TelemetryCallback>,
    events: impl IntoIterator<Item = TelemetryEvent>,
) {
    for event in events {
        emit(callback, event);
    }
}

pub(crate) fn record_failure(
    events: &mut Vec<TelemetryEvent>,
    operation: impl Into<String>,
    duration: Duration,
    error: &Error,
) {
    let operation = operation.into();
    if matches!(error, Error::Timeout(_)) {
        events.push(TelemetryEvent::Timeout {
            operation: operation.clone(),
            duration,
        });
    }
    events.push(TelemetryEvent::Error {
        operation,
        message: error.to_string(),
    });
}
