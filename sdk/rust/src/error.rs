use std::backtrace::Backtrace;

/// Errors returned by transport and queue operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("command failed: {0}")]
    Command(String),
    #[error("command timed out: {0}")]
    Timeout(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("connection closed by client")]
    Closed,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(self, Self::Command(message) if message.to_ascii_lowercase().contains("not found"))
    }
}

/// A processor failure. Mark it unrecoverable to skip retries and enter DLQ.
#[derive(Debug)]
pub struct ProcessError {
    message: String,
    unrecoverable: bool,
    stack: Vec<String>,
}

impl ProcessError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(message.into(), false)
    }

    pub fn unrecoverable(message: impl Into<String>) -> Self {
        Self::new(message.into(), true)
    }

    fn new(message: String, unrecoverable: bool) -> Self {
        let trace = Backtrace::force_capture().to_string();
        let mut stack = vec![format!("ProcessError: {message}")];
        stack.extend(trace.lines().map(str::to_owned));
        Self {
            message,
            unrecoverable,
            stack,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_unrecoverable(&self) -> bool {
        self.unrecoverable
    }

    pub(crate) fn stack(&self, limit: usize) -> Vec<String> {
        self.stack.iter().take(limit).cloned().collect()
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProcessError {}
