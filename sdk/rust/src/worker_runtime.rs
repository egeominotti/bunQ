use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Error, Result};

pub(crate) fn join_all(handles: Vec<thread::JoinHandle<Result<()>>>) -> Result<()> {
    let mut first_error = None;
    for handle in handles {
        let outcome = handle
            .join()
            .map_err(|_| Error::Command("processor thread panicked".into()))
            .and_then(|result| result);
        if first_error.is_none() {
            if let Err(error) = outcome {
                first_error = Some(error);
            }
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

pub(crate) fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
