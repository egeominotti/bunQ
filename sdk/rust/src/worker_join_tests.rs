use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::worker::join_all;
use crate::{Error, Result};

#[test]
fn join_all_waits_for_every_handle_after_the_first_error() {
    let completed = Arc::new(AtomicBool::new(false));
    let later = completed.clone();
    let handles = vec![
        thread::spawn(|| -> Result<()> { Err(Error::Connection("first failed".into())) }),
        thread::spawn(move || -> Result<()> {
            thread::sleep(Duration::from_millis(150));
            later.store(true, Ordering::Release);
            Ok(())
        }),
    ];
    let started = Instant::now();

    assert!(join_all(handles).is_err());
    assert!(completed.load(Ordering::Acquire));
    assert!(started.elapsed() >= Duration::from_millis(100));
}
