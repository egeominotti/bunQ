mod support;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bunqueue_client::{
    ConnectionOptions, JobOptions, ProcessError, Queue, Value, Worker, WorkerOptions,
};
use support::Server;

fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.as_map().and_then(|entries| {
        entries
            .iter()
            .find(|(key, _)| key.as_str() == Some(name))
            .map(|(_, value)| value)
    })
}

fn assert_late_processor_outcome(fails: bool) {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let queue_name = format!(
        "rust-timeout-authority-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let queue = Queue::new(queue_name.clone(), options.clone());
    let job = queue
        .add(
            "slow",
            Value::Map(Vec::new()),
            JobOptions {
                timeout: Some(60),
                attempts: Some(1),
                durable: Some(true),
                ..Default::default()
            },
        )
        .expect("add timed job");
    let worker = Worker::new(
        queue_name,
        move |_| {
            thread::sleep(Duration::from_millis(250));
            if fails {
                Err(ProcessError::retryable("late processor failure"))
            } else {
                Ok(Value::from("late processor result"))
            }
        },
        WorkerOptions {
            connection: options,
            concurrency: 1,
            batch_size: 1,
            poll_timeout_ms: 100,
            heartbeat_interval: None,
            ..Default::default()
        },
    );

    // run_once reports the handler attempt that settled. It does not claim
    // that this late local outcome replaced the broker's terminal decision.
    assert_eq!(worker.run_once().expect("run timed job"), 1);
    assert_eq!(
        queue.get_state(&job.id()).expect("read final state"),
        "failed"
    );
    let stored = queue
        .get_job(&job.id())
        .expect("query timed job")
        .expect("timed job remains available");
    let stored_raw = Value::Map(stored.raw().clone());
    let failed_reason = field(&stored_raw, "failedReason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        failed_reason.to_ascii_lowercase().contains("timeout"),
        "broker timeout must remain authoritative, got {failed_reason:?}"
    );
    assert!(queue.get_result(&job.id()).expect("read result").is_nil());
    let counts = queue.get_job_counts().expect("read queue counts");
    assert_eq!(counts.get("completed"), Some(&0));
    assert_eq!(counts.get("failed"), Some(&1));

    worker.close();
    queue.obliterate().expect("timeout test cleanup");
    queue.close();
}

#[test]
fn late_success_does_not_override_broker_timeout() {
    assert_late_processor_outcome(false);
}

#[test]
fn late_failure_does_not_override_broker_timeout() {
    assert_late_processor_outcome(true);
}
