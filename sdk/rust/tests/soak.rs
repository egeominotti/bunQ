mod support;

use std::time::{Duration, Instant};

use bunqueue_client::{BulkEntry, ConnectionOptions, JobOptions, Queue, Value};
use support::Server;

#[test]
#[ignore = "set BUNQUEUE_SDK_SOAK_SECONDS and run with --ignored"]
fn sustained_producer_soak() {
    let seconds = std::env::var("BUNQUEUE_SDK_SOAK_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .expect("BUNQUEUE_SDK_SOAK_SECONDS must be a positive integer");
    let batch_size = std::env::var("BUNQUEUE_SDK_SOAK_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100);
    let server = Server::start();
    let queue = Queue::new(
        "rust-sustained-soak",
        ConnectionOptions {
            host: "127.0.0.1".into(),
            port: server.port,
            ..Default::default()
        },
    );
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut iterations = 0;
    let mut jobs = 0;
    while Instant::now() < deadline {
        let entries = (0..batch_size)
            .map(|index| BulkEntry {
                name: "soak".into(),
                data: Value::Map(vec![
                    (Value::from("iteration"), Value::from(iterations)),
                    (Value::from("index"), Value::from(index as i64)),
                ]),
                options: JobOptions::default(),
            })
            .collect();
        let ids = queue.add_bulk(entries).expect("soak add");
        assert_eq!(ids.len(), batch_size);
        assert_eq!(queue.count().expect("soak count"), batch_size as i64);
        assert!(queue.get_job(&ids[0]).expect("soak query").is_some());
        queue.obliterate().expect("soak reset");
        iterations += 1;
        jobs += ids.len();
    }
    eprintln!(
        "profile=rust-soak seconds={seconds} batch={batch_size} iterations={iterations} jobs={jobs}"
    );
    queue.close();
}
