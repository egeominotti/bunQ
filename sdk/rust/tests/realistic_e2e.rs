mod support;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bunqueue_client::{
    BulkEntry, ConnectionOptions, JobOptions, Queue, Value, Worker, WorkerOptions,
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

#[test]
fn concurrent_invoice_burst_preserves_every_persisted_result() {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let queue_name = format!(
        "rust-invoices-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let queue = Queue::new(queue_name.clone(), options.clone());
    let worker = Worker::new(
        queue_name,
        |job| {
            let data = job.data();
            let invoice = field(&data, "invoice")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let cents = field(&data, "cents")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            Ok(Value::Map(vec![
                (Value::from("invoice"), Value::from(invoice)),
                (Value::from("total"), Value::from(cents * 2)),
            ]))
        },
        WorkerOptions {
            connection: options,
            concurrency: 12,
            batch_size: 32,
            poll_timeout_ms: 100,
            ..Default::default()
        },
    );
    let entries = (0..32)
        .map(|invoice| BulkEntry {
            name: "reconcile".into(),
            data: Value::Map(vec![
                (Value::from("invoice"), Value::from(invoice)),
                (Value::from("cents"), Value::from(101 + invoice)),
            ]),
            options: JobOptions::default(),
        })
        .collect();
    let ids = queue.add_bulk(entries).expect("bulk invoice ingestion");

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        worker.run_once().expect("process invoice batch");
        let counts = queue.get_job_counts().expect("invoice counts");
        if counts.get("completed") == Some(&(ids.len() as i64)) {
            break;
        }
    }
    assert_eq!(
        queue
            .get_job_counts()
            .expect("final counts")
            .get("completed"),
        Some(&(ids.len() as i64))
    );

    let mut checksum = 0;
    for (invoice, id) in ids.iter().enumerate() {
        let result = queue.get_result(id).expect("persisted invoice result");
        assert_eq!(
            field(&result, "invoice").and_then(Value::as_i64),
            Some(invoice as i64)
        );
        let expected = (101 + invoice as i64) * 2;
        assert_eq!(
            field(&result, "total").and_then(Value::as_i64),
            Some(expected)
        );
        checksum += expected;
    }
    assert_eq!(checksum, 7_456);
    worker.close();
    queue.obliterate().expect("invoice cleanup");
    queue.close();
}
