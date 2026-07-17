mod support;

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bunqueue_client::{BulkEntry, Connection, ConnectionOptions, Error, JobOptions, Queue, Value};
use support::Server;

fn options(server: &Server) -> ConnectionOptions {
    ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    }
}

fn queue_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rust-{prefix}-{suffix}")
}

fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.as_map().and_then(|entries| map_field(entries, name))
}

fn map_field<'a>(entries: &'a [(Value, Value)], name: &str) -> Option<&'a Value> {
    entries
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
}

#[test]
fn concurrent_custom_id_retries_are_idempotent() {
    let server = Server::start();
    let name = queue_name("idempotency-race");
    let mut handles = Vec::new();
    for attempt in 0..24 {
        let queue = Queue::new(name.clone(), options(&server));
        handles.push(thread::spawn(move || {
            let job = queue
                .add(
                    "charge",
                    Value::Map(vec![(Value::from("attempt"), Value::from(attempt))]),
                    JobOptions {
                        job_id: Some("same-operation-id".into()),
                        ..Default::default()
                    },
                )
                .expect("concurrent retry");
            queue.close();
            job.id()
        }));
    }
    let ids = handles
        .into_iter()
        .map(|handle| handle.join().expect("retry thread"))
        .collect::<BTreeSet<_>>();
    let observer = Queue::new(name, options(&server));
    assert_eq!(ids.len(), 1);
    assert_eq!(observer.count().expect("queue count"), 1);
    observer.obliterate().expect("idempotency cleanup");
    observer.close();
}

#[test]
fn simultaneous_dequeues_lease_exactly_once() {
    let server = Server::start();
    let name = queue_name("double-dequeue");
    let queue = Queue::new(name.clone(), options(&server));
    let expected = queue
        .add("only-once", Value::from(1), JobOptions::default())
        .expect("seed job");
    let mut handles = Vec::new();
    for owner in 0..12 {
        let connection = Connection::new(options(&server));
        let queue_name = name.clone();
        handles.push(thread::spawn(move || {
            let result = connection.call(vec![
                (Value::from("cmd"), Value::from("PULL")),
                (Value::from("queue"), Value::from(queue_name)),
                (
                    Value::from("owner"),
                    Value::from(format!("contender-{owner}")),
                ),
                (Value::from("timeout"), Value::from(250)),
            ]);
            (connection, result)
        }));
    }
    let contenders = handles
        .into_iter()
        .map(|handle| handle.join().expect("pull thread"))
        .collect::<Vec<_>>();
    let leased = contenders
        .iter()
        .filter_map(|(_, result)| result.as_ref().ok())
        .filter_map(|response| map_field(response, "job"))
        .filter_map(|job| field(job, "id"))
        .filter_map(|id| id.as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(leased, vec![expected.id()]);
    for (connection, result) in contenders {
        result.expect("contender pull");
        connection.close();
    }
    queue.obliterate().expect("dequeue cleanup");
    queue.close();
}

#[test]
fn generated_payloads_preserve_all_user_fields() {
    let server = Server::start();
    let queue = Queue::new(queue_name("generated"), options(&server));
    let mut state = 0x0BADC0DE_u32;
    let mut expected = Vec::new();
    let entries = (0..64)
        .map(|index| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let payload = Value::Map(vec![
                (Value::from("index"), Value::from(index)),
                (
                    Value::from("signed"),
                    Value::from(i64::from(state % 2_000_001) - 1_000_000),
                ),
                (Value::from("flag"), Value::from(state & 1 == 1)),
                (
                    Value::from("text"),
                    Value::from(format!("case-{state:x}-🧪")),
                ),
                (
                    Value::from("nested"),
                    Value::Array(vec![
                        Value::from(state % 97),
                        Value::from((state ^ index) % 1_000_003),
                    ]),
                ),
            ]);
            expected.push(payload.clone());
            BulkEntry {
                name: format!("generated-{}", index % 7),
                data: payload,
                options: JobOptions::default(),
            }
        })
        .collect();
    let ids = queue.add_bulk(entries).expect("generated bulk");
    assert_eq!(ids.len(), expected.len());
    for (index, id) in ids.iter().enumerate() {
        let job = queue
            .get_job(id)
            .expect("generated query")
            .expect("generated job");
        assert_eq!(job.name(), format!("generated-{}", index % 7));
        assert_eq!(job.data(), expected[index]);
    }
    queue.obliterate().expect("generated cleanup");
    queue.close();
}

#[test]
fn spike_burst_recovers_without_loss() {
    let server = Server::start();
    let queue = Queue::new(queue_name("spike"), options(&server));
    let entries = (0..512)
        .map(|index| BulkEntry {
            name: "spike".into(),
            data: Value::Map(vec![(Value::from("index"), Value::from(index))]),
            options: JobOptions::default(),
        })
        .collect();
    let ids = queue.add_bulk(entries).expect("spike ingestion");
    assert_eq!(ids.len(), 512);
    assert_eq!(queue.count().expect("spike count"), 512);
    assert_eq!(queue.drain().expect("spike drain"), 512);
    assert_eq!(queue.count().expect("post-spike count"), 0);
    queue.obliterate().expect("spike cleanup");
    queue.close();
}

#[test]
fn malformed_mutation_corpus_is_typed_and_connection_stays_healthy() {
    let server = Server::start();
    let connection = Connection::new(options(&server));
    for depth in 1..=16 {
        let mut invalid = Value::Ext(7, vec![depth]);
        for _ in 0..depth {
            invalid = Value::Array(vec![invalid]);
        }
        let result = connection.call(vec![
            (Value::from("cmd"), Value::from("Ping")),
            (Value::from("payload"), invalid),
        ]);
        assert!(matches!(result, Err(Error::Serialization(_))));
    }
    assert!(connection.ping().expect("ping after malformed corpus"));
    connection.close();
}

#[test]
fn durable_job_survives_sigkill_and_client_reconnects() {
    let mut server = Server::start();
    let queue = Queue::new(queue_name("crash"), options(&server));
    let job = queue
        .add(
            "before-crash",
            Value::from(1),
            JobOptions {
                durable: Some(true),
                ..Default::default()
            },
        )
        .expect("durable add");
    assert_eq!(queue.count().expect("pre-crash count"), 1);
    server.crash();
    server.restart();

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && queue.ping().is_err() {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(queue.ping().expect("client reconnect"));
    assert!(
        queue
            .get_job(&job.id())
            .expect("post-crash query")
            .is_some()
    );
    assert_eq!(queue.count().expect("post-crash count"), 1);
    queue.obliterate().expect("crash cleanup");
    queue.close();
}
