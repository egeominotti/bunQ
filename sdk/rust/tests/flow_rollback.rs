mod support;

use std::time::{SystemTime, UNIX_EPOCH};

use bunqueue_client::{ChainStep, ConnectionOptions, FlowProducer, JobOptions, Queue, Value};
use support::Server;

#[test]
fn chain_failure_rolls_back_jobs_created_before_the_error() {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let queue_name = format!(
        "rust-flow-rollback-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let flow = FlowProducer::new(options.clone());
    let result = flow.add_chain(vec![
        ChainStep {
            name: "first".into(),
            queue_name: queue_name.clone(),
            data: Value::Map(Vec::new()),
            options: JobOptions::default(),
        },
        ChainStep {
            name: "invalid".into(),
            queue_name: queue_name.clone(),
            data: Value::Map(Vec::new()),
            options: JobOptions {
                attempts: Some(0),
                ..Default::default()
            },
        },
    ]);

    assert!(result.is_err(), "invalid second step must fail");
    let queue = Queue::new(queue_name, options);
    assert_eq!(queue.count().expect("count remaining jobs"), 0);
    queue.close();
    flow.close();
}
