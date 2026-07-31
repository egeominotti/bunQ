mod support;

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bunqueue_client::{
    ChainStep, ConnectionOptions, FlowJob, FlowProducer, JobOptions, Queue, Value, Worker,
    WorkerOptions,
};
use support::Server;

fn empty_data() -> Value {
    Value::Map(Vec::new())
}

fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.as_map().and_then(|entries| {
        entries
            .iter()
            .find(|(key, _)| key.as_str() == Some(name))
            .map(|(_, value)| value)
    })
}

#[test]
fn atomic_tree_preserves_planned_ids_and_runs_child_before_parent() {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let queue_name = format!("rust-atomic-flow-{suffix}");
    let parent_id = format!("rust-flow-parent-{suffix}");
    let child_id = format!("rust-flow-child-{suffix}");
    let producer = FlowProducer::new(options.clone());
    let node = producer
        .add(FlowJob {
            name: "parent".into(),
            queue_name: queue_name.clone(),
            data: empty_data(),
            options: JobOptions {
                job_id: Some(parent_id.clone()),
                ..Default::default()
            },
            children: vec![FlowJob {
                name: "child".into(),
                queue_name: queue_name.clone(),
                data: empty_data(),
                options: JobOptions {
                    job_id: Some(child_id.clone()),
                    ..Default::default()
                },
                children: Vec::new(),
            }],
        })
        .expect("commit atomic flow");

    assert_eq!(node.job.id(), parent_id);
    assert_eq!(node.children.len(), 1);
    assert_eq!(node.children[0].job.id(), child_id);
    assert_eq!(
        field(&node.children[0].job.data(), "__parentId").and_then(Value::as_str),
        Some(parent_id.as_str())
    );
    assert_eq!(
        node.children[0]
            .job
            .raw()
            .iter()
            .find(|(key, _)| key.as_str() == Some("parentId"))
            .and_then(|(_, value)| value.as_str()),
        Some(parent_id.as_str()),
        "FlowNode must be built from the authoritative PUSHF snapshot"
    );

    let queue = Queue::new(queue_name.clone(), options.clone());
    let by_custom = queue
        .get_job_by_custom_id(&parent_id)
        .expect("lookup custom id")
        .expect("custom-id owner");
    assert_eq!(by_custom.id(), parent_id);

    let observed = Arc::new(Mutex::new(Vec::new()));
    let worker_observed = observed.clone();
    let worker = Worker::new(
        queue_name,
        move |job| {
            worker_observed
                .lock()
                .expect("observation mutex")
                .push(job.name());
            Ok(Value::from(job.name()))
        },
        WorkerOptions {
            connection: options,
            concurrency: 1,
            batch_size: 1,
            poll_timeout_ms: 100,
            ..Default::default()
        },
    );

    assert_eq!(worker.run_once().expect("run child"), 1);
    assert_eq!(worker.run_once().expect("run parent"), 1);
    assert_eq!(
        *observed.lock().expect("observation mutex"),
        vec!["child".to_owned(), "parent".to_owned()]
    );

    worker.close();
    queue.obliterate().expect("flow cleanup");
    queue.close();
    producer.close();
}

#[test]
fn atomic_chain_preserves_ids_and_runs_in_dependency_order() {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let queue_name = format!("rust-atomic-chain-{suffix}");
    let expected_ids = (0..3)
        .map(|index| format!("rust-chain-{suffix}-{index}"))
        .collect::<Vec<_>>();
    let producer = FlowProducer::new(options.clone());
    let steps = expected_ids
        .iter()
        .enumerate()
        .map(|(index, id)| ChainStep {
            name: format!("step-{index}"),
            queue_name: queue_name.clone(),
            data: empty_data(),
            options: JobOptions {
                job_id: Some(id.clone()),
                ..Default::default()
            },
        })
        .collect();

    assert_eq!(
        producer.add_chain(steps).expect("commit atomic chain"),
        expected_ids
    );

    let observed = Arc::new(Mutex::new(Vec::new()));
    let worker_observed = observed.clone();
    let worker = Worker::new(
        queue_name.clone(),
        move |job| {
            worker_observed
                .lock()
                .expect("observation mutex")
                .push(job.name());
            Ok(Value::Nil)
        },
        WorkerOptions {
            connection: options.clone(),
            concurrency: 1,
            batch_size: 1,
            poll_timeout_ms: 100,
            ..Default::default()
        },
    );
    for _ in 0..3 {
        assert_eq!(worker.run_once().expect("run chain step"), 1);
    }
    assert_eq!(
        *observed.lock().expect("observation mutex"),
        vec![
            "step-0".to_owned(),
            "step-1".to_owned(),
            "step-2".to_owned()
        ]
    );

    let queue = Queue::new(queue_name, options);
    worker.close();
    queue.obliterate().expect("chain cleanup");
    queue.close();
    producer.close();
}
