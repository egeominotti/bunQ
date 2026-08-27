use bunqueue_client::{ConnectionOptions, FlowJob, FlowProducer, JobOptions, json_to_value};
use serde_json::{Value as Json, json};

pub fn add(
    options: ConnectionOptions,
    queue: &str,
    parent_id: &str,
    child_id: &str,
) -> bunqueue_client::Result<Json> {
    let producer = FlowProducer::new(options);
    let created = producer.add(FlowJob {
        name: "parent".into(),
        queue_name: queue.into(),
        data: json_to_value(json!({"kind": "parent"})),
        options: JobOptions {
            job_id: Some(parent_id.into()),
            ..Default::default()
        },
        children: vec![FlowJob {
            name: "child".into(),
            queue_name: queue.into(),
            data: json_to_value(json!({"kind": "child"})),
            options: JobOptions {
                job_id: Some(child_id.into()),
                ..Default::default()
            },
            children: Vec::new(),
        }],
    });
    producer.close();
    let node = created?;
    Ok(json!({
        "parentId": node.job.id(),
        "childId": node.children[0].job.id(),
    }))
}
