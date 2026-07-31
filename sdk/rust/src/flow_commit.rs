use std::collections::HashMap;

use rmpv::Value;

use crate::flow::FlowNode;
use crate::flow_plan::{PlannedJob, PlannedNode};
use crate::wire::{Map, command, get, map};
use crate::{Connection, Error, Job, Result};

pub(crate) fn commit_flow(
    connection: &Connection,
    jobs: &[PlannedJob],
) -> Result<HashMap<String, Map>> {
    if jobs.is_empty() {
        return Ok(HashMap::new());
    }
    let request_jobs = jobs
        .iter()
        .map(|job| {
            Value::Map(map([
                ("id", Value::from(job.id.clone())),
                ("queue", Value::from(job.queue.clone())),
                ("input", Value::Map(job.input.clone())),
            ]))
        })
        .collect();
    let response = connection.call(command(
        "PUSHF",
        map([("jobs", Value::Array(request_jobs))]),
    ))?;
    let snapshots = get(&response, "data")
        .and_then(Value::as_map)
        .and_then(|data| get(data, "jobs"));
    validate_snapshots(jobs, snapshots)
}

pub(crate) fn validate_snapshots(
    jobs: &[PlannedJob],
    snapshots: Option<&Value>,
) -> Result<HashMap<String, Map>> {
    let snapshots = snapshots
        .and_then(Value::as_array)
        .filter(|items| items.len() == jobs.len())
        .ok_or_else(|| {
            Error::Serialization(
                "Invalid PUSHF response: committed job snapshots are missing".into(),
            )
        })?;
    let expected = jobs
        .iter()
        .map(|job| (job.id.as_str(), job.queue.as_str()))
        .collect::<HashMap<_, _>>();
    let mut by_id = HashMap::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let raw = snapshot.as_map().ok_or_else(invalid_snapshot)?;
        let id = get(raw, "id")
            .and_then(Value::as_str)
            .ok_or_else(invalid_ids)?;
        let queue = get(raw, "queue")
            .and_then(Value::as_str)
            .ok_or_else(invalid_ids)?;
        if expected.get(id).copied() != Some(queue) || by_id.contains_key(id) {
            return Err(invalid_ids());
        }
        by_id.insert(id.to_owned(), raw.clone());
    }
    if by_id.len() != expected.len() {
        return Err(invalid_ids());
    }
    Ok(by_id)
}

pub(crate) fn build_node(
    node: &PlannedNode,
    snapshots: &HashMap<String, Map>,
    connection: &Connection,
) -> Result<FlowNode> {
    let raw = snapshots.get(&node.id).cloned().ok_or_else(invalid_ids)?;
    let children = node
        .children
        .iter()
        .map(|child| build_node(child, snapshots, connection))
        .collect::<Result<Vec<_>>>()?;
    Ok(FlowNode {
        job: Job::new(raw, connection.clone(), None),
        children,
    })
}

fn invalid_snapshot() -> Error {
    Error::Serialization("Invalid PUSHF response: job snapshot is invalid".into())
}

fn invalid_ids() -> Error {
    Error::Serialization(
        "Invalid PUSHF response: committed job IDs do not match the request".into(),
    )
}
