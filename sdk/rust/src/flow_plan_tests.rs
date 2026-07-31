use std::collections::{HashMap, HashSet};

use proptest::prelude::*;
use rmpv::Value;

use crate::flow_commit::{commit_flow, validate_snapshots};
use crate::flow_plan::{PlannedJob, TreePlan, plan_chain, plan_tree};
use crate::wire::{Map, get, map};
use crate::{ChainStep, Connection, ConnectionOptions, Deduplication, FlowJob, JobOptions};

fn node_parts() -> impl Strategy<Value = (String, String, Value)> {
    (
        "[a-z][a-z0-9_-]{0,15}",
        "[a-z][a-z0-9_.-]{0,15}",
        proptest::collection::btree_map("[a-z][a-z0-9_]{0,10}", any::<i32>(), 0..5),
    )
        .prop_map(|(name, queue, data)| {
            let data = data
                .into_iter()
                .map(|(key, value)| (Value::from(key), Value::from(value)))
                .collect();
            (name, queue, Value::Map(data))
        })
}

fn flow_tree() -> impl Strategy<Value = FlowJob> {
    node_parts()
        .prop_map(|(name, queue_name, data)| FlowJob {
            name,
            queue_name,
            data,
            options: JobOptions::default(),
            children: Vec::new(),
        })
        .prop_recursive(4, 64, 4, |child| {
            (node_parts(), proptest::collection::vec(child, 0..4)).prop_map(
                |((name, queue_name, data), children)| FlowJob {
                    name,
                    queue_name,
                    data,
                    options: JobOptions::default(),
                    children,
                },
            )
        })
}

fn chain_steps() -> impl Strategy<Value = Vec<ChainStep>> {
    proptest::collection::vec(node_parts(), 0..30).prop_map(|steps| {
        steps
            .into_iter()
            .map(|(name, queue_name, data)| ChainStep {
                name,
                queue_name,
                data,
                options: JobOptions::default(),
            })
            .collect()
    })
}

fn value<'a>(map: &'a Map, key: &str) -> Option<&'a Value> {
    get(map, key)
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn assert_tree_invariants(plan: &TreePlan) {
    let by_id = plan
        .jobs
        .iter()
        .map(|job| (job.id.as_str(), job))
        .collect::<HashMap<_, _>>();
    assert_eq!(by_id.len(), plan.jobs.len());
    assert!(plan.jobs.iter().all(|job| !job.id.contains(':')));

    let mut planned_node_ids = Vec::new();
    collect_node_ids(&plan.root, &mut planned_node_ids);
    assert_eq!(planned_node_ids.len(), plan.jobs.len());
    assert_eq!(
        planned_node_ids.into_iter().collect::<HashSet<_>>(),
        by_id.keys().copied().collect()
    );

    for job in &plan.jobs {
        let data = value(&job.input, "data")
            .and_then(Value::as_map)
            .expect("canonical flow data");
        assert!(value(data, "name").and_then(Value::as_str).is_some());
        let children = strings(value(&job.input, "childrenIds"));
        assert_eq!(children, strings(value(&job.input, "dependsOn")));
        assert_eq!(children, strings(value(data, "__childrenIds")));
        for child_id in children {
            let child = by_id.get(child_id.as_str()).expect("planned child");
            assert_eq!(
                value(&child.input, "parentId").and_then(Value::as_str),
                Some(job.id.as_str())
            );
            let child_data = value(&child.input, "data")
                .and_then(Value::as_map)
                .expect("child data");
            assert_eq!(
                value(child_data, "__parentId").and_then(Value::as_str),
                Some(job.id.as_str())
            );
            assert_eq!(
                value(child_data, "__parentQueue").and_then(Value::as_str),
                Some(job.queue.as_str())
            );
        }
    }
}

fn collect_node_ids<'a>(node: &'a crate::flow_plan::PlannedNode, ids: &mut Vec<&'a str>) {
    ids.push(&node.id);
    for child in &node.children {
        collect_node_ids(child, ids);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn planned_trees_preserve_reciprocal_topology(flow in flow_tree()) {
        let plan = plan_tree(flow).expect("valid generated flow");
        assert_tree_invariants(&plan);
    }

    #[test]
    fn custom_ids_are_the_server_custom_id(id in "[A-Za-z0-9_-]{1,64}") {
        let plan = plan_tree(FlowJob {
            name: "job".into(),
            queue_name: "queue".into(),
            data: Value::Nil,
            options: JobOptions {
                job_id: Some(id.clone()),
                ..Default::default()
            },
            children: Vec::new(),
        }).expect("valid custom id");
        prop_assert_eq!(&plan.jobs[0].id, &id);
        prop_assert_eq!(
            value(&plan.jobs[0].input, "customId").and_then(Value::as_str),
            Some(id.as_str())
        );
    }

    #[test]
    fn chains_depend_only_on_the_previous_step(steps in chain_steps()) {
        let plan = plan_chain(steps).expect("valid generated chain");
        prop_assert_eq!(plan.jobs.len(), plan.ids.len());
        let unique = plan.ids.iter().collect::<HashSet<_>>();
        prop_assert_eq!(unique.len(), plan.ids.len());
        for (index, job) in plan.jobs.iter().enumerate() {
            let previous = index.checked_sub(1).map(|value| plan.ids[value].as_str());
            let expected = previous.map(|id| vec![id.to_owned()]).unwrap_or_default();
            prop_assert_eq!(strings(value(&job.input, "dependsOn")), expected);
            let marker = value(&job.input, "data")
                .and_then(Value::as_map)
                .and_then(|data| value(data, "__flowParentId"));
            prop_assert_eq!(marker.and_then(Value::as_str), previous);
            prop_assert_eq!(marker.map(Value::is_nil), Some(previous.is_none()));
        }
    }
}

fn one_job(options: JobOptions, data: Value) -> FlowJob {
    FlowJob {
        name: "job".into(),
        queue_name: "queue".into(),
        data,
        options,
        children: Vec::new(),
    }
}

#[test]
fn rejects_unsupported_and_user_owned_options() {
    let cases = [
        JobOptions {
            repeat: Some(Value::Map(Vec::new())),
            ..Default::default()
        },
        JobOptions {
            unique_key: Some("dedup".into()),
            ..Default::default()
        },
        JobOptions {
            deduplication: Some(Deduplication {
                id: "dedup".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
        JobOptions {
            debounce_id: Some("debounce".into()),
            ..Default::default()
        },
        JobOptions {
            debounce_ttl: Some(1),
            ..Default::default()
        },
        JobOptions {
            parent_id: Some("parent".into()),
            ..Default::default()
        },
        JobOptions {
            depends_on: vec!["dependency".into()],
            ..Default::default()
        },
        JobOptions {
            children_ids: vec!["child".into()],
            ..Default::default()
        },
    ];
    for options in cases {
        assert!(plan_tree(one_job(options, Value::Nil)).is_err());
    }
}

#[test]
fn rejects_reserved_or_duplicate_data_keys() {
    for key in ["name", "__parentId", "__private"] {
        let data = Value::Map(vec![(Value::from(key), Value::from("owned"))]);
        assert!(plan_tree(one_job(JobOptions::default(), data)).is_err());
    }
    let duplicated = Value::Map(vec![
        (Value::from("value"), Value::from(1)),
        (Value::from("value"), Value::from(2)),
    ]);
    assert!(plan_tree(one_job(JobOptions::default(), duplicated)).is_err());
}

fn planned_job(id: &str, queue: &str) -> PlannedJob {
    PlannedJob {
        id: id.into(),
        queue: queue.into(),
        input: Vec::new(),
    }
}

fn snapshot(id: &str, queue: &str) -> Value {
    Value::Map(map([
        ("id", Value::from(id)),
        ("queue", Value::from(queue)),
        ("data", Value::Map(map([("name", Value::from("job"))]))),
    ]))
}

#[test]
fn snapshot_validation_requires_an_exact_id_and_queue_bijection() {
    let jobs = [planned_job("one", "queue"), planned_job("two", "other")];
    let valid = Value::Array(vec![snapshot("two", "other"), snapshot("one", "queue")]);
    assert_eq!(
        validate_snapshots(&jobs, Some(&valid))
            .expect("valid snapshots")
            .len(),
        2
    );

    let invalid = [
        Value::Nil,
        Value::Array(vec![snapshot("one", "queue")]),
        Value::Array(vec![snapshot("one", "queue"), snapshot("one", "queue")]),
        Value::Array(vec![snapshot("one", "wrong"), snapshot("two", "other")]),
        Value::Array(vec![snapshot("one", "queue"), snapshot("unknown", "other")]),
        Value::Array(vec![snapshot("one", "queue"), Value::from("not-a-map")]),
    ];
    for snapshots in &invalid {
        assert!(validate_snapshots(&jobs, Some(snapshots)).is_err());
    }
    assert!(validate_snapshots(&jobs, None).is_err());
}

#[test]
fn empty_commit_skips_the_connection() {
    let connection = Connection::new(ConnectionOptions::default());
    assert!(
        commit_flow(&connection, &[])
            .expect("empty flow")
            .is_empty()
    );
}
