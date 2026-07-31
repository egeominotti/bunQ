use rmpv::Value;

use crate::flow_plan::{next_job_count, plan_chain, plan_tree, validate_job_count};
use crate::{ChainStep, FlowJob, JobOptions};

fn flow(name: impl Into<String>, queue: impl Into<String>, id: Option<String>) -> FlowJob {
    FlowJob {
        name: name.into(),
        queue_name: queue.into(),
        data: Value::Nil,
        options: JobOptions {
            job_id: id,
            ..Default::default()
        },
        children: Vec::new(),
    }
}

#[test]
fn name_queue_and_id_boundaries_are_enforced() {
    assert!(plan_tree(flow("n", "q", Some("a".repeat(1_024)))).is_ok());
    assert!(plan_tree(flow("n".repeat(256), "q".repeat(256), None)).is_ok());

    let invalid = [
        flow("", "queue", None),
        flow("n".repeat(257), "queue", None),
        flow("job", "", None),
        flow("job", "q".repeat(257), None),
        flow("job", "bad queue", None),
        flow("job", "queue", Some(String::new())),
        flow("job", "queue", Some("has:colon".into())),
        flow("job", "queue", Some("a".repeat(1_025))),
    ];
    for job in invalid {
        assert!(plan_tree(job).is_err());
    }
}

#[test]
fn depth_boundary_is_not_bypassed() {
    assert!(plan_tree(nested_flow(100)).is_ok());
    assert!(plan_tree(nested_flow(101)).is_err());
}

#[test]
fn job_count_boundary_accepts_limit_and_rejects_next() {
    assert!(validate_job_count(0).is_ok());
    assert!(validate_job_count(10_000).is_ok());
    assert!(validate_job_count(10_001).is_err());
    assert_eq!(next_job_count(9_999).expect("last allowed job"), 10_000);
    assert!(next_job_count(10_000).is_err());

    assert!(plan_chain(Vec::new()).is_ok());
    assert!(plan_chain(vec![chain_step("only")]).is_ok());
}

#[test]
fn duplicate_explicit_ids_are_rejected() {
    let mut root = flow("parent", "queue", Some("same".into()));
    root.children
        .push(flow("child", "queue", Some("same".into())));
    assert!(plan_tree(root).is_err());
}

fn nested_flow(depth: usize) -> FlowJob {
    let mut node = flow("leaf", "queue", None);
    for _ in 0..depth {
        let mut parent = flow("parent", "queue", None);
        parent.children.push(node);
        node = parent;
    }
    node
}

fn chain_step(id: &str) -> ChainStep {
    ChainStep {
        name: "step".into(),
        queue_name: "queue".into(),
        data: Value::Nil,
        options: JobOptions {
            job_id: Some(id.into()),
            ..Default::default()
        },
    }
}
