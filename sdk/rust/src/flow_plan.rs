use std::collections::HashSet;

use rmpv::Value;

use crate::wire::Map;
use crate::{ChainStep, Error, FlowJob, JobOptions, Result};

const MAX_FLOW_DEPTH: usize = 100;
const MAX_FLOW_JOBS: usize = 10_000;

#[derive(Clone, Debug)]
pub(crate) struct PlannedJob {
    pub(crate) id: String,
    pub(crate) queue: String,
    pub(crate) input: Map,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedNode {
    pub(crate) id: String,
    pub(crate) children: Vec<PlannedNode>,
}

pub(crate) struct TreePlan {
    pub(crate) jobs: Vec<PlannedJob>,
    pub(crate) root: PlannedNode,
}

pub(crate) struct ChainPlan {
    pub(crate) jobs: Vec<PlannedJob>,
    pub(crate) ids: Vec<String>,
}

struct PlanState {
    ids: HashSet<String>,
    jobs: Vec<PlannedJob>,
    count: usize,
}

pub(crate) fn plan_tree(flow: FlowJob) -> Result<TreePlan> {
    let mut state = PlanState {
        ids: HashSet::new(),
        jobs: Vec::new(),
        count: 0,
    };
    let root = visit(flow, None, 0, &mut state)?;
    Ok(TreePlan {
        jobs: state.jobs,
        root,
    })
}

pub(crate) fn plan_chain(steps: Vec<ChainStep>) -> Result<ChainPlan> {
    validate_job_count(steps.len())?;
    let mut ids = HashSet::new();
    let planned_ids = steps
        .iter()
        .map(|step| {
            validate_name_queue(&step.name, &step.queue_name, 0)?;
            validate_options(&step.options)?;
            allocate_id(&step.options, &mut ids)
        })
        .collect::<Result<Vec<_>>>()?;
    let jobs = steps
        .into_iter()
        .enumerate()
        .map(|(index, step)| {
            let dependency = index
                .checked_sub(1)
                .map(|previous| planned_ids[previous].clone());
            let data = flow_data(
                &step.name,
                step.data,
                vec![(
                    "__flowParentId",
                    dependency.clone().map(Value::from).unwrap_or(Value::Nil),
                )],
            )?;
            Ok(PlannedJob {
                id: planned_ids[index].clone(),
                queue: step.queue_name,
                input: flow_input(
                    &step.options,
                    data,
                    None,
                    dependency.map(|id| vec![id]),
                    None,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ChainPlan {
        jobs,
        ids: planned_ids,
    })
}

fn visit(
    node: FlowJob,
    parent: Option<(String, String)>,
    depth: usize,
    state: &mut PlanState,
) -> Result<PlannedNode> {
    validate_name_queue(&node.name, &node.queue_name, depth)?;
    validate_options(&node.options)?;
    state.count = next_job_count(state.count)?;
    let id = allocate_id(&node.options, &mut state.ids)?;
    let mut children = Vec::with_capacity(node.children.len());
    for child in node.children {
        children.push(visit(
            child,
            Some((id.clone(), node.queue_name.clone())),
            depth + 1,
            state,
        )?);
    }
    let child_ids = children
        .iter()
        .map(|child| child.id.clone())
        .collect::<Vec<_>>();
    let mut internal = Vec::new();
    if let Some((parent_id, parent_queue)) = &parent {
        internal.push(("__parentId", Value::from(parent_id.clone())));
        internal.push(("__parentQueue", Value::from(parent_queue.clone())));
    }
    if !child_ids.is_empty() {
        internal.push((
            "__childrenIds",
            Value::Array(child_ids.iter().cloned().map(Value::from).collect()),
        ));
    }
    let data = flow_data(&node.name, node.data, internal)?;
    state.jobs.push(PlannedJob {
        id: id.clone(),
        queue: node.queue_name,
        input: flow_input(
            &node.options,
            data,
            parent.map(|value| value.0),
            (!child_ids.is_empty()).then_some(child_ids.clone()),
            (!child_ids.is_empty()).then_some(child_ids),
        )?,
    });
    Ok(PlannedNode { id, children })
}

fn validate_name_queue(name: &str, queue: &str, depth: usize) -> Result<()> {
    if name.is_empty() || name.chars().count() > 256 {
        return invalid("flow job name must be a non-empty string of at most 256 characters");
    }
    if !(1..=256).contains(&queue.len())
        || !queue
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
    {
        return invalid(format!("invalid flow queue: {queue}"));
    }
    if depth > MAX_FLOW_DEPTH {
        return invalid(format!(
            "flow exceeds the {MAX_FLOW_DEPTH} level depth limit"
        ));
    }
    Ok(())
}

pub(crate) fn validate_job_count(count: usize) -> Result<()> {
    if count > MAX_FLOW_JOBS {
        invalid(format!("flow exceeds the {MAX_FLOW_JOBS} job limit"))
    } else {
        Ok(())
    }
}

pub(crate) fn next_job_count(count: usize) -> Result<usize> {
    let next = count
        .checked_add(1)
        .ok_or_else(|| Error::Command("flow job count overflow".into()))?;
    validate_job_count(next)?;
    Ok(next)
}

fn validate_options(options: &JobOptions) -> Result<()> {
    if options.repeat.is_some() {
        return invalid("repeat is not supported inside an atomic flow");
    }
    if options.unique_key.is_some() || options.deduplication.is_some() {
        return invalid("deduplication is not supported inside an atomic flow");
    }
    if options.debounce_id.is_some() || options.debounce_ttl.is_some() {
        return invalid("debounce is not supported inside an atomic flow");
    }
    if options.parent_id.is_some()
        || !options.depends_on.is_empty()
        || !options.children_ids.is_empty()
    {
        return invalid("flow topology options are owned by FlowProducer");
    }
    Ok(())
}

fn allocate_id(options: &JobOptions, ids: &mut HashSet<String>) -> Result<String> {
    let id = match &options.job_id {
        Some(id) => id.clone(),
        None => random_id()?,
    };
    if id.is_empty() || id.chars().count() > 1_024 || id.contains(':') {
        return invalid("flow jobId must be non-empty and cannot contain a colon");
    }
    if !ids.insert(id.clone()) {
        return invalid(format!("duplicate flow job id: {id}"));
    }
    Ok(id)
}

fn random_id() -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| Error::Command(format!("secure flow id generation failed: {error}")))?;
    let mut id = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        id.push(HEX[(byte >> 4) as usize] as char);
        id.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(id)
}

fn flow_data(name: &str, data: Value, internal: Vec<(&str, Value)>) -> Result<Value> {
    let mut entries = match data {
        Value::Map(entries) => validate_user_data(entries)?,
        Value::Nil => Vec::new(),
        other => vec![(Value::from("payload"), other)],
    };
    entries.push((Value::from("name"), Value::from(name)));
    entries.extend(
        internal
            .into_iter()
            .map(|(key, value)| (Value::from(key), value)),
    );
    Ok(Value::Map(entries))
}

fn validate_user_data(entries: Map) -> Result<Map> {
    let mut keys = HashSet::new();
    for (key, _) in &entries {
        let key = key
            .as_str()
            .ok_or_else(|| Error::Command("flow job data keys must be strings".into()))?;
        if key == "name" || key.starts_with("__") {
            return invalid(format!("flow job data key is reserved: {key}"));
        }
        if !keys.insert(key.to_owned()) {
            return invalid(format!("duplicate flow job data key: {key}"));
        }
    }
    Ok(entries)
}

fn flow_input(
    options: &JobOptions,
    data: Value,
    parent_id: Option<String>,
    depends_on: Option<Vec<String>>,
    children_ids: Option<Vec<String>>,
) -> Result<Map> {
    validate_options(options)?;
    let mut input = options.to_wire(true);
    input
        .retain(|(key, _)| !matches!(key.as_str(), Some("parentId" | "dependsOn" | "childrenIds")));
    input.push((Value::from("data"), data));
    append_ids(&mut input, "parentId", parent_id.map(Value::from));
    append_ids(&mut input, "dependsOn", depends_on.map(string_array));
    append_ids(&mut input, "childrenIds", children_ids.map(string_array));
    Ok(input)
}

fn append_ids(input: &mut Map, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        input.push((Value::from(key), value));
    }
}

fn string_array(ids: Vec<String>) -> Value {
    Value::Array(ids.into_iter().map(Value::from).collect())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Command(message.into()))
}
