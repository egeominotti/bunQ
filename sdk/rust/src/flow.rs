use rmpv::Value;

use crate::{Connection, ConnectionOptions, JobOptions, Queue, Result};

#[derive(Clone, Debug)]
pub struct FlowJob {
    pub name: String,
    pub queue_name: String,
    pub data: Value,
    pub options: JobOptions,
    pub children: Vec<FlowJob>,
}

#[derive(Clone, Debug)]
pub struct ChainStep {
    pub name: String,
    pub queue_name: String,
    pub data: Value,
    pub options: JobOptions,
}

#[derive(Clone)]
pub struct FlowNode {
    pub job: crate::Job,
    pub children: Vec<FlowNode>,
}

pub struct FlowProducer {
    connection: Connection,
}

impl FlowProducer {
    pub fn new(options: ConnectionOptions) -> Self {
        Self {
            connection: Connection::new(options),
        }
    }

    pub fn add(&self, flow: FlowJob) -> Result<FlowNode> {
        let mut created = Vec::new();
        match self.add_node(flow, None, &mut created) {
            Ok(node) => Ok(node),
            Err(error) => {
                self.rollback(&created);
                Err(error)
            }
        }
    }

    pub fn add_chain(&self, steps: Vec<ChainStep>) -> Result<Vec<String>> {
        let mut ids: Vec<String> = Vec::new();
        for step in steps {
            let mut options = step.options;
            if let Some(previous) = ids.last() {
                options.depends_on = vec![previous.clone()];
            }
            let data = if let Some(previous) = ids.last() {
                with_fields(
                    step.data,
                    vec![("__flowParentId", Value::from(previous.clone()))],
                )
            } else {
                step.data
            };
            match Queue::with_connection(step.queue_name, self.connection.clone())
                .add(step.name, data, options)
            {
                Ok(job) => ids.push(job.id()),
                Err(error) => {
                    self.rollback(&ids);
                    return Err(error);
                }
            }
        }
        Ok(ids)
    }

    pub fn close(&self) {
        self.connection.close();
    }

    fn add_node(
        &self,
        node: FlowJob,
        parent: Option<(String, String)>,
        created: &mut Vec<String>,
    ) -> Result<FlowNode> {
        let mut children = Vec::new();
        for child in node.children {
            children.push(self.add_node(
                child,
                Some(("pending".into(), node.queue_name.clone())),
                created,
            )?);
        }
        let mut options = node.options;
        let child_ids: Vec<String> = children.iter().map(|child| child.job.id()).collect();
        options.children_ids = child_ids.clone();
        options.depends_on = child_ids.clone();
        if let Some((parent_id, _)) = &parent {
            options.parent_id = Some(parent_id.clone());
        }
        let mut markers = Vec::new();
        if let Some((parent_id, parent_queue)) = &parent {
            markers.push(("__parentId", Value::from(parent_id.clone())));
            markers.push(("__parentQueue", Value::from(parent_queue.clone())));
        }
        if !child_ids.is_empty() {
            markers.push((
                "__childrenIds",
                Value::Array(child_ids.iter().cloned().map(Value::from).collect()),
            ));
        }
        let queue = Queue::with_connection(node.queue_name, self.connection.clone());
        let job = queue.add(node.name, with_fields(node.data, markers), options)?;
        created.push(job.id());
        for child in &children {
            queue.unit(
                "UpdateParent",
                crate::wire::map([
                    ("childId", Value::from(child.job.id())),
                    ("parentId", Value::from(job.id())),
                ]),
            )?;
        }
        Ok(FlowNode { job, children })
    }

    fn rollback(&self, ids: &[String]) {
        let queue = Queue::with_connection("", self.connection.clone());
        for id in ids.iter().rev() {
            let _ = queue.remove(id);
        }
    }
}

fn with_fields(data: Value, fields: Vec<(&str, Value)>) -> Value {
    let mut entries = match data {
        Value::Map(entries) => entries,
        Value::Nil => Vec::new(),
        other => vec![(Value::from("payload"), other)],
    };
    entries.extend(
        fields
            .into_iter()
            .map(|(key, value)| (Value::from(key), value)),
    );
    Value::Map(entries)
}
