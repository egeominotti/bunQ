use rmpv::Value;

use crate::flow_commit::{build_node, commit_flow};
use crate::flow_plan::{plan_chain, plan_tree};
use crate::{Connection, ConnectionOptions, JobOptions, Result};

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

    /// Plans the complete tree and commits it with one atomic `PUSHF` command.
    pub fn add(&self, flow: FlowJob) -> Result<FlowNode> {
        let plan = plan_tree(flow)?;
        let snapshots = commit_flow(&self.connection, &plan.jobs)?;
        build_node(&plan.root, &snapshots, &self.connection)
    }

    /// Plans the complete dependency chain and commits it atomically.
    pub fn add_chain(&self, steps: Vec<ChainStep>) -> Result<Vec<String>> {
        let plan = plan_chain(steps)?;
        commit_flow(&self.connection, &plan.jobs)?;
        Ok(plan.ids)
    }

    pub fn close(&self) {
        self.connection.close();
    }
}
