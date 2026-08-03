//! Official synchronous Rust SDK for bunqueue's TCP protocol.
//!
//! The broker owns scheduling, retry, DLQ, and persistence semantics. This
//! crate provides typed producer, worker, administration, and flow APIs.

mod connection;
mod connection_io;
mod error;
mod flow;
mod flow_commit;
mod flow_plan;
mod job;
mod options;
mod queue;
mod queue_admin;
mod queue_control;
mod queue_query;
mod telemetry;
mod transport;
mod wire;
mod worker;
mod worker_limits;
mod worker_runtime;

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod flow_plan_boundary_tests;
#[cfg(test)]
mod flow_plan_tests;
#[cfg(test)]
mod name_data_tests;
#[cfg(test)]
mod options_tests;
#[cfg(test)]
mod telemetry_tests;
#[cfg(test)]
mod wire_tests;
#[cfg(test)]
mod worker_join_tests;

pub use connection::{Connection, ConnectionOptions, TlsOptions};
pub use error::{Error, ProcessError, Result};
pub use flow::{ChainStep, FlowJob, FlowNode, FlowProducer};
pub use job::Job;
pub use options::{Backoff, BulkEntry, Deduplication, JobOptions};
pub use queue::Queue;
pub use queue_admin::{SchedulerRepeat, SchedulerTemplate};
pub use rmpv::Value;
pub use telemetry::{TelemetryCallback, TelemetryEvent};
pub use wire::{MAX_FRAME_SIZE, Message, PROTOCOL_VERSION, json_to_value, value_to_json};
pub use worker::{Processor, Worker, WorkerOptions};
