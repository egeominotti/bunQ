use rmpv::Value;

use crate::wire::{Map, map};

#[derive(Clone, Debug)]
pub enum Backoff {
    Milliseconds(i64),
    Strategy {
        kind: String,
        delay: i64,
        max_delay: Option<i64>,
    },
}

impl Backoff {
    fn to_value(&self) -> Value {
        match self {
            Self::Milliseconds(delay) => Value::from(*delay),
            Self::Strategy {
                kind,
                delay,
                max_delay,
            } => Value::Map(map([
                ("type", Value::from(kind.clone())),
                ("delay", Value::from(*delay)),
                ("maxDelay", max_delay.map(Value::from).unwrap_or(Value::Nil)),
            ])),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Deduplication {
    pub id: String,
    pub ttl: Option<i64>,
    pub extend: Option<bool>,
    pub replace: Option<bool>,
}

/// Per-job options. Static fields make silently ignored options impossible.
#[derive(Clone, Debug, Default)]
pub struct JobOptions {
    pub priority: Option<i64>,
    pub delay: Option<i64>,
    pub attempts: Option<i64>,
    pub backoff: Option<Backoff>,
    pub ttl: Option<i64>,
    pub timeout: Option<i64>,
    pub job_id: Option<String>,
    pub unique_key: Option<String>,
    pub deduplication: Option<Deduplication>,
    pub depends_on: Vec<String>,
    pub parent_id: Option<String>,
    pub children_ids: Vec<String>,
    pub tags: Vec<String>,
    pub group_id: Option<String>,
    pub lifo: Option<bool>,
    pub remove_on_complete: Option<bool>,
    pub remove_on_fail: Option<bool>,
    pub stall_timeout: Option<i64>,
    pub durable: Option<bool>,
    pub repeat: Option<Value>,
    pub debounce_id: Option<String>,
    pub debounce_ttl: Option<i64>,
    pub stack_trace_limit: Option<i64>,
    pub keep_logs: Option<i64>,
    pub size_limit: Option<i64>,
    pub timestamp: Option<i64>,
    pub fail_parent_on_failure: Option<bool>,
    pub remove_dependency_on_failure: Option<bool>,
    pub ignore_dependency_on_failure: Option<bool>,
    pub continue_parent_on_failure: Option<bool>,
}

impl JobOptions {
    pub(crate) fn to_wire(&self, bulk: bool) -> Map {
        let dedup = self.deduplication.as_ref();
        let unique_key = self
            .unique_key
            .clone()
            .or_else(|| dedup.map(|value| value.id.clone()));
        let dedup_value = dedup.map(|value| {
            Value::Map(map([
                ("ttl", value.ttl.map(Value::from).unwrap_or(Value::Nil)),
                (
                    "extend",
                    value.extend.map(Value::from).unwrap_or(Value::Nil),
                ),
                (
                    "replace",
                    value.replace.map(Value::from).unwrap_or(Value::Nil),
                ),
            ]))
        });
        let custom_key = if bulk { "customId" } else { "jobId" };
        map([
            (
                "priority",
                self.priority.map(Value::from).unwrap_or(Value::Nil),
            ),
            ("delay", self.delay.map(Value::from).unwrap_or(Value::Nil)),
            (
                "maxAttempts",
                self.attempts.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "backoff",
                self.backoff
                    .as_ref()
                    .map(Backoff::to_value)
                    .unwrap_or(Value::Nil),
            ),
            ("ttl", self.ttl.map(Value::from).unwrap_or(Value::Nil)),
            (
                "timeout",
                self.timeout.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                custom_key,
                self.job_id.clone().map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "uniqueKey",
                unique_key.map(Value::from).unwrap_or(Value::Nil),
            ),
            ("dedup", dedup_value.unwrap_or(Value::Nil)),
            ("dependsOn", string_array(&self.depends_on)),
            (
                "parentId",
                self.parent_id
                    .clone()
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            ("childrenIds", string_array(&self.children_ids)),
            ("tags", string_array(&self.tags)),
            (
                "groupId",
                self.group_id.clone().map(Value::from).unwrap_or(Value::Nil),
            ),
            ("lifo", self.lifo.map(Value::from).unwrap_or(Value::Nil)),
            (
                "removeOnComplete",
                self.remove_on_complete
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "removeOnFail",
                self.remove_on_fail.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "stallTimeout",
                self.stall_timeout.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "durable",
                self.durable.map(Value::from).unwrap_or(Value::Nil),
            ),
            ("repeat", self.repeat.clone().unwrap_or(Value::Nil)),
            (
                "debounceId",
                self.debounce_id
                    .clone()
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "debounceTtl",
                self.debounce_ttl.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "stackTraceLimit",
                self.stack_trace_limit
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "keepLogs",
                self.keep_logs.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "sizeLimit",
                self.size_limit.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "timestamp",
                self.timestamp.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "failParentOnFailure",
                self.fail_parent_on_failure
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "removeDependencyOnFailure",
                self.remove_dependency_on_failure
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "ignoreDependencyOnFailure",
                self.ignore_dependency_on_failure
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "continueParentOnFailure",
                self.continue_parent_on_failure
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
        ])
    }

    pub(crate) fn cron_options(&self) -> Value {
        Value::Map(map([
            (
                "maxAttempts",
                self.attempts.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "backoff",
                self.backoff
                    .as_ref()
                    .map(Backoff::to_value)
                    .unwrap_or(Value::Nil),
            ),
            (
                "timeout",
                self.timeout.map(Value::from).unwrap_or(Value::Nil),
            ),
            ("delay", self.delay.map(Value::from).unwrap_or(Value::Nil)),
            (
                "stallTimeout",
                self.stall_timeout.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "removeOnComplete",
                self.remove_on_complete
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "removeOnFail",
                self.remove_on_fail.map(Value::from).unwrap_or(Value::Nil),
            ),
        ]))
    }
}

fn string_array(items: &[String]) -> Value {
    if items.is_empty() {
        Value::Nil
    } else {
        Value::Array(items.iter().cloned().map(Value::from).collect())
    }
}

#[derive(Clone, Debug)]
pub struct BulkEntry {
    pub name: String,
    pub data: Value,
    pub options: JobOptions,
}
