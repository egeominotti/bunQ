use rmpv::Value;

use crate::queue::job_payload;
use crate::wire::{Map, as_map, command, get, map};
use crate::{JobOptions, Queue, Result};

#[derive(Clone, Debug, Default)]
pub struct SchedulerRepeat {
    pub pattern: Option<String>,
    pub every_ms: Option<i64>,
    pub limit: Option<i64>,
    pub timezone: Option<String>,
    pub immediately: bool,
    pub skip_if_no_worker: bool,
}

#[derive(Clone, Debug)]
pub struct SchedulerTemplate {
    pub name: Option<String>,
    pub data: Value,
    pub options: JobOptions,
}

impl Default for SchedulerTemplate {
    fn default() -> Self {
        Self {
            name: None,
            data: Value::Nil,
            options: JobOptions::default(),
        }
    }
}

impl Queue {
    pub fn get_dlq(&self, count: Option<i64>) -> Result<Vec<Map>> {
        let response = self.call(command(
            "Dlq",
            map([
                ("queue", Value::from(self.name.clone())),
                ("count", count.map(Value::from).unwrap_or(Value::Nil)),
            ]),
        ))?;
        Ok(get(&response, "jobs")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| as_map(item.clone()).ok())
                    .collect()
            })
            .unwrap_or_default())
    }

    pub fn retry_dlq(&self, job_id: Option<&str>, count: Option<i64>) -> Result<i64> {
        let response = self.call(command(
            "RetryDlq",
            map([
                ("queue", Value::from(self.name.clone())),
                ("jobId", job_id.map(Value::from).unwrap_or(Value::Nil)),
                ("count", count.map(Value::from).unwrap_or(Value::Nil)),
            ]),
        ))?;
        Ok(get(&response, "count").and_then(Value::as_i64).unwrap_or(0))
    }

    pub fn purge_dlq(&self) -> Result<i64> {
        let response = self.call(command(
            "PurgeDlq",
            map([("queue", Value::from(self.name.clone()))]),
        ))?;
        Ok(get(&response, "count").and_then(Value::as_i64).unwrap_or(0))
    }

    pub fn upsert_job_scheduler(
        &self,
        id: &str,
        repeat: SchedulerRepeat,
        template: SchedulerTemplate,
    ) -> Result<()> {
        let name = template.name.as_deref().unwrap_or(id);
        let mut fields = map([
            ("name", Value::from(id)),
            ("queue", Value::from(self.name.clone())),
            ("data", job_payload(name, template.data)),
            (
                "schedule",
                repeat.pattern.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "repeatEvery",
                repeat.every_ms.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "maxLimit",
                repeat.limit.map(Value::from).unwrap_or(Value::Nil),
            ),
            (
                "timezone",
                repeat.timezone.map(Value::from).unwrap_or(Value::Nil),
            ),
            ("immediately", Value::from(repeat.immediately)),
            ("skipIfNoWorker", Value::from(repeat.skip_if_no_worker)),
        ]);
        let cron_options = template.options.cron_options();
        if cron_options.as_map().is_some_and(|map| !map.is_empty()) {
            fields.push((Value::from("jobOptions"), cron_options));
        }
        self.unit("Cron", fields)
    }

    pub fn get_job_scheduler(&self, id: &str) -> Result<Option<Map>> {
        match self.call(command("CronGet", map([("name", Value::from(id))]))) {
            Ok(response) => match get(&response, "cron") {
                None | Some(Value::Nil) => Ok(None),
                Some(value) => Ok(Some(as_map(value.clone())?)),
            },
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn remove_job_scheduler(&self, id: &str) -> Result<()> {
        self.unit("CronDelete", map([("name", Value::from(id))]))
    }

    pub fn set_rate_limit(
        &self,
        limit: i64,
        duration_ms: Option<i64>,
        ttl_ms: Option<i64>,
    ) -> Result<()> {
        self.unit(
            "RateLimit",
            map([
                ("queue", Value::from(self.name.clone())),
                ("limit", Value::from(limit)),
                (
                    "duration",
                    duration_ms.map(Value::from).unwrap_or(Value::Nil),
                ),
                ("ttl", ttl_ms.map(Value::from).unwrap_or(Value::Nil)),
            ]),
        )
    }

    pub fn clear_rate_limit(&self) -> Result<()> {
        self.unit(
            "RateLimitClear",
            map([("queue", Value::from(self.name.clone()))]),
        )
    }

    pub fn ping(&self) -> Result<bool> {
        self.connection.ping()
    }
}
