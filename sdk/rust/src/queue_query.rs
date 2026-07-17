use std::collections::BTreeMap;
use std::time::Duration;

use rmpv::Value;

use crate::wire::{Map, as_map, command, get, map};
use crate::{Error, Job, Queue, Result};

impl Queue {
    pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
        match self.call(command("GetJob", map([("id", Value::from(id))]))) {
            Ok(response) => job_from(&response, "job", self),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn get_job_by_custom_id(&self, custom_id: &str) -> Result<Option<Job>> {
        match self.call(command(
            "GetJobByCustomId",
            map([
                ("queue", Value::from(self.name.clone())),
                ("customId", Value::from(custom_id)),
            ]),
        )) {
            Ok(response) => job_from(&response, "job", self),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn get_jobs(&self, state: Value, offset: i64, limit: i64) -> Result<Vec<Job>> {
        let response = self.call(command(
            "GetJobs",
            map([
                ("queue", Value::from(self.name.clone())),
                ("state", state),
                ("offset", Value::from(offset.max(0))),
                ("limit", Value::from(limit.max(0))),
            ]),
        ))?;
        Ok(get(&response, "jobs")
            .and_then(Value::as_array)
            .map(|jobs| {
                jobs.iter()
                    .filter_map(|raw| as_map(raw.clone()).ok())
                    .map(|raw| Job::new(raw, self.connection.clone(), None))
                    .collect()
            })
            .unwrap_or_default())
    }

    pub fn get_state(&self, id: &str) -> Result<String> {
        let response = self.call(command("GetState", map([("id", Value::from(id))])))?;
        Ok(get(&response, "state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned())
    }

    pub fn get_result(&self, id: &str) -> Result<Value> {
        let response = self.call(command("GetResult", map([("id", Value::from(id))])))?;
        Ok(get(&response, "result").cloned().unwrap_or(Value::Nil))
    }

    pub fn wait_for_job(&self, id: &str, timeout_ms: i64) -> Result<Value> {
        let timeout_ms = timeout_ms.clamp(0, 600_000);
        let response = self.connection.call_timeout(
            command(
                "WaitJob",
                map([
                    ("id", Value::from(id)),
                    ("timeout", Value::from(timeout_ms)),
                ]),
            ),
            Duration::from_millis(timeout_ms as u64) + Duration::from_secs(5),
        )?;
        if get(&response, "completed").and_then(Value::as_bool) == Some(true) {
            return Ok(get(&response, "result").cloned().unwrap_or(Value::Nil));
        }
        match self.get_state(id) {
            Ok(state) if state == "failed" => {
                return Err(Error::Command(format!("job {id} failed")));
            }
            Ok(_) => {}
            Err(error) => return Err(error),
        }
        Err(Error::Timeout(format!(
            "wait_for_job timed out after {timeout_ms}ms"
        )))
    }

    pub fn get_job_counts(&self) -> Result<BTreeMap<String, i64>> {
        let response = self.call(command(
            "GetJobCounts",
            map([("queue", Value::from(self.name.clone()))]),
        ))?;
        let mut counts = BTreeMap::new();
        if let Some(entries) = get(&response, "counts").and_then(Value::as_map) {
            for (key, value) in entries {
                if let (Some(key), Some(value)) = (key.as_str(), value.as_i64()) {
                    counts.insert(key.to_owned(), value);
                }
            }
        }
        Ok(counts)
    }

    pub fn count(&self) -> Result<i64> {
        let response = self.call(command(
            "Count",
            map([("queue", Value::from(self.name.clone()))]),
        ))?;
        Ok(get(&response, "count").and_then(Value::as_i64).unwrap_or(0))
    }

    pub fn get_job_logs(&self, id: &str) -> Result<Vec<String>> {
        let response = self.call(command("GetLogs", map([("id", Value::from(id))])))?;
        Ok(get(&response, "data")
            .and_then(Value::as_map)
            .and_then(|data| {
                data.iter()
                    .find(|(key, _)| key.as_str() == Some("logs"))
                    .map(|(_, logs)| crate::wire::strings(logs))
            })
            .unwrap_or_default())
    }
}

fn job_from(response: &Map, key: &str, queue: &Queue) -> Result<Option<Job>> {
    match get(response, key) {
        None | Some(Value::Nil) => Ok(None),
        Some(raw) => Ok(Some(Job::new(
            as_map(raw.clone())?,
            queue.connection.clone(),
            None,
        ))),
    }
}
