use rmpv::Value;

use crate::wire::{command, get, map};
use crate::{Queue, Result};

impl Queue {
    pub fn pause(&self) -> Result<()> {
        self.unit("Pause", map([("queue", Value::from(self.name.clone()))]))
    }

    pub fn resume(&self) -> Result<()> {
        self.unit("Resume", map([("queue", Value::from(self.name.clone()))]))
    }

    pub fn is_paused(&self) -> Result<bool> {
        let response = self.call(command(
            "IsPaused",
            map([("queue", Value::from(self.name.clone()))]),
        ))?;
        Ok(get(&response, "paused").and_then(Value::as_bool) == Some(true))
    }

    pub fn drain(&self) -> Result<i64> {
        let response = self.call(command(
            "Drain",
            map([("queue", Value::from(self.name.clone()))]),
        ))?;
        Ok(get(&response, "count").and_then(Value::as_i64).unwrap_or(0))
    }

    pub fn clean(&self, grace_ms: i64, limit: i64, state: &str) -> Result<Vec<String>> {
        let response = self.call(command(
            "Clean",
            map([
                ("queue", Value::from(self.name.clone())),
                ("grace", Value::from(grace_ms)),
                ("limit", Value::from(limit)),
                ("state", Value::from(state)),
            ]),
        ))?;
        Ok(get(&response, "ids")
            .map(crate::wire::strings)
            .unwrap_or_default())
    }

    pub fn obliterate(&self) -> Result<()> {
        self.unit(
            "Obliterate",
            map([("queue", Value::from(self.name.clone()))]),
        )
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        self.unit("Cancel", map([("id", Value::from(id))]))
    }

    pub fn promote(&self, id: &str) -> Result<()> {
        self.unit("Promote", map([("id", Value::from(id))]))
    }

    pub fn retry_job(&self, id: &str) -> Result<()> {
        self.unit("MoveToWait", map([("id", Value::from(id))]))
    }

    pub fn change_priority(&self, id: &str, priority: i64) -> Result<()> {
        self.unit(
            "ChangePriority",
            map([("id", Value::from(id)), ("priority", Value::from(priority))]),
        )
    }

    pub fn change_delay(&self, id: &str, delay_ms: i64) -> Result<()> {
        self.unit(
            "ChangeDelay",
            map([("id", Value::from(id)), ("delay", Value::from(delay_ms))]),
        )
    }

    pub fn update_job_data(&self, id: &str, data: Value) -> Result<()> {
        self.unit("Update", map([("id", Value::from(id)), ("data", data)]))
    }

    pub(crate) fn unit(&self, name: &str, fields: crate::wire::Map) -> Result<()> {
        self.call(command(name, fields))?;
        Ok(())
    }
}
