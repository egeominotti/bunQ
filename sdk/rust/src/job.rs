use rmpv::Value;

use crate::wire::{Map, command, get, id_string, map, strings};
use crate::{Connection, Result};

#[derive(Clone)]
pub struct Job {
    raw: Map,
    token: Option<String>,
    connection: Connection,
}

impl Job {
    pub(crate) fn new(raw: Map, connection: Connection, token: Option<String>) -> Self {
        Self {
            raw,
            token,
            connection,
        }
    }

    pub fn id(&self) -> String {
        get(&self.raw, "id").and_then(id_string).unwrap_or_default()
    }

    pub fn name(&self) -> String {
        get(&self.raw, "data")
            .and_then(Value::as_map)
            .and_then(|data| {
                data.iter()
                    .find(|(key, _)| key.as_str() == Some("name"))
                    .and_then(|(_, value)| value.as_str())
            })
            .unwrap_or_default()
            .to_owned()
    }

    /// User payload with the reserved wire `name` field removed.
    pub fn data(&self) -> Value {
        match get(&self.raw, "data") {
            Some(Value::Map(entries)) => Value::Map(
                entries
                    .iter()
                    .filter(|(key, _)| key.as_str() != Some("name"))
                    .cloned()
                    .collect(),
            ),
            Some(value) => value.clone(),
            None => Value::Nil,
        }
    }

    pub fn raw(&self) -> &Map {
        &self.raw
    }

    pub fn state(&self) -> Option<&str> {
        get(&self.raw, "state").and_then(Value::as_str)
    }

    pub fn attempts_made(&self) -> i64 {
        get(&self.raw, "attempts")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    }

    pub fn stacktrace(&self) -> Vec<String> {
        get(&self.raw, "stacktrace")
            .map(strings)
            .unwrap_or_default()
    }

    pub fn update_progress(&self, progress: f64, message: Option<&str>) -> Result<()> {
        self.connection.call(command(
            "Progress",
            map([
                ("id", Value::from(self.id())),
                ("progress", Value::from(progress)),
                ("message", message.map(Value::from).unwrap_or(Value::Nil)),
            ]),
        ))?;
        Ok(())
    }

    pub fn log(&self, message: &str, level: Option<&str>) -> Result<()> {
        self.connection.call(command(
            "AddLog",
            map([
                ("id", Value::from(self.id())),
                ("message", Value::from(message)),
                ("level", level.map(Value::from).unwrap_or(Value::Nil)),
            ]),
        ))?;
        Ok(())
    }

    pub fn extend_lock(&self, duration_ms: i64) -> Result<()> {
        self.connection.call(command(
            "ExtendLock",
            map([
                ("id", Value::from(self.id())),
                (
                    "token",
                    self.token.clone().map(Value::from).unwrap_or(Value::Nil),
                ),
                ("duration", Value::from(duration_ms)),
            ]),
        ))?;
        Ok(())
    }
}
