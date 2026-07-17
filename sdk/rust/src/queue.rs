use rmpv::Value;

use crate::wire::{Map, command, get, map};
use crate::{BulkEntry, Connection, ConnectionOptions, Job, JobOptions, Result};

#[derive(Clone)]
pub struct Queue {
    pub name: String,
    pub(crate) connection: Connection,
    owns_connection: bool,
}

impl Queue {
    pub fn new(name: impl Into<String>, options: ConnectionOptions) -> Self {
        Self {
            name: name.into(),
            connection: Connection::new(options),
            owns_connection: true,
        }
    }

    pub fn with_connection(name: impl Into<String>, connection: Connection) -> Self {
        Self {
            name: name.into(),
            connection,
            owns_connection: false,
        }
    }

    pub fn add(&self, name: impl Into<String>, data: Value, options: JobOptions) -> Result<Job> {
        let name = name.into();
        let payload = job_payload(&name, data);
        let mut fields = map([
            ("queue", Value::from(self.name.clone())),
            ("data", payload.clone()),
        ]);
        fields.extend(options.to_wire(false));
        let response = self.call(command("PUSH", fields))?;
        let raw = map([
            ("id", get(&response, "id").cloned().unwrap_or(Value::Nil)),
            ("queue", Value::from(self.name.clone())),
            ("data", payload),
        ]);
        Ok(Job::new(raw, self.connection.clone(), None))
    }

    pub fn add_bulk(&self, entries: Vec<BulkEntry>) -> Result<Vec<String>> {
        let jobs = entries
            .into_iter()
            .map(|entry| {
                let mut fields = map([("data", job_payload(&entry.name, entry.data))]);
                fields.extend(entry.options.to_wire(true));
                Value::Map(fields)
            })
            .collect();
        let response = self.call(command(
            "PUSHB",
            map([
                ("queue", Value::from(self.name.clone())),
                ("jobs", Value::Array(jobs)),
            ]),
        ))?;
        Ok(get(&response, "ids")
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(crate::wire::id_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default())
    }

    pub fn close(&self) {
        if self.owns_connection {
            self.connection.close();
        }
    }

    pub(crate) fn call(&self, command: Map) -> Result<Map> {
        self.connection.call(command)
    }
}

pub(crate) fn job_payload(name: &str, data: Value) -> Value {
    match data {
        Value::Map(mut entries) => {
            let mut out = vec![(Value::from("name"), Value::from(name))];
            out.append(&mut entries);
            Value::Map(out)
        }
        Value::Nil => Value::Map(map([("name", Value::from(name))])),
        other => Value::Map(map([("name", Value::from(name)), ("payload", other)])),
    }
}
