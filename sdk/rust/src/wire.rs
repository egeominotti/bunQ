use std::collections::BTreeMap;

use rmpv::Value;

use crate::{Error, Result};

pub const PROTOCOL_VERSION: i64 = 2;
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
pub type Message = Vec<(Value, Value)>;
pub(crate) type Map = Message;

pub(crate) fn map(entries: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Map {
    entries
        .into_iter()
        .filter(|(_, value)| !value.is_nil())
        .map(|(key, value)| (Value::from(key.into()), value))
        .collect()
}

pub(crate) fn command(name: &str, entries: Map) -> Map {
    let mut out = vec![(Value::from("cmd"), Value::from(name))];
    out.extend(entries);
    out
}

pub(crate) fn get<'a>(map: &'a Map, key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(candidate, _)| candidate.as_str() == Some(key))
        .map(|(_, value)| value)
}

pub(crate) fn as_map(value: Value) -> Result<Map> {
    match value {
        Value::Map(map) => Ok(map),
        _ => Err(Error::Serialization("expected a MessagePack map".into())),
    }
}

pub(crate) fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| items.iter().filter_map(id_string).collect())
        .unwrap_or_default()
}

pub(crate) fn id_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .or_else(|| value.as_u64().map(|number| number.to_string()))
        .or_else(|| value.as_f64().map(|number| format!("{number:.0}")))
}

/// Validates protocol-safe shapes and converts integers outside int32 to f64.
pub(crate) fn prepare_outgoing(value: Value) -> Result<Value> {
    match value {
        Value::Integer(integer) => {
            if let Some(number) = integer.as_i64() {
                if !((i32::MIN as i64)..=(i32::MAX as i64)).contains(&number) {
                    return Ok(Value::F64(number as f64));
                }
            } else if let Some(number) = integer.as_u64() {
                if number > i32::MAX as u64 {
                    return Ok(Value::F64(number as f64));
                }
            }
            Ok(Value::Integer(integer))
        }
        Value::Array(items) => Ok(Value::Array(
            items
                .into_iter()
                .map(prepare_outgoing)
                .collect::<Result<_>>()?,
        )),
        Value::Map(entries) => {
            let mut prepared = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                if key.as_str().is_none() {
                    return Err(Error::Serialization(
                        "MessagePack map keys must be valid UTF-8 strings".into(),
                    ));
                }
                prepared.push((key, prepare_outgoing(value)?));
            }
            Ok(Value::Map(prepared))
        }
        Value::Ext(_, _) => Err(Error::Serialization(
            "outgoing MessagePack extensions are not allowed".into(),
        )),
        other => Ok(other),
    }
}

/// Converts msgpackr's ext-0 `undefined` extension to nil recursively.
pub(crate) fn normalize(value: Value) -> Value {
    match value {
        Value::Ext(0, _) => Value::Nil,
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (normalize(key), normalize(value)))
                .collect(),
        ),
        other => other,
    }
}

/// Converts JSON values used by the conformance driver into MessagePack values.
pub fn json_to_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Nil,
        serde_json::Value::Bool(value) => Value::Boolean(value),
        serde_json::Value::Number(number) => number
            .as_i64()
            .map(Value::from)
            .or_else(|| number.as_u64().map(Value::from))
            .or_else(|| number.as_f64().map(Value::from))
            .unwrap_or(Value::Nil),
        serde_json::Value::String(value) => Value::from(value),
        serde_json::Value::Array(items) => {
            Value::Array(items.into_iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (Value::from(key), json_to_value(value)))
                .collect(),
        ),
    }
}

/// Converts MessagePack values to JSON, treating ext-0 as null.
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Nil | Value::Ext(0, _) => serde_json::Value::Null,
        Value::Boolean(value) => serde_json::Value::Bool(*value),
        Value::Integer(value) => value
            .as_i64()
            .map(serde_json::Value::from)
            .or_else(|| value.as_u64().map(serde_json::Value::from))
            .unwrap_or(serde_json::Value::Null),
        Value::F32(value) => serde_json::json!(value),
        Value::F64(value) => serde_json::json!(value),
        Value::String(value) => serde_json::Value::String(value.to_string()),
        Value::Binary(value) => serde_json::Value::Array(
            value
                .iter()
                .map(|byte| serde_json::Value::from(*byte))
                .collect(),
        ),
        Value::Array(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let mut out = BTreeMap::new();
            for (key, value) in entries {
                if let Some(key) = key.as_str() {
                    out.insert(key.to_owned(), value_to_json(value));
                }
            }
            serde_json::to_value(out).unwrap_or(serde_json::Value::Null)
        }
        Value::Ext(_, _) => serde_json::Value::Null,
    }
}
