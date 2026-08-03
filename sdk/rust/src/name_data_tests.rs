use rmpv::Value;

use crate::queue::job_payload;
use crate::wire::{get, map};
use crate::{Connection, ConnectionOptions, Job, value_to_json};

#[test]
fn named_payload_keeps_user_data_and_primitives_separate() {
    let data = Value::Map(map([
        ("name", Value::from("customer-visible")),
        ("value", Value::from(1)),
    ]));
    let fields = job_payload("modern-op", data.clone());
    let fields = fields.as_map().expect("named payload fields");

    assert_eq!(
        get(fields, "name").and_then(Value::as_str),
        Some("modern-op")
    );
    assert_eq!(get(fields, "data"), Some(&data));

    let scalar = job_payload("scalar-op", Value::from(false));
    let scalar = scalar.as_map().expect("scalar named payload fields");
    assert_eq!(
        get(scalar, "name").and_then(Value::as_str),
        Some("scalar-op")
    );
    assert_eq!(get(scalar, "data").and_then(Value::as_bool), Some(false));
}

#[test]
fn job_prefers_top_level_name_and_only_unwraps_legacy_data() {
    let connection = Connection::new(ConnectionOptions::default());
    let modern_data = Value::Map(map([
        ("name", Value::from("user-name")),
        ("value", Value::from(1)),
    ]));
    let modern = Job::new(
        map([
            ("name", Value::from("modern-op")),
            ("data", modern_data.clone()),
        ]),
        connection.clone(),
        None,
    );
    let legacy = Job::new(
        map([(
            "data",
            Value::Map(map([
                ("name", Value::from("legacy-op")),
                ("value", Value::from(2)),
            ])),
        )]),
        connection.clone(),
        None,
    );
    let scalar = Job::new(
        map([
            ("name", Value::from("scalar-op")),
            ("data", Value::from(42)),
        ]),
        connection,
        None,
    );

    assert_eq!(modern.name(), "modern-op");
    assert_eq!(modern.data(), modern_data);
    assert_eq!(legacy.name(), "legacy-op");
    assert_eq!(legacy.data(), Value::Map(map([("value", Value::from(2))])));
    assert_eq!(scalar.name(), "scalar-op");
    assert_eq!(scalar.data(), Value::from(42));
}

#[test]
fn json_conversion_preserves_utf8_strings_without_display_quotes() {
    let data = Value::Map(map([
        ("name", Value::from("customer-visible")),
        ("to", Value::from("a@b.c")),
    ]));

    assert_eq!(
        value_to_json(&data),
        serde_json::json!({"name": "customer-visible", "to": "a@b.c"})
    );
}
