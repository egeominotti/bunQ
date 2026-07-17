use rmpv::Value;

use crate::wire::{normalize, prepare_outgoing};

#[test]
fn outgoing_maps_reject_non_string_keys() {
    let value = Value::Map(vec![(Value::from(7), Value::from("invalid"))]);

    assert!(prepare_outgoing(value).is_err());
}

#[test]
fn outgoing_values_reject_extensions_recursively() {
    let value = Value::Map(vec![(
        Value::from("nested"),
        Value::Array(vec![Value::Ext(0, vec![0])]),
    )]);

    assert!(prepare_outgoing(value).is_err());
}

#[test]
fn outgoing_int64_values_become_float64_recursively() {
    let value = Value::Array(vec![
        Value::from(42),
        Value::Map(vec![(
            Value::from("timestamp"),
            Value::from(9_999_999_999_999_i64),
        )]),
    ]);
    let prepared = prepare_outgoing(value).unwrap();
    let items = prepared.as_array().unwrap();
    let nested = items[1].as_map().unwrap();

    assert_eq!(items[0].as_i64(), Some(42));
    assert_eq!(nested[0].1.as_f64(), Some(9_999_999_999_999_f64));
}

#[test]
fn incoming_ext_zero_becomes_nil_at_any_depth() {
    let value = Value::Map(vec![(
        Value::from("items"),
        Value::Array(vec![Value::Ext(0, vec![0])]),
    )]);
    let normalized = normalize(value);
    let items = normalized.as_map().unwrap()[0].1.as_array().unwrap();

    assert!(items[0].is_nil());
}
