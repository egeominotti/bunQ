use rmpv::Value;

use crate::wire::get;
use crate::{Backoff, Deduplication, JobOptions};

#[test]
fn single_and_bulk_custom_ids_use_their_distinct_wire_names() {
    let options = JobOptions {
        attempts: Some(5),
        job_id: Some("custom".into()),
        ..Default::default()
    };
    let single = options.to_wire(false);
    let bulk = options.to_wire(true);

    assert_eq!(get(&single, "maxAttempts").and_then(Value::as_i64), Some(5));
    assert_eq!(
        get(&single, "jobId").and_then(Value::as_str),
        Some("custom")
    );
    assert!(get(&single, "customId").is_none());
    assert_eq!(
        get(&bulk, "customId").and_then(Value::as_str),
        Some("custom")
    );
    assert!(get(&bulk, "jobId").is_none());
}

#[test]
fn deduplication_and_backoff_keep_every_advertised_field() {
    let options = JobOptions {
        backoff: Some(Backoff::Strategy {
            kind: "exponential".into(),
            delay: 250,
            max_delay: Some(5_000),
        }),
        deduplication: Some(Deduplication {
            id: "dedup-key".into(),
            ttl: Some(10_000),
            extend: Some(true),
            replace: Some(false),
        }),
        ..Default::default()
    };
    let wire = options.to_wire(false);
    let backoff = get(&wire, "backoff").and_then(Value::as_map).unwrap();
    let dedup = get(&wire, "dedup").and_then(Value::as_map).unwrap();

    assert_eq!(
        get(&wire, "uniqueKey").and_then(Value::as_str),
        Some("dedup-key")
    );
    assert_eq!(
        get(backoff, "type").and_then(Value::as_str),
        Some("exponential")
    );
    assert_eq!(
        get(backoff, "maxDelay").and_then(Value::as_i64),
        Some(5_000)
    );
    assert_eq!(get(dedup, "ttl").and_then(Value::as_i64), Some(10_000));
    assert_eq!(get(dedup, "extend").and_then(Value::as_bool), Some(true));
    assert_eq!(get(dedup, "replace").and_then(Value::as_bool), Some(false));
}

#[test]
fn cron_options_only_emit_the_supported_subset() {
    let options = JobOptions {
        priority: Some(9),
        attempts: Some(3),
        timeout: Some(500),
        remove_on_complete: Some(true),
        ..Default::default()
    };
    let cron = options.cron_options();
    let cron = cron.as_map().unwrap();

    assert_eq!(get(cron, "maxAttempts").and_then(Value::as_i64), Some(3));
    assert_eq!(get(cron, "timeout").and_then(Value::as_i64), Some(500));
    assert_eq!(
        get(cron, "removeOnComplete").and_then(Value::as_bool),
        Some(true)
    );
    assert!(get(cron, "priority").is_none());
}
