use crate::{Error, ProcessError};

#[test]
fn not_found_detection_is_case_insensitive_and_specific() {
    assert!(Error::Command("Job NOT FOUND".into()).is_not_found());
    assert!(!Error::Command("database unavailable".into()).is_not_found());
    assert!(!Error::Connection("not found".into()).is_not_found());
}

#[test]
fn process_errors_keep_the_message_first_and_honor_the_limit() {
    let error = ProcessError::unrecoverable("poison payload");
    let stack = error.stack(1);

    assert!(error.is_unrecoverable());
    assert_eq!(error.message(), "poison payload");
    assert_eq!(stack, vec!["ProcessError: poison payload"]);
}
