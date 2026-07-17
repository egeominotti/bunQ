mod support;

use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use bunqueue_client::{
    Connection, ConnectionOptions, Error, TelemetryCallback, TelemetryEvent, TlsOptions, Value,
    Worker, WorkerOptions,
};
use support::Server;

#[test]
fn auth_timeout_and_lazy_reconnect_are_observable() {
    let server = Server::start_with_env(&[("AUTH_TOKENS", "rust-secret")]);
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let telemetry: TelemetryCallback = Arc::new(move |event| {
        captured.lock().expect("telemetry lock").push(event);
    });
    let connection = Connection::new(ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        token: Some("rust-secret".into()),
        telemetry: Some(telemetry),
        ..Default::default()
    });

    assert!(connection.ping().expect("initial authenticated ping"));
    let timed_out = connection.call_timeout(
        vec![
            (Value::from("cmd"), Value::from("PULLB")),
            (Value::from("queue"), Value::from("rust-timeout-empty")),
            (Value::from("count"), Value::from(1)),
            (Value::from("timeout"), Value::from(500)),
            (Value::from("owner"), Value::from("rust-timeout-test")),
        ],
        Duration::from_millis(25),
    );
    assert!(matches!(timed_out, Err(Error::Timeout(_))));
    assert!(!connection.is_connected());
    assert!(connection.ping().expect("ping after lazy reconnect"));

    let events = events.lock().expect("telemetry lock");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, TelemetryEvent::Authenticated { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, TelemetryEvent::Connected { .. }))
            .count(),
        2
    );
    assert!(events.iter().any(|event| matches!(
        event,
        TelemetryEvent::Reconnecting {
            previous_generation: 0
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TelemetryEvent::Timeout { operation, .. } if operation == "command:PULLB"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TelemetryEvent::CommandStarted { command, .. } if command == "Ping"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TelemetryEvent::CommandFinished {
            command,
            ok: true,
            ..
        } if command == "Ping"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TelemetryEvent::Error { operation, .. } if operation == "command:PULLB"
    )));
    drop(events);
    connection.close();
}

#[test]
fn authentication_failure_is_typed_and_emits_an_error() {
    let server = Server::start_with_env(&[("AUTH_TOKENS", "right-token")]);
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let telemetry: TelemetryCallback = Arc::new(move |event| {
        captured.lock().expect("telemetry lock").push(event);
    });
    let connection = Connection::new(ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        token: Some("wrong-token".into()),
        telemetry: Some(telemetry),
        ..Default::default()
    });

    assert!(matches!(connection.ping(), Err(Error::Auth(_))));
    assert!(events.lock().expect("telemetry lock").iter().any(|event| {
        matches!(
            event,
            TelemetryEvent::Error { operation, .. } if operation == "auth"
        )
    }));
}

#[test]
fn custom_ca_verifies_tls_and_a_different_ca_is_rejected() {
    let trusted = Server::start_tls();
    let other = Server::start_tls();
    let connection = Connection::new(ConnectionOptions {
        host: "127.0.0.1".into(),
        port: trusted.port,
        tls: Some(TlsOptions {
            ca_file: trusted.ca_file(),
        }),
        ..Default::default()
    });
    assert!(connection.ping().expect("verified TLS ping"));
    connection.close();

    let untrusted = Connection::new(ConnectionOptions {
        host: "127.0.0.1".into(),
        port: trusted.port,
        tls: Some(TlsOptions {
            ca_file: other.ca_file(),
        }),
        ..Default::default()
    });
    assert!(matches!(untrusted.ping(), Err(Error::Connection(_))));
}

#[test]
fn worker_registration_is_visible_to_the_broker() {
    let server = Server::start();
    let options = ConnectionOptions {
        host: "127.0.0.1".into(),
        port: server.port,
        ..Default::default()
    };
    let worker = Worker::new(
        "rust-registration",
        |_| Ok(Value::Nil),
        WorkerOptions {
            connection: options.clone(),
            poll_timeout_ms: 0,
            ..Default::default()
        },
    );
    assert_eq!(worker.run_once().expect("register and poll"), 0);

    let observer = Connection::new(options);
    let response = observer
        .call(vec![(Value::from("cmd"), Value::from("ListWorkers"))])
        .expect("list workers");
    let workers = response
        .iter()
        .find(|(key, _)| key.as_str() == Some("data"))
        .and_then(|(_, value)| value.as_map())
        .and_then(|data| {
            data.iter()
                .find(|(key, _)| key.as_str() == Some("workers"))
                .and_then(|(_, value)| value.as_array())
        })
        .expect("workers payload");
    assert_eq!(workers.len(), 1);
    worker.close();
    observer.close();
}

#[test]
fn worker_retry_errors_are_sent_to_telemetry() {
    let server = Server::start_with_env(&[("AUTH_TOKENS", "right-token")]);
    let (send, receive) = mpsc::channel();
    let telemetry: TelemetryCallback = Arc::new(move |event| {
        if matches!(event, TelemetryEvent::WorkerRetry { .. }) {
            let _ = send.send(event);
        }
    });
    let worker = Worker::new(
        "rust-worker-errors",
        |_| Ok(Value::Nil),
        WorkerOptions {
            connection: ConnectionOptions {
                host: "127.0.0.1".into(),
                port: server.port,
                token: Some("wrong-token".into()),
                telemetry: Some(telemetry),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let runner = worker.clone();
    let handle = thread::spawn(move || runner.run());

    assert!(matches!(
        receive.recv_timeout(Duration::from_secs(3)),
        Ok(TelemetryEvent::WorkerRetry { queue, .. }) if queue == "rust-worker-errors"
    ));
    worker.stop();
    assert!(handle.join().expect("worker thread").is_ok());
}
