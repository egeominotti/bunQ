use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmpv::Value;

use crate::telemetry::{TelemetryCallback, TelemetryEvent, emit_all};
use crate::transport::Socket;
use crate::wire::{command, get, map};
use crate::{PROTOCOL_VERSION, Result};

#[derive(Clone, Debug, Default)]
pub struct TlsOptions {
    /// PEM bundle added to the system trust store.
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone)]
pub struct ConnectionOptions {
    pub host: String,
    pub port: u16,
    pub token: Option<String>,
    pub tls: Option<TlsOptions>,
    pub connect_timeout: Duration,
    pub command_timeout: Duration,
    pub telemetry: Option<TelemetryCallback>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 6789,
            token: None,
            tls: None,
            connect_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(30),
            telemetry: None,
        }
    }
}

impl fmt::Debug for ConnectionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("telemetry", &self.telemetry.as_ref().map(|_| "<callback>"))
            .finish()
    }
}

pub(crate) struct Inner {
    pub(crate) socket: Option<Socket>,
    pub(crate) generation: i64,
    pub(crate) request_counter: u32,
    pub(crate) closed: bool,
}

/// A cloneable, mutex-serialized TCP connection with lazy reconnect.
#[derive(Clone)]
pub struct Connection {
    pub(crate) options: ConnectionOptions,
    pub(crate) inner: Arc<Mutex<Inner>>,
}

impl Connection {
    pub fn new(options: ConnectionOptions) -> Self {
        Self {
            options,
            inner: Arc::new(Mutex::new(Inner {
                socket: None,
                generation: -1,
                request_counter: 0,
                closed: false,
            })),
        }
    }

    pub fn options(&self) -> &ConnectionOptions {
        &self.options
    }

    pub fn generation(&self) -> i64 {
        self.inner
            .lock()
            .expect("connection mutex poisoned")
            .generation
    }

    pub fn is_connected(&self) -> bool {
        self.inner
            .lock()
            .expect("connection mutex poisoned")
            .socket
            .is_some()
    }

    pub fn ensure_connected(&self) -> Result<()> {
        let mut events = Vec::new();
        let result = {
            let mut inner = self.inner.lock().expect("connection mutex poisoned");
            self.connect_locked(&mut inner, &mut events)
        };
        emit_all(self.options.telemetry.as_ref(), events);
        result
    }

    /// Sends one protocol command. Calls are safe from multiple threads.
    pub fn call(&self, command: crate::Message) -> Result<crate::Message> {
        self.call_timeout(command, self.options.command_timeout)
    }

    pub fn call_timeout(
        &self,
        command_map: crate::Message,
        timeout: Duration,
    ) -> Result<crate::Message> {
        let mut events = Vec::new();
        let result = {
            let mut inner = self.inner.lock().expect("connection mutex poisoned");
            self.call_locked(&mut inner, command_map, timeout, &mut events)
        };
        emit_all(self.options.telemetry.as_ref(), events);
        result
    }

    pub fn hello(&self) -> Result<crate::Message> {
        self.call(command(
            "Hello",
            map([
                ("protocolVersion", Value::from(PROTOCOL_VERSION)),
                (
                    "capabilities",
                    Value::Array(vec![Value::from("separate-job-name")]),
                ),
            ]),
        ))
    }

    pub fn protocol_version(&self) -> Result<i64> {
        let response = self.hello()?;
        Ok(get(&response, "protocolVersion")
            .and_then(Value::as_i64)
            .unwrap_or(0))
    }

    pub fn capabilities(&self) -> Result<Vec<String>> {
        let response = self.hello()?;
        Ok(get(&response, "capabilities")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default())
    }

    pub fn ping(&self) -> Result<bool> {
        let response = self.call(command("Ping", Vec::new()))?;
        Ok(get(&response, "data")
            .and_then(Value::as_map)
            .and_then(|data| {
                data.iter()
                    .find(|(key, _)| key.as_str() == Some("pong"))
                    .and_then(|(_, value)| value.as_bool())
            })
            == Some(true))
    }

    /// Permanently closes this connection and rejects later calls.
    pub fn close(&self) {
        {
            let mut inner = self.inner.lock().expect("connection mutex poisoned");
            inner.closed = true;
            self.teardown_locked(&mut inner);
        }
        emit_all(self.options.telemetry.as_ref(), [TelemetryEvent::Closed]);
    }
}
