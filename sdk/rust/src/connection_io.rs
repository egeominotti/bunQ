use std::io::{Read, Write};
use std::time::{Duration, Instant};

use rmpv::{Value, decode::read_value, encode::write_value};

use crate::connection::{Connection, Inner};
use crate::telemetry::{TelemetryEvent, record_failure};
use crate::transport::Socket;
use crate::wire::{MAX_FRAME_SIZE, Map, as_map, command, get, map, normalize, prepare_outgoing};
use crate::{Error, Result};

impl Connection {
    pub(crate) fn call_locked(
        &self,
        inner: &mut Inner,
        mut command_map: Map,
        timeout: Duration,
        events: &mut Vec<TelemetryEvent>,
    ) -> Result<Map> {
        self.connect_locked(inner, events)?;
        inner.request_counter = inner.request_counter.wrapping_add(1) & 0x7fff_ffff;
        let request_id = format!("rust-{}", inner.request_counter);
        let name = get(&command_map, "cmd")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_owned();
        command_map.push((Value::from("reqId"), Value::from(request_id.clone())));
        events.push(TelemetryEvent::CommandStarted {
            command: name.clone(),
            request_id: request_id.clone(),
        });
        let started = Instant::now();
        let response = match self.round_trip_locked(inner, command_map, timeout) {
            Ok(response) => response,
            Err(error) => {
                finish(events, &name, &request_id, started.elapsed(), false);
                record_failure(events, format!("command:{name}"), started.elapsed(), &error);
                return Err(error);
            }
        };
        if get(&response, "reqId").and_then(Value::as_str) != Some(request_id.as_str())
            && get(&response, "reqId").is_some()
        {
            self.teardown_locked(inner);
            let error = Error::Connection("response reqId did not match request".into());
            finish(events, &name, &request_id, started.elapsed(), false);
            record_failure(events, format!("command:{name}"), started.elapsed(), &error);
            return Err(error);
        }
        if get(&response, "ok").and_then(Value::as_bool) != Some(true) {
            let error = Error::Command(
                get(&response, "error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown server error")
                    .to_owned(),
            );
            finish(events, &name, &request_id, started.elapsed(), false);
            record_failure(events, format!("command:{name}"), started.elapsed(), &error);
            return Err(error);
        }
        finish(events, &name, &request_id, started.elapsed(), true);
        Ok(response)
    }

    pub(crate) fn connect_locked(
        &self,
        inner: &mut Inner,
        events: &mut Vec<TelemetryEvent>,
    ) -> Result<()> {
        if inner.socket.is_some() {
            return Ok(());
        }
        if inner.closed {
            return Err(Error::Closed);
        }
        if inner.generation >= 0 {
            events.push(TelemetryEvent::Reconnecting {
                previous_generation: inner.generation,
            });
        }
        events.push(TelemetryEvent::Connecting {
            host: self.options.host.clone(),
            port: self.options.port,
        });
        inner.socket = match Socket::open(&self.options) {
            Ok(socket) => Some(socket),
            Err(error) => {
                record_failure(events, "connect", Duration::ZERO, &error);
                return Err(error);
            }
        };
        inner.generation += 1;
        events.push(TelemetryEvent::Connected {
            generation: inner.generation,
        });
        if let Some(token) = &self.options.token {
            self.authenticate_locked(inner, token, events)?;
        }
        Ok(())
    }

    fn authenticate_locked(
        &self,
        inner: &mut Inner,
        token: &str,
        events: &mut Vec<TelemetryEvent>,
    ) -> Result<()> {
        let request_id = "rust-auth";
        events.push(TelemetryEvent::CommandStarted {
            command: "Auth".into(),
            request_id: request_id.into(),
        });
        let started = Instant::now();
        let auth = command(
            "Auth",
            map([
                ("token", Value::from(token)),
                ("reqId", Value::from(request_id)),
            ]),
        );
        let response = match self.round_trip_locked(inner, auth, self.options.command_timeout) {
            Ok(response) => response,
            Err(error) => {
                self.teardown_locked(inner);
                finish(events, "Auth", request_id, started.elapsed(), false);
                record_failure(events, "auth", started.elapsed(), &error);
                return Err(error);
            }
        };
        if get(&response, "ok").and_then(Value::as_bool) != Some(true) {
            let message = get(&response, "error")
                .and_then(Value::as_str)
                .unwrap_or("authentication failed")
                .to_owned();
            let error = Error::Auth(message);
            self.teardown_locked(inner);
            finish(events, "Auth", request_id, started.elapsed(), false);
            record_failure(events, "auth", started.elapsed(), &error);
            return Err(error);
        }
        finish(events, "Auth", request_id, started.elapsed(), true);
        events.push(TelemetryEvent::Authenticated {
            generation: inner.generation,
        });
        Ok(())
    }

    fn round_trip_locked(
        &self,
        inner: &mut Inner,
        command_map: Map,
        timeout: Duration,
    ) -> Result<Map> {
        let mut payload = Vec::new();
        write_value(&mut payload, &prepare_outgoing(Value::Map(command_map))?)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        ensure_frame_size(payload.len())?;
        let socket = inner
            .socket
            .as_mut()
            .ok_or_else(|| Error::Connection("not connected".into()))?;
        if let Err(error) = socket.tcp().set_read_timeout(Some(timeout)) {
            self.teardown_locked(inner);
            return Err(classify_io(error));
        }
        if let Err(error) = socket.tcp().set_write_timeout(Some(timeout)) {
            self.teardown_locked(inner);
            return Err(classify_io(error));
        }
        let length = (payload.len() as u32).to_be_bytes();
        if let Err(error) = socket
            .write_all(&length)
            .and_then(|_| socket.write_all(&payload))
        {
            self.teardown_locked(inner);
            return Err(classify_io(error));
        }
        let mut header = [0_u8; 4];
        if let Err(error) = socket.read_exact(&mut header) {
            self.teardown_locked(inner);
            return Err(classify_io(error));
        }
        let length = u32::from_be_bytes(header) as usize;
        if let Err(error) = ensure_frame_size(length) {
            self.teardown_locked(inner);
            return Err(error);
        }
        let mut body = vec![0_u8; length];
        if let Err(error) = socket.read_exact(&mut body) {
            self.teardown_locked(inner);
            return Err(classify_io(error));
        }
        let decoded = read_value(&mut body.as_slice()).map_err(|error| {
            self.teardown_locked(inner);
            Error::Serialization(error.to_string())
        })?;
        as_map(normalize(decoded))
    }

    pub(crate) fn teardown_locked(&self, inner: &mut Inner) {
        inner.socket.take();
    }
}

pub(crate) fn ensure_frame_size(length: usize) -> Result<()> {
    if length > MAX_FRAME_SIZE {
        Err(Error::Serialization(
            "frame exceeds the 64 MiB protocol limit".into(),
        ))
    } else {
        Ok(())
    }
}

fn finish(
    events: &mut Vec<TelemetryEvent>,
    command: &str,
    request_id: &str,
    duration: Duration,
    ok: bool,
) {
    events.push(TelemetryEvent::CommandFinished {
        command: command.into(),
        request_id: request_id.into(),
        duration,
        ok,
    });
}

fn classify_io(error: std::io::Error) -> Error {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        Error::Timeout("socket torn down; the next call will reconnect".into())
    } else {
        Error::Connection(error.to_string())
    }
}
