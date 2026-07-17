use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmpv::Value;

use crate::telemetry::{TelemetryEvent, emit};
use crate::wire::{as_map, command, get, map};
use crate::worker_limits::pull_count;
pub(crate) use crate::worker_runtime::join_all;
use crate::worker_runtime::now_ms;
use crate::{Connection, ConnectionOptions, Job, ProcessError, Result};

pub type Processor =
    Arc<dyn Fn(Job) -> std::result::Result<Value, ProcessError> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct WorkerOptions {
    pub connection: ConnectionOptions,
    pub concurrency: usize,
    pub batch_size: usize,
    pub poll_timeout_ms: i64,
    pub lock_ttl_ms: i64,
    pub heartbeat_interval: Option<Duration>,
    pub name: Option<String>,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            connection: ConnectionOptions::default(),
            concurrency: 4,
            batch_size: 10,
            poll_timeout_ms: 5_000,
            lock_ttl_ms: 30_000,
            heartbeat_interval: Some(Duration::from_secs(10)),
            name: None,
        }
    }
}

#[derive(Clone)]
pub struct Worker {
    queue: String,
    id: String,
    connection: Connection,
    processor: Processor,
    options: WorkerOptions,
    stopped: Arc<AtomicBool>,
    registered_generation: Arc<Mutex<i64>>,
}

impl Worker {
    pub fn new(
        queue: impl Into<String>,
        processor: impl Fn(Job) -> std::result::Result<Value, ProcessError> + Send + Sync + 'static,
        mut options: WorkerOptions,
    ) -> Self {
        options.concurrency = options.concurrency.max(1);
        options.batch_size = options.batch_size.clamp(1, 1_000);
        options.poll_timeout_ms = options.poll_timeout_ms.clamp(0, 30_000);
        let id = format!(
            "rust-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Self {
            queue: queue.into(),
            id,
            connection: Connection::new(options.connection.clone()),
            processor: Arc::new(processor),
            options,
            stopped: Arc::new(AtomicBool::new(false)),
            registered_generation: Arc::new(Mutex::new(-1)),
        }
    }

    pub fn batch_size(&self) -> usize {
        self.options.batch_size
    }

    pub fn run(&self) -> Result<()> {
        self.stopped.store(false, Ordering::Release);
        let mut backoff = Duration::from_millis(100);
        while !self.stopped.load(Ordering::Acquire) {
            match self.run_once() {
                Ok(_) => backoff = Duration::from_millis(100),
                Err(error) => {
                    if self.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    emit(
                        self.options.connection.telemetry.as_ref(),
                        TelemetryEvent::Error {
                            operation: format!("worker:{}", self.queue),
                            message: error.to_string(),
                        },
                    );
                    if matches!(error, crate::Error::Closed) {
                        return Err(error);
                    }
                    emit(
                        self.options.connection.telemetry.as_ref(),
                        TelemetryEvent::WorkerRetry {
                            queue: self.queue.clone(),
                            message: error.to_string(),
                            retry_in: backoff,
                        },
                    );
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
        self.unregister();
        Ok(())
    }

    pub fn run_once(&self) -> Result<usize> {
        self.ensure_registered()?;
        let response = self.connection.call_timeout(
            command(
                "PULLB",
                map([
                    ("queue", Value::from(self.queue.clone())),
                    (
                        "count",
                        Value::from(
                            pull_count(self.options.batch_size, self.options.concurrency) as i64,
                        ),
                    ),
                    ("timeout", Value::from(self.options.poll_timeout_ms)),
                    ("owner", Value::from(self.id.clone())),
                    ("lockTtl", Value::from(self.options.lock_ttl_ms)),
                ]),
            ),
            Duration::from_millis(self.options.poll_timeout_ms as u64) + Duration::from_secs(5),
        )?;
        let jobs = get(&response, "jobs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tokens = get(&response, "tokens")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut handles = Vec::new();
        let mut processed = 0;
        for (index, raw) in jobs.into_iter().enumerate() {
            processed += 1;
            let raw = as_map(raw)?;
            let token = tokens
                .get(index)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let worker = self.clone();
            handles.push(thread::spawn(move || worker.process(raw, token)));
            if handles.len() == self.options.concurrency {
                join_all(std::mem::take(&mut handles))?;
            }
        }
        join_all(handles)?;
        Ok(processed)
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    pub fn close(&self) {
        self.stop();
        self.unregister();
        self.connection.close();
    }

    fn process(&self, raw: crate::wire::Map, token: String) -> Result<()> {
        let job = Job::new(raw.clone(), self.connection.clone(), Some(token.clone()));
        let id = job.id();
        let heartbeat = self.start_job_heartbeat(id.clone(), token.clone());
        let outcome = match (self.processor)(job) {
            Ok(result) => self.connection.call(command(
                "ACK",
                map([
                    ("id", Value::from(id)),
                    ("token", Value::from(token)),
                    ("result", result),
                ]),
            )),
            Err(error) => {
                let limit = get(&raw, "stackTraceLimit")
                    .and_then(Value::as_i64)
                    .unwrap_or(10)
                    .max(1) as usize;
                self.connection.call(command(
                    "FAIL",
                    map([
                        ("id", Value::from(id)),
                        ("token", Value::from(token)),
                        ("error", Value::from(error.message())),
                        (
                            "stack",
                            Value::Array(error.stack(limit).into_iter().map(Value::from).collect()),
                        ),
                        ("unrecoverable", Value::from(error.is_unrecoverable())),
                    ]),
                ))
            }
        };
        if let Some((stop, handle)) = heartbeat {
            let _ = stop.send(());
            let _ = handle.join();
        }
        outcome.map(|_| ())
    }

    fn start_job_heartbeat(
        &self,
        id: String,
        token: String,
    ) -> Option<(mpsc::Sender<()>, thread::JoinHandle<()>)> {
        let interval = self.options.heartbeat_interval?;
        if interval.is_zero() {
            return None;
        }
        let connection = Connection::new(self.options.connection.clone());
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            loop {
                match receive.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let _ = connection.call(command(
                            "JobHeartbeatB",
                            map([
                                ("ids", Value::Array(vec![Value::from(id.clone())])),
                                ("tokens", Value::Array(vec![Value::from(token.clone())])),
                            ]),
                        ));
                    }
                }
            }
            connection.close();
        });
        Some((send, handle))
    }

    fn ensure_registered(&self) -> Result<()> {
        self.connection.ensure_connected()?;
        let generation = self.connection.generation();
        if *self
            .registered_generation
            .lock()
            .expect("worker mutex poisoned")
            == generation
        {
            return Ok(());
        }
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".into());
        self.connection.call(command(
            "RegisterWorker",
            map([
                ("workerId", Value::from(self.id.clone())),
                (
                    "name",
                    Value::from(self.options.name.clone().unwrap_or_else(|| self.id.clone())),
                ),
                (
                    "queues",
                    Value::Array(vec![Value::from(self.queue.clone())]),
                ),
                ("concurrency", Value::from(self.options.concurrency as i64)),
                ("hostname", Value::from(hostname)),
                ("pid", Value::from(std::process::id())),
                ("startedAt", Value::from(now_ms())),
            ]),
        ))?;
        *self
            .registered_generation
            .lock()
            .expect("worker mutex poisoned") = generation;
        Ok(())
    }

    fn unregister(&self) {
        let _ = self.connection.call(command(
            "UnregisterWorker",
            map([("workerId", Value::from(self.id.clone()))]),
        ));
    }
}
