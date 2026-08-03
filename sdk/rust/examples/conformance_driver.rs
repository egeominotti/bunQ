use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bunqueue_client::{
    BulkEntry, Connection, ConnectionOptions, Job, JobOptions, ProcessError, Queue,
    SchedulerRepeat, SchedulerTemplate, Value, Worker, WorkerOptions, json_to_value, value_to_json,
};
use serde_json::{Map, Value as Json, json};

struct Driver {
    options: ConnectionOptions,
    queues: HashMap<String, Queue>,
}

impl Driver {
    fn new() -> Self {
        Self {
            options: ConnectionOptions::default(),
            queues: HashMap::new(),
        }
    }

    fn queue(&mut self, name: &str) -> Queue {
        self.queues
            .entry(name.to_owned())
            .or_insert_with(|| Queue::new(name, self.options.clone()))
            .clone()
    }

    fn handle(&mut self, request: &Json) -> bunqueue_client::Result<Json> {
        let op = text(request, "op");
        match op {
            "connect" => {
                self.options.host = text(request, "host").to_owned();
                self.options.port = number(request, "port", 6789) as u16;
                self.options.token = request["token"].as_str().map(str::to_owned);
                Ok(json!({}))
            }
            "add" => {
                let job = self.queue(text(request, "queue")).add(
                    text(request, "name"),
                    json_to_value(request["data"].clone()),
                    options(&request["opts"]),
                )?;
                Ok(json!({"jobId": job.id()}))
            }
            "addBulk" => {
                let entries = request["entries"]
                    .as_array()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|entry| BulkEntry {
                        name: text(entry, "name").to_owned(),
                        data: json_to_value(entry["data"].clone()),
                        options: options(&entry["opts"]),
                    })
                    .collect();
                let ids = self.queue(text(request, "queue")).add_bulk(entries)?;
                Ok(json!({"ids": ids}))
            }
            "getJob" => Ok(
                json!({"job": job_view(self.queue("conf-lookup").get_job(text(request, "jobId"))?)}),
            ),
            "getJobByCustomId" => {
                let job = self
                    .queue(text(request, "queue"))
                    .get_job_by_custom_id(text(request, "customId"))?;
                Ok(json!({"job": job.map(|job| json!({"id": job.id()}))}))
            }
            "getState" => {
                Ok(json!({"state": self.queue("conf-lookup").get_state(text(request, "jobId"))?}))
            }
            "getResult" => Ok(
                json!({"result": value_to_json(&self.queue("conf-lookup").get_result(text(request, "jobId"))?)}),
            ),
            "count" => Ok(json!({"count": self.queue(text(request, "queue")).count()?})),
            "isPaused" => Ok(json!({"paused": self.queue(text(request, "queue")).is_paused()?})),
            "pause" => {
                self.queue(text(request, "queue")).pause()?;
                Ok(json!({}))
            }
            "resume" => {
                self.queue(text(request, "queue")).resume()?;
                Ok(json!({}))
            }
            "drain" => Ok(json!({"count": self.queue(text(request, "queue")).drain()?})),
            "promote" => {
                self.queue("conf-lookup").promote(text(request, "jobId"))?;
                Ok(json!({}))
            }
            "upsertScheduler" => {
                let repeat = &request["repeat"];
                let template = &request["template"];
                self.queue(text(request, "queue")).upsert_job_scheduler(
                    text(request, "schedulerId"),
                    SchedulerRepeat {
                        pattern: repeat["pattern"].as_str().map(str::to_owned),
                        every_ms: repeat["every"].as_i64(),
                        limit: repeat["limit"].as_i64(),
                        timezone: repeat["tz"].as_str().map(str::to_owned),
                        ..Default::default()
                    },
                    SchedulerTemplate {
                        name: template["name"].as_str().map(str::to_owned),
                        data: json_to_value(template["data"].clone()),
                        options: options(&template["opts"]),
                    },
                )?;
                Ok(json!({}))
            }
            "getScheduler" => {
                let scheduler = self
                    .queue("conf-lookup")
                    .get_job_scheduler(text(request, "schedulerId"))?;
                Ok(json!({"scheduler": scheduler.map(|_| json!({}))}))
            }
            "removeScheduler" => {
                self.queue("conf-lookup")
                    .remove_job_scheduler(text(request, "schedulerId"))?;
                Ok(json!({}))
            }
            "waitForJob" => Ok(
                json!({"result": value_to_json(&self.queue("conf-lookup").wait_for_job(
                text(request, "jobId"), number(request, "timeoutMs", 30_000)
            )?)}),
            ),
            "getDlqCount" => {
                Ok(json!({"count": self.queue(text(request, "queue")).get_dlq(None)?.len()}))
            }
            "retryDlq" => {
                Ok(json!({"count": self.queue(text(request, "queue")).retry_dlq(None, None)?}))
            }
            "hello" => {
                let connection = Connection::new(self.options.clone());
                Ok(json!({
                    "protocolVersion": connection.protocol_version()?,
                    "capabilities": connection.capabilities()?,
                }))
            }
            "process" => {
                self.process_until(request)?;
                Ok(json!({}))
            }
            "close" => {
                for queue in self.queues.values() {
                    queue.close();
                }
                Ok(json!({}))
            }
            _ => Err(bunqueue_client::Error::Command(format!("unknown op: {op}"))),
        }
    }

    fn process_until(&mut self, request: &Json) -> bunqueue_client::Result<()> {
        let queue_name = text(request, "queue").to_owned();
        let behavior = text(request, "behavior").to_owned();
        let result = json_to_value(request["result"].clone());
        let failed_once = Arc::new(Mutex::new(HashSet::new()));
        let seen = failed_once.clone();
        let processor = move |job: Job| match behavior.as_str() {
            "unrecoverable" => Err(ProcessError::unrecoverable("conformance poison")),
            "deepThrow" => deep_error(25),
            "failOnce" => {
                let mut seen = seen.lock().expect("seen mutex poisoned");
                if seen.insert(job.id()) {
                    Err(ProcessError::retryable("conformance transient"))
                } else {
                    Ok(result.clone())
                }
            }
            _ => Ok(result.clone()),
        };
        let worker = Worker::new(
            queue_name.clone(),
            processor,
            WorkerOptions {
                connection: self.options.clone(),
                batch_size: number(request, "batchSize", 10) as usize,
                poll_timeout_ms: 300,
                ..Default::default()
            },
        );
        let runner = worker.clone();
        let handle = thread::spawn(move || runner.run());
        let deadline =
            Instant::now() + Duration::from_millis(number(request, "timeoutMs", 20_000) as u64);
        let until = &request["until"];
        while Instant::now() < deadline {
            let counts = self.queue(&queue_name).get_job_counts().unwrap_or_default();
            let completed = count(&counts, "completed") >= until["completed"].as_i64().unwrap_or(0);
            let failed = count(&counts, "failed") >= until["failed"].as_i64().unwrap_or(0);
            let dlq = self
                .queue(&queue_name)
                .get_dlq(None)
                .map(|v| v.len())
                .unwrap_or(0)
                >= until["dlq"].as_u64().unwrap_or(0) as usize;
            if completed && failed && dlq {
                worker.stop();
                let _ = handle.join();
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        worker.stop();
        let _ = handle.join();
        Err(bunqueue_client::Error::Timeout(
            "until condition not reached".into(),
        ))
    }
}

fn options(value: &Json) -> JobOptions {
    JobOptions {
        delay: value["delay"].as_i64(),
        attempts: value["attempts"].as_i64(),
        backoff: value["backoff"]
            .as_i64()
            .map(bunqueue_client::Backoff::Milliseconds),
        job_id: value["jobId"].as_str().map(str::to_owned),
        ..Default::default()
    }
}

fn job_view(job: Option<Job>) -> Json {
    job.map(|job| {
        json!({
            "id": job.id(),
            "name": job.name(),
            "data": value_to_json(&job.data()),
            "stacktrace": job.stacktrace()
        })
    })
    .unwrap_or(Json::Null)
}

fn deep_error(depth: usize) -> Result<Value, ProcessError> {
    if depth == 0 {
        Err(ProcessError::retryable("BOOM-CONFORMANCE"))
    } else {
        deep_error(depth - 1)
    }
}

fn text<'a>(value: &'a Json, key: &str) -> &'a str {
    value[key].as_str().unwrap_or_default()
}

fn number(value: &Json, key: &str, default: i64) -> i64 {
    value[key].as_i64().unwrap_or(default)
}

fn count(counts: &BTreeMap<String, i64>, key: &str) -> i64 {
    counts.get(key).copied().unwrap_or(0)
}

fn main() {
    let mut driver = Driver::new();
    for line in io::stdin().lock().lines().map_while(Result::ok) {
        let request: Json = serde_json::from_str(&line).unwrap_or(Json::Null);
        let id = request["id"].clone();
        let answer = match driver.handle(&request) {
            Ok(Json::Object(fields)) => {
                let mut out = Map::from_iter([("id".into(), id), ("ok".into(), Json::Bool(true))]);
                out.extend(fields);
                Json::Object(out)
            }
            Ok(_) => json!({"id": id, "ok": true}),
            Err(error) => json!({"id": id, "ok": false, "error": error.to_string()}),
        };
        println!("{answer}");
        io::stdout().flush().ok();
        if text(&request, "op") == "close" {
            break;
        }
    }
}
