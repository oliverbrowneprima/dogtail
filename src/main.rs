use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, ValueEnum};
use reqwest::{RequestBuilder, Response};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs::File, sync::mpsc};
use tracing::{debug, info, warn, Instrument};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};
use tracing_tree::{time::UtcDateTime, HierarchicalLayer};

const LOG_RETURN_LIMIT: u32 = 1000;
const BE_A_LITTLE_EVIL: bool = false;

#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    File,
    Stdout,
}

impl Mode {
    // Only async to force an async caller, since we tokio::spawn
    fn get_writer(&self, writer_id: String, format: LogFormat) -> mpsc::Sender<WriterMessage> {
        let (tx, rx) = mpsc::channel(100);
        match self {
            Mode::File => tokio::spawn(file_writer(writer_id, format, rx)),
            Mode::Stdout => tokio::spawn(stdout_writer(format, rx)),
        };
        tx
    }
}

/// Tail datadog logs to files, or stdout
#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// A query string, the same as you would use in the UI, e.g. "service:my-service"
    query_string: String,
    /// The domain to use for the API
    #[arg(short = 'd', long, default_value = "api.datadoghq.eu")]
    domain: String,
    /// Mode - If file, log events will be partitioned by split_key and written to files, if stdout, logs will be written to stdout
    #[arg(short = 'm', long, default_value = "file")]
    mode: Mode,
    /// If mode is file, this is the event attribute lookup key to use for partitioning logs. Uses json-pointer syntax, e.g. "attributes.tags.pod_name".
    /// Note that event tags are unpacked into a map, so you can use tags "attributes.tags.pod_name" for this purpose. If an event doesn't have the
    /// split key, it is written to output.log
    #[arg(short = 'k', long)]
    split_key: Option<String>,
    /// A file to load a formatting config from. The formatting config if a newline separated list of json-pointer keys - each output line will be
    /// the found value of each of those keys, joined by a space. If none is provided, a default logging format of "timestamp status message" will be used.
    #[arg(long)]
    format_file: Option<PathBuf>,
    /// If true, structured json will be written to the output instead of formatted logs, with one event written per line.
    #[arg(short = 's', long)]
    structured: bool,
}

// Tokio main function
#[tokio::main]
async fn main() {
    Registry::default()
        .with(EnvFilter::from_default_env())
        .with(
            HierarchicalLayer::new(2)
                .with_targets(true)
                .with_bracketed_fields(true)
                .with_indent_lines(true)
                .with_timer(UtcDateTime),
        )
        .init();
    let args = Args::parse();

    let api_key = std::env::var("DD_API_KEY").expect("Expected DD_API_KEY env var");
    let app_key = std::env::var("DD_APP_KEY").expect("Expected DD_APP_KEY env var");

    let format = if args.structured {
        LogFormat::Structured
    } else {
        get_format_config(args.format_file).await.unwrap()
    };

    let mut pool = WriterPool::new(args.split_key.map(String::into), format, args.mode);

    let mut tailer = Tailer::new(args.domain, api_key, app_key, args.query_string);
    loop {
        for event in tailer.get_next().await.unwrap() {
            pool.consume(event).await.unwrap();
        }
    }
}

// Kinda json-pointer, but not really
#[derive(Clone)]
struct JsonKey(Vec<String>);

impl JsonKey {
    fn get(&self, event: &Value) -> Option<Value> {
        let mut current = event;
        for key in &self.0 {
            current = current.get(key)?;
        }
        Some(current.clone())
    }
}

impl From<String> for JsonKey {
    fn from(s: String) -> Self {
        JsonKey(s.split('.').map(|s| s.to_string()).collect())
    }
}

impl From<&str> for JsonKey {
    fn from(s: &str) -> Self {
        JsonKey(s.split('.').map(|s| s.to_string()).collect())
    }
}

#[derive(Clone)]
enum LogFormat {
    Text { sep: String, keys: Vec<JsonKey> },
    Structured,
}

impl LogFormat {
    fn text(sep: String, keys: Vec<JsonKey>) -> Self {
        LogFormat::Text { sep, keys }
    }

    fn format(&self, event: &Value) -> String {
        match self {
            LogFormat::Text { sep, keys } => Self::format_text(sep, keys, event),
            LogFormat::Structured => Self::format_raw(event),
        }
    }

    fn format_raw(event: &Value) -> String {
        serde_json::to_string(event).unwrap()
    }

    fn format_text(sep: &str, keys: &[JsonKey], event: &Value) -> String {
        let mut output = String::with_capacity(256);
        // Do this to avoid printing sep before the first key
        let (first, rest) = keys.split_first().unwrap();

        if let Some(value) = first.get(event) {
            output.push_str(value.as_str().unwrap_or(format!("{:?}", value).as_str()));
        } else {
            output.push_str("KEY_NOT_FOUND");
        }

        for key in rest {
            output.push_str(&sep);
            if let Some(value) = key.get(event) {
                output.push_str(value.as_str().unwrap_or(format!("{:?}", value).as_str()));
            } else {
                output.push_str("KEY_NOT_FOUND");
            }
        }
        return output;
    }
}

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::text(
            " | ".to_string(),
            vec![
                JsonKey::from("attributes.timestamp"),
                JsonKey::from("attributes.status"),
                JsonKey::from("attributes.message"),
            ],
        )
    }
}

struct WriterPool {
    writers: HashMap<String, mpsc::Sender<WriterMessage>>,
    split_key: Option<JsonKey>,
    format: LogFormat,
    mode: Mode,
}

impl WriterPool {
    fn new(split_key: Option<JsonKey>, format: LogFormat, mode: Mode) -> Self {
        WriterPool {
            writers: HashMap::new(),
            split_key,
            format,
            mode,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, event))]
    async fn consume(&mut self, event: Value) -> Result<(), anyhow::Error> {
        let event = unpack_tags(event);
        let writer_id = if let Some(split_key) = &self.split_key {
            split_key
                .get(&event)
                .unwrap_or(json!("output"))
                .as_str()
                .ok_or(anyhow::anyhow!("Writer id not a string"))?
                .to_string()
        } else {
            "output".to_string()
        };

        let writer_id = if let Mode::Stdout = self.mode {
            "stdout".to_string()
        } else {
            writer_id
        };

        let writer = self
            .writers
            .entry(writer_id.clone())
            .or_insert_with(|| self.mode.get_writer(writer_id, self.format.clone()));

        writer.send(WriterMessage::NewLog(event)).await?;

        Ok(())
    }
}

enum WriterMessage {
    NewLog(Value),
}

// I love that async functions mean I don't even need a struct here - the implied future holds all my state
// We can be liberal with unwraps here because if this task panics the recv is dropped, propagating the error
// to the parent task
async fn file_writer(
    writer_id: String,
    format: LogFormat,
    mut recv: mpsc::Receiver<WriterMessage>,
) {
    info!("Started writing to file: {}.log", writer_id);
    let mut file = File::options()
        .append(true)
        .create(true)
        .open(format!("{}.log", writer_id))
        .await
        .unwrap();

    while let Some(msg) = recv.recv().await {
        match msg {
            WriterMessage::NewLog(event) => {
                let mut buf: Vec<u8> = Vec::new();
                writeln!(buf, "{}", format.format(&event)).unwrap();
                let span = tracing::trace_span!("write_to_file", writer_id = writer_id.as_str());
                file.write_all(&buf).instrument(span).await.unwrap();
                file.flush().await.unwrap();
            }
        }
    }
}

async fn stdout_writer(format: LogFormat, mut recv: mpsc::Receiver<WriterMessage>) {
    info!("Started writing to stdout");
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = recv.recv().await {
        let mut buf: Vec<u8> = Vec::new();
        match msg {
            WriterMessage::NewLog(event) => {
                writeln!(buf, "{}", format.format(&event)).unwrap();
                stdout.write_all(&buf).await.unwrap();
                stdout.flush().await.unwrap();
            }
        }
    }
}

struct Tailer {
    search_url: String,
    query_string: String,
    client: reqwest::Client,
    api_key: String,
    app_key: String,
    seen_event_ids: HashSet<String>, // We don't want to return the same event twice
    total_returned: usize,
}

impl Tailer {
    fn new(dd_domain: String, api_key: String, app_key: String, query_string: String) -> Self {
        Tailer {
            search_url: format!("https://{}/api/v2/logs/events/search", dd_domain),
            client: reqwest::Client::new(),
            api_key,
            app_key,
            query_string,
            seen_event_ids: HashSet::new(),
            total_returned: 0,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_next(&mut self) -> Result<Vec<Value>, anyhow::Error> {
        let returned = self.get_events().await?;

        let returned_count = returned.len();
        debug!("Got {} events", returned_count);

        let mut unseen = Vec::with_capacity(returned.len());
        for event in returned {
            let event_id = event["id"].as_str().unwrap().to_string();
            if self.seen_event_ids.insert(event_id) {
                unseen.push(event);
            }
        }

        if unseen.len() == returned_count {
            // We filtered no events based on the existing seen event ids, so we can clear them
            // and only retain the ones returned this time
            debug!("Clearing seen event ids");
            self.seen_event_ids.clear();
            for event in unseen.iter() {
                let event_id = event["id"].as_str().unwrap().to_string();
                self.seen_event_ids.insert(event_id);
            }
        }

        self.total_returned += unseen.len();
        info!(
            "Found {} events to write, total written: {}",
            unseen.len(),
            self.total_returned
        );
        Ok(unseen)
    }

    fn headers(&self, builder: RequestBuilder) -> RequestBuilder {
        builder
            .header("Accept", "application/json")
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.app_key)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_events(&mut self) -> Result<Vec<Value>, anyhow::Error> {
        // TODO - the width of this sliding window should shrink if we are regularly following
        // next links, and grow if we aren't
        let from = "now - 60s";

        let query = json!({
            "filter": {
                "from": from,
                "query": self.query_string
            },
            "page": {
                "limit": LOG_RETURN_LIMIT
            },
            "sort": "timestamp"
        });

        debug!("Running query: {:?}", query);

        let first = self
            .headers(self.client.post(&self.search_url).json(&query))
            .send()
            .await?;

        RateLimitStatus::from(&first).pause().await;

        // We pause before checking status to ensure that if we get a 429,
        // we don't make another request until the rate limit is reset
        if !first.status().is_success() {
            return self.handle_error(first).await;
        }

        let mut body: Value = first.json().await?;
        let mut returned = get_events_from_response_body(&body)?;

        while let Some(next_url) = body.get("links").map(|l| l.get("next")).flatten() {
            let next_url = next_url
                .as_str()
                .ok_or(anyhow::anyhow!("Next url not a string"))?;
            debug!("Next url: {}", next_url);

            let response = self.headers(self.client.get(next_url)).send().await?;
            RateLimitStatus::from(&response).pause().await;

            if !response.status().is_success() {
                self.handle_error(response).await?;
                continue; // If we hit a 429 while following next links, we should just re-request that page
            }

            body = response.json().await?;
            returned.extend(get_events_from_response_body(&body)?);
        }

        Ok(returned)
    }

    async fn handle_error(&mut self, response: Response) -> Result<Vec<Value>, anyhow::Error> {
        match response.status() {
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                warn!("Got too_many_requests, waiting and retrying");
                Ok(vec![]) // We have the correct interval period, we can just wait and then retry
            }
            _ => Err(anyhow::anyhow!("Error: {}", response.text().await.unwrap())),
        }
    }
}

fn unpack_tags(mut event: Value) -> Value {
    if let Some(tags) = event["attributes"]["tags"].as_array() {
        let mut unpacked = HashMap::new();
        for tag in tags {
            if let Some(str) = tag.as_str() {
                let mut split = str.split(':');
                let key = split.next().unwrap();
                let value = split.next().unwrap();
                unpacked.insert(key, value);
            }
        }
        event["attributes"]["tags"] = json!(unpacked);
    }
    event
}

fn get_events_from_response_body(body: &Value) -> Result<Vec<Value>, anyhow::Error> {
    let Some(events) = body.get("data") else {
        return Ok(vec![]);
    };

    Ok(events
        .as_array()
        .ok_or(anyhow::anyhow!("Log query data not a list"))?
        .into_iter()
        .cloned()
        .collect())
}

#[derive(Debug)]
struct RateLimitStatus {
    limit: u32,
    period: Duration,
    remaining: u32,
    time_until_reset: Duration,
}

impl From<&Response> for RateLimitStatus {
    fn from(response: &Response) -> Self {
        let get = |key: &str| response.headers().get(key).unwrap().to_str().unwrap();

        let limit = get("x-ratelimit-limit").parse().unwrap();
        let period = Duration::from_secs(get("x-ratelimit-period").parse().unwrap());
        let remaining = get("x-ratelimit-remaining").parse().unwrap();
        let time_until_reset = Duration::from_secs(get("x-ratelimit-reset").parse().unwrap());

        let status = RateLimitStatus {
            limit,
            period,
            remaining,
            time_until_reset,
        };
        debug!("Rate limit status: {:?}", status);
        status
    }
}

// In order to be a good citizen, we always wait until reset + [0.0..5.0) seconds before requesting again
// TODO - this is an antipattern - it should be impossible to make another request until the rate limit is reset
impl RateLimitStatus {
    async fn pause(&self) {
        let wait = self.time_until_reset;
        let jitter = Duration::from_secs_f32(rand::random::<f32>() * 5.0);
        let wait = wait + jitter;
        if BE_A_LITTLE_EVIL && self.remaining > 0 {
            return; // If we have api budget left, use it, even if this means other users are likely to hit a 429
        }
        if wait > Duration::from_secs(0) {
            debug!("Waiting {}s", wait.as_secs());
            tokio::time::sleep(wait).await;
        }
    }
}

async fn get_format_config(path: Option<PathBuf>) -> Result<LogFormat, anyhow::Error> {
    let Some(path) = path else {
        return Ok(LogFormat::default());
    };

    let mut file = File::open(path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;
    let keys = buf
        .lines()
        .map(|line| JsonKey::from(line.to_string()))
        .collect();
    Ok(LogFormat::text(" | ".to_string(), keys))
}
