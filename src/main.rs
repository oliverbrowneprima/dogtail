use std::{collections::HashMap, io::Write, path::PathBuf};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use logs::LogQuery;
use reqwest::{Client, RequestBuilder};
use serde_json::{json, Value};
use tailer::Tailer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio::{fs::File, sync::mpsc};
use tracing::{info, trace, Instrument};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};
use tracing_tree::{time::UtcDateTime, HierarchicalLayer};

mod logs;
mod tailer;

#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    File,
    Stdout,
}
/// Tail datadog logs to files, or stdout
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// The domain to use for the API
    #[arg(short = 'd', long, default_value = "api.datadoghq.eu")]
    domain: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Logs(LogsCommand),
}

#[derive(Args)]
struct LogsCommand {
    /// A query string, the same as you would use in the UI, e.g. "service:my-service"
    query_string: String,
    /// The domain to use for the API
    #[arg(short = 'd', long, default_value = "api.datadoghq.eu")]
    domain: String,
    /// Mode - If file, log events will be partitioned by split_key and written to files, if stdout, logs will be written to stdout
    #[arg(short = 'o', long, default_value = "file")]
    output_mode: Mode,
    /// If mode is file, this is the event attribute lookup key to use for partitioning logs. Uses json-pointer syntax, e.g. "attributes.tags.pod_name".
    /// Note that event tags are unpacked into a map, so you can use tags "attributes.tags.pod_name" for this purpose. If an event doesn't have the
    /// split key, it is written to the default file.
    #[arg(short = 'k', long)]
    split_key: Option<String>,
    /// The place logs that can't be split by split-key will be written to. If mode is stdout, this is ignored.
    #[arg(short = 'f', long, default_value = "output.log")]
    default_output: String,
    /// A file to load a formatting config from. The formatting config is a newline separated list of json-pointer keys - each output line will be
    /// the found value of each of those keys, joined by a space. If none is provided, a default logging format of "timestamp status message" will be used.
    #[arg(long)]
    format_file: Option<PathBuf>,
    /// If true, structured json will be written to the output instead of formatted logs, with one event written per line.
    #[arg(short = 's', long)]
    structured: bool,

    /// Provide a number of seconds in the past to start tailing from.
    #[arg(short = 'h', long, default_value = "60")]
    history: u64,

    /// Run the search once, rather than tailing the logs. If this is set, `history` becomes the number of seconds after this instant to get logs
    /// from. Accepts rfc3339 timestamps, e.g. "2021-01-01T00:00:00Z"
    #[arg(short = 't', long, value_parser = parse_date_time)]
    from: Option<DateTime<Utc>>,
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
    let args = Cli::parse();

    let api_key = std::env::var("DD_API_KEY").expect("Expected DD_API_KEY env var");
    let app_key = std::env::var("DD_APP_KEY").expect("Expected DD_APP_KEY env var");

    match args.command {
        Command::Logs(logs) => run_logs(logs, api_key, app_key).await.unwrap(),
    }
}

async fn run_logs(
    logs: LogsCommand,
    api_key: String,
    app_key: String,
) -> Result<(), anyhow::Error> {
    let format = if logs.structured {
        LogFormat::Structured
    } else {
        get_format_config(logs.format_file).await?
    };

    let mut pool = WriterPool::new(
        logs.split_key.map(String::into),
        format,
        logs.output_mode,
        logs.default_output,
    );

    let tailer = Tailer::new(
        api_key,
        app_key,
        Box::new(LogQuery::new(
            logs.domain,
            logs.query_string,
            logs.history,
            logs.from,
        )),
    );

    let mut tail = tailer.start(logs.from.is_some()).await;

    while let Some(event) = tail.recv().await {
        trace!("Received event");
        pool.consume(event).await?;
    }

    pool.finish().await;

    Ok(())
}

pub trait Query: Send + Sync {
    fn get_query(&mut self, client: &Client) -> RequestBuilder;
    fn get_results(&mut self, body: Value) -> Result<Vec<Value>, anyhow::Error>;
    fn get_next<'a>(&mut self, body: &'a Value) -> Result<Option<String>, anyhow::Error> {
        let Some(next) = body.get("links").map(|l| l.get("next")).flatten() else {
            return Ok(None);
        };
        next.as_str()
            .ok_or(anyhow::anyhow!("Next url not a string"))
            .map(|s| Some(s.to_string()))
    }

    fn get_batch_size(&mut self) -> usize;
}

struct WriterPool {
    writers: HashMap<String, (JoinHandle<()>, mpsc::Sender<WriterMessage>)>,
    split_key: Option<JsonKey>,
    format: LogFormat,
    mode: Mode,
    default: String,
}

// Kinda json-pointer, but not really
#[derive(Clone)]
struct JsonKey(Vec<String>);

#[derive(Clone)]
enum LogFormat {
    Text { sep: String, keys: Vec<JsonKey> },
    Structured,
}

enum WriterMessage {
    NewLog(Value),
}

impl Mode {
    // Only async to force an async caller, since we tokio::spawn
    fn get_writer(
        &self,
        writer_id: String,
        format: LogFormat,
    ) -> (JoinHandle<()>, mpsc::Sender<WriterMessage>) {
        let (tx, rx) = mpsc::channel(100);
        let handle = match self {
            Mode::File => tokio::spawn(file_writer(writer_id, format, rx)),
            Mode::Stdout => tokio::spawn(stdout_writer(format, rx)),
        };
        (handle, tx)
    }
}

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

impl WriterPool {
    fn new(split_key: Option<JsonKey>, format: LogFormat, mode: Mode, default: String) -> Self {
        WriterPool {
            writers: HashMap::new(),
            split_key,
            format,
            mode,
            default,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, event))]
    async fn consume(&mut self, event: Value) -> Result<(), anyhow::Error> {
        let writer_id = if let Some(split_key) = &self.split_key {
            split_key
                .get(&event)
                .unwrap_or(json!(&self.default))
                .as_str()
                .ok_or(anyhow::anyhow!("Writer id not a string"))?
                .to_string()
        } else {
            self.default.clone()
        };

        let writer_id = if let Mode::Stdout = self.mode {
            "stdout".to_string()
        } else {
            writer_id
        };

        let (_, writer) = self
            .writers
            .entry(writer_id.clone())
            .or_insert_with(|| self.mode.get_writer(writer_id, self.format.clone()));

        writer.send(WriterMessage::NewLog(event)).await?;

        Ok(())
    }

    async fn finish(mut self) {
        for (_, (handle, tx)) in self.writers.drain() {
            drop(tx);
            let _ = handle.await;
        }
    }
}

// I love that async functions mean I don't even need a struct here - the implied future holds all my state
// We can be liberal with unwraps here because if this task panics the recv is dropped, propagating the error
// to the parent task
async fn file_writer(
    writer_id: String,
    format: LogFormat,
    mut recv: mpsc::Receiver<WriterMessage>,
) {
    info!("Started writing to file: {}", writer_id);
    let mut file = File::options()
        .append(true)
        .create(true)
        .open(format!("{}", writer_id))
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
    info!("Finished writing to file: {}", writer_id);
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
    info!("Finished writing to stdout");
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

fn parse_date_time(s: &str) -> Result<DateTime<Utc>, anyhow::Error> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}
