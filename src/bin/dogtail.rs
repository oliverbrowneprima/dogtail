use std::{io::Write, path::PathBuf};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use dogtail::logs::{Follow, LogFormat, LogSource, Snapshot};
use dogtail::sink::{ConsumerPool, Sink, SinkMessage, SinkSet};
use dogtail::tailer::Tailer;
use dogtail::{JsonKey, Source};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs::File, sync::mpsc};
use tracing::{info, trace, Instrument};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};
use tracing_tree::{time::UtcDateTime, HierarchicalLayer};

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

    let sink_set = OutputMode::new(
        logs.output_mode,
        logs.split_key,
        format,
        logs.default_output,
    );
    let mut pool = ConsumerPool::new(Box::new(sink_set));

    let source = if let Some(from) = logs.from {
        let mode = Snapshot::new(from, logs.history);
        let source = LogSource::new(logs.domain, logs.query_string, mode);
        Box::new(source) as Box<dyn Source>
    } else {
        let mode = Follow::new(logs.history);
        let source = LogSource::new(logs.domain, logs.query_string, mode);
        Box::new(source) as Box<dyn Source>
    };

    let tailer = Tailer::new(api_key, app_key, source);

    let mut tail = tailer.start().await;

    while let Some(event) = tail.recv().await {
        trace!("Received event");
        pool.consume(event).await?;
    }

    pool.finish(5).await;

    Ok(())
}

struct OutputMode {
    mode: Mode,
    split_key: Option<JsonKey>,
    format: LogFormat,
    default: String,
}

impl OutputMode {
    fn new(mode: Mode, split_key: Option<String>, format: LogFormat, default: String) -> Self {
        let split_key = split_key.map(|s| JsonKey::from(s));
        OutputMode {
            mode,
            split_key,
            format,
            default,
        }
    }
}

impl SinkSet for OutputMode {
    fn construct_output(
        &self,
        event: &Value,
        runtime: &tokio::runtime::Handle,
    ) -> dogtail::sink::Sink {
        let id = self.get_sink_id(event);
        let (tx, rx) = mpsc::channel(100);
        let handle = match self.mode {
            Mode::File => runtime.spawn(file_writer(id.clone(), self.format.clone(), rx)),
            Mode::Stdout => runtime.spawn(stdout_writer(self.format.clone(), rx)),
        };
        Sink::new(id, handle, tx)
    }

    fn get_sink_id(&self, event: &Value) -> String {
        let Some(key) = &self.split_key else {
            return self.default.clone();
        };
        let id = key
            .get(event)
            .map(|v| v.as_str().map(|s| s.to_string()))
            .flatten()
            .unwrap_or(self.default.clone());

        id
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

fn parse_date_time(s: &str) -> Result<DateTime<Utc>, anyhow::Error> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}

// I love that async functions mean I don't even need a struct here - the implied future holds all my state
// We can be liberal with unwraps here because if this task panics the recv is dropped, propagating the error
// to the parent task
async fn file_writer(writer_id: String, format: LogFormat, mut recv: mpsc::Receiver<SinkMessage>) {
    info!("Started writing to file: {}", writer_id);
    let mut file = File::options()
        .append(true)
        .create(true)
        .open(format!("{}", writer_id))
        .await
        .unwrap();

    while let Some(msg) = recv.recv().await {
        match msg {
            SinkMessage::New(event) => {
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

async fn stdout_writer(format: LogFormat, mut recv: mpsc::Receiver<SinkMessage>) {
    info!("Started writing to stdout");
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = recv.recv().await {
        let mut buf: Vec<u8> = Vec::new();
        match msg {
            SinkMessage::New(event) => {
                writeln!(buf, "{}", format.format(&event)).unwrap();
                stdout.write_all(&buf).await.unwrap();
                stdout.flush().await.unwrap();
            }
        }
    }
    info!("Finished writing to stdout");
}
