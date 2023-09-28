use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, RequestBuilder};
use serde_json::{json, Value};

use crate::{JsonKey, Source};

pub struct LogSource<Mode> {
    search_url: String,
    query: String,
    seen_event_ids: HashSet<String>, // We don't want to return the same event twice
    mode: Mode,
}

/// Produces time windows, overlapping by 10 seconds, forever. Useful
/// for constantly following logs
pub struct Follow {
    next_window_start: DateTime<Utc>,
    next_window_end: Option<DateTime<Utc>>,
}

/// Produces a single time window, and then stops. Useful for taking
/// a snapshot of logs from a given period.
pub struct Snapshot {
    next_window_start: DateTime<Utc>,
    next_window_end: Option<DateTime<Utc>>,
}

impl<Mode> LogSource<Mode> {
    pub fn new(dd_domain: String, query: String, mode: Mode) -> Self {
        Self {
            search_url: format!("https://{}/api/v2/logs/events/search", dd_domain),
            query,
            seen_event_ids: HashSet::new(),
            mode,
        }
    }
}

impl<Mode> Source for LogSource<Mode>
where
    Mode: Iterator<Item = (DateTime<Utc>, DateTime<Utc>)> + Send + Sync,
{
    fn construct_query(&mut self, client: &Client) -> Option<RequestBuilder> {
        let builder = client.post(&self.search_url);

        let (start, end) = self.mode.next()?;

        let from = start.to_rfc3339();
        let to = end.to_rfc3339();

        let query = json!({
            "filter": {
                "from": from,
                "to": to,
                "query": self.query
            },
            "page": {
                "limit": self.get_batch_size()
            },
            "sort": "timestamp"
        });
        Some(builder.json(&query))
    }

    fn extract_results(&mut self, mut body: Value) -> Result<Vec<Value>, anyhow::Error> {
        let Some(events) = body.get_mut("data") else {
            return Ok(vec![]);
        };
        let mut events: Vec<_> = std::mem::take(
            events
                .as_array_mut()
                .ok_or(anyhow::anyhow!("Log query data not a list"))?,
        )
        .into_iter()
        .map(|e| unpack_tags(e))
        .collect();

        events.retain(|event| {
            self.seen_event_ids
                .insert(event["id"].as_str().unwrap().to_string())
        });

        Ok(events)
    }

    fn get_batch_size(&mut self) -> usize {
        1000
    }
}

fn unpack_tags(mut event: Value) -> Value {
    if let Some(tags) = event["attributes"]["tags"].as_array() {
        let mut unpacked = HashMap::new();
        for tag in tags {
            if let Some(str) = tag.as_str() {
                let mut split = str.split(':');
                let key = split.next().unwrap();
                let value = split.next().unwrap_or_default();
                unpacked.insert(key, value);
            }
        }
        event["attributes"]["tags"] = json!(unpacked);
    }
    event
}

#[derive(Clone)]
pub enum LogFormat {
    Text { sep: String, keys: Vec<JsonKey> },
    Structured,
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

impl LogFormat {
    pub fn text(sep: String, keys: Vec<JsonKey>) -> Self {
        LogFormat::Text { sep, keys }
    }

    pub fn format(&self, event: &Value) -> String {
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

impl Follow {
    pub fn new(initial_window: u64) -> Self {
        let start = Utc::now() - Duration::seconds(initial_window as i64);
        let end = Some(Utc::now());
        Self {
            next_window_start: start,
            next_window_end: end,
        }
    }
}

impl Iterator for Follow {
    type Item = (DateTime<Utc>, DateTime<Utc>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next_window_start;
        let end = self.next_window_end.take().unwrap_or(Utc::now());

        // Use time windows that overlap by 10 seconds to avoid missing events
        self.next_window_start = end - Duration::seconds(10);

        Some((start, end))
    }
}

impl Snapshot {
    pub fn new(from: DateTime<Utc>, window: u64) -> Self {
        let start = from;
        let end = from + Duration::seconds(window as i64);
        Self {
            next_window_start: start,
            next_window_end: Some(end),
        }
    }
}

impl Iterator for Snapshot {
    type Item = (DateTime<Utc>, DateTime<Utc>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next_window_start;
        let end = self.next_window_end.take()?;
        Some((start, end))
    }
}
