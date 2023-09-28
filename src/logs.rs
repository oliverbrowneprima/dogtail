use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, RequestBuilder};
use serde_json::{json, Value};

use crate::Query;

pub struct LogQuery {
    search_url: String,
    query: String,
    seen_event_ids: HashSet<String>, // We don't want to return the same event twice
    next_window_start: DateTime<Utc>,
    next_window_end: Option<DateTime<Utc>>,
}

impl LogQuery {
    pub fn new(
        dd_domain: String,
        query: String,
        initial_window_width: u64,
        from: Option<DateTime<Utc>>,
    ) -> Self {
        let (start, end) = if let Some(from) = from {
            assert!(from < Utc::now());
            let start = from;
            let end = from + Duration::seconds(initial_window_width as i64);
            (start, Some(end))
        } else {
            let start = Utc::now() - Duration::seconds(initial_window_width as i64);
            (start, None)
        };
        Self {
            search_url: format!("https://{}/api/v2/logs/events/search", dd_domain),
            query,
            seen_event_ids: HashSet::new(),
            next_window_start: start,
            next_window_end: end,
        }
    }
}

impl Query for LogQuery {
    fn get_query(&mut self, client: &Client) -> RequestBuilder {
        let builder = client.post(&self.search_url);

        let start = self.next_window_start;
        let end = self.next_window_end.take().unwrap_or(Utc::now());

        let from = start.to_rfc3339();
        let to = end.to_rfc3339();

        // Use time windows that overlap by 10 seconds to avoid missing events
        self.next_window_start = end - Duration::seconds(10);

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
        builder.json(&query)
    }

    fn get_results(&mut self, mut body: Value) -> Result<Vec<Value>, anyhow::Error> {
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
