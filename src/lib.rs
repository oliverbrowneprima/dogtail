use reqwest::{Client, RequestBuilder};
use serde_json::Value;

pub mod logs;
pub mod sink;
pub mod tailer;

/// A thing which knows how talk to some subset of the datadog API - more or less the part of
/// dogtail that implements some endpoints schema
pub trait Source: Send + Sync {
    /// Construct a query to send to the API. If None is returned, the tailer will stop, dropping
    /// itself and the associated channel.
    fn construct_query(&mut self, client: &Client) -> Option<RequestBuilder>;
    /// Extract the results from the response body. This should handle deduplication of events,
    /// if needed
    fn extract_results(&mut self, body: Value) -> Result<Vec<Value>, anyhow::Error>;
    /// Extract the next url from the response body - this is fairly standard across the datadog API,
    /// so we provide a default implementation
    fn extract_next<'a>(&mut self, body: &'a Value) -> Result<Option<String>, anyhow::Error> {
        let Some(next) = body.get("links").map(|l| l.get("next")).flatten() else {
            return Ok(None);
        };
        next.as_str()
            .ok_or(anyhow::anyhow!("Next url not a string"))
            .map(|s| Some(s.to_string()))
    }

    /// Get the batch size for this source
    fn get_batch_size(&mut self) -> usize;
}

// Kinda json-pointer, but not really
#[derive(Clone)]
pub struct JsonKey(Vec<String>);

impl JsonKey {
    pub fn get(&self, event: &Value) -> Option<Value> {
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
