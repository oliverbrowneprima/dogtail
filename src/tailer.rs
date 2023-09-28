use std::time::{Duration, Instant};

use reqwest::{RequestBuilder, Response};
use serde_json::Value;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, info, instrument, warn};

use crate::Source;

/// The Tailer handles authentication, rate limiting, and pagination for a given source,
/// and returns a receiver that will emit events as they are received. This lets you only
/// worry about implementing the Source. The Tailer assumes the datadog rate-limit headers
/// are present, and will scale how long it waits between requests based on 1) not exceeding
/// the rate limit, and 2) how many useful results it got from the last request.
pub struct Tailer {
    source: Box<dyn Source>,
    client: reqwest::Client,
    api_key: String,
    app_key: String,
    last_limit_stats: Option<RateLimitStatus>,
}

impl Tailer {
    /// Construct a tailer from a source, and the necessary API keys.
    pub fn new(api_key: String, app_key: String, source: Box<dyn Source>) -> Self {
        Tailer {
            source,
            client: reqwest::Client::new(),
            api_key,
            app_key,
            last_limit_stats: None,
        }
    }

    /// Start tailing from the passed source, returning a receiver that will emit
    /// events as they are received
    pub async fn start(self) -> Receiver<Value> {
        let (send, recv) = mpsc::channel(100);
        tokio::spawn(async move {
            let mut _self = self; // "But we can open the box" said toad. "That's true" said frog.
            _self.tail(send).await;
        });
        recv
    }

    #[instrument(level = "debug", skip_all)]
    async fn tail(&mut self, event_sink: mpsc::Sender<Value>) {
        // We just eat the errors here, since we've been tokio::spawn'd. Dropping
        // the sender should be enough to signal to consumer that we're done
        while let Ok(Some(count)) = self.run_query(&event_sink).await {
            info!("Returned {} events", count);
            let seconds_to_next_call = self
                .last_limit_stats
                .as_ref()
                .map(|l| {
                    l.next_request_allowed
                        .duration_since(Instant::now())
                        .as_secs()
                })
                .unwrap_or(0);

            println!("Waiting {}s", seconds_to_next_call);
        }
    }

    fn headers(&self, builder: RequestBuilder) -> RequestBuilder {
        builder
            .header("Accept", "application/json")
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.app_key)
    }

    #[instrument(level = "debug", skip_all)]
    async fn send(&mut self, q: RequestBuilder) -> Result<Response, anyhow::Error> {
        if let Some(limit_stats) = self.last_limit_stats.take() {
            limit_stats.pause().await;
        }
        let response = q.send().await?;
        self.last_limit_stats = Some(RateLimitStatus::from(&response));
        Ok(response)
    }

    #[instrument(level = "debug", skip_all)]
    async fn run_query(
        &mut self,
        event_sink: &mpsc::Sender<Value>,
    ) -> Result<Option<usize>, anyhow::Error> {
        let Some(req) = self.source.construct_query(&self.client) else {
            info!("Source returned None, stopping");
            return Ok(None);
        };

        let first = self.send(self.headers(req)).await?;

        if !first.status().is_success() {
            return self.handle_error(first, 0).await.map(|o| Some(o));
        }

        let body: Value = first.json().await?;

        let mut returned = 0;

        let mut next = self.source.extract_next(&body)?;

        for v in self.source.extract_results(body)? {
            returned += 1;
            event_sink.send(v).await?;
        }

        self.last_limit_stats.as_mut().map(|s| {
            s.scale_remaining_by(returned, self.source.get_batch_size());
        });

        while let Some(next_url) = &next {
            debug!("Following next link: {}", next_url);
            let response = self.send(self.headers(self.client.get(next_url))).await?;

            if !response.status().is_success() {
                self.handle_error(response, returned).await?;
                continue; // If we hit a 429 while following next links, we should just re-request that page
            }

            let body = response.json().await?;

            next = self.source.extract_next(&body)?;

            let results = self.source.extract_results(body)?;

            self.last_limit_stats
                .as_mut()
                .map(|s| s.scale_remaining_by(results.len(), self.source.get_batch_size()));

            for v in results {
                returned += 1;
                event_sink.send(v).await?;
            }
        }

        Ok(Some(returned))
    }

    async fn handle_error(
        &mut self,
        response: Response,
        returned_so_far: usize,
    ) -> Result<usize, anyhow::Error> {
        match response.status() {
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                warn!("Got too_many_requests, waiting and retrying");
                Ok(returned_so_far) // We have the correct interval period, we can just wait and then retry
            }
            _ => Err(anyhow::anyhow!("Error: {}", response.text().await.unwrap())),
        }
    }
}

#[derive(Debug)]
struct RateLimitStatus {
    period: Duration,
    remaining_budget: u32,
    next_request_allowed: Instant,
}

impl From<&Response> for RateLimitStatus {
    fn from(response: &Response) -> Self {
        let get = |key: &str| response.headers().get(key).unwrap().to_str().unwrap();

        // TODO - figure out a use for this in the wait time calculation
        // let limit = get("x-ratelimit-limit").parse().unwrap();
        let period = Duration::from_secs(
            get("x-ratelimit-period")
                .parse()
                .expect("Header x-ratelimit-period not found"),
        );
        let remaining_budget = get("x-ratelimit-remaining")
            .parse()
            .expect("Header x-ratelimit-remaining not found");
        let reset_time = Instant::now()
            + Duration::from_secs(
                get("x-ratelimit-reset")
                    .parse()
                    .expect("Header x-ratelimit-reset not found"),
            );

        let status = RateLimitStatus {
            period,
            remaining_budget,
            next_request_allowed: reset_time,
        };
        debug!("Rate limit status: {:?}", status);
        status
    }
}

// In order to be a good citizen, we always wait until reset + [0.0..5.0) seconds before requesting again
// TODO - this is an antipattern - it should be impossible to make another request until the rate limit is reset
impl RateLimitStatus {
    async fn pause(&self) {
        let wait = self.next_request_allowed.duration_since(Instant::now());
        let jitter = Duration::from_secs_f32(rand::random::<f32>() * 5.0);
        let wait = wait + jitter;
        if wait > Duration::from_secs(0) {
            debug!("Waiting {}s", wait.as_secs());
            tokio::time::sleep(wait).await;
        }
    }

    // We scale how long we wait by portion of time period until next budget allocation and
    // by how likely we are to get useful results from our query (basically, the last time we
    // hit the server, how many useful result did we get).
    fn scale_remaining_by(&mut self, returned: usize, limit: usize) {
        let useful_results_factor = returned as f32 / limit as f32;
        // The more useful results we got, the less of our budget period we want to wait
        let desired_wait = self.period.as_millis() as f32 * (1.0 - useful_results_factor);

        if self.remaining_budget > 0 {
            let wait = desired_wait
                .min(
                    self.next_request_allowed
                        .duration_since(Instant::now())
                        .as_millis() as f32,
                )
                .max(0.0);
            debug!(
                "Scaled wait by {}, desired wait was {}ms, waiting {}ms",
                useful_results_factor, desired_wait, wait
            );
            self.next_request_allowed = Instant::now() + Duration::from_millis(wait as u64);
        } else {
            debug!("No remaining budget, not scaling wait time");
        }
    }
}
