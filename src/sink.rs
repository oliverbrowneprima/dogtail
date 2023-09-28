use std::collections::HashMap;

use futures::future::join_all;
use serde_json::Value;
use std::time::Duration;
use tokio::{runtime, sync::mpsc, task::JoinHandle};

/// A thing that knows how to construct an output stream given a value,
/// and how to construct an output ID from an event. A "SinkSet" is really
/// more like an abstraction over a set of possible consumers, and is used in combination
/// with something that caches those consumers like [drains::ConsumerPool] to
/// consume events on the fly
pub trait SinkSet: Send + Sync {
    /// Construct an output actor, and spawn it on the given runtime, then return
    fn construct_output(&self, event: &Value, runtime: &runtime::Handle) -> Sink;
    /// Given an event, return an actor name. This should be infallible, so
    /// implementers are expected to provide a sensible default. This is mainly
    /// meant to be used for dispatching events to the correct output actor, and
    /// to ensure only one actor is created per output id
    fn get_sink_id(&self, event: &Value) -> String;
}

/// An "actor" that consumes messages, and exits when the other side of the channel
/// returns None
pub struct Sink {
    id: String,
    handle: JoinHandle<()>,
    sender: mpsc::Sender<SinkMessage>,
}

pub enum SinkMessage {
    New(Value),
}

/// A thing which can consume values, and dispatch them to the correct output
/// stream. I really wish I had a better name for this
pub struct ConsumerPool {
    sinks: HashMap<String, Sink>,
    sink_set: Box<dyn SinkSet>,
}

impl ConsumerPool {
    pub fn new(sink_set: Box<dyn SinkSet>) -> Self {
        ConsumerPool {
            sinks: HashMap::new(),
            sink_set,
        }
    }

    /// Consume an event, dispatching it to the correct output stream
    #[tracing::instrument(level = "trace", skip(self, event))]
    pub async fn consume(&mut self, event: Value) -> Result<(), anyhow::Error> {
        let id = self.sink_set.get_sink_id(&event);

        let sink = self.sinks.entry(id).or_insert_with(|| {
            self.sink_set
                .construct_output(&event, &tokio::runtime::Handle::current())
        });

        sink.send(SinkMessage::New(event)).await?;

        Ok(())
    }

    /// Drop all output stream channels and join all output streams, waiting at most
    /// `wait` seconds for them to finish
    pub async fn finish(mut self, wait: u64) {
        join_all(
            self.sinks
                .drain()
                .map(|(_, s)| s.finish(Duration::from_secs(wait))),
        )
        .await;
    }
}

impl Sink {
    pub fn new(id: String, handle: JoinHandle<()>, sender: mpsc::Sender<SinkMessage>) -> Self {
        Sink { id, handle, sender }
    }

    pub async fn send(&self, value: SinkMessage) -> Result<(), anyhow::Error> {
        self.sender.send(value).await?;
        Ok(())
    }

    pub async fn finish(self, wait: Duration) {
        drop(self.sender);
        let _ = tokio::time::timeout(wait, self.handle).await;
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}
