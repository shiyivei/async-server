use std::pin::Pin;
use std::sync::Arc;

use crate::{CommandResponse, Publish, Subscribe, Unsubscribe};
use futures::stream;
use futures::Stream;
use sled::Subscriber;
use tokio_stream::wrappers::ReceiverStream;

use super::top::Topic;

pub type StreamingResponse = Pin<Box<dyn Stream<Item = Arc<CommandResponse>> + Send>>;
pub trait TopicService {
    fn execute(self, chan: impl Topic) -> StreamingResponse;
}

impl TopicService for Subscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        let rx = topic.subscribe(self.topic);
        Box::pin(ReceiverStream::new(rx))
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        topic.unsubscribe(self.topic, self.id);
        Box::pin(stream::once(async { Arc::new(CommandResponse::ok()) }))
    }
}

impl TopicService for Publish {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        topic.publish(self.topic, Arc::new(self.data.into()));
        Box::pin(stream::once(async { Arc::new(CommandResponse::ok()) }))
    }
}
