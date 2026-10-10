use std::{
    sync::Arc,
    task::{Context, Poll},
};

use super::{NatsError, config::NatsPublisher, request_builder::NatsRequest};
use crate::sinks::prelude::*;

#[derive(Clone)]
pub(super) struct NatsService {
    pub(super) publisher: Arc<NatsPublisher>,
}

pub(super) struct NatsResponse {
    metadata: RequestMetadata,
}

impl DriverResponse for NatsResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.metadata.events_estimated_json_encoded_byte_size()
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.metadata.request_encoded_size())
    }
}

impl Service<NatsRequest> for NatsService {
    type Response = NatsResponse;

    type Error = NatsError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: NatsRequest) -> Self::Future {
        let publisher = Arc::clone(&self.publisher);

        Box::pin(async move {
            publisher
                .publish(req.subject, req.headers, req.bytes)
                .await?;
            Ok(NatsResponse {
                metadata: req.metadata,
            })
        })
    }
}
