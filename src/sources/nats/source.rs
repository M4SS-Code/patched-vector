use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use snafu::ResultExt;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{DecoderFramedRead, decoding::StreamDecodingError},
    config::{LegacyKey, LogNamespace},
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, EventsReceived, EventsReceivedHandle,
        InternalEventHandle as _, Protocol,
    },
    lookup::owned_value_path,
};
use watermelon::{
    core::{Client, Subscription},
    jetstream::{ConsumerStream, JetstreamClient},
    proto::{MessageBase, QueueGroup, Subject},
};

use crate::{
    SourceSender,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    event::Event,
    internal_events::StreamClosedError,
    shutdown::ShutdownSignal,
    sources::nats::config::{
        BuildError, ConsumerSnafu, InvalidQueueGroupSnafu, InvalidSubjectSnafu, JetStreamConfig,
        NatsSourceConfig, SubscribeSnafu,
    },
};

/// How long a JetStream pull request waits before expiring if not fulfilled.
const JETSTREAM_PULL_EXPIRES: Duration = Duration::from_secs(30);

/// The outcome of processing a single NATS message.
pub enum ProcessingStatus {
    /// The message payload was fully decoded and sent downstream.
    Success,
    /// A non-recoverable error occurred while decoding the payload.
    Failed,
    /// The downstream channel is closed, and the source should shut down.
    ChannelClosed,
}

/// Processes a single NATS message, sending decoded events downstream.
///
/// This function contains the common logic for both Core and JetStream NATS.
pub async fn process_message(
    msg: &MessageBase,
    config: &NatsSourceConfig,
    decoder: &Decoder,
    log_namespace: LogNamespace,
    out: &mut SourceSender,
    events_received: &EventsReceivedHandle,
) -> ProcessingStatus {
    let mut framed = DecoderFramedRead::new(msg.payload.as_ref(), decoder.clone());
    let mut success = true;

    while let Some(next) = framed.next().await {
        match next {
            Ok((events, _byte_size)) => {
                let count = events.len();
                if count == 0 {
                    continue;
                }

                let byte_size = events.estimated_json_encoded_size_of();
                events_received.emit(CountByteSize(count, byte_size));
                let now = Utc::now();
                let events = events.into_iter().map(|mut event| {
                    if let Event::Log(ref mut log) = event {
                        log_namespace.insert_standard_vector_source_metadata(
                            log,
                            NatsSourceConfig::NAME,
                            now,
                        );
                        let legacy_subject_key_field = config
                            .subject_key_field
                            .path
                            .as_ref()
                            .map(LegacyKey::InsertIfEmpty);
                        log_namespace.insert_source_metadata(
                            NatsSourceConfig::NAME,
                            log,
                            legacy_subject_key_field,
                            &owned_value_path!("subject"),
                            msg.subject.as_str(),
                        );
                    }
                    event
                });

                if out.send_batch(events).await.is_err() {
                    emit!(StreamClosedError { count });
                    return ProcessingStatus::ChannelClosed;
                }
            }
            Err(error) => {
                success = false;
                // Error is logged by `vector_lib::codecs::Decoder`, no further
                // handling is needed here.
                if !error.can_continue() {
                    break;
                }
            }
        }
    }

    if success {
        ProcessingStatus::Success
    } else {
        ProcessingStatus::Failed
    }
}

pub(crate) async fn create_consumer_stream(
    connection: &Client,
    js_config: &JetStreamConfig,
) -> Result<ConsumerStream, BuildError> {
    let js = JetstreamClient::new(connection.clone());
    let consumer = js
        .consumer(&js_config.stream, &js_config.consumer)
        .await
        .context(ConsumerSnafu)?
        .ok_or_else(|| BuildError::ConsumerNotFound {
            stream: js_config.stream.clone(),
            consumer: js_config.consumer.clone(),
        })?;
    Ok(js.consumer_stream(
        consumer,
        JETSTREAM_PULL_EXPIRES,
        js_config.batch_config.batch,
    ))
}

pub async fn run_nats_jetstream(
    config: NatsSourceConfig,
    connection: Client,
    initial_messages: ConsumerStream,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));
    let mut backoff = ExponentialBackoff::default().max_delay(std::time::Duration::from_secs(30));

    let js_config = config
        .jetstream
        .as_ref()
        .expect("jetstream config must be present");

    let mut messages = Box::pin(initial_messages);

    loop {
        // `ShutdownSignal` fires once then polls `Pending` forever, so shutdown must be handled here via `select!`, not re-polled afterwards.
        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => return Ok(()),

                maybe_msg = messages.next() => {
                    match maybe_msg {
                        Some(Ok(mut msg)) => {
                            backoff.reset();
                            bytes_received.emit(ByteSize(msg.message.base.payload.len()));

                            let status = process_message(
                                &msg.message.base,
                                &config,
                                &decoder,
                                log_namespace,
                                &mut out,
                                &events_received,
                            )
                            .await;

                            match status {
                                ProcessingStatus::Success => {
                                    if let Err(err) = msg.ack().await {
                                        error!(message = "Failed to acknowledge JetStream message.", %err);
                                    }
                                }
                                ProcessingStatus::ChannelClosed => return Err(()),
                                // Do not acknowledge on failure; the message will be redelivered.
                                ProcessingStatus::Failed => {}
                            }
                        }
                        Some(Err(err)) => {
                            warn!(message = "JetStream consumer stream error, recreating.", %err);
                            break;
                        }
                        // The pull stream ended; recover the consumer.
                        None => break,
                    }
                }
            }
        }

        // Reconnect: rebuild the consumer stream with backoff.
        // The durable consumer on the server tracks delivery state,
        // so we pick up where we left off.
        warn!(message = "JetStream pull stream terminated. Recovering consumer...");
        // Drop the failed stream so it stops buffering ack-pending deliveries
        // while we back off.
        drop(messages);
        loop {
            let delay = backoff.next().expect("backoff never ends");
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(delay) => {},
            }

            match create_consumer_stream(&connection, js_config).await {
                Ok(m) => {
                    // Don't reset backoff on construction; a built stream hasn't pulled
                    // yet. Backoff is reset only after a message is successfully pulled.
                    messages = Box::pin(m);
                    break;
                }
                Err(err) => {
                    warn!(message = "Failed to recreate JetStream consumer stream, retrying.", %err);
                }
            }
        }
    }
}

pub async fn run_nats_core(
    config: NatsSourceConfig,
    _connection: Client,
    mut subscriber: Subscription,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));

    loop {
        tokio::select! {
            biased;

             _ = &mut shutdown => {
                info!("Shutdown signal received. Draining NATS subscription...");
                if let Err(err) = subscriber.close().await {
                    error!(message = "Failed to drain NATS subscription.", %err);
                }
            },

            maybe_msg = subscriber.next() => {
                match maybe_msg {
                    Some(Ok(msg)) => {
                        bytes_received.emit(ByteSize(msg.base.payload.len()));
                        let status = process_message(
                            &msg.base,
                            &config,
                            &decoder,
                            log_namespace,
                            &mut out,
                            &events_received,
                        )
                        .await;

                        if let ProcessingStatus::ChannelClosed = status {
                            return Err(());
                        }
                    },
                    Some(Err(err)) => {
                        warn!(message = "NATS subscription error.", %err);
                    },
                    None => {
                        // The stream has ended. This happens naturally after a successful
                        // drain or if the connection is lost.
                        break;
                    }
                }
            }
        }
    }

    info!("NATS source drained and shut down gracefully.");
    Ok(())
}

pub async fn create_subscription(
    config: &NatsSourceConfig,
) -> Result<(Client, Subscription), BuildError> {
    let subject = Subject::try_from(config.subject.clone()).context(InvalidSubjectSnafu)?;
    let queue_group = config
        .queue
        .clone()
        .map(QueueGroup::try_from)
        .transpose()
        .context(InvalidQueueGroupSnafu)?;

    let nc = config.connect().await?;
    let subscription = nc
        .subscribe(subject, queue_group)
        .await
        .context(SubscribeSnafu)?;

    Ok((nc, subscription))
}
