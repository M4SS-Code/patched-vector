use std::{collections::BTreeMap, num::NonZero, time::Duration};

use bytes::Bytes;
use futures_util::StreamExt;
use serde::Deserialize;
use vector_lib::{
    codecs::{JsonSerializerConfig, TextSerializerConfig},
    event::{EventArray, LogEvent},
};
use vrl::value;
use watermelon::{
    core::Client,
    jetstream::{
        AckPolicy, ConsumerConfig, ConsumerDurability, ConsumerSpecificConfig, ConsumerStorage,
        DeliverPolicy, DiscardPolicy, JetstreamClient, ReplayPolicy, Storage, StreamConfig,
    },
    proto::Subject,
};
use watermelon_testkit::NatsServer;

use super::{
    NatsError,
    config::{NatsHeaderConfig, NatsSinkConfig},
    sink::NatsSink,
};
use crate::{
    nats::{NatsAuthConfig, NatsAuthCredentialsFile, NatsAuthUserPassword, parse_server_address},
    sinks::{nats::config::JetStreamConfig, prelude::*},
    test_util::{
        components::{SINK_TAGS, run_and_assert_sink_compliance},
        random_lines_with_stream, random_string, trace_init,
    },
};

/// Starts a disposable `nats-server`, or returns from the test if it isn't installed.
macro_rules! start_server {
    ($builder:expr) => {
        match $builder.start().await {
            Some(server) => server,
            None => return,
        }
    };
}

/// The `nats-server` configuration for JWT authentication, without TLS.
fn jwt_server_config() -> String {
    let config = std::fs::read_to_string("tests/integration/nats/data/nats-jwt.conf")
        .expect("Failed to read the JWT server configuration");
    let (config, _tls) = config
        .split_once("\ntls:")
        .expect("Missing the TLS block in the JWT server configuration");
    config.to_owned()
}

fn stream_config(name: &str, subject: &str) -> StreamConfig {
    StreamConfig {
        name: name.to_owned(),
        subjects: vec![subject.parse().unwrap()],
        max_consumers: None,
        max_messages: None,
        max_bytes: None,
        max_age: Duration::ZERO,
        max_messages_per_subject: None,
        max_message_size: None,
        discard_policy: DiscardPolicy::Old,
        storage: Storage::Memory,
        replicas: NonZero::new(1).unwrap(),
        duplicate_window: Duration::from_secs(120),
        compression: None,
        allow_direct: false,
        mirror_direct: false,
        sealed: false,
        allow_delete: true,
        allow_purge: true,
        allow_rollup_hdrs: false,
    }
}

fn consumer_config(name: &str) -> ConsumerConfig {
    ConsumerConfig {
        durability: ConsumerDurability::Durable,
        name: name.to_owned(),
        description: String::new(),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit {
            wait: Duration::from_secs(30),
            max_pending: None,
        },
        max_deliver: None,
        backoff: Vec::new(),
        filter_subjects: Vec::new(),
        replay_policy: ReplayPolicy::Instant,
        rate_limit: None,
        headers_only: false,
        specs: ConsumerSpecificConfig::Pull {
            max_waiting: None,
            max_request_batch: None,
            max_request_expires: Duration::ZERO,
            max_request_max_bytes: None,
        },
        inactive_threshold: Duration::ZERO,
        replicas: None,
        storage: ConsumerStorage::Memory,
        metadata: BTreeMap::new(),
    }
}

async fn jetstream_client(url: &str) -> JetstreamClient {
    let client = Client::builder()
        .connect(url.parse().unwrap())
        .await
        .expect("Failed to connect to NATS");
    JetstreamClient::new(client)
}

/// Waits for the server to process every command previously sent by `client`.
async fn sync(client: &Client) {
    // Nobody responds to this subject, but the server only replies after
    // having processed all of the previous commands sent by this client.
    let response = client
        .request(Subject::from_static("_VECTOR_TEST.sync"))
        .payload(Bytes::new())
        .await
        .unwrap();
    drop(response.await);
}

fn generate_sink_config(url: &str, subject: &str) -> NatsSinkConfig {
    NatsSinkConfig {
        acknowledgements: Default::default(),
        encoding: TextSerializerConfig::default().into(),
        connection_name: "".to_owned(),
        subject: Template::try_from(subject).unwrap(),
        url: url.to_string(),
        auth: None,
        request: Default::default(),
        jetstream: false.into(),
        confinement: Default::default(),
    }
}

async fn publish_and_check(conf: NatsSinkConfig) -> Result<(), NatsError> {
    // Publish `N` messages to NATS.
    //
    // Verify with a separate subscriber that the messages were
    // successfully published.

    // Create Sink
    let confined_subject = conf
        .subject
        .clone()
        .confine(&conf.confinement, "nats", "subject")
        .expect("subject should confine");
    let server_address = parse_server_address(&conf.url).expect("server address should parse");
    let sink = NatsSink::new(conf.clone(), confined_subject, server_address.clone()).await?;
    let sink = VectorSink::from_event_streamsink(sink);

    // Establish the consumer subscription.
    let subject = conf.subject.to_string();
    let consumer = conf
        .connect(server_address)
        .await
        .expect("failed to connect with test consumer");
    let mut sub = consumer
        .subscribe(subject.parse().unwrap(), None)
        .await
        .expect("failed to subscribe with test consumer");
    sync(&consumer).await;

    // Publish events.
    let num_events = 10;
    let (input, events) = random_lines_with_stream(100, num_events, None);

    run_and_assert_sink_compliance(sink, events, &SINK_TAGS).await;

    // Unsubscribe from the channel.
    tokio::time::sleep(Duration::from_secs(3)).await;
    sub.close().await.unwrap();

    let mut output: Vec<String> = Vec::new();
    while let Some(msg) = sub.next().await {
        let msg = msg.expect("subscription error");
        output.push(String::from_utf8_lossy(&msg.base.payload).to_string())
    }

    assert_eq!(output.len(), input.len());
    assert_eq!(output, input);

    Ok(())
}

#[tokio::test]
async fn nats_no_auth() {
    trace_init();

    let subject = format!("test-{}", random_string(10));
    let server = start_server!(NatsServer::builder());
    let url = server.client_url().to_owned();

    let conf = generate_sink_config(&url, &subject);

    let r = publish_and_check(conf).await;
    assert!(
        r.is_ok(),
        "publish_and_check failed, expected Ok(()), got: {r:?}"
    );
}

#[tokio::test]
async fn nats_userpass_auth_valid() {
    trace_init();

    let subject = format!("test-{}", random_string(10));
    let server = start_server!(NatsServer::builder().user("natsuser", "natspass"));
    let url = server.client_url().to_owned();

    let mut conf = generate_sink_config(&url, &subject);
    conf.auth = Some(NatsAuthConfig::UserPassword {
        user_password: NatsAuthUserPassword {
            user: "natsuser".to_string(),
            password: "natspass".to_string().into(),
        },
    });

    publish_and_check(conf)
        .await
        .expect("publish_and_check failed");
}

#[tokio::test]
async fn nats_userpass_auth_invalid() {
    trace_init();

    let subject = format!("test-{}", random_string(10));
    let server = start_server!(NatsServer::builder().user("natsuser", "natspass"));
    let url = server.client_url().to_owned();

    let mut conf = generate_sink_config(&url, &subject);
    conf.auth = Some(NatsAuthConfig::UserPassword {
        user_password: NatsAuthUserPassword {
            user: "natsuser".to_string(),
            password: "wrongpass".to_string().into(),
        },
    });

    let r = publish_and_check(conf).await;
    assert!(
        matches!(r, Err(NatsError::Connect { .. })),
        "publish_and_check failed, expected NatsError::Connect, got: {r:?}"
    );
}

#[tokio::test]
async fn nats_credentials_file_auth_valid() {
    trace_init();

    let subject = format!("test-{}", random_string(10));
    let server = start_server!(NatsServer::builder().config(jwt_server_config()));
    let url = server.client_url().to_owned();

    let mut conf = generate_sink_config(&url, &subject);
    conf.auth = Some(NatsAuthConfig::CredentialsFile {
        credentials_file: NatsAuthCredentialsFile {
            path: "tests/integration/nats/data/nats.creds".into(),
        },
    });

    let r = publish_and_check(conf).await;
    assert!(
        r.is_ok(),
        "publish_and_check failed, expected Ok(()), got: {r:?}"
    );
}

#[tokio::test]
async fn nats_credentials_file_auth_invalid() {
    trace_init();

    let subject = format!("test-{}", random_string(10));
    let server = start_server!(NatsServer::builder().config(jwt_server_config()));
    let url = server.client_url().to_owned();

    let mut conf = generate_sink_config(&url, &subject);
    conf.auth = Some(NatsAuthConfig::CredentialsFile {
        credentials_file: NatsAuthCredentialsFile {
            path: "tests/integration/nats/data/nats-bad.creds".into(),
        },
    });

    let r = publish_and_check(conf).await;
    assert!(
        matches!(r, Err(NatsError::Config { .. })),
        "publish_and_check failed, expected NatsError::Config, got: {r:?}"
    );
}

#[tokio::test]
async fn nats_jetstream_valid() {
    trace_init();
    let stream_name = format!("EVENTS_{}", random_string(10));
    let subject = format!("events.test-{}", random_string(10));

    let server = start_server!(NatsServer::builder().jetstream());
    let url = server.client_url().to_owned();

    let js = jetstream_client(&url).await;
    js.create_stream(&stream_config(&stream_name, &subject))
        .await
        .expect("Failed to create JetStream stream");

    let mut conf = generate_sink_config(&url, &subject);
    conf.jetstream = JetStreamConfig {
        enabled: true,
        ..Default::default()
    };

    let r = publish_and_check(conf).await;
    assert!(
        r.is_ok(),
        "publish_and_check failed, expected Ok(()), got: {r:?}"
    );
}

#[derive(Debug, Deserialize)]
struct JetstreamTestEvent {
    id: String,
    message: String,
}

// Check that message ids are dynamically assigned
// and that they are correctly deduplicated at the stream level
#[tokio::test]
async fn nats_jetstream_message_id_valid() {
    trace_init();
    let stream_name = format!("EVENTS_MSG_ID_{}", random_string(10));
    let subject = format!("events.test.msg.id-{}", random_string(10));

    let server = start_server!(NatsServer::builder().jetstream());
    let url = server.client_url().to_owned();

    let js = jetstream_client(&url).await;
    js.create_stream(&StreamConfig {
        duplicate_window: Duration::from_secs(60),
        ..stream_config(&stream_name, &subject)
    })
    .await
    .unwrap();

    let mut conf = generate_sink_config(&url, &subject);
    conf.encoding = JsonSerializerConfig::default().into();

    let header_config = NatsHeaderConfig {
        message_id: Some(UnconfinedTemplate::try_from("{{ id }}").unwrap()),
    };

    conf.jetstream = JetStreamConfig {
        enabled: true,
        headers: Some(header_config),
    };

    let confined_subject = conf
        .subject
        .clone()
        .confine(&conf.confinement, "nats", "subject")
        .expect("subject should confine");
    let server_address = parse_server_address(&conf.url).expect("server address should parse");
    let sink = NatsSink::new(conf.clone(), confined_subject, server_address)
        .await
        .unwrap();
    let sink = VectorSink::from_event_streamsink(sink);

    let event_id = "123";

    let event1 = LogEvent::from(value!({
        "id": event_id,
        "message": "first message",
    }));

    let event2 = LogEvent::from(value!({
        "id": event_id,
        "message": "second message",
    }));

    let event3 = LogEvent::from(value!({
        "id": event_id,
        "message": "third message",
    }));

    let event_array = EventArray::Logs(vec![event1, event2, event3]);
    sink.run(futures::stream::iter(vec![event_array]).boxed())
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let consumer = js
        .create_consumer(
            &stream_name,
            &consumer_config(&format!("test-consumer-{}", random_string(5))),
        )
        .await
        .unwrap();

    let mut processed_messages = Vec::new();

    let batch = js
        .consumer_batch(&consumer, Duration::from_secs(2), 2)
        .await
        .expect("Failed to get batch");
    let mut batch = std::pin::pin!(batch);

    while let Some(Ok(mut msg)) = batch.next().await {
        msg.ack().await.unwrap();
        processed_messages.push(msg);
    }

    assert_eq!(
        processed_messages.len(),
        1,
        "Expected only one message due to deduplication"
    );

    let msg = &processed_messages[0];
    let received_event: JetstreamTestEvent =
        serde_json::from_slice(&msg.message.base.payload).unwrap();
    assert_eq!(received_event.id, event_id);
    assert_eq!(received_event.message, "first message");
}
