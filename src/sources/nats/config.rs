use snafu::{ResultExt, Snafu};
use vector_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig},
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    lookup::{lookup_v2::OptionalValuePath, owned_value_path},
};
use vrl::value::Kind;
use watermelon::{
    core::{Client, error::ConnectHandlerError},
    jetstream::error::JetstreamError,
};

use crate::{
    codecs::DecodingConfig,
    config::{GenerateConfig, SourceConfig, SourceContext, SourceOutput},
    nats::{NatsAuthConfig, NatsConfigError, client_builder, parse_server_address},
    serde::{default_decoding, default_framing_message_based},
    sources::{
        Source,
        nats::source::{
            create_consumer_stream, create_subscription, run_nats_core, run_nats_jetstream,
        },
    },
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum BuildError {
    #[snafu(display("NATS Config Error: {}", source))]
    Config { source: NatsConfigError },
    #[snafu(display("NATS Connect Error: {}", source))]
    Connect { source: ConnectHandlerError },
    #[snafu(display("NATS Subscribe Error: {}", source))]
    Subscribe {
        source: watermelon::core::error::ClientClosedError,
    },
    #[snafu(display("Invalid NATS subject: {}", source))]
    InvalidSubject {
        source: watermelon::proto::error::SubjectValidateError,
    },
    #[snafu(display("Invalid NATS queue group: {}", source))]
    InvalidQueueGroup {
        source: watermelon::proto::error::QueueGroupValidateError,
    },
    #[snafu(display("Failed to get NATS consumer: {}", source))]
    Consumer { source: JetstreamError },
    #[snafu(display("NATS consumer {} not found in stream {}", consumer, stream))]
    ConsumerNotFound { stream: String, consumer: String },
}

/// Batch settings for a JetStream pull consumer.
///
/// By default, messages are pulled in batches of up to 200.
/// Each pull request expires after 30 seconds if not fulfilled.
#[configurable_component]
#[derive(Clone, Debug)]
pub(super) struct BatchConfig {
    /// The maximum number of messages to pull in a single batch.
    #[serde(default = "default_batch")]
    pub(super) batch: usize,
}

const fn default_batch() -> usize {
    200
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch: default_batch(),
        }
    }
}

/// Configuration for NATS JetStream.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct JetStreamConfig {
    /// The name of the stream to bind to.
    pub stream: String,
    /// The name of the durable consumer to pull from.
    pub consumer: String,

    #[serde(default)]
    pub(super) batch_config: BatchConfig,
}

/// Configuration for the `nats` source.
#[configurable_component(source(
    "nats",
    "Read observability data from subjects on the NATS messaging system."
))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct NatsSourceConfig {
    /// The NATS URL to connect to.
    ///
    /// The URL takes the form of `nats://server:port`.
    /// If the port is not specified it defaults to 4222.
    ///
    /// Use the `tls://` scheme to connect using TLS-first. With the `nats://`
    /// scheme the connection is upgraded to TLS if required by the server.
    #[configurable(metadata(docs::examples = "nats://demo.nats.io"))]
    #[configurable(metadata(docs::examples = "nats://127.0.0.1:4242"))]
    #[configurable(metadata(docs::examples = "tls://demo.nats.io"))]
    pub url: String,

    /// A [name][nats_connection_name] assigned to the NATS connection.
    ///
    /// [nats_connection_name]: https://docs.nats.io/using-nats/developer/connecting/name
    #[serde(alias = "name")]
    #[configurable(metadata(docs::examples = "vector"))]
    pub connection_name: String,

    /// The NATS [subject][nats_subject] to pull messages from.
    ///
    /// [nats_subject]: https://docs.nats.io/nats-concepts/subjects
    #[configurable(metadata(docs::examples = "foo"))]
    #[configurable(metadata(docs::examples = "time.us.east"))]
    #[configurable(metadata(docs::examples = "time.*.east"))]
    #[configurable(metadata(docs::examples = "time.>"))]
    #[configurable(metadata(docs::examples = ">"))]
    pub subject: String,

    /// The NATS queue group to join.
    pub queue: Option<String>,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub log_namespace: Option<bool>,

    pub auth: Option<NatsAuthConfig>,

    #[serde(default = "default_framing_message_based")]
    #[derivative(Default(value = "default_framing_message_based()"))]
    pub framing: FramingConfig,

    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    /// The `NATS` subject key.
    #[serde(default = "default_subject_key_field")]
    pub subject_key_field: OptionalValuePath,

    #[serde(default)]
    pub jetstream: Option<JetStreamConfig>,
}

pub fn default_subject_key_field() -> OptionalValuePath {
    OptionalValuePath::from(owned_value_path!("subject"))
}

impl GenerateConfig for NatsSourceConfig {
    fn generate_config() -> serde_json::Value {
        serde_yaml::from_str(indoc::indoc! {
            r#"
            connection_name: vector
            subject: from.vector
            url: "nats://127.0.0.1:4222""#,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "nats")]
impl SourceConfig for NatsSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);
        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;

        match self.mode() {
            NatsMode::JetStream(js_config) => {
                let connection = self.connect().await?;
                let messages = create_consumer_stream(&connection, js_config).await?;

                Ok(Box::pin(run_nats_jetstream(
                    self.clone(),
                    connection,
                    messages,
                    decoder,
                    log_namespace,
                    cx.shutdown,
                    cx.out,
                )))
            }
            NatsMode::Core => {
                let (connection, subscription) = create_subscription(self).await?;

                Ok(Box::pin(run_nats_core(
                    self.clone(),
                    connection,
                    subscription,
                    decoder,
                    log_namespace,
                    cx.shutdown,
                    cx.out,
                )))
            }
        }
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let legacy_subject_key_field = self
            .subject_key_field
            .clone()
            .path
            .map(LegacyKey::InsertIfEmpty);
        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                NatsSourceConfig::NAME,
                legacy_subject_key_field,
                &owned_value_path!("subject"),
                Kind::bytes(),
                None,
            );

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    // Acknowledgment is only possible with Jetstream.
    fn can_acknowledge(&self) -> bool {
        true
    }
}

enum NatsMode<'a> {
    JetStream(&'a JetStreamConfig),
    Core,
}

impl NatsSourceConfig {
    pub async fn connect(&self) -> Result<Client, BuildError> {
        let builder = client_builder(&self.connection_name, &self.auth).context(ConfigSnafu)?;
        let server_addr = parse_server_address(&self.url).context(ConfigSnafu)?;
        builder.connect(server_addr).await.context(ConnectSnafu)
    }

    const fn mode(&self) -> NatsMode<'_> {
        match &self.jetstream {
            Some(config) => NatsMode::JetStream(config),
            None => NatsMode::Core,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::print_stdout)]

    use vector_lib::{
        lookup::{OwnedTargetPath, owned_value_path},
        schema::Definition,
    };
    use vrl::value::{Kind, kind::Collection};

    use super::*;
    use crate::sources::nats::config::default_subject_key_field;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<NatsSourceConfig>();
    }

    #[test]
    fn output_schema_definition_vector_namespace() {
        let config = NatsSourceConfig {
            log_namespace: Some(true),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let definitions = config
            .outputs(LogNamespace::Vector)
            .remove(0)
            .schema_definition(true);

        let expected_definition =
            Definition::new_with_default_metadata(Kind::bytes(), [LogNamespace::Vector])
                .with_meaning(OwnedTargetPath::event_root(), "message")
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None,
                )
                .with_metadata_field(&owned_value_path!("nats", "subject"), Kind::bytes(), None);

        assert_eq!(definitions, Some(expected_definition));
    }

    #[test]
    fn output_schema_definition_legacy_namespace() {
        let config = NatsSourceConfig {
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };
        let definitions = config
            .outputs(LogNamespace::Legacy)
            .remove(0)
            .schema_definition(true);

        let expected_definition = Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [LogNamespace::Legacy],
        )
        .with_event_field(
            &owned_value_path!("message"),
            Kind::bytes(),
            Some("message"),
        )
        .with_event_field(&owned_value_path!("timestamp"), Kind::timestamp(), None)
        .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        .with_event_field(&owned_value_path!("subject"), Kind::bytes(), None);

        assert_eq!(definitions, Some(expected_definition));
    }
}
