use bytes::Bytes;
use snafu::ResultExt;
use vector_lib::codecs::JsonSerializerConfig;
use watermelon::{
    core::{Client, ClientBuilder},
    jetstream::JetstreamClient,
    proto::{ServerAddr, Subject},
};

use super::{ConfigSnafu, ConnectSnafu, InvalidSubjectSnafu, NatsError, sink::NatsSink};
use crate::{
    config::ValidatedSink,
    nats::{NatsAuthConfig, client_builder, parse_server_address},
    sinks::{prelude::*, util::service::TowerRequestConfigDefaults},
    template::ConfinementConfig,
};

#[derive(Clone, Copy, Debug)]
pub struct NatsTowerRequestConfigDefaults;

impl TowerRequestConfigDefaults for NatsTowerRequestConfigDefaults {
    const CONCURRENCY: Concurrency = Concurrency::None;
}

/// A set of NATS headers that can be added to each message.
#[configurable_component]
#[serde_with::serde_as]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NatsHeaderConfig {
    /// A unique identifier for the message. Useful for deduplication.
    ///
    /// Can be a template that references fields in the event, e.g., `{{ event_id }}`.
    #[configurable(metadata(docs::templateable))]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[configurable(metadata(docs::examples = "event-{{ event_id }}"))]
    pub(super) message_id: Option<UnconfinedTemplate>,
}

impl NatsHeaderConfig {
    pub fn build_headers(&self, event: &Event) -> NatsHeaders {
        let message_id = self
            .message_id
            .as_ref()
            .and_then(|template| template.render_string(event).ok());

        NatsHeaders { message_id }
    }
}

/// The NATS headers rendered for a single message.
#[derive(Clone, Debug, Default)]
pub struct NatsHeaders {
    pub(super) message_id: Option<String>,
}

/// Configuration for sending messages using NATS JetStream.
#[configurable_component]
#[serde_with::serde_as]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct JetStreamConfig {
    /// Whether to enable Jetstream.
    #[serde(default)]
    pub enabled: bool,

    /// A map of NATS headers to be included in each message.
    #[configurable(metadata(docs::templateable))]
    #[serde(default)]
    pub(super) headers: Option<NatsHeaderConfig>,
}

impl From<bool> for JetStreamConfig {
    fn from(enabled: bool) -> Self {
        Self {
            enabled,
            ..Default::default()
        }
    }
}

/// Configuration for the `nats` sink.
#[configurable_component(sink(
    "nats",
    "Publish observability data to subjects on the NATS messaging system."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct NatsSinkConfig {
    pub(super) encoding: EncodingConfig,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// A NATS [name][nats_connection_name] assigned to the NATS connection.
    ///
    /// [nats_connection_name]: https://docs.nats.io/using-nats/developer/connecting/name
    #[serde(default = "default_name", alias = "name")]
    #[configurable(metadata(docs::examples = "foo"))]
    pub(super) connection_name: String,

    /// The NATS [subject][nats_subject] to publish messages to.
    ///
    /// [nats_subject]: https://docs.nats.io/nats-concepts/subjects
    #[configurable(metadata(docs::templateable))]
    #[configurable(metadata(
        docs::examples = "events-{{ host }}",
        docs::examples = "foo",
        docs::examples = "time.us.east",
        docs::examples = "time.*.east",
        docs::examples = "time.>",
        docs::examples = ">"
    ))]
    pub(super) subject: Template,

    /// The NATS [URL][nats_url] to connect to.
    ///
    /// The URL must take the form of `nats://server:port`.
    /// If the port is not specified it defaults to 4222.
    ///
    /// Use the `tls://` scheme to connect using TLS-first. With the `nats://`
    /// scheme the connection is upgraded to TLS if required by the server.
    ///
    /// [nats_url]: https://docs.nats.io/using-nats/developer/connecting#nats-url
    #[configurable(metadata(docs::examples = "nats://demo.nats.io"))]
    #[configurable(metadata(docs::examples = "nats://127.0.0.1:4242"))]
    #[configurable(metadata(docs::examples = "tls://demo.nats.io"))]
    pub(super) url: String,

    pub(super) auth: Option<NatsAuthConfig>,

    #[serde(default)]
    pub(super) request: TowerRequestConfig<NatsTowerRequestConfigDefaults>,

    /// Send messages using [Jetstream][jetstream].
    ///
    /// If set, the `subject` must belong to an existing JetStream stream.
    ///
    /// [jetstream]: https://docs.nats.io/nats-concepts/jetstream
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub(super) jetstream: JetStreamConfig,

    #[serde(flatten)]
    pub confinement: ConfinementConfig,
}

fn default_name() -> String {
    String::from("vector")
}

impl GenerateConfig for NatsSinkConfig {
    fn generate_config() -> serde_json::Value {
        serde_json::to_value(Self {
            acknowledgements: Default::default(),
            auth: None,
            connection_name: "vector".into(),
            encoding: JsonSerializerConfig::default().into(),
            subject: Template::try_from("from.vector").unwrap(),
            url: "nats://127.0.0.1:4222".into(),
            request: Default::default(),
            jetstream: JetStreamConfig {
                enabled: true,
                headers: Some(NatsHeaderConfig {
                    message_id: Some(UnconfinedTemplate::try_from("event-{{ event_id }}").unwrap()),
                }),
            },
            confinement: ConfinementConfig::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "nats")]
impl SinkConfig for NatsSinkConfig {
    fn confinement_config(&self) -> Option<&crate::template::ConfinementConfig> {
        Some(&self.confinement)
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type() & DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedNatsSink {
    subject: ConfinedTemplate,
    server_address: ServerAddr,
}

#[async_trait::async_trait]
impl ValidatedSink for NatsSinkConfig {
    type Validated = ValidatedNatsSink;

    fn validate(&self) -> crate::Result<ValidatedNatsSink> {
        let subject = self
            .subject
            .clone()
            .confine(&self.confinement, Self::NAME, "subject")?;
        let server_address = parse_server_address(&self.url).context(ConfigSnafu)?;
        self.encoding.validate()?;

        Ok(ValidatedNatsSink {
            subject,
            server_address,
        })
    }

    async fn build(
        &self,
        validated: &ValidatedNatsSink,
        _cx: SinkContext,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let ValidatedNatsSink {
            subject,
            server_address,
        } = validated.clone();
        let sink = NatsSink::new(self.clone(), subject, server_address.clone()).await?;
        let healthcheck = healthcheck(self.clone(), server_address).boxed();
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

impl NatsSinkConfig {
    fn client_builder(&self) -> Result<ClientBuilder, NatsError> {
        client_builder(&self.connection_name, &self.auth).context(ConfigSnafu)
    }

    pub(super) async fn connect(&self, server_address: ServerAddr) -> Result<Client, NatsError> {
        self.client_builder()?
            .connect(server_address)
            .await
            .context(ConnectSnafu)
    }

    async fn create_client(&self, server_address: ServerAddr) -> Result<Client, NatsError> {
        if cfg!(test) {
            self.connect(server_address).await
        } else {
            // Keep retrying the initial connection in the background.
            Ok(self.client_builder()?.connect_lazy(server_address))
        }
    }

    pub(super) async fn publisher(
        &self,
        server_address: ServerAddr,
    ) -> Result<NatsPublisher, NatsError> {
        let connection = self.create_client(server_address).await?;

        if self.jetstream.enabled {
            Ok(NatsPublisher::JetStream(JetstreamClient::new(connection)))
        } else {
            Ok(NatsPublisher::Core(connection))
        }
    }
}

async fn healthcheck(config: NatsSinkConfig, server_address: ServerAddr) -> crate::Result<()> {
    config.connect(server_address).await?.close().await;
    Ok(())
}

pub enum NatsPublisher {
    Core(Client),
    JetStream(JetstreamClient),
}

impl NatsPublisher {
    /// Gracefully close the connection, flushing any buffered messages.
    pub(super) async fn close(&self) {
        let client = match self {
            NatsPublisher::Core(client) => client,
            NatsPublisher::JetStream(jetstream) => jetstream.client(),
        };

        // `Client::close` drops the commands that are still queued in the client
        // and stalls until the connection times out. The server only replies to a
        // request after having processed every previous publish, so wait for one
        // to round-trip first. Nobody is expected to respond to it.
        if let Ok(response) = client
            .request(Subject::from_static("_VECTOR.flush"))
            .payload(Bytes::new())
            .await
        {
            drop(response.await);
        }
        client.close().await;
    }

    pub(super) async fn publish(
        &self,
        subject: String,
        headers: NatsHeaders,
        payload: Bytes,
    ) -> Result<(), NatsError> {
        let subject = Subject::try_from(subject).context(InvalidSubjectSnafu)?;

        match self {
            NatsPublisher::Core(client) => {
                client.publish(subject).payload(payload).await.map_err(|e| {
                    NatsError::PublishError {
                        source: Box::new(e),
                    }
                })
            }
            NatsPublisher::JetStream(jetstream) => {
                let mut publish = jetstream.publish(subject);
                if let Some(message_id) = &headers.message_id {
                    publish = publish.message_id(message_id);
                }
                publish
                    .payload(payload)
                    .await
                    .map(|_| ())
                    .map_err(|e| NatsError::PublishError {
                        source: Box::new(e),
                    })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ValidatedSink;
    use crate::template::{ConfinementConfig, Template};

    #[test]
    fn validate_rejects_malformed_url() {
        let config: NatsSinkConfig = serde_yaml::from_str(
            r#"
            subject: "test-subject"
            url: "not a valid url"
            encoding:
                codec: "json"
            "#,
        )
        .unwrap();
        assert!(
            config.validate().is_err(),
            "a malformed url should fail validation"
        );
    }

    #[test]
    fn validate_rejects_comma_separated_urls() {
        let config: NatsSinkConfig = serde_yaml::from_str(
            r#"
            subject: "test-subject"
            url: "nats://localhost:4222,nats://localhost:5222"
            encoding:
                codec: "json"
            "#,
        )
        .unwrap();
        assert!(
            config.validate().is_err(),
            "a comma-separated list of urls is not supported"
        );
    }

    #[test]
    fn validate_returns_confined_subject() {
        let config: NatsSinkConfig = serde_yaml::from_str(
            r#"
            subject: "test-subject"
            url: "nats://127.0.0.1:4222"
            encoding:
                codec: "json"
            "#,
        )
        .unwrap();
        let validated = config.validate().expect("validation should succeed");
        assert_eq!(validated.subject.to_string(), "test-subject");
    }

    #[test]
    fn confinement_rejects_unconfined_subject() {
        let template = Template::try_from("{{ subject }}").unwrap();
        let config = ConfinementConfig::default();
        let result = template.confine(&config, "nats", "subject");
        assert!(result.is_err());
    }

    #[test]
    fn confinement_opt_out_allows_unconfined_subject() {
        let template = Template::try_from("{{ subject }}").unwrap();
        let config = ConfinementConfig {
            dangerously_allow_unconfined_template_resolution: true,
        };
        let result = template.confine(&config, "nats", "subject");
        assert!(result.is_ok());
    }

    #[test]
    fn confinement_allows_prefixed_subject() {
        let template = Template::try_from("events-{{ env }}").unwrap();
        let config = ConfinementConfig::default();
        let result = template.confine(&config, "nats", "subject");
        assert!(result.is_ok());
    }
}
