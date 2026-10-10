//! `NATS` sink
//! Publishes data using [NATS](nats.io)(Neural Autonomic Transport System).

use snafu::Snafu;

use crate::nats::NatsConfigError;

mod config;
#[cfg(feature = "nats-integration-tests")]
#[cfg(test)]
mod integration_tests;
mod request_builder;
mod service;
mod sink;
#[cfg(test)]
mod tests;

#[derive(Debug, Snafu)]
enum NatsError {
    #[snafu(display("invalid encoding: {}", source))]
    Encoding {
        source: vector_lib::codecs::encoding::BuildError,
    },
    #[snafu(display("NATS Config Error: {}", source))]
    Config { source: NatsConfigError },
    #[snafu(display("NATS Connect Error: {}", source))]
    Connect {
        source: watermelon::core::error::ConnectHandlerError,
    },
    #[snafu(display("Invalid NATS subject: {}", source))]
    InvalidSubject {
        source: watermelon::proto::error::SubjectValidateError,
    },
    #[snafu(display("NATS Publish Error: {}", source))]
    PublishError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
