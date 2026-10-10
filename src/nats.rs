//! Shared helper functions for NATS source and sink.
#![allow(missing_docs)]

use snafu::{ResultExt, Snafu};
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};
use watermelon::{
    core::{AuthenticationMethod, ClientBuilder, error::CredsParseError},
    proto::{ServerAddr, error::ServerAddrError},
};

/// Errors that can occur during NATS configuration.
#[derive(Debug, Snafu)]
pub enum NatsConfigError {
    #[snafu(display("NATS Credentials file error: {}", source))]
    CredentialsFileError { source: std::io::Error },
    #[snafu(display("NATS Credentials parse error: {}", source))]
    CredentialsParseError { source: CredsParseError },
    #[snafu(display("NATS URL parse error: {}", source))]
    UrlParseError { source: ServerAddrError },
}

/// Configuration of the authentication strategy when interacting with NATS.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case", tag = "strategy")]
#[configurable(metadata(
    docs::enum_tag_description = "The strategy used to authenticate with the NATS server.

More information on NATS authentication, and the various authentication strategies, can be found in the
NATS [documentation][nats_auth_docs].

[nats_auth_docs]: https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro"
))]
pub enum NatsAuthConfig {
    /// Username/password authentication.
    UserPassword { user_password: NatsAuthUserPassword },

    /// Credentials file authentication. (JWT-based)
    CredentialsFile {
        credentials_file: NatsAuthCredentialsFile,
    },
}

impl std::fmt::Display for NatsAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use NatsAuthConfig::*;
        let word = match self {
            UserPassword { .. } => "user_password",
            CredentialsFile { .. } => "credentials_file",
        };
        write!(f, "{word}")
    }
}

/// Username and password configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct NatsAuthUserPassword {
    /// Username.
    pub(crate) user: String,

    /// Password.
    pub(crate) password: SensitiveString,
}

/// Credentials file configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct NatsAuthCredentialsFile {
    /// Path to credentials file.
    #[configurable(metadata(docs::examples = "/etc/nats/nats.creds"))]
    pub(crate) path: String,
}

impl NatsAuthConfig {
    pub(crate) fn to_authentication_method(&self) -> Result<AuthenticationMethod, NatsConfigError> {
        match self {
            NatsAuthConfig::UserPassword { user_password } => {
                Ok(AuthenticationMethod::UserAndPassword {
                    username: user_password.user.clone(),
                    password: user_password.password.inner().to_string(),
                })
            }
            NatsAuthConfig::CredentialsFile { credentials_file } => {
                let contents = std::fs::read_to_string(&credentials_file.path)
                    .context(CredentialsFileSnafu)?;
                AuthenticationMethod::from_creds(&contents).context(CredentialsParseSnafu)
            }
        }
    }
}

/// Parse the NATS server URL.
///
/// Use the `tls://` scheme to connect using TLS-first, or `nats://` to let the
/// server decide whether to upgrade the connection to TLS.
pub(crate) fn parse_server_address(url: &str) -> Result<ServerAddr, NatsConfigError> {
    url.parse().context(UrlParseSnafu)
}

pub(crate) fn client_builder(
    connection_name: &str,
    auth_config: &Option<NatsAuthConfig>,
) -> Result<ClientBuilder, NatsConfigError> {
    let auth_method = auth_config
        .as_ref()
        .map(NatsAuthConfig::to_authentication_method)
        .transpose()?;

    Ok(ClientBuilder::default()
        .client_name(connection_name)
        .authentication_method(auth_method))
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    fn parse_auth(s: &str) -> Result<AuthenticationMethod, crate::Error> {
        serde_yaml::from_str(s)
            .map_err(Into::into)
            .and_then(|config: NatsAuthConfig| {
                config.to_authentication_method().map_err(Into::into)
            })
    }

    #[test]
    fn auth_user_password_ok() {
        parse_auth(indoc! {r#"
            strategy: user_password
            user_password:
              user: username
              password: password
        "#})
        .unwrap();
    }

    #[test]
    fn auth_user_password_missing_user() {
        parse_auth(indoc! {r#"
            strategy: user_password
            user_password:
              password: password
        "#})
        .unwrap_err();
    }

    #[test]
    fn auth_user_password_missing_password() {
        parse_auth(indoc! {r#"
            strategy: user_password
            user_password:
              user: username
        "#})
        .unwrap_err();
    }

    #[test]
    fn auth_user_password_missing_all() {
        parse_auth(indoc! {r#"
            strategy: user_password
            token:
              value: foobar
        "#})
        .unwrap_err();
    }

    #[test]
    fn auth_credentials_file_ok() {
        parse_auth(indoc! {r#"
            strategy: credentials_file
            credentials_file:
              path: tests/integration/nats/data/nats.creds
        "#})
        .unwrap();
    }

    #[test]
    fn auth_credentials_file_missing() {
        parse_auth(indoc! {r#"
            strategy: credentials_file
            token:
              value: foobar
        "#})
        .unwrap_err();
    }

    #[test]
    fn auth_token_unsupported() {
        parse_auth(indoc! {r#"
            strategy: token
            token:
              value: token
        "#})
        .unwrap_err();
    }

    #[test]
    fn auth_nkey_unsupported() {
        parse_auth(indoc! {r#"
            strategy: nkey
            nkey:
              nkey: UC435ZYS52HF72E2VMQF4GO6CUJOCHDUUPEBU7XDXW5AQLIC6JZ46PO5
              seed: SUAAEZYNLTEA2MDTG7L5X7QODZXYHPOI2LT2KH5I4GD6YVP24SE766EGPA
        "#})
        .unwrap_err();
    }

    #[test]
    fn server_address_tls_first() {
        parse_server_address("tls://127.0.0.1:4222").unwrap();
    }

    #[test]
    fn server_address_rejects_multiple_urls() {
        parse_server_address("nats://localhost:4222,nats://localhost:5222").unwrap_err();
    }
}
