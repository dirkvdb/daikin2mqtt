use std::{
    fs,
    path::{Path, PathBuf},
};

use openid::Configurable;
use reqwest::{StatusCode, Url};
use serde::{Deserialize as _, Serialize};

use crate::MqttBridgeError;

const ISSUER: &str = "https://api.onecta.daikineurope.com";
const API: &str = "https://api.onecta.daikineurope.com/v1/";
const AUTH_URI: &str = "https://idp.onecta.daikineurope.com/v1/oidc/authorize";
const TOKEN_URI: &str = "https://idp.onecta.daikineurope.com/v1/oidc/token";
const REDIRECT: &str = "onectaintegration://callback/";

struct OnectaProvider {
    config: openid::Config,
}

impl Configurable for OnectaProvider {
    fn config(&self) -> &openid::Config {
        &self.config
    }
}

impl openid::Provider for OnectaProvider {
    fn auth_uri(&self) -> &Url {
        &self.config.authorization_endpoint
    }

    fn token_uri(&self) -> &Url {
        &self.config.token_endpoint
    }
}
impl OnectaProvider {
    fn new() -> Self {
        let config = openid::Config {
            issuer: Url::parse(ISSUER).unwrap(),
            authorization_endpoint: Url::parse(AUTH_URI).unwrap(),
            token_endpoint: Url::parse(TOKEN_URI).unwrap(),
            jwks_uri: Url::parse("https://idp.onecta.daikineurope.com/v1/oidc/jwks").unwrap(),
            response_types_supported: vec!["code".to_string()],
            subject_types_supported: vec!["public".to_string()],
            id_token_signing_alg_values_supported: vec!["RS256".to_string()],
            token_endpoint_auth_methods_supported: Some(vec!["client_secret_basic".to_string()]),
            scopes_supported: Some(vec!["openid".to_string()]),
            introspection_endpoint: None,
            userinfo_endpoint: None,
            end_session_endpoint: None,
            registration_endpoint: None,
            response_modes_supported: None,
            grant_types_supported: None,
            acr_values_supported: None,
            id_token_encryption_alg_values_supported: None,
            id_token_encryption_enc_values_supported: None,
            userinfo_signing_alg_values_supported: None,
            userinfo_encryption_alg_values_supported: None,
            userinfo_encryption_enc_values_supported: None,
            request_object_signing_alg_values_supported: None,
            request_object_encryption_alg_values_supported: None,
            request_object_encryption_enc_values_supported: None,
            token_endpoint_auth_signing_alg_values_supported: None,
            display_values_supported: None,
            claim_types_supported: None,
            claims_supported: None,
            service_documentation: None,
            claims_locales_supported: None,
            ui_locales_supported: None,
            claims_parameter_supported: false,
            request_parameter_supported: false,
            request_uri_parameter_supported: true,
            require_request_uri_registration: false,
            op_policy_uri: None,
            op_tos_uri: None,
            code_challenge_methods_supported: None,
        };

        OnectaProvider { config }
    }
}

fn token_serialize<S>(x: &Option<openid::TemporalBearerGuard>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match x {
        Some(token) => token.as_ref().serialize(s),
        None => s.serialize_none(),
    }
}

fn token_deserialize<'de, D>(d: D) -> Result<Option<openid::TemporalBearerGuard>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bearer = Option::<openid::Bearer>::deserialize(d)?;
    match bearer {
        Some(b) => Ok(Some(openid::TemporalBearerGuard::from(b))),
        None => Ok(None),
    }
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct AuthenticationData {
    #[serde(serialize_with = "token_serialize", deserialize_with = "token_deserialize")]
    token: Option<openid::TemporalBearerGuard>,
    auth_code: String,
}

pub struct DaikinCloudConnection {
    http_client: reqwest::Client,
    openid_client: openid::Client<OnectaProvider>,
    data_dir: std::path::PathBuf,
    authentication_data: AuthenticationData,
}

impl DaikinCloudConnection {
    pub fn new(client_id: String, client_secret: String, data_dir: &Path) -> DaikinCloudConnection {
        let authentication_data = DaikinCloudConnection::restore_authentication_data(data_dir).unwrap_or_default();

        let openid_client: openid::Client<_> = openid::Client::new(
            OnectaProvider::new(),
            client_id,
            client_secret,
            REDIRECT.to_string(),
            reqwest::Client::new(),
            None,
        );

        DaikinCloudConnection {
            http_client: reqwest::Client::new(),
            openid_client,
            data_dir: data_dir.to_path_buf(),
            authentication_data,
        }
    }

    fn auth_url(&self) -> Url {
        let options = openid::Options {
            scope: Some("openid onecta:basic.integration".to_string()),
            state: Some("123456".to_string()),
            nonce: None,
            display: None,
            prompt: None,
            max_age: None,
            ui_locales: None,
            claims_locales: None,
            id_token_hint: None,
            login_hint: None,
            acr_values: None,
        };

        self.openid_client.auth_url(&options)
    }

    pub fn set_authorization_code(&mut self, code: String) {
        self.clear_authentication_data();
        self.authentication_data.auth_code = code;
    }

    pub async fn update(&mut self) -> Result<String, MqttBridgeError> {
        log::info!("Updating cloud connection");

        if self.authentication_data.auth_code.is_empty() {
            return Err(MqttBridgeError::AuthorizationRequired(self.auth_url()));
        }

        if self.authentication_data.token.is_none() {
            log::info!("Obtain access token");
            match self.obtain_access_token().await {
                Ok(_) => log::debug!("Token obtained"),
                Err(MqttBridgeError::NotAuhrorized) => {
                    return Err(MqttBridgeError::AuthorizationRequired(self.auth_url()))
                }
                Err(err) => return Err(err),
            }
            self.store_authentication_data()?;
        } else {
            log::debug!(
                "Token present: {}",
                self.authentication_data.token.as_ref().unwrap().as_ref().access_token
            );
        }

        if let Some(token) = self.authentication_data.token.take() {
            self.authentication_data.token = Some(self.openid_client.ensure_token(token).await.unwrap());
        } else {
            log::info!("No token present");
            return Err(MqttBridgeError::NotAuhrorized);
        }

        Ok(self.update_status().await?)
    }

    async fn obtain_access_token(&mut self) -> Result<(), MqttBridgeError> {
        match self
            .openid_client
            .request_token(&self.authentication_data.auth_code)
            .await
        {
            Ok(token) => {
                self.authentication_data.token = Some(token.into());
                log::info!("Access token obtained: {:?}", self.authentication_data.token);
                Ok(())
            }
            Err(err) => {
                log::error!("Failed to obtain access token: {}", err);
                Err(MqttBridgeError::NotAuhrorized)
            }
        }
    }

    async fn update_status(&mut self) -> Result<String, MqttBridgeError> {
        // Perform a request to the cloud API to update the status of the devices
        let url = format!("{}gateway-devices", API);

        log::debug!("Updating status: {}", url);

        let res = self
            .http_client
            .get(&url)
            .header("Accept", "application/json")
            .header(
                "Authorization",
                format!(
                    "Bearer {}",
                    self.authentication_data.token.as_ref().unwrap().as_ref().access_token
                ),
            )
            .send()
            .await
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to obtain access token: {}", err)))?;

        if res.status() == StatusCode::UNAUTHORIZED {
            Err(MqttBridgeError::AuthorizationRequired(self.auth_url()))
        } else if res.status().is_success() {
            let body = res
                .text()
                .await
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to read response: {}", err)))?;
            Ok(body)
        } else {
            Err(MqttBridgeError::NetworkError(format!(
                "Failed to obtain gateway devices: {} {}",
                res.status(),
                res.text().await.unwrap(),
            )))
        }
    }

    fn authentication_data_path(base_dir: &Path) -> PathBuf {
        base_dir.join("openid_authentication.json")
    }

    fn clear_authentication_data(&mut self) {
        self.authentication_data = AuthenticationData::default();
        if let Err(err) = std::fs::remove_file(DaikinCloudConnection::authentication_data_path(&self.data_dir)) {
            log::warn!("Failed to remove file: {}", err);
        }
    }

    fn store_authentication_data(&mut self) -> Result<(), MqttBridgeError> {
        let path = DaikinCloudConnection::authentication_data_path(&self.data_dir);
        if !self.authentication_data.auth_code.is_empty() {
            let json_str = serde_json::to_string(&self.authentication_data)?;
            fs::create_dir_all(path.parent().unwrap())
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to create directory: {}", err)))?;
            std::fs::write(path, json_str)
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to write file: {}", err)))?;
        } else {
            self.clear_authentication_data();
        }

        Ok(())
    }

    fn restore_authentication_data(data_dir: &Path) -> Result<AuthenticationData, MqttBridgeError> {
        let path = DaikinCloudConnection::authentication_data_path(&data_dir);
        if path.exists() {
            let json_str = std::fs::read_to_string(&path)
                .map_err(|err| MqttBridgeError::RuntimeError(format!("Failed to read file: {}", err)))?;
            let auth_data: AuthenticationData = serde_json::from_str(&json_str)
                .map_err(|err| MqttBridgeError::RuntimeError(format!("Failed to parse file: {}", err)))?;
            return Ok(auth_data);
        }

        Ok(AuthenticationData::default())
    }
}
