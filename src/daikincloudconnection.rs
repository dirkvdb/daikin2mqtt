use std::{
    fs,
    path::{Path, PathBuf},
};

use openid::Token;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::MqttBridgeError;

struct OnectaProvider {
    auth_uri: Url,
    token_uri: Url,
}

impl openid::Provider for OnectaProvider {
    fn auth_uri(&self) -> &Url {
        &self.auth_uri
    }
    
    fn token_uri(&self) -> &Url {
        &self.token_uri
    }
}
impl OnectaProvider {
    fn new() -> Self {
        OnectaProvider {
            auth_uri: Url::parse("https://idp.onecta.daikineurope.com/v1/oidc/authorize").unwrap(),
            token_uri: Url::parse("https://idp.onecta.daikineurope.com/v1/oidc/token").unwrap(),
        }
    }
}

const ISSUER: &str = "https://idp.onecta.daikineurope.com";
const AUTH_API: &str = "https://idp.onecta.daikineurope.com/v1/";
const API: &str = "https://api.onecta.daikineurope.com/v1/";
const REDIRECT: &str = "onectaintegration://callback/";

pub struct DaikinCloudConnection {
    http_client: reqwest::Client,
    openid_client: openid::Client<OnectaProvider>,
    cloud_client_code: String,
    data_dir: std::path::PathBuf,
    token: Option<openid::Bearer>,
}

impl DaikinCloudConnection {
    pub fn new(
        client_id: String,
        client_secret: String,
        client_code: String,
        data_dir: &Path,
    ) -> DaikinCloudConnection {
        let token = DaikinCloudConnection::restore_authenticate_data(data_dir).unwrap_or_default();

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
            cloud_client_code: client_code,
            data_dir: data_dir.to_path_buf(),
            token,
        }
    }

    pub async fn update(&mut self) -> Result<(), MqttBridgeError> {
        log::info!("Updating cloud connection");

        if self.token.is_none() {
            log::info!("Obtain access token");
            self.obtain_access_token().await?;
            self.store_authenticate_data()?;
        } else {
            log::debug!("Token present: {}", self.token.as_ref().unwrap().access_token);
        }

        if let Some(token) = self.token.take() {
            self.token = Some(self.openid_client.ensure_token(token).await.unwrap());
        } else {
            log::info!("No token present");
            return Err(MqttBridgeError::NotAuhrorized);
        }

        self.update_status().await?;
        // match res {
        //     Ok(_) => log::info!("Status updated"),
        //     Err(MqttBridgeError::NotAuhrorized) => {
        //         log::info!("Refresh access token");
        //         self.refresh_access_token().await?;
        //         self.update_status().await?;
        //     }
        //     Err(err) => return Err(err),
        // }

        Ok(())
    }

    async fn obtain_access_token(&mut self) -> Result<(), MqttBridgeError> {
        self.token = Some(self.openid_client.request_token(&self.cloud_client_code).await.unwrap());
        log::info!("Access token obtained: {:?}", self.token);
        Ok(())
    }

    async fn update_status(&mut self) -> Result<(), MqttBridgeError> {
        // Perform a request to the cloud API to update the status of the devices
        let url = format!("{}gateway-devices", API);

        log::debug!("Updating status: {}", url);

        let res = self
            .http_client
            .get(&url)
            .header("Accept", "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", self.token.as_ref().unwrap().access_token),
            )
            .send()
            .await
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to obtain access token: {}", err)))?;

        if res.status() == StatusCode::UNAUTHORIZED {
            Err(MqttBridgeError::NotAuhrorized)
        } else if res.status().is_success() {
            let body = res
                .text()
                .await
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to read response: {}", err)))?;
            DaikinCloudConnection::process_status_json(
                serde_json::from_str(&body).map_err(|_| MqttBridgeError::ParseError)?,
            )?;
            Ok(())
        } else {
            Err(MqttBridgeError::NetworkError(format!(
                "Failed to obtain gateway devices: {} {}",
                res.status(),
                res.text().await.unwrap(),
            )))
        }
    }

    fn process_status_json(json: Value) -> Result<(), MqttBridgeError> {
        let nodes = json.as_array().ok_or(MqttBridgeError::ParseError)?;
        for node in nodes {
            // Process the status JSON and publish the MQTT messages
            let mangement_points = node["managementPoints"].as_array().ok_or(MqttBridgeError::ParseError)?;
            for mp in mangement_points {
                // ...
                if let Some(id) = mp["embeddedId"].as_str() {
                    log::info!("Management point: {}", id);
                } else {
                    log::info!("No id present");
                }
            }
        }

        Ok(())
    }

    fn authentication_data_path(base_dir: &Path) -> PathBuf {
        base_dir.join("openid_authentication.json")
    }

    fn store_authenticate_data(&self) -> Result<(), MqttBridgeError> {
        let path = DaikinCloudConnection::authentication_data_path(&self.data_dir);
        match &self.token {
            Some(token) =>  {
                let json_str = serde_json::to_string(&token)?;
                fs::create_dir_all(path.parent().unwrap())
                    .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to create directory: {}", err)))?;
                std::fs::write(path, json_str)
                    .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to write file: {}", err)))?;
            } None => {
                if let Err(err) = std::fs::remove_file(path) {
                    log::warn!("Failed to remove file: {}", err);
                }
            }
        }

        Ok(())
    }

    fn restore_authenticate_data(data_dir: &Path) -> Result<Option<openid::Bearer>, MqttBridgeError> {
        let path = DaikinCloudConnection::authentication_data_path(&data_dir);
        if path.exists() {
            let json_str = std::fs::read_to_string(&path)
                .map_err(|err| MqttBridgeError::RuntimeError(format!("Failed to read file: {}", err)))?;
            let token: openid::Bearer = serde_json::from_str(&json_str)
                .map_err(|err| MqttBridgeError::RuntimeError(format!("Failed to parse file: {}", err)))?;
            return Ok(Some(token));
        }

        Ok(None)
    }
}

// Test for parsing response json
#[cfg(test)]
mod tests {
    fn test_data_dir() -> std::path::PathBuf {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test");
        d
    }

    #[test_log::test]
    fn process_status_json() {
        let json = std::fs::read_to_string(test_data_dir().join("managementpoints.json")).unwrap();
        DaikinCloudConnection::process_status_json(serde_json::from_str(&json).unwrap()).unwrap();
    }

    use super::*;
}
