use std::{
    fs,
    path::{Path, PathBuf},
};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::MqttBridgeError;

const AUTH_API: &str = "https://idp.onecta.daikineurope.com/v1/";
const API: &str = "https://api.onecta.daikineurope.com/v1/";

#[derive(Default, Serialize, Deserialize)]
struct AuthenticationData {
    access_token: String,
    refresh_token: String,
}

impl AuthenticationData {
    fn is_valid(&self) -> bool {
        !self.access_token.is_empty()
    }
}

pub struct DaikinCloudConnection {
    client: reqwest::Client,
    cloud_client_id: String,
    cloud_client_secret: String,
    cloud_client_code: String,
    openid_authentication: AuthenticationData,
    data_dir: std::path::PathBuf,
}

impl DaikinCloudConnection {
    pub fn new(
        client_id: String,
        client_secret: String,
        client_code: String,
        data_dir: &Path,
    ) -> DaikinCloudConnection {
        let authentication_data = DaikinCloudConnection::restore_authenticate_data(data_dir).unwrap_or_else(|err| {
            log::warn!("Failed to restore authentication data: {}", err);
            AuthenticationData::default()
        });

        DaikinCloudConnection {
            client: reqwest::Client::new(),
            cloud_client_id: client_id,
            cloud_client_secret: client_secret,
            cloud_client_code: client_code,
            openid_authentication: authentication_data,
            data_dir: data_dir.to_path_buf(),
        }
    }

    pub async fn update(&mut self) -> Result<(), MqttBridgeError> {
        log::info!("Updating cloud connection");

        if !self.openid_authentication.is_valid() {
            log::info!("Obtain access token");
            self.obtain_access_token().await?;

            log::info!("Access token: '{}'", self.openid_authentication.access_token);
            log::info!("Refresh token: '{}'", self.openid_authentication.refresh_token);
        }

        let res = self.update_status().await;
        match res {
            Ok(_) => log::info!("Status updated"),
            Err(MqttBridgeError::NotAuhrorized) => {
                log::info!("Refresh access token");
                self.refresh_access_token().await?;
                self.update_status().await?;
            }
            Err(err) => return Err(err),
        }

        Ok(())
    }

    async fn obtain_access_token(&mut self) -> Result<(), MqttBridgeError> {
        let url = format!("{}oidc/token", AUTH_API);

        log::debug!("Obtaining access token Id={}", &self.cloud_client_id,);
        log::debug!("Obtaining access token Secret={}", &self.cloud_client_secret,);
        log::debug!("Obtaining access token Code={}", &self.cloud_client_code,);

        let params = [
            ("grant_type", "authorization_code"),
            ("client_id", &self.cloud_client_id),
            ("client_secret", &self.cloud_client_secret),
            ("code", &self.cloud_client_code),
            ("redirect_uri", "onectaintegration://callback/"),
        ];

        let res = self
            .client
            .post(&url)
            .query(&params)
            .send()
            .await
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to obtain access token: {}", err)))?;

        if res.status().is_success() {
            let body = res
                .text()
                .await
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to read response: {}", err)))?;
            let json: Value = serde_json::from_str(&body).map_err(|_| MqttBridgeError::ParseError)?;
            self.openid_authentication.access_token = json["access_token"]
                .as_str()
                .ok_or(MqttBridgeError::ParseError)?
                .to_string();
            self.openid_authentication.refresh_token = json["refresh_token"]
                .as_str()
                .ok_or(MqttBridgeError::ParseError)?
                .to_string();
            self.store_authenticate_data()?;
            Ok(())
        } else {
            Err(MqttBridgeError::NetworkError(format!(
                "Failed to obtain access token: {} ({})",
                res.status(),
                res.text().await.unwrap(),
            )))
        }
    }

    async fn refresh_access_token(&mut self) -> Result<(), MqttBridgeError> {
        let url = format!("{}oidc/token", AUTH_API);

        let params = [
            ("grant_type", "refresh_token"),
            ("client_id", &self.cloud_client_id),
            ("client_secret", &self.cloud_client_secret),
            ("refresh_token", &self.openid_authentication.refresh_token),
        ];

        let res = self
            .client
            .post(&url)
            .query(&params)
            .send()
            .await
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to obtain access token: {}", err)))?;

        if res.status().is_success() {
            let body = res
                .text()
                .await
                .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to read response: {}", err)))?;
            let json: Value = serde_json::from_str(&body).map_err(|_| MqttBridgeError::ParseError)?;
            self.openid_authentication.access_token = json["access_token"]
                .as_str()
                .ok_or(MqttBridgeError::ParseError)?
                .to_string();
            self.openid_authentication.refresh_token = json["refresh_token"]
                .as_str()
                .ok_or(MqttBridgeError::ParseError)?
                .to_string();
            self.store_authenticate_data()?;
            Ok(())
        } else {
            Err(MqttBridgeError::NetworkError(format!(
                "Failed to refresh access token: {} {}",
                res.status(),
                res.text().await.unwrap(),
            )))
        }
    }

    async fn update_status(&mut self) -> Result<(), MqttBridgeError> {
        // Perform a request to the cloud API to update the status of the devices
        let url = format!("{}gateway-devices", API);

        log::debug!("Updating status: {}", url);

        let res = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", self.openid_authentication.access_token),
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
        let json_str = serde_json::to_string(&self.openid_authentication)?;
        let path = DaikinCloudConnection::authentication_data_path(&self.data_dir);
        fs::create_dir_all(path.parent().unwrap())
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to create directory: {}", err)))?;
        std::fs::write(path, json_str)
            .map_err(|err| MqttBridgeError::NetworkError(format!("Failed to write file: {}", err)))?;
        Ok(())
    }

    fn restore_authenticate_data(data_dir: &Path) -> Result<AuthenticationData, MqttBridgeError> {
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
