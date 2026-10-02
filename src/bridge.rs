use crate::daikincloudconnection::DaikinCloudConnection;
use crate::mqtt::{MqttConfig, MqttConnection, MqttData};
use crate::MqttBridgeError;
use log;

use serde::Serialize;
use serde_json::{to_string, Value};
use std::sync::Arc;
use tokio::{
    sync::{Mutex, Notify},
    time,
};

use crate::web;

//const HASS_DISCOVERY_TOPIC: &str = "homeassistant";

#[derive(Default, Serialize)]
struct ClimateControl {
    holiday_mode_active: bool,
    is_in_warning_state: bool,
    is_in_error_state: bool,
    on_off_mode: String,
    operation_mode: String,
    leaving_water_temperature: i8,
    outdoor_temperature: i8,
    room_temperature: f32,
    heating_setpoint_leaving_water_offset: i8,
    heating_setpoint_room_temperature: i8,
}

#[derive(Default, Serialize)]
struct DomesticHotWaterTank {
    is_in_warning_state: bool,
    is_in_error_state: bool,
    on_off_mode: String,
    operation_mode: String,
    water_temperature: i8,
    water_setpoint: i8,
    powerfull_mode: bool,
}

#[derive(Default)]
struct DaikinStatus {
    climate_control: ClimateControl,
    domestic_hot_water_tank: DomesticHotWaterTank,
}

pub struct DaikinMqttBridgeConfig {
    pub mqtt_config: MqttConfig,
    pub cloud_client_id: String,
    pub cloud_client_secret: String,
    pub cloud_redirect_uri: String,
    pub web_listen: std::net::SocketAddr,
    //pub hass_discovery: bool,
    pub poll_interval: time::Duration,
    pub data_dir: std::path::PathBuf,
}

#[derive(Default)]
struct MqttStatus {
    online: bool,
}

pub struct DaikinMqttBridge {
    mqtt: MqttConnection,
    cloud_client_id: String,
    cloud_client_secret: String,
    cloud_redirect_uri: String,
    web_listen: std::net::SocketAddr,
    //hass_discovery: bool,
    poll_interval: time::Duration,
    data_dir: std::path::PathBuf,
    status: MqttStatus,
}

impl DaikinMqttBridge {
    pub fn new(cfg: DaikinMqttBridgeConfig) -> DaikinMqttBridge {
        DaikinMqttBridge {
            mqtt: MqttConnection::new(cfg.mqtt_config),
            cloud_client_id: cfg.cloud_client_id,
            cloud_client_secret: cfg.cloud_client_secret,
            cloud_redirect_uri: cfg.cloud_redirect_uri,
            web_listen: cfg.web_listen,
            //hass_discovery: cfg.hass_discovery,
            poll_interval: cfg.poll_interval,
            data_dir: cfg.data_dir,
            status: MqttStatus::default(),
        }
    }

    pub async fn run(self) -> Result<(), MqttBridgeError> {
        let connection = Arc::new(Mutex::new(DaikinCloudConnection::new(
            self.cloud_client_id.clone(),
            self.cloud_client_secret.clone(),
            self.cloud_redirect_uri.clone(),
            &self.data_dir,
        )?));
        let refresh = Arc::new(Notify::new());
        let web_state = web::WebState::new(
            connection.clone(),
            refresh.clone(),
            self.cloud_redirect_uri.clone(),
            self.web_listen,
        )?;
        let listener = tokio::net::TcpListener::bind(self.web_listen).await?;
        log::info!("Authentication UI listening on http://{}", listener.local_addr()?);
        log::info!("Registered OAuth callback: {}", self.cloud_redirect_uri);

        // Drive the HTTP server independently of MQTT connectivity and cloud polling.
        tokio::try_join!(
            async {
                axum::serve(listener, web::router(web_state))
                    .await
                    .map_err(MqttBridgeError::from)
            },
            self.run_bridge(connection, refresh),
        )?;
        Ok(())
    }

    async fn run_bridge(
        mut self,
        connection: Arc<Mutex<DaikinCloudConnection>>,
        refresh: Arc<Notify>,
    ) -> Result<(), MqttBridgeError> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            tokio::select! {
                mqtt_msg = self.mqtt.poll() => {
                    if let Ok(Some(msg)) = mqtt_msg {
                        // Authentication commands are deliberately no longer accepted over MQTT.
                        log::warn!("Ignored MQTT command: {}", msg.topic);
                    }
                }
                _ = interval.tick() => {
                    if let Err(err) = self.poll_cloud(&connection).await {
                        log::error!("Failed to poll cloud: ({err})");
                    }
                }
                _ = refresh.notified() => {
                    if let Err(err) = self.poll_cloud(&connection).await {
                        log::error!("Failed to poll cloud: ({err})");
                    }
                }
            }
        }
    }

    async fn poll_cloud(&mut self, connection: &Mutex<DaikinCloudConnection>) -> Result<(), MqttBridgeError> {
        // Release the cloud lock before publishing, which may wait for the MQTT broker.
        let result = connection.lock().await.update().await;
        match result {
            Ok(status_json) => {
                self.update_online_status(true).await?;
                self.status.online = true;
                match DaikinMqttBridge::process_status_json(status_json) {
                    Ok(status) => {
                        self.update_heatpump_status(status).await?;
                    }
                    Err(err) => {
                        log::error!("Failed to process status: {}", err);
                    }
                }
            }
            Err(MqttBridgeError::AuthorizationRequired(_)) => {
                self.update_online_status(false).await?;
            }
            Err(err) => {
                self.update_online_status(false).await?;
                log::error!("Failed to update cloud status: {err}");
            }
        }

        Ok(())
    }

    async fn update_online_status(&mut self, online: bool) -> Result<(), MqttBridgeError> {
        if online != self.status.online {
            if online {
                self.mqtt.publish_online().await?;
            } else {
                self.mqtt.publish_offline().await?;
            }

            self.status.online = online;
        }

        Ok(())
    }

    async fn update_heatpump_status(&mut self, status: DaikinStatus) -> Result<(), MqttBridgeError> {
        let climate_control_json = to_string(&status.climate_control)?;
        let domestic_hot_water_tank_json = to_string(&status.domestic_hot_water_tank)?;
        self.mqtt
            .publish(
                MqttData::new(
                    format!("{}/climate_control", self.mqtt.base_topic()),
                    climate_control_json,
                ),
                true,
            )
            .await?;

        self.mqtt
            .publish(
                MqttData::new(
                    format!("{}/domestic_hot_water", self.mqtt.base_topic()),
                    domestic_hot_water_tank_json,
                ),
                true,
            )
            .await
    }

    fn process_status_json(json_string: String) -> Result<DaikinStatus, MqttBridgeError> {
        let mut status = DaikinStatus::default();

        let json: Value = serde_json::from_str(&json_string)?;
        let nodes = json.as_array().ok_or(MqttBridgeError::ParseError)?;
        for node in nodes {
            // Process the status JSON and publish the MQTT messages
            let mangement_points = node["managementPoints"].as_array().ok_or(MqttBridgeError::ParseError)?;
            for mp in mangement_points {
                // ...
                if let Some(id) = mp["embeddedId"].as_str() {
                    log::debug!("Management point: {}", id);
                    match id {
                        "climateControlMainZone" => {
                            log::info!("climateControlMainZone");
                            status.climate_control.holiday_mode_active =
                                parse_bool(&mp["isHolidayModeActive"]["value"])?;
                            status.climate_control.is_in_warning_state = parse_bool(&mp["isInWarningState"]["value"])?;
                            status.climate_control.is_in_error_state = parse_bool(&mp["isInErrorState"]["value"])?;
                            status.climate_control.operation_mode = parse_str(&mp["operationMode"]["value"])?;
                            status.climate_control.on_off_mode = parse_str(&mp["onOffMode"]["value"])?;
                            status.climate_control.leaving_water_temperature =
                                parse_i8(&mp["sensoryData"]["value"]["leavingWaterTemperature"]["value"])?;
                            status.climate_control.outdoor_temperature =
                                parse_i8(&mp["sensoryData"]["value"]["outdoorTemperature"]["value"])?;
                            status.climate_control.room_temperature =
                                parse_f32(&mp["sensoryData"]["value"]["roomTemperature"]["value"])?;
                            status.climate_control.heating_setpoint_leaving_water_offset = parse_i8(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]
                                    ["leavingWaterOffset"]["value"],
                            )?;
                            status.climate_control.heating_setpoint_room_temperature = parse_i8(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]
                                    ["roomTemperature"]["value"],
                            )?;
                        }
                        "domesticHotWaterTank" => {
                            log::info!("domesticHotWaterTank");
                            status.domestic_hot_water_tank.is_in_warning_state =
                                parse_bool(&mp["isInWarningState"]["value"])?;
                            status.domestic_hot_water_tank.is_in_error_state =
                                parse_bool(&mp["isInErrorState"]["value"])?;
                            status.domestic_hot_water_tank.on_off_mode = parse_str(&mp["onOffMode"]["value"])?;
                            status.domestic_hot_water_tank.operation_mode = parse_str(&mp["operationMode"]["value"])?;
                            status.domestic_hot_water_tank.water_temperature =
                                parse_i8(&mp["sensoryData"]["value"]["tankTemperature"]["value"])?;
                            status.domestic_hot_water_tank.water_setpoint = parse_i8(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]
                                    ["domesticHotWaterTemperature"]["value"],
                            )?;
                            status.domestic_hot_water_tank.powerfull_mode =
                                parse_bool(&mp["isPowerfulModeActive"]["value"])?;
                        }
                        _ => {
                            log::info!("Ignored management point: {}", id);
                        }
                    }
                } else {
                    log::info!("No id present");
                }
            }
        }

        Ok(status)
    }
}

fn parse_i8(value: &Value) -> Result<i8, MqttBridgeError> {
    value.as_i64().ok_or(MqttBridgeError::ParseError).map(|v| v as i8)
}

fn parse_f32(value: &Value) -> Result<f32, MqttBridgeError> {
    value.as_f64().ok_or(MqttBridgeError::ParseError).map(|v| v as f32)
}

fn parse_str(value: &Value) -> Result<String, MqttBridgeError> {
    value.as_str().ok_or(MqttBridgeError::ParseError).map(|v| v.to_string())
}

fn parse_bool(value: &Value) -> Result<bool, MqttBridgeError> {
    value.as_bool().ok_or(MqttBridgeError::ParseError)
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
        let daikin_status = DaikinMqttBridge::process_status_json(json).unwrap();

        assert_eq!(daikin_status.climate_control.holiday_mode_active, false);
        assert_eq!(daikin_status.climate_control.on_off_mode, "on");
        assert_eq!(daikin_status.climate_control.operation_mode, "heating");
        assert_eq!(daikin_status.climate_control.leaving_water_temperature, 56);
        assert_eq!(daikin_status.climate_control.outdoor_temperature, 14);
        assert_eq!(daikin_status.climate_control.room_temperature, 21.0);
        assert_eq!(daikin_status.climate_control.heating_setpoint_leaving_water_offset, 5);
        assert_eq!(daikin_status.climate_control.heating_setpoint_room_temperature, 19);
        assert_eq!(daikin_status.climate_control.is_in_error_state, false);
        assert_eq!(daikin_status.climate_control.is_in_warning_state, false);

        assert_eq!(daikin_status.domestic_hot_water_tank.on_off_mode, "on");
        assert_eq!(daikin_status.domestic_hot_water_tank.operation_mode, "heating");
        assert_eq!(daikin_status.domestic_hot_water_tank.water_temperature, 54);
        assert_eq!(daikin_status.domestic_hot_water_tank.water_setpoint, 45);
        assert_eq!(daikin_status.domestic_hot_water_tank.powerfull_mode, false);
        assert_eq!(daikin_status.domestic_hot_water_tank.is_in_error_state, false);
        assert_eq!(daikin_status.domestic_hot_water_tank.is_in_warning_state, false);
    }

    #[test_log::test]
    fn process_status_json2() {
        let json = std::fs::read_to_string(test_data_dir().join("managementpoints2.json")).unwrap();
        let daikin_status = DaikinMqttBridge::process_status_json(json).unwrap();
        assert_eq!(daikin_status.domestic_hot_water_tank.on_off_mode, "on");
        assert_eq!(daikin_status.climate_control.room_temperature, 20.3);
    }

    use std::path::PathBuf;

    use super::*;
}
