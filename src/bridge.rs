use std::os::macos::raw::stat;

use crate::daikincloudconnection::DaikinCloudConnection;
use crate::mqtt::{MqttConfig, MqttConnection, MqttData};
use crate::MqttBridgeError;
use log;
use serde::Serialize;
use serde_json::{to_string, Value};
use tokio::time;

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
    pub hass_discovery: bool,
    pub cloud_client_code: String,
    pub poll_interval: time::Duration,
    pub data_dir: std::path::PathBuf,
}

pub struct DaikinMqttBridge {
    mqtt: MqttConnection,
    mqtt_base_topic: String,
    hass_discovery: bool,
    cloud_client_code: String,
    poll_interval: time::Duration,
    data_dir: std::path::PathBuf,
}

impl DaikinMqttBridge {
    pub fn new(cfg: DaikinMqttBridgeConfig) -> DaikinMqttBridge {
        let mqtt_base_topic = format!("{}/", cfg.mqtt_config.base_topic);
        DaikinMqttBridge {
            mqtt: MqttConnection::new(cfg.mqtt_config),
            mqtt_base_topic,
            hass_discovery: cfg.hass_discovery,
            cloud_client_code: cfg.cloud_client_code,
            poll_interval: cfg.poll_interval,
            data_dir: cfg.data_dir,
        }
    }

    pub async fn run(mut self) -> Result<(), MqttBridgeError> {
        let mut interval = time::interval(self.poll_interval);

        let client_id = "emU20GdJDiiUxI_HnFGz69dD".to_string();
        let client_secret =
            "TNL1ePwnOkf6o2gKiI8InS8nVwTz2G__VYkv6WznzJGUnwLHLTmKYp-7RZc6FA3yS6D0Wgj_snvqsU5H_LPHQA".to_string();

        let mut connection = DaikinCloudConnection::new(
            client_id,
            client_secret,
            self.cloud_client_code.clone(),
            &self.data_dir,
        );

        loop {
            tokio::select! {
                mqtt_msg = self.mqtt.poll() => {
                    if let Ok(Some(msg)) = mqtt_msg {
                        log::debug!("MQTT cmnd: {} {}", msg.topic, msg.payload);
                        // if let Err(err) = self.handle_node_command(&msg).await {
                        //     log::error!("Failed to process command: {}: {} ({err})", msg.topic, msg.payload);
                        // }
                    }
                }
                _ = interval.tick() => {
                    log::info!("Poll cloud");
                    match connection.update().await {
                        Ok(status_json) => {
                            match DaikinMqttBridge::process_status_json(status_json) {
                                Ok(status) => {self.publish_status(&status).await?;}
                                Err(err) => {log::error!("Failed to process status: {}", err);}
                            }
                        }
                        Err(err) => {
                            log::error!("Failed to update cloud status: {err}");
                        }
                    }
                }
            }
        }
    }

    async fn publish_status(&mut self, status: &DaikinStatus) -> Result<(), MqttBridgeError> {
        let climate_control_json = to_string(&status.climate_control)?;
        let domestic_hot_water_tank_json = to_string(&status.domestic_hot_water_tank)?;
        self.mqtt
            .publish(MqttData::new(
                format!("{}climate_control", self.mqtt_base_topic),
                climate_control_json,
            ))
            .await?;

        self.mqtt
            .publish(MqttData::new(
                format!("{}domestic_hot_water", self.mqtt_base_topic),
                domestic_hot_water_tank_json,
            ))
            .await
    }

    fn process_status_json(json_string: String) -> Result<DaikinStatus, MqttBridgeError> {
        log::info!("Processing status JSON: {}", json_string);
        let mut status = DaikinStatus::default();

        let json: Value = serde_json::from_str(&json_string)?;
        let nodes = json.as_array().ok_or(MqttBridgeError::ParseError)?;
        for node in nodes {
            // Process the status JSON and publish the MQTT messages
            let mangement_points = node["managementPoints"]
                .as_array()
                .ok_or(MqttBridgeError::ParseError)?;
            for mp in mangement_points {
                // ...
                if let Some(id) = mp["embeddedId"].as_str() {
                    log::info!("Management point: {}", id);
                    match id {
                        "climateControlMainZone" => {
                            log::info!("climateControlMainZone");
                            status.climate_control.holiday_mode_active =
                                parse_bool(&mp["isHolidayModeActive"]["value"])?;
                            status.climate_control.is_in_warning_state =
                                parse_bool(&mp["isInWarningState"]["value"])?;
                            status.climate_control.is_in_error_state =
                                parse_bool(&mp["isInErrorState"]["value"])?;
                            status.climate_control.operation_mode =
                                parse_str(&mp["operationMode"]["value"])?;
                            status.climate_control.on_off_mode =
                                parse_str(&mp["onOffMode"]["value"])?;
                            status.climate_control.leaving_water_temperature = parse_i8(
                                &mp["sensoryData"]["value"]["leavingWaterTemperature"]["value"],
                            )?;
                            status.climate_control.outdoor_temperature = parse_i8(
                                &mp["sensoryData"]["value"]["outdoorTemperature"]["value"],
                            )?;
                            status.climate_control.room_temperature =
                                parse_f32(&mp["sensoryData"]["value"]["roomTemperature"]["value"])?;
                            status.climate_control.heating_setpoint_leaving_water_offset =
                                parse_i8(
                                    &mp["temperatureControl"]["value"]["operationModes"]["heating"]
                                        ["setpoints"]["leavingWaterOffset"]["value"],
                                )?;
                            status.climate_control.heating_setpoint_room_temperature = parse_i8(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]
                                    ["setpoints"]["roomTemperature"]["value"],
                            )?;
                        }
                        "domesticHotWaterTank" => {
                            log::info!("domesticHotWaterTank");
                            status.domestic_hot_water_tank.is_in_warning_state =
                                parse_bool(&mp["isInWarningState"]["value"])?;
                            status.domestic_hot_water_tank.is_in_error_state =
                                parse_bool(&mp["isInErrorState"]["value"])?;
                            status.domestic_hot_water_tank.on_off_mode =
                                parse_str(&mp["onOffMode"]["value"])?;
                            status.domestic_hot_water_tank.operation_mode =
                                parse_str(&mp["operationMode"]["value"])?;
                            status.domestic_hot_water_tank.water_temperature =
                                parse_i8(&mp["sensoryData"]["value"]["tankTemperature"]["value"])?;
                            status.domestic_hot_water_tank.water_setpoint = parse_i8(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]
                                    ["setpoints"]["domesticHotWaterTemperature"]["value"],
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
    value
        .as_i64()
        .ok_or(MqttBridgeError::ParseError)
        .map(|v| v as i8)
}

fn parse_f32(value: &Value) -> Result<f32, MqttBridgeError> {
    value
        .as_f64()
        .ok_or(MqttBridgeError::ParseError)
        .map(|v| v as f32)
}

fn parse_str(value: &Value) -> Result<String, MqttBridgeError> {
    value
        .as_str()
        .ok_or(MqttBridgeError::ParseError)
        .map(|v| v.to_string())
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
        assert_eq!(
            daikin_status
                .climate_control
                .heating_setpoint_leaving_water_offset,
            5
        );
        assert_eq!(
            daikin_status
                .climate_control
                .heating_setpoint_room_temperature,
            19
        );
        assert_eq!(daikin_status.climate_control.is_in_error_state, false);
        assert_eq!(daikin_status.climate_control.is_in_warning_state, false);

        assert_eq!(daikin_status.domestic_hot_water_tank.on_off_mode, "on");
        assert_eq!(
            daikin_status.domestic_hot_water_tank.operation_mode,
            "heating"
        );
        assert_eq!(daikin_status.domestic_hot_water_tank.water_temperature, 54);
        assert_eq!(daikin_status.domestic_hot_water_tank.water_setpoint, 45);
        assert_eq!(daikin_status.domestic_hot_water_tank.powerfull_mode, false);
        assert_eq!(
            daikin_status.domestic_hot_water_tank.is_in_error_state,
            false
        );
        assert_eq!(
            daikin_status.domestic_hot_water_tank.is_in_warning_state,
            false
        );
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
