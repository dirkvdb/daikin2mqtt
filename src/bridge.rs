use crate::commands;
use crate::daikincloudconnection::DaikinCloudConnection;
use crate::hassdiscovery;
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
    heating_setpoint_room_temperature: f32,
}

#[derive(Default, Serialize)]
struct DomesticHotWaterTank {
    is_in_warning_state: bool,
    is_in_error_state: bool,
    on_off_mode: String,
    operation_mode: String,
    water_temperature: i8,
    water_setpoint: f32,
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
    pub hass_discovery: bool,
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
    hass_discovery: bool,
    poll_interval: time::Duration,
    data_dir: std::path::PathBuf,
    status: MqttStatus,
    devices: Value,
}

impl DaikinMqttBridge {
    pub fn new(cfg: DaikinMqttBridgeConfig) -> DaikinMqttBridge {
        DaikinMqttBridge {
            mqtt: MqttConnection::new(cfg.mqtt_config),
            cloud_client_id: cfg.cloud_client_id,
            cloud_client_secret: cfg.cloud_client_secret,
            cloud_redirect_uri: cfg.cloud_redirect_uri,
            web_listen: cfg.web_listen,
            hass_discovery: cfg.hass_discovery,
            poll_interval: cfg.poll_interval,
            data_dir: cfg.data_dir,
            status: MqttStatus::default(),
            devices: Value::Null,
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
        let web_state = web::WebState::new(connection.clone(), refresh.clone(), self.cloud_redirect_uri.clone(), self.web_listen)?;
        let listener = tokio::net::TcpListener::bind(self.web_listen).await?;
        log::info!("Authentication UI listening on http://{}", listener.local_addr()?);
        log::info!("Registered OAuth callback: {}", self.cloud_redirect_uri);

        // Drive the HTTP server independently of MQTT connectivity and cloud polling.
        tokio::try_join!(
            async { axum::serve(listener, web::router(web_state)).await.map_err(MqttBridgeError::from) },
            self.run_bridge(connection, refresh),
        )?;
        Ok(())
    }

    async fn run_bridge(mut self, connection: Arc<Mutex<DaikinCloudConnection>>, refresh: Arc<Notify>) -> Result<(), MqttBridgeError> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            tokio::select! {
                mqtt_msg = self.mqtt.poll() => {
                    if let Ok(Some(msg)) = mqtt_msg {
                        if let Err(err) = self.handle_command(&connection, &refresh, &msg).await {
                            log::warn!("MQTT command {} failed: {err}", msg.topic);
                        }
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

    async fn handle_command(&mut self, connection: &Mutex<DaikinCloudConnection>, refresh: &Notify, message: &MqttData) -> Result<(), MqttBridgeError> {
        let updates = commands::updates(self.mqtt.base_topic(), message, &self.devices)?;
        let mut cloud = connection.lock().await;
        for update in updates {
            let result = cloud
                .set_characteristic(
                    &update.gateway_id,
                    &update.management_point,
                    update.characteristic,
                    update.path.as_deref(),
                    &update.value,
                )
                .await;
            // Re-read actual state even if a multi-write mode command partly fails.
            // Never optimistically publish an unconfirmed device setting.
            refresh.notify_one();
            result?;
        }
        log::info!("Sent MQTT command: {}", message.topic);
        Ok(())
    }

    async fn poll_cloud(&mut self, connection: &Mutex<DaikinCloudConnection>) -> Result<(), MqttBridgeError> {
        // Release the cloud lock before publishing, which may wait for the MQTT broker.
        let result = connection.lock().await.update().await;
        match result {
            Ok(status_json) => {
                self.update_online_status(true).await?;
                self.status.online = true;
                self.devices = serde_json::from_str(&status_json)?;
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
        if self.hass_discovery {
            // Republish with status so discovery also recovers after a broker restart.
            let climate = hassdiscovery::with_device_capabilities(
                hassdiscovery::climate_control_topic(self.mqtt.base_topic())?,
                self.mqtt.base_topic(),
                &self.devices,
                false,
            )?;
            let water_heater = hassdiscovery::with_device_capabilities(
                hassdiscovery::domestic_hot_water_topic(self.mqtt.base_topic())?,
                self.mqtt.base_topic(),
                &self.devices,
                true,
            )?;
            self.mqtt.publish(climate, true).await?;
            self.mqtt.publish(water_heater, true).await?;
        }

        let climate_control_json = to_string(&status.climate_control)?;
        let domestic_hot_water_tank_json = to_string(&status.domestic_hot_water_tank)?;
        self.mqtt
            .publish(MqttData::new(format!("{}/climate_control", self.mqtt.base_topic()), climate_control_json), true)
            .await?;

        self.mqtt
            .publish(
                MqttData::new(format!("{}/domestic_hot_water", self.mqtt.base_topic()), domestic_hot_water_tank_json),
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
                            status.climate_control.holiday_mode_active = parse_bool(&mp["isHolidayModeActive"]["value"])?;
                            status.climate_control.is_in_warning_state = parse_bool(&mp["isInWarningState"]["value"])?;
                            status.climate_control.is_in_error_state = parse_bool(&mp["isInErrorState"]["value"])?;
                            status.climate_control.operation_mode = parse_str(&mp["operationMode"]["value"])?;
                            status.climate_control.on_off_mode = parse_str(&mp["onOffMode"]["value"])?;
                            status.climate_control.leaving_water_temperature = parse_i8(&mp["sensoryData"]["value"]["leavingWaterTemperature"]["value"])?;
                            status.climate_control.outdoor_temperature = parse_i8(&mp["sensoryData"]["value"]["outdoorTemperature"]["value"])?;
                            status.climate_control.room_temperature = parse_f32(&mp["sensoryData"]["value"]["roomTemperature"]["value"])?;
                            status.climate_control.heating_setpoint_leaving_water_offset =
                                parse_i8(&mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]["leavingWaterOffset"]["value"])?;
                            status.climate_control.heating_setpoint_room_temperature =
                                parse_f32(&mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]["roomTemperature"]["value"])?;
                        }
                        "domesticHotWaterTank" => {
                            log::info!("domesticHotWaterTank");
                            status.domestic_hot_water_tank.is_in_warning_state = parse_bool(&mp["isInWarningState"]["value"])?;
                            status.domestic_hot_water_tank.is_in_error_state = parse_bool(&mp["isInErrorState"]["value"])?;
                            status.domestic_hot_water_tank.on_off_mode = parse_str(&mp["onOffMode"]["value"])?;
                            status.domestic_hot_water_tank.operation_mode = parse_str(&mp["operationMode"]["value"])?;
                            status.domestic_hot_water_tank.water_temperature = parse_i8(&mp["sensoryData"]["value"]["tankTemperature"]["value"])?;
                            status.domestic_hot_water_tank.water_setpoint = parse_f32(
                                &mp["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]["domesticHotWaterTemperature"]["value"],
                            )?;
                            status.domestic_hot_water_tank.powerfull_mode = parse_bool(&mp["isPowerfulModeActive"]["value"])?;
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
    use bytes::BytesMut;
    use rumqttc::v5::mqttbytes::{v5::Packet, Error as MqttError, QoS};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_packet(stream: &mut tokio::net::TcpStream, buffer: &mut BytesMut) -> Packet {
        loop {
            match Packet::read(buffer, None) {
                Ok(packet) => return packet,
                Err(MqttError::InsufficientBytes(_)) => {
                    assert!(stream.read_buf(buffer).await.expect("read MQTT packet") > 0);
                }
                Err(err) => panic!("invalid MQTT packet: {err}"),
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn publishes_retained_discovery_only_when_enabled() {
        for enabled in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind test MQTT broker");
            let mut bridge = DaikinMqttBridge::new(DaikinMqttBridgeConfig {
                mqtt_config: MqttConfig {
                    server: "127.0.0.1".to_owned(),
                    port: listener.local_addr().expect("broker address").port(),
                    client_id: "discovery-test".to_owned(),
                    user: String::new(),
                    password: String::new(),
                    base_topic: "test/daikin".to_owned(),
                },
                cloud_client_id: String::new(),
                cloud_client_secret: String::new(),
                cloud_redirect_uri: "http://localhost:8076/oauth/callback".to_owned(),
                web_listen: "127.0.0.1:0".parse().expect("web address"),
                hass_discovery: enabled,
                poll_interval: time::Duration::from_secs(600),
                data_dir: PathBuf::new(),
            });
            bridge.devices = serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture");
            let discovery = [
                hassdiscovery::with_device_capabilities(
                    hassdiscovery::climate_control_topic("test/daikin").expect("climate discovery data"),
                    "test/daikin",
                    &bridge.devices,
                    false,
                )
                .expect("climate capabilities"),
                hassdiscovery::with_device_capabilities(
                    hassdiscovery::domestic_hot_water_topic("test/daikin").expect("water heater discovery data"),
                    "test/daikin",
                    &bridge.devices,
                    true,
                )
                .expect("water heater capabilities"),
            ];
            let mut expected_topics = Vec::new();
            for _ in 0..2 {
                bridge.update_heatpump_status(DaikinStatus::default()).await.expect("queue status update");
                if enabled {
                    expected_topics.extend(discovery.iter().map(|data| data.topic.clone()));
                }
                expected_topics.push("test/daikin/climate_control".to_owned());
                expected_topics.push("test/daikin/domestic_hot_water".to_owned());
            }

            let broker = async {
                let (mut stream, _) = listener.accept().await.expect("accept MQTT connection");
                let mut buffer = BytesMut::new();
                assert!(matches!(read_packet(&mut stream, &mut buffer).await, Packet::Connect(..)));
                // MQTT 5 CONNACK: successful connection, no existing session or properties.
                stream.write_all(&[0x20, 0x03, 0x00, 0x00, 0x00]).await.expect("send CONNACK");
                for topic in expected_topics {
                    loop {
                        if let Packet::Publish(publish) = read_packet(&mut stream, &mut buffer).await {
                            assert_eq!(publish.topic.as_ref(), topic.as_bytes());
                            assert!(publish.retain);
                            assert_eq!(publish.qos, QoS::AtLeastOnce);
                            if let Some(data) = discovery.iter().find(|data| data.topic == topic) {
                                assert_eq!(publish.payload.as_ref(), data.payload.as_bytes());
                            }
                            break;
                        }
                    }
                }
            };
            let drive_mqtt = async {
                loop {
                    bridge.mqtt.poll().await.expect("drive MQTT connection");
                }
            };
            time::timeout(time::Duration::from_secs(5), async {
                tokio::select! {
                    _ = broker => {}
                    _ = drive_mqtt => panic!("MQTT event loop unexpectedly stopped"),
                }
            })
            .await
            .expect("MQTT publishing completes promptly");
        }
    }

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
        assert_eq!(daikin_status.climate_control.heating_setpoint_room_temperature, 19.0);
        assert_eq!(daikin_status.climate_control.is_in_error_state, false);
        assert_eq!(daikin_status.climate_control.is_in_warning_state, false);

        assert_eq!(daikin_status.domestic_hot_water_tank.on_off_mode, "on");
        assert_eq!(daikin_status.domestic_hot_water_tank.operation_mode, "heating");
        assert_eq!(daikin_status.domestic_hot_water_tank.water_temperature, 54);
        assert_eq!(daikin_status.domestic_hot_water_tank.water_setpoint, 45.0);
        assert_eq!(daikin_status.domestic_hot_water_tank.powerfull_mode, false);
        assert_eq!(daikin_status.domestic_hot_water_tank.is_in_error_state, false);
        assert_eq!(daikin_status.domestic_hot_water_tank.is_in_warning_state, false);
    }

    #[test_log::test]
    fn fractional_setpoints_survive_state_processing() {
        let mut devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture");
        for point in devices[0]["managementPoints"].as_array_mut().expect("points") {
            let (name, temperature) = match point["embeddedId"].as_str() {
                Some("climateControlMainZone") => ("roomTemperature", 20.5),
                Some("domesticHotWaterTank") => ("domesticHotWaterTemperature", 48.5),
                _ => continue,
            };
            point["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"][name]["value"] = serde_json::json!(temperature);
        }
        let status = DaikinMqttBridge::process_status_json(devices.to_string()).expect("process state");
        assert_eq!(status.climate_control.heating_setpoint_room_temperature, 20.5);
        assert_eq!(status.domestic_hot_water_tank.water_setpoint, 48.5);
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
