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

const COMMAND_REFRESH_DELAY: time::Duration = time::Duration::from_secs(2);

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

#[derive(Default, Serialize)]
struct UnitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    software_version: Option<String>,
}

impl UnitInfo {
    fn from_management_point(mp: &Value) -> Result<Self, MqttBridgeError> {
        Ok(Self {
            name: parse_optional_str(&mp["name"]["value"])?,
            model_info: parse_optional_str(&mp["modelInfo"]["value"])?,
            software_version: parse_optional_str(&mp["softwareVersion"]["value"])?,
        })
    }
}

#[derive(Default, Serialize)]
struct GatewayInfo {
    #[serde(flatten)]
    unit_info: UnitInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    firmware_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mac_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_firmware_update_supported: Option<bool>,
}

impl GatewayInfo {
    fn from_management_point(mp: &Value) -> Result<Self, MqttBridgeError> {
        Ok(Self {
            unit_info: UnitInfo::from_management_point(mp)?,
            firmware_version: parse_optional_str(&mp["firmwareVersion"]["value"])?,
            ip_address: parse_optional_str(&mp["ipAddress"]["value"])?,
            mac_address: parse_optional_str(&mp["macAddress"]["value"])?,
            is_firmware_update_supported: if mp["isFirmwareUpdateSupported"]["value"].is_null() {
                None
            } else {
                Some(parse_bool(&mp["isFirmwareUpdateSupported"]["value"])?)
            },
        })
    }
}

#[derive(Default)]
struct DaikinStatus {
    climate_control: ClimateControl,
    domestic_hot_water_tank: DomesticHotWaterTank,
    indoor_unit_hydro: Option<UnitInfo>,
    outdoor_unit: Option<UnitInfo>,
    user_interface: Option<UnitInfo>,
    gateway: Option<GatewayInfo>,
}

impl DaikinStatus {
    fn mqtt_messages(&self, base_topic: &str) -> Result<Vec<MqttData>, MqttBridgeError> {
        let mut messages = vec![
            MqttData::new(format!("{}/climate_control", base_topic), to_string(&self.climate_control)?),
            MqttData::new(format!("{}/domestic_hot_water", base_topic), to_string(&self.domestic_hot_water_tank)?),
        ];
        for (topic, info) in [
            ("indoor_unit_hydro", &self.indoor_unit_hydro),
            ("outdoor_unit", &self.outdoor_unit),
            ("user_interface", &self.user_interface),
        ] {
            if let Some(info) = info {
                messages.push(MqttData::new(format!("{}/{}", base_topic, topic), to_string(info)?));
            }
        }
        if let Some(gateway) = &self.gateway {
            messages.push(MqttData::new(format!("{}/gateway", base_topic), to_string(gateway)?));
        }
        Ok(messages)
    }
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
    command_refresh_deadline: Option<time::Instant>,
}

impl DaikinMqttBridge {
    pub fn new(cfg: DaikinMqttBridgeConfig) -> Result<DaikinMqttBridge, MqttBridgeError> {
        Ok(DaikinMqttBridge {
            mqtt: MqttConnection::new(cfg.mqtt_config)?,
            cloud_client_id: cfg.cloud_client_id,
            cloud_client_secret: cfg.cloud_client_secret,
            cloud_redirect_uri: cfg.cloud_redirect_uri,
            web_listen: cfg.web_listen,
            hass_discovery: cfg.hass_discovery,
            poll_interval: cfg.poll_interval,
            data_dir: cfg.data_dir,
            status: MqttStatus::default(),
            devices: Value::Null,
            command_refresh_deadline: None,
        })
    }

    pub async fn run(self) -> Result<(), MqttBridgeError> {
        let connection = DaikinCloudConnection::new(
            self.cloud_client_id.clone(),
            self.cloud_client_secret.clone(),
            self.cloud_redirect_uri.clone(),
            &self.data_dir,
        )?;
        let cloud_connected = connection.connection_status();
        let connection = Arc::new(Mutex::new(connection));
        let refresh = Arc::new(Notify::new());
        let web_state = web::WebState::new(
            connection.clone(),
            cloud_connected,
            self.mqtt.connection_status(),
            refresh.clone(),
            self.cloud_redirect_uri.clone(),
            self.web_listen,
        )?;
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
            let command_refresh_deadline = self.command_refresh_deadline;
            tokio::select! {
                mqtt_msg = self.mqtt.poll() => {
                    if let Ok(Some(msg)) = mqtt_msg {
                        if let Err(err) = self.handle_command(&connection, &msg).await {
                            log::warn!("MQTT command failed: {}, error={err}", commands::details(self.mqtt.base_topic(), &msg));
                        }
                    }
                }
                _ = interval.tick() => {
                    // A scheduled command refresh also covers a regular poll due
                    // during the debounce window; do not read stale state early.
                    if self.command_refresh_deadline.is_none_or(|deadline| deadline <= time::Instant::now()) {
                        self.command_refresh_deadline = None;
                        if let Err(err) = self.poll_cloud(&connection).await {
                            log::error!("Failed to poll cloud: ({err})");
                        }
                        interval.reset();
                    }
                }
                _ = refresh.notified() => {
                    // Authentication changes still request an immediate refresh.
                    self.command_refresh_deadline = None;
                    if let Err(err) = self.poll_cloud(&connection).await {
                        log::error!("Failed to poll cloud: ({err})");
                    }
                    interval.reset();
                }
                _ = wait_for_command_refresh(command_refresh_deadline) => {
                    // Consume the deadline even on failure: this is one read,
                    // not a retry loop, because Daikin limits daily API calls.
                    self.command_refresh_deadline = None;
                    log::info!("Refreshing cloud status after MQTT command");
                    if let Err(err) = self.poll_cloud(&connection).await {
                        log::error!("Failed to poll cloud after MQTT command: ({err})");
                    }
                    interval.reset();
                }
            }
        }
    }

    async fn handle_command(&mut self, connection: &Mutex<DaikinCloudConnection>, message: &MqttData) -> Result<(), MqttBridgeError> {
        let details = commands::details(self.mqtt.base_topic(), message);
        log::info!("Received MQTT command: {details}");
        let updates = commands::updates(self.mqtt.base_topic(), message, &self.devices)?;
        if self.command_refresh_deadline.is_some() {
            self.command_refresh_deadline = Some(time::Instant::now() + COMMAND_REFRESH_DELAY);
        }
        let update_count = updates.len();
        let mut cloud = connection.lock().await;
        for (index, update) in updates.into_iter().enumerate() {
            log::debug!("Sending Daikin update {}/{}: {details}, {update}", index + 1, update_count);
            let result = cloud
                .set_characteristic(
                    &update.gateway_id,
                    &update.management_point,
                    update.characteristic,
                    update.path.as_deref(),
                    &update.value,
                )
                .await;

            if let Err(err) = result {
                log::warn!(
                    "Daikin update {}/{} failed: {details}, {update}, sent_updates={index}, error={err}",
                    index + 1,
                    update_count,
                );
                return Err(err);
            }
            log::info!("Sent Daikin update {}/{}: {details}, {update}", index + 1, update_count);
        }
        log::info!("Sent MQTT command: {details}, updates={update_count}");
        self.command_refresh_deadline = Some(time::Instant::now() + COMMAND_REFRESH_DELAY);
        log::debug!("Scheduled one cloud status refresh in 2 seconds: {details}");
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
            for message in hassdiscovery::create_discovery_messages(self.mqtt.base_topic(), &self.devices)? {
                self.mqtt.publish(message, true).await?;
            }
        }

        for message in status.mqtt_messages(self.mqtt.base_topic())? {
            self.mqtt.publish(message, true).await?;
        }
        Ok(())
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
                        "gateway" => {
                            status.gateway = Some(GatewayInfo::from_management_point(mp)?);
                        }
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
                        "indoorUnitHydro" => {
                            status.indoor_unit_hydro = Some(UnitInfo::from_management_point(mp)?);
                        }
                        "outdoorUnit" => {
                            status.outdoor_unit = Some(UnitInfo::from_management_point(mp)?);
                        }
                        "userInterface" => {
                            status.user_interface = Some(UnitInfo::from_management_point(mp)?);
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

async fn wait_for_command_refresh(deadline: Option<time::Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
#[path = "bridge/command_refresh_tests.rs"]
mod command_refresh_tests;

fn parse_i8(value: &Value) -> Result<i8, MqttBridgeError> {
    value.as_i64().ok_or(MqttBridgeError::ParseError).map(|v| v as i8)
}

fn parse_f32(value: &Value) -> Result<f32, MqttBridgeError> {
    value.as_f64().ok_or(MqttBridgeError::ParseError).map(|v| v as f32)
}

fn parse_str(value: &Value) -> Result<String, MqttBridgeError> {
    value.as_str().ok_or(MqttBridgeError::ParseError).map(|v| v.to_string())
}

fn parse_optional_str(value: &Value) -> Result<Option<String>, MqttBridgeError> {
    if value.is_null() {
        Ok(None)
    } else {
        parse_str(value).map(Some)
    }
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
                    ca_cert: None,
                    insecure: false,
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
            })
            .expect("create test bridge");
            bridge.devices = serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture");
            let fixture = include_str!("../resources/test/managementpoints.json");
            let status_messages = DaikinMqttBridge::process_status_json(fixture.to_owned())
                .expect("process status fixture")
                .mqtt_messages("test/daikin")
                .expect("status messages");
            assert_eq!(status_messages.len(), 6);
            let mut expected_messages = Vec::new();
            for poll in 0..3 {
                if poll == 2 {
                    bridge.devices = Value::Null;
                }
                let status = DaikinMqttBridge::process_status_json(fixture.to_owned()).expect("process status fixture");
                bridge.update_heatpump_status(status).await.expect("queue status update");
                if enabled {
                    expected_messages.extend(hassdiscovery::create_discovery_messages("test/daikin", &bridge.devices).expect("discovery messages"));
                }
                expected_messages.extend(status_messages.iter().map(|data| MqttData::new(data.topic.clone(), data.payload.clone())));
            }

            let broker = async {
                let (mut stream, _) = listener.accept().await.expect("accept MQTT connection");
                let mut buffer = BytesMut::new();
                assert!(matches!(read_packet(&mut stream, &mut buffer).await, Packet::Connect(..)));
                // MQTT 5 CONNACK: successful connection, no existing session or properties.
                stream.write_all(&[0x20, 0x03, 0x00, 0x00, 0x00]).await.expect("send CONNACK");
                for data in expected_messages {
                    loop {
                        if let Packet::Publish(publish) = read_packet(&mut stream, &mut buffer).await {
                            assert_eq!(publish.topic.as_ref(), data.topic.as_bytes());
                            assert!(publish.retain);
                            assert_eq!(publish.qos, QoS::AtLeastOnce);
                            assert_eq!(publish.payload.as_ref(), data.payload.as_bytes());
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

    #[test_log::test]
    fn exposes_unit_information_from_both_fixtures() {
        for fixture in ["managementpoints.json", "managementpoints2.json"] {
            let json = std::fs::read_to_string(test_data_dir().join(fixture)).unwrap();
            let status = DaikinMqttBridge::process_status_json(json).unwrap();
            let messages = status.mqtt_messages("daikin").unwrap();

            assert_eq!(messages.len(), 6);
            assert_eq!(messages[0].topic, "daikin/climate_control");
            assert_eq!(messages[1].topic, "daikin/domestic_hot_water");
            for (topic, expected) in [
                (
                    "daikin/indoor_unit_hydro",
                    serde_json::json!({
                        "name": "Indoor Hydro Unit",
                        "model_info": "EHVH08S23EJ9W",
                        "software_version": "0221"
                    }),
                ),
                (
                    "daikin/outdoor_unit",
                    serde_json::json!({
                        "name": "Outdoor Unit",
                        "model_info": "",
                        "software_version": "FFFF"
                    }),
                ),
                (
                    "daikin/user_interface",
                    serde_json::json!({
                        "name": "User Interface",
                        "software_version": "6.3.0"
                    }),
                ),
            ] {
                let message = messages.iter().find(|message| message.topic == topic).unwrap();
                assert_eq!(serde_json::from_str::<Value>(&message.payload).unwrap(), expected);
            }
        }
    }

    #[test_log::test]
    fn exposes_gateway_information_from_both_fixtures() {
        for (fixture, mac_address) in [("managementpoints.json", "20:4e:f6:a6:00:00"), ("managementpoints2.json", "20:4e:f6:a6:d8:9a")] {
            let json = std::fs::read_to_string(test_data_dir().join(fixture)).expect("read fixture");
            let status = DaikinMqttBridge::process_status_json(json).expect("process fixture");
            let messages = status.mqtt_messages("daikin").expect("status messages");
            let gateway = messages.iter().find(|message| message.topic == "daikin/gateway").expect("gateway message");
            assert_eq!(
                serde_json::from_str::<Value>(&gateway.payload).expect("gateway JSON"),
                serde_json::json!({
                    "name": "Gateway",
                    "model_info": "BRP069A78",
                    "firmware_version": "3.2.5",
                    "ip_address": "192.168.1.176",
                    "mac_address": mac_address,
                    "is_firmware_update_supported": true
                })
            );
        }
    }

    #[test_log::test]
    fn missing_gateway_information_is_omitted_and_false_is_preserved() {
        for (point, expected) in [
            (serde_json::json!({"embeddedId": "gateway"}), serde_json::json!({})),
            (
                serde_json::json!({
                    "embeddedId": "gateway",
                    "firmwareVersion": {"value": null},
                    "ipAddress": {"value": ""},
                    "isFirmwareUpdateSupported": {"value": false}
                }),
                serde_json::json!({"ip_address": "", "is_firmware_update_supported": false}),
            ),
            (
                serde_json::json!({"embeddedId": "gateway", "isFirmwareUpdateSupported": {"value": null}}),
                serde_json::json!({}),
            ),
        ] {
            let json = serde_json::json!([{"managementPoints": [point]}]);
            let status = DaikinMqttBridge::process_status_json(json.to_string()).expect("process gateway");
            let messages = status.mqtt_messages("daikin").expect("status messages");
            assert_eq!(messages.len(), 3);
            assert_eq!(messages[2].topic, "daikin/gateway");
            assert_eq!(serde_json::from_str::<Value>(&messages[2].payload).expect("gateway JSON"), expected);
        }
    }

    #[test_log::test]
    fn invalid_gateway_information_returns_parse_error() {
        for point in [
            serde_json::json!({"embeddedId": "gateway", "firmwareVersion": {"value": 123}}),
            serde_json::json!({"embeddedId": "gateway", "isFirmwareUpdateSupported": {"value": "false"}}),
        ] {
            let json = serde_json::json!([{"managementPoints": [point]}]);
            assert!(matches!(
                DaikinMqttBridge::process_status_json(json.to_string()),
                Err(MqttBridgeError::ParseError)
            ));
        }
    }

    #[test_log::test]
    fn absent_unit_management_points_are_not_published() {
        let json = serde_json::json!([{"managementPoints": []}]);
        let status = DaikinMqttBridge::process_status_json(json.to_string()).unwrap();
        let messages = status.mqtt_messages("daikin").unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].topic, "daikin/climate_control");
        assert_eq!(messages[1].topic, "daikin/domestic_hot_water");
    }

    #[test_log::test]
    fn missing_unit_information_is_omitted() {
        let json = serde_json::json!([{"managementPoints": [
            {"embeddedId": "indoorUnitHydro"},
            {"embeddedId": "outdoorUnit", "softwareVersion": {"value": null}},
            {"embeddedId": "userInterface", "name": {"value": "User Interface"}}
        ]}]);
        let status = DaikinMqttBridge::process_status_json(json.to_string()).unwrap();
        let messages = status.mqtt_messages("daikin").unwrap();

        assert_eq!(messages.len(), 5);
        assert_eq!(serde_json::from_str::<Value>(&messages[2].payload).unwrap(), serde_json::json!({}));
        assert_eq!(serde_json::from_str::<Value>(&messages[3].payload).unwrap(), serde_json::json!({}));
        assert_eq!(
            serde_json::from_str::<Value>(&messages[4].payload).unwrap(),
            serde_json::json!({"name": "User Interface"})
        );
    }

    #[test_log::test]
    fn invalid_unit_information_returns_parse_error() {
        let json = serde_json::json!([{"managementPoints": [
            {"embeddedId": "outdoorUnit", "modelInfo": {"value": 123}}
        ]}]);
        assert!(matches!(
            DaikinMqttBridge::process_status_json(json.to_string()),
            Err(MqttBridgeError::ParseError)
        ));
    }

    use std::path::PathBuf;

    use super::*;
}
