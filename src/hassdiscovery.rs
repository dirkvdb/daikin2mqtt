use serde::Serialize;

use crate::{commands, mqtt::MqttData, MqttBridgeError};

const HASS_DISCOVERY_TOPIC: &str = "homeassistant";
const MODE_STATE_TEMPLATE: &str =
    "{{ 'off' if value_json.on_off_mode == 'off' else {'heating': 'heat', 'auto': 'auto'}.get(value_json.operation_mode, 'None') }}";
const WATER_HEATER_MODE_STATE_TEMPLATE: &str =
    "{{ 'off' if value_json.on_off_mode == 'off' else 'performance' if value_json.powerfull_mode | default(false) else {'heating': 'heat_pump'}.get(value_json.operation_mode, 'None') }}";

#[derive(Serialize)]
struct Origin {
    name: &'static str,
    sw_version: &'static str,
    support_url: &'static str,
}

impl Origin {
    fn daikin2mqtt() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME"),
            sw_version: env!("CARGO_PKG_VERSION"),
            support_url: "https://github.com/dirkvdb/daikin2mqtt",
        }
    }
}

#[derive(Serialize)]
struct Device {
    identifiers: Vec<String>,
    name: &'static str,
    manufacturer: &'static str,
}

impl Device {
    fn heat_pump(identifier: String) -> Self {
        Self {
            identifiers: vec![identifier],
            name: "Daikin heat pump",
            manufacturer: "Daikin",
        }
    }
}

#[derive(Serialize)]
struct TemperatureEntity {
    origin: Origin,
    device: Device,
    name: &'static str,
    unique_id: String,
    availability_topic: String,
    mode_command_topic: String,
    temperature_command_topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    power_command_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_on: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_off: Option<&'static str>,
    qos: u8,
    retain: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    temp_step: Option<f64>,
    mode_state_topic: String,
    mode_state_template: &'static str,
    modes: Vec<&'static str>,
    current_temperature_topic: String,
    current_temperature_template: &'static str,
    temperature_state_topic: String,
    temperature_state_template: &'static str,
    temperature_unit: &'static str,
    json_attributes_topic: String,
}

fn device_id(base_topic: &str) -> String {
    // Hex encoding keeps discovery IDs MQTT-safe and distinct for different base topics.
    let topic_id: String = base_topic.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect();
    format!("daikin_{topic_id}")
}

pub fn climate_control_topic(base_topic: &str) -> Result<MqttData, MqttBridgeError> {
    let device_id = device_id(base_topic);
    let unique_id = format!("{device_id}_climate_control");
    let state_topic = format!("{base_topic}/climate_control");
    let topic = format!("{HASS_DISCOVERY_TOPIC}/climate/{unique_id}/config");
    let climate = TemperatureEntity {
        origin: Origin::daikin2mqtt(),
        device: Device::heat_pump(device_id),
        name: "Daikin Climate control",
        unique_id,
        availability_topic: format!("{base_topic}/state"),
        mode_state_topic: state_topic.clone(),
        mode_state_template: MODE_STATE_TEMPLATE,
        mode_command_topic: commands::topic(base_topic, commands::CLIMATE_MODE),
        temperature_command_topic: commands::topic(base_topic, commands::CLIMATE_TEMPERATURE),
        power_command_topic: Some(commands::topic(base_topic, commands::CLIMATE_POWER)),
        payload_on: Some("on"),
        payload_off: Some("off"),
        qos: 1,
        retain: false,
        temp_step: Some(1.0),
        modes: vec!["off", "heat", "auto"],
        current_temperature_topic: state_topic.clone(),
        current_temperature_template: "{{ value_json.room_temperature }}",
        temperature_state_topic: state_topic.clone(),
        temperature_state_template: "{{ value_json.heating_setpoint_room_temperature }}",
        temperature_unit: "C",
        json_attributes_topic: state_topic,
    };

    Ok(MqttData::new(topic, serde_json::to_string(&climate)?))
}

pub fn domestic_hot_water_topic(base_topic: &str) -> Result<MqttData, MqttBridgeError> {
    let device_id = device_id(base_topic);
    let unique_id = format!("{device_id}_domestic_hot_water");
    let state_topic = format!("{base_topic}/domestic_hot_water");
    let topic = format!("{HASS_DISCOVERY_TOPIC}/water_heater/{unique_id}/config");
    let water_heater = TemperatureEntity {
        origin: Origin::daikin2mqtt(),
        device: Device::heat_pump(device_id),
        name: "Daikin Domestic hot water",
        unique_id,
        availability_topic: format!("{base_topic}/state"),
        mode_state_topic: state_topic.clone(),
        mode_state_template: WATER_HEATER_MODE_STATE_TEMPLATE,
        mode_command_topic: commands::topic(base_topic, commands::WATER_MODE),
        temperature_command_topic: commands::topic(base_topic, commands::WATER_TEMPERATURE),
        power_command_topic: Some(commands::topic(base_topic, commands::WATER_POWER)),
        payload_on: Some("on"),
        payload_off: Some("off"),
        qos: 1,
        retain: false,
        temp_step: None,
        modes: vec!["off", "heat_pump", "performance"],
        current_temperature_topic: state_topic.clone(),
        current_temperature_template: "{{ value_json.water_temperature }}",
        temperature_state_topic: state_topic.clone(),
        temperature_state_template: "{{ value_json.water_setpoint }}",
        temperature_unit: "C",
        json_attributes_topic: state_topic,
    };

    Ok(MqttData::new(topic, serde_json::to_string(&water_heater)?))
}

pub(crate) fn with_temperature_metadata(mut data: MqttData, metadata: Option<&serde_json::Value>) -> Result<MqttData, MqttBridgeError> {
    if let Some(metadata) = metadata {
        let mut payload: serde_json::Value = serde_json::from_str(&data.payload)?;
        for (source, field) in [("minValue", "min_temp"), ("maxValue", "max_temp")] {
            if metadata[source].is_number() {
                payload[field] = metadata[source].clone();
            }
        }
        if payload.get("temp_step").is_some() && metadata["stepValue"].as_f64().is_some_and(|step| step > 0.0) {
            payload["temp_step"] = metadata["stepValue"].clone();
        }
        data.payload = serde_json::to_string(&payload)?;
    }
    Ok(data)
}

pub(crate) fn with_device_capabilities(data: MqttData, base_topic: &str, devices: &serde_json::Value, water: bool) -> Result<MqttData, MqttBridgeError> {
    let mut data = with_temperature_metadata(data, commands::temperature_metadata(devices, water))?;
    let mut payload: serde_json::Value = serde_json::from_str(&data.payload)?;
    // Use the command planner itself so discovery cannot offer modes whose
    // required power, operation-mode, or boost writes would be rejected.
    let accepts = |field: &str, value: &str| {
        payload[field].as_str().is_some_and(|topic| {
            let message = MqttData::new(topic.into(), value.into());
            commands::updates(base_topic, &message, devices).is_ok()
        })
    };
    let modes: Vec<serde_json::Value> = payload["modes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|mode| mode.as_str().is_some_and(|mode| accepts("mode_command_topic", mode)))
        .cloned()
        .collect();
    let power_supported = accepts("power_command_topic", "on") && accepts("power_command_topic", "off");
    if modes.is_empty() {
        // Keep recognized state modes for read-only entities, but no mode control.
        payload.as_object_mut().expect("discovery object").remove("mode_command_topic");
    } else {
        payload["modes"] = modes.into();
    }
    if !power_supported {
        for field in ["power_command_topic", "payload_on", "payload_off"] {
            payload.as_object_mut().expect("discovery object").remove(field);
        }
    }
    if !commands::temperature_writable(devices, water) {
        payload.as_object_mut().expect("discovery object").remove("temperature_command_topic");
    }
    data.payload = serde_json::to_string(&payload)?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test_log::test]
    fn climate_discovery_uses_existing_status_topi    use serde_json::{json, Value};

            fn configured_discovery(devices: &Value, water: bool) -> Value {
                let data = if water {
                    domestic_hot_water_topic("house/daikin")
                } else {
                    climate_control_topic("house/daikin")
                }
                .expect("discovery");
                let data = with_device_capabilities(data, "house/daikin", devices, water).expect("device capabilities");
                serde_json::from_str(&data.payload).expect("discovery JSON")
            }

            fn point(devices: &mut Value, water: bool) -> &mut Value {
                let id = if water { "domesticHotWaterTank" } else { "climateControlMainZone" };
                devices[0]["managementPoints"]
                    .as_array_mut()
                    .expect("management points")
                    .iter_mut()
                    .find(|point| point["embeddedId"] == id)
                    .expect("management point")
            }

            #[test_log::test].expect("serialize climate discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");

        assert_eq!(data.topic, "homeassistant/climate/daikin_6461696b696e_climate_control/config");
        assert_eq!(payload["unique_id"], "daikin_6461696b696e_climate_control");
        assert_eq!(payload["availability_topic"], "daikin/state");
        for field in [
            "mode_state_topic",
            "current_temperature_topic",
            "temperature_state_topic",
            "json_attributes_topic",
        ] {
            assert_eq!(payload[field], "daikin/climate_control");
        }
        assert_eq!(payload["current_temperature_template"], "{{ value_json.room_temperature }}");
        assert_eq!(payload["temperature_state_template"], "{{ value_json.heating_setpoint_room_temperature }}");
        assert_eq!(payload["mode_state_template"], MODE_STATE_TEMPLATE);
        assert_eq!(payload["modes"], json!(["off", "heat", "auto"]));
        assert_eq!(payload["temperature_unit"], "C");
        assert_eq!(payload["device"]["identifiers"], json!(["daikin_6461696b696e"]));
        assert_eq!(payload["device"]["manufacturer"], "Daikin");
        assert_eq!(payload["origin"]["name"], env!("CARGO_PKG_NAME"));
        assert_eq!(payload["origin"]["sw_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(payload["origin"]["support_url"], "https://github.com/dirkvdb/daikin2mqtt");
        assert_eq!(payload["mode_command_topic"], "daikin/cmnd/climate_mode");
        assert_eq!(payload["temperature_command_topic"], "daikin/cmnd/climate_temperature");
        assert_eq!(payload["qos"], 1);
        assert_eq!(payload["retain"], false);
        assert_eq!(payload["temp_step"], 1.0);
        assert_eq!(payload["power_command_topic"], "daikin/cmnd/climate_power");
        assert_eq!(payload["payload_on"], "on");
        assert_eq!(payload["payload_off"], "off");
    }

    #[test_log::test]
    fn climate_discovery_maps_and_advertises_auto_mode() {
        let data = climate_control_topic("daikin").expect("serialize climate discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");

        assert_eq!(
            payload["mode_state_template"],
            "{{ 'off' if value_json.on_off_mode == 'off' else {'heating': 'heat', 'auto': 'auto'}.get(value_json.operation_mode, 'None') }}"
        );
        assert!(payload["modes"].as_array().expect("advertised HVAC modes").contains(&json!("auto")));
    }

    #[test_log::test]
    fn water_heater_discovery_uses_existing_status_topics_and_shared_device() {
        let data = domestic_hot_water_topic("daikin").expect("serialize water heater discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");
        let climate = climate_control_topic("daikin").expect("serialize climate discovery");
        let climate_payload: Value = serde_json::from_str(&climate.payload).expect("valid discovery JSON");

        assert_eq!(data.topic, "homeassistant/water_heater/daikin_6461696b696e_domestic_hot_water/config");
        assert_eq!(payload["unique_id"], "daikin_6461696b696e_domestic_hot_water");
        assert_ne!(payload["unique_id"], climate_payload["unique_id"]);
        assert_eq!(payload["device"], climate_payload["device"]);
        assert_eq!(payload["origin"], climate_payload["origin"]);
        assert_eq!(payload["availability_topic"], "daikin/state");
        for field in [
            "mode_state_topic",
            "current_temperature_topic",
            "temperature_state_topic",
            "json_attributes_topic",
        ] {
            assert_eq!(payload[field], "daikin/domestic_hot_water");
        }
        assert_eq!(payload["current_temperature_template"], "{{ value_json.water_temperature }}");
        assert_eq!(payload["temperature_state_template"], "{{ value_json.water_setpoint }}");
        assert_eq!(payload["mode_state_template"], WATER_HEATER_MODE_STATE_TEMPLATE);
        assert_eq!(payload["modes"], json!(["off", "heat_pump", "performance"]));
        assert_eq!(payload["temperature_unit"], "C");
        assert_eq!(payload["mode_command_topic"], "daikin/cmnd/domestic_hot_water_mode");
        assert_eq!(payload["temperature_command_topic"], "daikin/cmnd/domestic_hot_water_temperature");
        assert_eq!(payload["qos"], 1);
        assert_eq!(payload["retain"], false);
        assert!(payload.get("temp_step").is_none());
    }

    #[test_log::test]
    fn water_heater_discovery_exposes_performance_and_reports_active_boost() {
        let data = domestic_hot_water_topic("home/daikin").expect("discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("discovery JSON");
        assert_eq!(payload["modes"], json!(["off", "heat_pump", "performance"]));
        assert_eq!(payload["mode_state_template"],
            "{{ 'off' if value_json.on_off_mode == 'off' else 'performance' if value_json.powerfull_mode | default(false) else {'heating': 'heat_pump'}.get(value_json.operation_mode, 'None') }}");
        let devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture");
        let message = MqttData::new(payload["mode_command_topic"].as_str().expect("mode topic").into(), "performance".into());
        let updates = commands::updates("home/daikin", &message, &devices).expect("performance command");
        assert_eq!(updates.last().expect("boost update").characteristic, "powerfulMode");
        assert_eq!(updates.last().expect("boost update").value, "on");
    }

    #[test_log::test]
    fn water_heater_power_discovery_routes_both_payloads_to_on_off_mode() {
        let devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture");
        for base in ["daikin", "home/daikin"] {
            let data = domestic_hot_water_topic(base).expect("water heater discovery");
            let payload: Value = serde_json::from_str(&data.payload).expect("discovery JSON");
            assert_eq!(payload["power_command_topic"], format!("{base}/cmnd/domestic_hot_water_power"));
            assert_eq!(payload["payload_on"], "on");
            assert_eq!(payload["payload_off"], "off");
            for (field, power) in [("payload_on", "on"), ("payload_off", "off")] {
                let message = MqttData::new(
                    payload["power_command_topic"].as_str().expect("power topic").into(),
                    payload[field].as_str().expect("power payload").into(),
                );
                let updates = commands::updates(base, &message, &devices).expect("power command");
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].management_point, "domesticHotWaterTank");
                assert_eq!(updates[0].characteristic, "onOffMode");
                assert_eq!(updates[0].path, None);
                assert_eq!(updates[0].value, power);
            }
        }
    }

    #[test_log::test]
    fn water_heater_discovery_ids_are_stable_safe_and_unique_per_base_topic() {
        let first = domestic_hot_water_topic("home/daikin").expect("serialize water heater discovery");
        let repeated = domestic_hot_water_topic("home/daikin").expect("serialize water heater discovery");
        let other = domestic_hot_water_topic("home_daikin").expect("serialize water heater discovery");
        let unicode = domestic_hot_water_topic("home/暖房").expect("serialize water heater discovery");

        assert_eq!(first.topic, repeated.topic);
        assert_eq!(first.payload, repeated.payload);
        assert_ne!(first.topic, other.topic);
        assert!(unicode.topic.is_ascii());
        let payload: Value = serde_json::from_str(&first.payload).expect("valid discovery JSON");
        assert_eq!(payload["mode_state_topic"], "home/daikin/domestic_hot_water");
        assert_eq!(payload["availability_topic"], "home/daikin/state");
    }

    #[test_log::test]
    fn discovery_uses_device_temperature_limits_and_climate_step() {
        for water in [false, true] {
            let metadata = json!({"minValue": 12, "maxValue": 60, "stepValue": 0.5});
            let data = if water {
                domestic_hot_water_topic("house/daikin")
            } else {
                climate_control_topic("house/daikin")
            }
            .expect("discovery");
            let data = with_temperature_metadata(data, Some(&metadata)).expect("temperature configuration");
            let payload: Value = serde_json::from_str(&data.payload).expect("discovery JSON");
            assert_eq!(payload["min_temp"], 12);
            assert_eq!(payload["max_temp"], 60);
            if water {
                assert!(payload.get("temp_step").is_none());
            } else {
                assert_eq!(payload["temp_step"], 0.5);
            }
            assert!(payload["temperature_command_topic"]
                .as_str()
                .expect("command topic")
                .starts_with("house/daikin/cmnd/"));
        }
    }

    #[test_log::test]
    fn fixture_discovery_only_advertises_usable_commands() {
        for fixture in [
            include_str!("../resources/test/managementpoints.json"),
            include_str!("../resources/test/managementpoints2.json"),
        ] {
            let devices: Value = serde_json::from_str(fixture).expect("device fixture");
            for water in [false, true] {
                let payload = configured_discovery(&devices, water);
                assert_eq!(payload["modes"], if water { json!(["off", "heat_pump", "performance"]) } else { json!(["off", "heat"]) });
                for mode in payload["modes"].as_array().expect("modes") {
                    let message = MqttData::new(
                        payload["mode_command_topic"].as_str().expect("mode topic").into(),
                        mode.as_str().expect("mode").into(),
                    );
                    assert!(commands::updates("house/daikin", &message, &devices).is_ok());
                }
                for field in ["payload_on", "payload_off"] {
                    let message = MqttData::new(
                        payload["power_command_topic"].as_str().expect("power topic").into(),
                        payload[field].as_str().expect("power payload").into(),
                    );
                    let updates = commands::updates("house/daikin", &message, &devices).expect("power command");
                    assert_eq!(updates.len(), 1);
                    assert_eq!(updates[0].characteristic, "onOffMode");
                    assert_eq!(updates[0].value, payload[field]);
                }
                assert_eq!(payload["min_temp"], if water { 30 } else { 12 });
                assert_eq!(payload["max_temp"], if water { 60 } else { 24 });
                if water {
                    assert!(payload.get("temperature_command_topic").is_none());
                    assert_eq!(payload["temperature_state_template"], "{{ value_json.water_setpoint }}");
                } else {
                    assert_eq!(payload["temp_step"], 0.5);
                    for temperature in [12.0, 20.5, 24.0] {
                        let message = MqttData::new(
                            payload["temperature_command_topic"].as_str().expect("temperature topic").into(),
                            temperature.to_string(),
                        );
                        assert!(commands::updates("house/daikin", &message, &devices).is_ok());
                    }
                }
            }
        }
    }

    #[test_log::test]
    fn discovery_preserves_supported_auto_and_filters_unavailable_boost() {
        let mut devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json"))
            .expect("device fixture");
        point(&mut devices, false)["operationMode"]["settable"] = json!(true);
        point(&mut devices, false)["operationMode"]["values"] = json!(["heating", "auto"]);
        assert_eq!(configured_discovery(&devices, false)["modes"], json!(["off", "heat", "auto"]));
        for boost in [Value::Null, json!({"settable": false, "value": "off"}), json!({"settable": true, "value": "off", "values": ["off"]})] {
            point(&mut devices, true)["powerfulMode"] = boost;
            assert_eq!(configured_discovery(&devices, true)["modes"], json!(["off", "heat_pump"]));
        }
    }

    #[test_log::test]
    fn discovery_only_exposes_writable_temperature_controls() {
        for water in [false, true] {
            for control_writable in [false, true] {
                for setpoint_writable in [false, true] {
                    let mut devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json"))
                        .expect("device fixture");
                    let control = &mut point(&mut devices, water)["temperatureControl"];
                    control["settable"] = json!(control_writable);
                    let setpoint = if water { "domesticHotWaterTemperature" } else { "roomTemperature" };
                    control["value"]["operationModes"]["heating"]["setpoints"][setpoint]["settable"] = json!(setpoint_writable);
                    let payload = configured_discovery(&devices, water);
                    assert_eq!(payload.get("temperature_command_topic").is_some(), control_writable && setpoint_writable);
                    assert!(payload.get("temperature_state_topic").is_some());
                    assert!(payload.get("current_temperature_topic").is_some());
                }
            }
        }
    }

    #[test_log::test]
    fn discovery_removes_commands_for_read_only_missing_or_ambiguous_targets() {
        let devices: Value = serde_json::from_str(include_str!("../resources/test/managementpoints.json"))
            .expect("device fixture");
        let mut read_only = devices.clone();
        for water in [false, true] {
            point(&mut read_only, water)["onOffMode"]["settable"] = json!(false);
            point(&mut read_only, water)["temperatureControl"]["settable"] = json!(false);
        }
        for snapshot in [read_only, Value::Null, json!([]), json!([devices[0], devices[0]])] {
            for water in [false, true] {
                let payload = configured_discovery(&snapshot, water);
                for field in ["mode_command_topic", "power_command_topic", "payload_on", "payload_off", "temperature_command_topic"] {
                    assert!(payload.get(field).is_none(), "{field}: {payload}");
                }
                assert!(payload.get("mode_state_topic").is_some());
            }
        }
    }

    #[test_log::test]
    fn discovery_ids_are_stable_safe_and_unique_per_base_topic() {
        let first = climate_control_topic("home/daikin").expect("serialize climate discovery");
        let repeated = climate_control_topic("home/daikin").expect("serialize climate discovery");
        let other = climate_control_topic("home_daikin").expect("serialize climate discovery");
        let unicode = climate_control_topic("home/暖房").expect("serialize climate discovery");

        assert_eq!(first.topic, repeated.topic);
        assert_eq!(first.payload, repeated.payload);
        assert_ne!(first.topic, other.topic);
        assert!(unicode.topic.is_ascii());
        let payload: Value = serde_json::from_str(&first.payload).expect("valid discovery JSON");
        assert_eq!(payload["mode_state_topic"], "home/daikin/climate_control");
        assert_eq!(payload["availability_topic"], "home/daikin/state");
    }
}
