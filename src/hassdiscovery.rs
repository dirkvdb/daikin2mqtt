use serde::Serialize;

use crate::{mqtt::MqttData, MqttBridgeError};

const HASS_DISCOVERY_TOPIC: &str = "homeassistant";
const MODE_STATE_TEMPLATE: &str =
    "{{ 'off' if value_json.on_off_mode == 'off' else {'heating': 'heat', 'auto': 'auto'}.get(value_json.operation_mode, 'None') }}";
const WATER_HEATER_MODE_STATE_TEMPLATE: &str =
    "{{ 'off' if value_json.on_off_mode == 'off' else {'heating': 'heat_pump'}.get(value_json.operation_mode, 'None') }}";

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
        modes: vec!["off", "heat_pump"],
        current_temperature_topic: state_topic.clone(),
        current_temperature_template: "{{ value_json.water_temperature }}",
        temperature_state_topic: state_topic.clone(),
        temperature_state_template: "{{ value_json.water_setpoint }}",
        temperature_unit: "C",
        json_attributes_topic: state_topic,
    };

    Ok(MqttData::new(topic, serde_json::to_string(&water_heater)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test_log::test]
    fn climate_discovery_uses_existing_status_topics() {
        let data = climate_control_topic("daikin").expect("serialize climate discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");

        assert_eq!(
            data.topic,
            "homeassistant/climate/daikin_6461696b696e_climate_control/config"
        );
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
        assert_eq!(
            payload["current_temperature_template"],
            "{{ value_json.room_temperature }}"
        );
        assert_eq!(
            payload["temperature_state_template"],
            "{{ value_json.heating_setpoint_room_temperature }}"
        );
        assert_eq!(payload["mode_state_template"], MODE_STATE_TEMPLATE);
        assert_eq!(payload["modes"], json!(["off", "heat", "auto"]));
        assert_eq!(payload["temperature_unit"], "C");
        assert_eq!(payload["device"]["identifiers"], json!(["daikin_6461696b696e"]));
        assert_eq!(payload["device"]["manufacturer"], "Daikin");
        assert_eq!(payload["origin"]["name"], env!("CARGO_PKG_NAME"));
        assert_eq!(payload["origin"]["sw_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            payload["origin"]["support_url"],
            "https://github.com/dirkvdb/daikin2mqtt"
        );
        let fields = payload.as_object().expect("discovery object");
        assert!(!fields.keys().any(|field| field.ends_with("_command_topic")));
    }

    #[test_log::test]
    fn climate_discovery_maps_and_advertises_auto_mode() {
        let data = climate_control_topic("daikin").expect("serialize climate discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");

        assert_eq!(
            payload["mode_state_template"],
            "{{ 'off' if value_json.on_off_mode == 'off' else {'heating': 'heat', 'auto': 'auto'}.get(value_json.operation_mode, 'None') }}"
        );
        assert!(payload["modes"]
            .as_array()
            .expect("advertised HVAC modes")
            .contains(&json!("auto")));
    }

    #[test_log::test]
    fn water_heater_discovery_uses_existing_status_topics_and_shared_device() {
        let data = domestic_hot_water_topic("daikin").expect("serialize water heater discovery");
        let payload: Value = serde_json::from_str(&data.payload).expect("valid discovery JSON");
        let climate = climate_control_topic("daikin").expect("serialize climate discovery");
        let climate_payload: Value = serde_json::from_str(&climate.payload).expect("valid discovery JSON");

        assert_eq!(
            data.topic,
            "homeassistant/water_heater/daikin_6461696b696e_domestic_hot_water/config"
        );
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
        assert_eq!(
            payload["current_temperature_template"],
            "{{ value_json.water_temperature }}"
        );
        assert_eq!(payload["temperature_state_template"], "{{ value_json.water_setpoint }}");
        assert_eq!(payload["mode_state_template"], WATER_HEATER_MODE_STATE_TEMPLATE);
        assert_eq!(payload["modes"], json!(["off", "heat_pump"]));
        assert_eq!(payload["temperature_unit"], "C");
        let fields = payload.as_object().expect("discovery object");
        assert!(!fields.keys().any(|field| field.ends_with("_command_topic")));
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
