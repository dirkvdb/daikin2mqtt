use serde_json::{json, Value};

use crate::{mqtt::MqttData, MqttBridgeError};

pub(crate) const CLIMATE_MODE: &str = "climate_mode";
pub(crate) const CLIMATE_POWER: &str = "climate_power";
pub(crate) const CLIMATE_TEMPERATURE: &str = "climate_temperature";
pub(crate) const CLIMATE_LEAVING_WATER_OFFSET: &str = "heating_setpoint_leaving_water_offset";
pub(crate) const WATER_MODE: &str = "domestic_hot_water_mode";
pub(crate) const WATER_POWER: &str = "domestic_hot_water_power";
pub(crate) const WATER_TEMPERATURE: &str = "domestic_hot_water_temperature";

pub(crate) fn topic(base: &str, command: &str) -> String {
    format!("{base}/cmnd/{command}")
}

#[derive(Debug, PartialEq)]
pub(crate) struct CharacteristicUpdate {
    pub gateway_id: String,
    pub management_point: String,
    pub characteristic: &'static str,
    pub path: Option<String>,
    pub value: Value,
}

impl std::fmt::Display for CharacteristicUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gateway={:?}, management_point={:?}, characteristic={:?}, path={:?}, value={}",
            self.gateway_id,
            self.management_point,
            self.characteristic,
            self.path.as_deref().unwrap_or("/"),
            self.value,
        )
    }
}

pub(crate) struct CommandDetails<'a> {
    message: &'a MqttData,
    log_payload: bool,
}

impl std::fmt::Display for CommandDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "topic={:?}, payload=", self.message.topic)?;
        if self.log_payload {
            write!(f, "{:?}", self.message.payload)
        } else {
            f.write_str("<redacted: unsupported command>")
        }
    }
}

pub(crate) fn details<'a>(base: &str, message: &'a MqttData) -> CommandDetails<'a> {
    CommandDetails {
        message,
        // Unknown topics may carry credentials or old MQTT authentication codes.
        log_payload: command_kind(base, &message.topic).is_ok(),
    }
}

fn invalid(message: &str) -> MqttBridgeError {
    MqttBridgeError::RuntimeError(message.into())
}

fn command_kind(base: &str, topic: &str) -> Result<(bool, bool), MqttBridgeError> {
    let command = topic
        .strip_prefix(&format!("{base}/cmnd/"))
        .ok_or_else(|| invalid("Unknown MQTT command topic"))?;
    match command {
        CLIMATE_MODE | CLIMATE_POWER => Ok((false, false)),
        CLIMATE_TEMPERATURE | CLIMATE_LEAVING_WATER_OFFSET => Ok((false, true)),
        WATER_MODE | WATER_POWER => Ok((true, false)),
        WATER_TEMPERATURE => Ok((true, true)),
        _ => Err(invalid("Unknown MQTT command; authentication is only available in the web UI")),
    }
}

// Resolve against the same snapshot used for state publication. Never guess a gateway
// or broadcast a command when several devices expose the same management point.
fn target<'a>(devices: &'a Value, embedded_id: &str) -> Result<(&'a str, &'a Value), MqttBridgeError> {
    let mut matches = devices
        .as_array()
        .ok_or_else(|| invalid("No cloud device snapshot available; wait for a successful poll"))?
        .iter()
        .flat_map(|device| {
            device["managementPoints"].as_array().into_iter().flatten().filter_map(move |point| {
                (point["embeddedId"].as_str() == Some(embedded_id))
                    .then(|| device["id"].as_str().map(|id| (id, point)))
                    .flatten()
            })
        });
    let first = matches.next().ok_or_else(|| invalid("Command management point not found"))?;
    if matches.next().is_some() {
        return Err(invalid("Command target is ambiguous: multiple gateways expose this entity"));
    }
    Ok(first)
}

fn validate(metadata: &Value, value: &Value) -> Result<(), MqttBridgeError> {
    if metadata["settable"].as_bool() != Some(true) {
        return Err(invalid("Requested setting is read-only"));
    }
    if let Some(values) = metadata["values"].as_array() {
        if !values.contains(value) {
            return Err(invalid("Requested value is not supported by this device"));
        }
    }
    if let Some(number) = value.as_f64() {
        let min = metadata["minValue"].as_f64();
        let max = metadata["maxValue"].as_f64();
        if min.is_some_and(|min| number < min) || max.is_some_and(|max| number > max) {
            return Err(invalid("Requested temperature is outside the device limits"));
        }
        if let Some(step) = metadata["stepValue"].as_f64().filter(|step| *step > 0.0) {
            let steps = (number - min.unwrap_or(0.0)) / step;
            if (steps - steps.round()).abs() > 1e-6 {
                return Err(invalid("Requested temperature does not match the device step"));
            }
        }
    }
    Ok(())
}

pub(crate) fn temperature_metadata(devices: &Value, water: bool) -> Option<&Value> {
    let (_, point) = target(devices, if water { "domesticHotWaterTank" } else { "climateControlMainZone" }).ok()?;
    Some(&point["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"][if water { "domesticHotWaterTemperature" } else { "roomTemperature" }])
}

pub(crate) fn temperature_writable(devices: &Value, water: bool) -> bool {
    target(devices, if water { "domesticHotWaterTank" } else { "climateControlMainZone" }).is_ok_and(|(_, point)| {
        point["temperatureControl"]["settable"].as_bool() == Some(true)
            && temperature_metadata(devices, water).is_some_and(|metadata| metadata["settable"].as_bool() == Some(true))
    })
}

pub(crate) fn leaving_water_offset_writable(devices: &Value) -> bool {
    target(devices, "climateControlMainZone").is_ok_and(|(_, point)| {
        point["temperatureControl"]["settable"].as_bool() == Some(true)
            && point["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]["leavingWaterOffset"]["settable"].as_bool() == Some(true)
    })
}

pub(crate) fn updates(base: &str, message: &MqttData, devices: &Value) -> Result<Vec<CharacteristicUpdate>, MqttBridgeError> {
    let command = message
        .topic
        .strip_prefix(&format!("{base}/cmnd/"))
        .ok_or_else(|| invalid("Unknown MQTT command topic"))?;
    let (water, temperature) = command_kind(base, &message.topic)?;
    let embedded_id = if water { "domesticHotWaterTank" } else { "climateControlMainZone" };
    let (gateway_id, point) = target(devices, embedded_id)?;
    let update = |characteristic, path, value| CharacteristicUpdate {
        gateway_id: gateway_id.into(),
        management_point: embedded_id.into(),
        characteristic,
        path,
        value,
    };
    let payload = message.payload.trim();
    if matches!(command, CLIMATE_POWER | WATER_POWER) {
        if !matches!(payload, "on" | "off") {
            return Err(invalid("Power command must be on or off"));
        }
        let value = json!(payload);
        validate(&point["onOffMode"], &value)?;
        return Ok(vec![update("onOffMode", None, value)]);
    }
    if temperature {
        let number: f64 = payload.parse().map_err(|_| invalid("Temperature command must be a number"))?;
        if !number.is_finite() {
            return Err(invalid("Temperature command must be finite"));
        }
        let offset = command == CLIMATE_LEAVING_WATER_OFFSET;
        if offset && (!(-10.0..=10.0).contains(&number) || number.fract() != 0.0) {
            return Err(invalid("Leaving water offset must be an integer between -10 and 10"));
        }
        let setpoint = if offset {
            "leavingWaterOffset"
        } else if water {
            "domesticHotWaterTemperature"
        } else {
            "roomTemperature"
        };
        let path = format!("/operationModes/heating/setpoints/{setpoint}");
        let control = &point["temperatureControl"];
        if control["settable"].as_bool() != Some(true) {
            return Err(invalid("Temperature control is read-only"));
        }
        let metadata = control["value"].pointer(&path).ok_or_else(|| invalid("Temperature setpoint not found"))?;
        let value = if offset { json!(number as i8) } else { json!(number) };
        validate(metadata, &value)?;
        return Ok(vec![update("temperatureControl", Some(path), value)]);
    }

    let operation = match (water, payload) {
        (_, "off") => None,
        (false, "heat") | (true, "heat_pump" | "performance") => Some("heating"),
        (false, "auto") => Some("auto"),
        _ => return Err(invalid("Unsupported Home Assistant mode")),
    };
    let power = json!(if operation.is_some() { "on" } else { "off" });
    validate(&point["onOffMode"], &power)?;
    let mut result = Vec::new();
    if let Some(operation) = operation {
        // Fixed-mode systems expose operationMode as read-only. Enabling their
        // current mode needs only an onOffMode write.
        if point["operationMode"]["value"] != operation {
            let value = json!(operation);
            validate(&point["operationMode"], &value)?;
            result.push(update("operationMode", None, value));
        }
    }
    let mut boost_update = None;
    if water {
        let boost = &point["powerfulMode"];
        let performance = payload == "performance";
        // Normal mode remains usable on devices without boost support. If boost
        // is requested or active, validate its write before sending any updates.
        if performance || boost["value"] == "on" || point["isPowerfulModeActive"]["value"] == true {
            let value = json!(if performance { "on" } else { "off" });
            validate(boost, &value)?;
            boost_update = Some(update("powerfulMode", None, value));
        }
    }
    if payload == "performance" {
        // Enable the tank before starting boost; otherwise cancel boost before
        // changing power when returning to normal heating or switching off.
        result.push(update("onOffMode", None, power));
        result.extend(boost_update);
    } else {
        result.extend(boost_update);
        result.push(update("onOffMode", None, power));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaving_water_offset_commands_use_integer_values_and_nested_path() {
        for number in -10..=10 {
            let updates = updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, &number.to_string()), &devices()).unwrap();
            assert_eq!(updates.len(), 1);
            let update = &updates[0];
            assert_eq!(update.gateway_id, devices()[0]["id"].as_str().unwrap());
            assert_eq!(update.management_point, "climateControlMainZone");
            assert_eq!(update.characteristic, "temperatureControl");
            assert_eq!(update.path.as_deref(), Some("/operationModes/heating/setpoints/leavingWaterOffset"));
            assert_eq!(update.value.as_i64(), Some(number));
        }
        assert!(updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, " 5.0 "), &devices()).is_ok());
        for payload in ["-11", "11", "0.5", "NaN", "inf", "", "invalid"] {
            assert!(
                updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, payload), &devices()).is_err(),
                "{payload}"
            );
        }
    }

    #[test]
    fn leaving_water_offset_commands_respect_device_capabilities() {
        let mut snapshot = devices();
        assert!(leaving_water_offset_writable(&snapshot));
        for path in [
            "/temperatureControl/settable",
            "/temperatureControl/value/operationModes/heating/setpoints/leavingWaterOffset/settable",
        ] {
            let mut snapshot = devices();
            *point(&mut snapshot, "climateControlMainZone").pointer_mut(path).unwrap() = json!(false);
            assert!(!leaving_water_offset_writable(&snapshot));
            assert!(updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, "5"), &snapshot).is_err());
        }
        point(&mut snapshot, "climateControlMainZone")["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]
            .as_object_mut()
            .unwrap()
            .remove("leavingWaterOffset");
        assert!(!leaving_water_offset_writable(&snapshot));
        assert!(updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, "5"), &snapshot).is_err());
        assert!(updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, "5"), &json!([])).is_err());
        let mut snapshot = devices();
        let duplicate = snapshot[0].clone();
        snapshot.as_array_mut().unwrap().push(duplicate);
        assert!(!leaving_water_offset_writable(&snapshot));
        assert!(updates("home/daikin", &message(CLIMATE_LEAVING_WATER_OFFSET, "5"), &snapshot).is_err());
    }

    fn devices() -> Value {
        serde_json::from_str(include_str!("../resources/test/managementpoints.json")).expect("device fixture")
    }

    fn message(command: &str, payload: &str) -> MqttData {
        MqttData::new(topic("home/daikin", command), payload.into())
    }

    fn point<'a>(devices: &'a mut Value, id: &str) -> &'a mut Value {
        devices[0]["managementPoints"]
            .as_array_mut()
            .expect("points")
            .iter_mut()
            .find(|p| p["embeddedId"] == id)
            .expect("point")
    }

    #[test]
    fn command_details_include_control_payloads_even_when_invalid() {
        for base in ["daikin", "house/heat-pump"] {
            for (command, payload) in [
                (CLIMATE_MODE, "auto"),
                (CLIMATE_POWER, "off"),
                (CLIMATE_TEMPERATURE, "20.5"),
                (WATER_MODE, "performance"),
                (WATER_POWER, "on"),
                (WATER_TEMPERATURE, "invalid-temperature"),
            ] {
                let topic = topic(base, command);
                let message = MqttData::new(topic.clone(), payload.into());
                assert_eq!(details(base, &message).to_string(), format!("topic={topic:?}, payload={payload:?}"));
            }
        }
    }

    #[test]
    fn command_details_redact_unknown_topics_and_escape_control_characters() {
        for topic in [
            "home/daikin/cmnd/auth",
            "home/daikin/cmnd/climate_mode/extra",
            "other/cmnd/climate_power",
            "home/daikin/cmnd/unknown\n",
        ] {
            let message = MqttData::new(topic.into(), "secret-authentication-code".into());
            let logged = details("home/daikin", &message).to_string();
            assert_eq!(logged, format!("topic={topic:?}, payload=<redacted: unsupported command>"));
            assert!(!logged.contains("secret-authentication-code"));
            assert!(!logged.contains('\n'));
        }
        let message = message(CLIMATE_TEMPERATURE, "20.5\n\t\"injected\"");
        let logged = details("home/daikin", &message).to_string();
        assert_eq!(logged, "topic=\"home/daikin/cmnd/climate_temperature\", payload=\"20.5\\n\\t\\\"injected\\\"\"");
        assert!(!logged.contains('\n'));
    }

    #[test]
    fn update_details_include_targets_paths_and_json_values() {
        let update = updates("home/daikin", &message(CLIMATE_TEMPERATURE, "20.5"), &devices())
            .expect("temperature command")
            .remove(0);
        assert_eq!(
            update.to_string(),
            "gateway=\"c6d05dff-db9b-4565-825b-000000000000\", management_point=\"climateControlMainZone\", characteristic=\"temperatureControl\", path=\"/operationModes/heating/setpoints/roomTemperature\", value=20.5",
        );
        let updates = updates("home/daikin", &message(WATER_MODE, "performance"), &devices()).expect("boost command");
        for (update, characteristic) in updates.iter().zip(["onOffMode", "powerfulMode"]) {
            assert_eq!(
                update.to_string(),
                format!("gateway=\"c6d05dff-db9b-4565-825b-000000000000\", management_point=\"domesticHotWaterTank\", characteristic={characteristic:?}, path=\"/\", value=\"on\""),
            );
        }
    }

    #[test]
    fn modes_control_power_for_both_entities() {
        for (command, mode, id) in [
            (CLIMATE_MODE, "heat", "climateControlMainZone"),
            (WATER_MODE, "heat_pump", "domesticHotWaterTank"),
        ] {
            for (payload, power) in [(mode, "on"), ("off", "off")] {
                let result = updates("home/daikin", &message(command, payload), &devices()).expect("mode command");
                assert_eq!(result.len(), 1);
                assert_eq!(result[0].gateway_id, devices()[0]["id"]);
                assert_eq!(result[0].management_point, id);
                assert_eq!(result[0].characteristic, "onOffMode");
                assert_eq!(result[0].value, power);
                assert_eq!(result[0].path, None);
            }
        }
    }

    #[test]
    fn boiler_performance_enables_power_then_powerful_mode() {
        let devices = devices();
        let result = updates("home/daikin", &message(WATER_MODE, "performance"), &devices).expect("performance command");
        assert_eq!(result.len(), 2);
        for update in &result {
            assert_eq!(update.gateway_id, devices[0]["id"]);
            assert_eq!(update.management_point, "domesticHotWaterTank");
            assert_eq!(update.path, None);
            assert_eq!(update.value, "on");
        }
        assert_eq!(result[0].characteristic, "onOffMode");
        assert_eq!(result[1].characteristic, "powerfulMode");
    }

    #[test]
    fn boiler_normal_modes_cancel_requested_or_active_boost() {
        for active_only in [false, true] {
            let mut devices = devices();
            let boiler = point(&mut devices, "domesticHotWaterTank");
            if active_only {
                boiler["isPowerfulModeActive"]["value"] = json!(true);
            } else {
                boiler["powerfulMode"]["value"] = json!("on");
            }
            for (mode, power) in [("heat_pump", "on"), ("off", "off")] {
                let result = updates("home/daikin", &message(WATER_MODE, mode), &devices).expect("normal mode");
                assert_eq!(result.len(), 2);
                assert_eq!(result[0].characteristic, "powerfulMode");
                assert_eq!(result[0].value, "off");
                assert_eq!(result[1].characteristic, "onOffMode");
                assert_eq!(result[1].value, power);
            }
        }
    }

    #[test]
    fn boiler_modes_validate_boost_support_before_any_writes() {
        for boost in [
            Value::Null,
            json!({"settable": false, "value": "off"}),
            json!({"settable": true, "value": "off", "values": ["off"]}),
        ] {
            let mut devices = devices();
            point(&mut devices, "domesticHotWaterTank")["powerfulMode"] = boost;
            assert!(updates("home/daikin", &message(WATER_MODE, "performance"), &devices).is_err());
            assert!(updates("home/daikin", &message(WATER_MODE, "heat_pump"), &devices).is_ok());
        }
        let mut devices = devices();
        let boiler = point(&mut devices, "domesticHotWaterTank");
        boiler["powerfulMode"]["value"] = json!("on");
        boiler["powerfulMode"]["settable"] = json!(false);
        assert!(updates("home/daikin", &message(WATER_MODE, "heat_pump"), &devices).is_err());
        point(&mut devices, "domesticHotWaterTank")["powerfulMode"]["settable"] = json!(true);
        point(&mut devices, "domesticHotWaterTank")["onOffMode"]["settable"] = json!(false);
        assert!(updates("home/daikin", &message(WATER_MODE, "performance"), &devices).is_err());
    }

    #[test]
    fn boiler_power_leaves_operation_and_powerful_modes_unchanged() {
        let mut devices = devices();
        let boiler = point(&mut devices, "domesticHotWaterTank");
        boiler["operationMode"]["value"] = json!("other_mode");
        boiler["powerfulMode"]["value"] = json!("on");
        for power in ["on", "off"] {
            let result = updates("home/daikin", &message(WATER_POWER, power), &devices).expect("power command");
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].gateway_id, devices[0]["id"]);
            assert_eq!(result[0].management_point, "domesticHotWaterTank");
            assert_eq!(result[0].characteristic, "onOffMode");
            assert_eq!(result[0].path, None);
            assert_eq!(result[0].value, power);
        }
        point(&mut devices, "domesticHotWaterTank")["onOffMode"]["values"] = json!(["off"]);
        assert!(updates("home/daikin", &message(WATER_POWER, "on"), &devices).is_err());
        point(&mut devices, "domesticHotWaterTank")["onOffMode"]["settable"] = json!(false);
        assert!(updates("home/daikin", &message(WATER_POWER, "off"), &devices).is_err());
    }

    #[test]
    fn climate_power_leaves_operation_and_powerful_modes_unchanged() {
        let mut devices = devices();
        let climate = point(&mut devices, "climateControlMainZone");
        climate["operationMode"] = json!({"settable": false, "value": "auto", "values": ["auto"]});
        climate["powerfulMode"] = json!({"settable": false, "value": "on", "values": ["on"]});
        climate["isPowerfulModeActive"] = json!({"value": true});
        for power in ["on", "off"] {
            let result = updates("home/daikin", &message(CLIMATE_POWER, power), &devices).expect("power command");
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].gateway_id, devices[0]["id"]);
            assert_eq!(result[0].management_point, "climateControlMainZone");
            assert_eq!(result[0].characteristic, "onOffMode");
            assert_eq!(result[0].path, None);
            assert_eq!(result[0].value, power);
        }
    }

    #[test]
    fn power_commands_validate_on_off_metadata() {
        for (command, id) in [(CLIMATE_POWER, "climateControlMainZone"), (WATER_POWER, "domesticHotWaterTank")] {
            for power in ["on", "off"] {
                for metadata in [
                    Value::Null,
                    json!({"settable": false, "values": ["on", "off"]}),
                    json!({"values": ["on", "off"]}),
                    json!({"settable": "true", "values": ["on", "off"]}),
                    json!({"settable": true, "values": []}),
                    json!({"settable": true, "values": [if power == "on" { "off" } else { "on" }]}),
                ] {
                    let mut devices = devices();
                    point(&mut devices, id)["onOffMode"] = metadata;
                    assert!(updates("home/daikin", &message(command, power), &devices).is_err());
                }
            }
        }
    }

    #[test]
    fn power_commands_use_custom_base_topics_and_trim_payloads() {
        for (command, id) in [(CLIMATE_POWER, "climateControlMainZone"), (WATER_POWER, "domesticHotWaterTank")] {
            for base in ["custom/heat-pump", "another/site/daikin"] {
                for power in ["on", "off"] {
                    let msg = MqttData::new(topic(base, command), format!(" \n{power}\t "));
                    let result = updates(base, &msg, &devices()).expect("custom power command");
                    assert_eq!(result.len(), 1);
                    assert_eq!(result[0].management_point, id);
                    assert_eq!(result[0].characteristic, "onOffMode");
                    assert_eq!(result[0].value, power);
                    assert!(updates("home/daikin", &msg, &devices()).is_err());
                }
            }
        }
    }

    #[test]
    fn power_commands_reject_invalid_payloads() {
        for command in [CLIMATE_POWER, WATER_POWER] {
            for payload in ["heat", "heat_pump", "auto", "performance", "ON", "OFF", "true", "false", "1", "0", "", " \t"] {
                assert!(updates("home/daikin", &message(command, payload), &devices()).is_err(), "{command}: {payload}");
            }
        }
    }

    #[test]
    fn auto_sets_operation_before_enabling_and_validates_all_writes() {
        let mut devices = devices();
        let p = point(&mut devices, "climateControlMainZone");
        p["operationMode"]["settable"] = json!(true);
        p["operationMode"]["values"] = json!(["heating", "auto"]);
        let result = updates("home/daikin", &message(CLIMATE_MODE, "auto"), &devices).expect("auto command");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].characteristic, "operationMode");
        assert_eq!(result[0].value, "auto");
        assert_eq!(result[1].characteristic, "onOffMode");
        point(&mut devices, "climateControlMainZone")["onOffMode"]["settable"] = json!(false);
        assert!(updates("home/daikin", &message(CLIMATE_MODE, "auto"), &devices).is_err());
    }

    #[test]
    fn temperatures_use_nested_paths_and_preserve_fractional_values() {
        for (command, id, setpoint, temperature) in [
            (CLIMATE_TEMPERATURE, "climateControlMainZone", "roomTemperature", 20.5),
            (WATER_TEMPERATURE, "domesticHotWaterTank", "domesticHotWaterTemperature", 48.5),
        ] {
            let mut devices = devices();
            let p = point(&mut devices, id);
            let metadata = &mut p["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"][setpoint];
            metadata["settable"] = json!(true);
            metadata["stepValue"] = json!(0.5);
            let result = updates("home/daikin", &message(command, &temperature.to_string()), &devices).expect("temperature command");
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].management_point, id);
            assert_eq!(result[0].characteristic, "temperatureControl");
            assert_eq!(
                result[0].path.as_deref(),
                Some(format!("/operationModes/heating/setpoints/{setpoint}").as_str())
            );
            assert_eq!(result[0].value, temperature);
        }
    }

    #[test]
    fn temperature_writable_requires_both_settable_flags() {
        for (water, id, setpoint) in [
            (false, "climateControlMainZone", "roomTemperature"),
            (true, "domesticHotWaterTank", "domesticHotWaterTemperature"),
        ] {
            for control_settable in [json!(true), json!(false), Value::Null, json!("true")] {
                for setpoint_settable in [json!(true), json!(false), Value::Null, json!("true")] {
                    let mut devices = devices();
                    let control = &mut point(&mut devices, id)["temperatureControl"];
                    control["settable"] = control_settable.clone();
                    control["value"]["operationModes"]["heating"]["setpoints"][setpoint]["settable"] = setpoint_settable.clone();
                    assert_eq!(
                        temperature_writable(&devices, water),
                        control_settable == true && setpoint_settable == true,
                        "{id}: control={control_settable}, setpoint={setpoint_settable}"
                    );
                }
            }
        }
    }

    #[test]
    fn temperature_writable_rejects_missing_metadata() {
        for (water, id, setpoint) in [
            (false, "climateControlMainZone", "roomTemperature"),
            (true, "domesticHotWaterTank", "domesticHotWaterTemperature"),
        ] {
            let mut devices = devices();
            let control = &mut point(&mut devices, id)["temperatureControl"];
            control["settable"] = json!(true);
            control["value"]["operationModes"]["heating"]["setpoints"][setpoint]["settable"] = json!(true);
            assert!(temperature_writable(&devices, water));
            for path in [
                "/temperatureControl".to_string(),
                "/temperatureControl/settable".to_string(),
                "/temperatureControl/value".to_string(),
                "/temperatureControl/value/operationModes/heating".to_string(),
                format!("/temperatureControl/value/operationModes/heating/setpoints/{setpoint}"),
                format!("/temperatureControl/value/operationModes/heating/setpoints/{setpoint}/settable"),
            ] {
                let mut missing = devices.clone();
                *point(&mut missing, id).pointer_mut(&path).expect("temperature metadata") = Value::Null;
                assert!(!temperature_writable(&missing, water), "{id}: {path}");
            }
            let mut missing = devices.clone();
            point(&mut missing, id).as_object_mut().expect("point").remove("temperatureControl");
            assert!(!temperature_writable(&missing, water));
        }
    }

    #[test]
    fn temperature_writable_rejects_missing_or_ambiguous_targets() {
        for (water, id) in [(false, "climateControlMainZone"), (true, "domesticHotWaterTank")] {
            assert!(!temperature_writable(&Value::Null, water));
            assert!(!temperature_writable(&json!([]), water));
            let mut devices = devices();
            assert!(!temperature_writable(&json!([devices[0], devices[0]]), water));
            point(&mut devices, id)["embeddedId"] = json!("otherPoint");
            assert!(!temperature_writable(&devices, water));
        }
    }

    #[test]
    fn rejects_invalid_unsupported_and_read_only_commands() {
        for (command, payload) in [
            (CLIMATE_MODE, "cool"),
            (CLIMATE_MODE, "performance"),
            (CLIMATE_MODE, "auto"),
            (WATER_MODE, "heat"),
            (WATER_POWER, "heat_pump"),
            (WATER_POWER, "ON"),
            (WATER_POWER, "true"),
            (WATER_POWER, ""),
            (CLIMATE_TEMPERATURE, "NaN"),
            (CLIMATE_TEMPERATURE, "inf"),
            (CLIMATE_TEMPERATURE, "warm"),
            (CLIMATE_TEMPERATURE, "100"),
            (CLIMATE_TEMPERATURE, "20.25"),
            (WATER_TEMPERATURE, "48"),
            ("auth", "code"),
            ("climate_mode/extra", "heat"),
        ] {
            assert!(updates("home/daikin", &message(command, payload), &devices()).is_err(), "{command}: {payload}");
        }
        assert!(updates("other/base", &message(CLIMATE_MODE, "heat"), &devices()).is_err());
    }

    #[test]
    fn rejects_missing_or_ambiguous_targets() {
        let msg = message(CLIMATE_MODE, "off");
        assert!(updates("home/daikin", &msg, &Value::Null).is_err());
        assert!(updates("home/daikin", &msg, &json!([])).is_err());
        let devices = devices();
        assert!(updates("home/daikin", &msg, &json!([devices[0], devices[0]])).is_err());
    }
}
