# Home Assistant MQTT discovery

Home Assistant discovery is disabled by default. Enable it with the CLI flag:

```sh
daikin2mqtt --mqtt-addr localhost --hass-discovery
```

Alternatively, set the environment variable:

```sh
DAIKIN_HASS_DISCOVERY=true daikin2mqtt --mqtt-addr localhost
```

Configure Home Assistant's MQTT integration to use the same broker, with MQTT
discovery enabled and the default discovery prefix `homeassistant`. Existing
SecretSpec credentials and other bridge configuration are still required.

## Discovery and state topics

The bridge publishes one retained device discovery configuration containing the
climate, water-heater, and writable leaving-water offset controls:

```text
homeassistant/device/{device_id}/config
```

The device ID is `daikin_<hex encoding of the UTF-8 MQTT base topic>`. Component
unique IDs append `_climate_control`, `_domestic_hot_water`, or
`_heating_setpoint_leaving_water_offset`. All components are grouped under one
**Daikin heat pump** device in Home Assistant. For the default base topic
`daikin`, the discovery topic is:

```text
homeassistant/device/daikin_6461696b696e/config
```

This requires a Home Assistant version that supports MQTT device discovery.
Entity unique IDs, device identifiers, and state/command topics are preserved.

The leaving-water offset number is included only when the cloud reports a
writable control. If it becomes unavailable or read-only, the bridge explicitly
removes the component, then retains a device configuration without it.

The entities use these topics, where `<base>` is configured with
`--mqtt-base-topic` or `DAIKIN_MQTT_BASE_TOPIC` (default: `daikin`):

| Purpose | Topic |
| --- | --- |
| Climate state | `<base>/climate_control` |
| Water-heater state | `<base>/domestic_hot_water` |
| Availability (all entities) | `<base>/state` |
| Climate mode command | `<base>/cmnd/climate_mode` |
| Climate power command | `<base>/cmnd/climate_power` |
| Climate temperature command | `<base>/cmnd/climate_temperature` |
| Water-heater mode command | `<base>/cmnd/domestic_hot_water_mode` |
| Water-heater power command | `<base>/cmnd/domestic_hot_water_power` |
| Water-heater temperature command | `<base>/cmnd/domestic_hot_water_temperature` |
| Leaving-water offset state | `<base>/climate_control` (`heating_setpoint_leaving_water_offset`) |
| Leaving-water offset command | `<base>/cmnd/heating_setpoint_leaving_water_offset` |

The climate entity reports `off`, `heat`, or `auto`: the Daikin operation modes
`heating` and `auto` map to `heat` and `auto`, while an off power state takes
precedence. Unrecognized operation modes are reported as unknown.

## Commands

Command coverage follows the [MQTT water-heater specification](https://www.home-assistant.io/integrations/water_heater.mqtt/)
and [MQTT climate specification](https://www.home-assistant.io/integrations/climate.mqtt/):

| Home Assistant action | Climate | Water heater |
| --- | --- | --- |
| Set mode | `climate.set_hvac_mode` | `water_heater.set_operation_mode` |
| Turn on/off | `climate.turn_on` / `climate.turn_off` | `water_heater.turn_on` / `water_heater.turn_off` |
| Set target temperature (when writable) | `climate.set_temperature` | `water_heater.set_temperature` |

The water-heater specification has no additional command topic types. Climate
fan, swing, humidity, and separate high/low temperature controls are not exposed
because the supplied Daikin device data does not support them. Holiday dates and
schedules are not mapped to these entities. The leaving-water offset is exposed
as a separate number control on the same device, rather than a climate preset.

Both discovered entities support mode, power, and writable target-temperature
commands. Home Assistant publishes commands at QoS 1 without retention. The
bridge handles climate modes `off`, `heat`, and `auto`, and boiler/water-heater
modes `off`, `heat_pump`, and `performance`. Discovery advertises only modes whose
required writes pass validation against the latest cloud snapshot. For example,
a heating-only device exposes `off` and `heat`, not an unusable `auto` option.
`off` switches power off. Other modes set the Daikin operation mode if needed,
then switch power on. Fixed-mode devices can still be enabled without attempting
to change their read-only operation mode.

The boiler's `performance` mode enables Daikin powerful/boost mode by writing
`powerfulMode: on` after enabling tank power. Selecting `heat_pump` or `off`
cancels requested or active boost with `powerfulMode: off` before changing power.
All required writes are validated before any are sent. Devices without writable
boost support reject `performance`; normal heating remains available when boost
is not active.

The reported water-heater mode is `off` when tank power is off, `performance`
when `isPowerfulModeActive` is true, and otherwise `heat_pump` for Daikin's
`heating` operation mode. This uses the existing MQTT `powerfull_mode` attribute
(the spelling is retained for compatibility) and reports actual cloud state,
not an optimistic boost selection.

Both entities advertise `power_command_topic`, with `payload_on: on` and
`payload_off: off`, when the device accepts both power states. Home Assistant's
`climate.turn_on` and `climate.turn_off` actions publish to
`<base>/cmnd/climate_power`; `water_heater.turn_on` and `water_heater.turn_off`
publish to `<base>/cmnd/domestic_hot_water_power`. Power commands only change
`onOffMode`; they preserve the operation mode and do not enable or cancel
powerful/boost mode. Actual state is reported through the existing
`mode_state_topic`, without optimistic updates. No `power_command_template` is
needed because the configured payloads already match Daikin's values.

Temperature payloads are numbers in degrees Celsius. Climate commands update
`/operationModes/heating/setpoints/roomTemperature`; water-heater commands update
`/operationModes/heating/setpoints/domesticHotWaterTemperature` within the
`temperatureControl` characteristic. Fractional temperatures are preserved.
Discovery uses the device's reported temperature bounds and climate step size.
A temperature command topic is advertised only when both `temperatureControl`
and its nested setpoint are writable. Read-only target and current temperatures
remain visible, but Home Assistant does not offer a control that the device would
reject. Similarly, mode and power command topics are omitted when their writes
cannot be validated or the command target is missing or ambiguous.

Commands require a successful cloud poll to identify the gateway and management
point. The bridge validates writable flags, supported values, temperature bounds,
and step sizes before sending authenticated PATCH requests to Daikin. Unsupported
modes and read-only setpoints are rejected and logged, not forced. This means a
boiler setpoint marked read-only by Daikin cannot be changed by the bridge.
Commands are rejected if multiple gateways expose the same entity, rather than
sending them to an arbitrary device. Authentication remains web-UI-only.

After Daikin accepts all writes for a command, the bridge schedules exactly one
status refresh after a 2-second delay. A new valid command received while that
refresh is pending resets the timer; successful completion starts a full
2-second delay again. Repeated temperature increases therefore share one status
read rather than causing a read for every increase. The delay does not block
MQTT processing, and there is no retry loop or optimistic state update.

The scheduled refresh replaces a regular poll due during the delay, and the
normal polling interval restarts after a status read to avoid back-to-back
requests. A failed command does not schedule a new refresh. If the single status
read fails or still reports old values, the bridge waits for the next regular
poll instead of spending additional daily API quota. API acceptance does not
guarantee that the cloud will reflect a change within 2 seconds.

Failed commands are logged and are not queued for later replay. Retained incoming
commands are ignored; do not retain commands in manual MQTT publishers either,
since brokers can deliver a live retained publication without marking it as
retained to existing subscribers.

Discovery configuration is published with retain enabled on every successful
status update. Home Assistant can discover the device after connecting, and a
broker restart that loses retained messages is repaired by the next successful
status update.

## Command logging

At the default `INFO` log level, each received control command includes its MQTT
topic and payload. Each successfully sent Daikin update also includes the target
gateway, management point, characteristic, nested path (`/` for a root value),
and JSON value. Update positions such as `1/2` identify multi-write commands
like water-heater boost. The final command summary includes the update count.
These messages confirm that the API accepted the writes, not that the device has
already reported the new state.

Validation failures include the topic, payload, and reason at `WARN`. A failed
API write also identifies the failed update and the number of previously sent
updates, making partial failures visible. Ignored retained commands include their
command details at `WARN`. Unknown command-topic payloads are redacted to avoid
logging credentials or legacy authentication codes; quoted strings escape
newlines and other control characters.

Set `RUST_LOG=daikin2mqtt=debug` to also log each update before the API request,
which helps distinguish an in-flight write from an accepted or failed write.
