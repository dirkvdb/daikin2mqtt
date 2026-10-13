# MQTT status topics

All topics below are relative to the configured MQTT base topic. Status messages
are retained and published with QoS 1 after a successful cloud update.

| Topic | Payload |
| --- | --- |
| `state` | `online` or `offline` (also used as the MQTT last will) |
| `climate_control` | Climate control status JSON |
| `domestic_hot_water` | Domestic hot water tank status JSON |
| `indoor_unit_hydro` | Indoor hydro unit information JSON |
| `outdoor_unit` | Outdoor unit information JSON |
| `user_interface` | User interface information JSON |
| `gateway` | Gateway information JSON |

The three unit information topics are published only when the corresponding
management point is present in the cloud response. Their payloads expose the
available `name`, `model_info`, and `software_version` characteristic values using
snake_case keys, consistent with the existing status payloads. Missing or null
values are omitted; empty strings returned by the cloud are preserved.

The `gateway` topic is likewise published only when its management point is
present. It exposes the available unit information fields, plus
`firmware_version`, `ip_address`, `mac_address`, and the boolean
`is_firmware_update_supported`. Missing or null values are omitted; `false` and
empty strings are preserved.

For example, `gateway` may contain:

```json
{
  "name": "Gateway",
  "model_info": "BRP069A78",
  "firmware_version": "3.2.5",
  "ip_address": "192.168.1.176",
  "mac_address": "20:4e:f6:a6:00:00",
  "is_firmware_update_supported": true
}
```

The gateway payload includes network identifiers, so MQTT access should be
restricted to trusted consumers.

For example, `indoor_unit_hydro` may contain:

```json
{
  "name": "Indoor Hydro Unit",
  "model_info": "EHVH08S23EJ9W",
  "software_version": "0221"
}
```

A user interface without a model characteristic may contain:

```json
{
  "name": "User Interface",
  "software_version": "6.3.0"
}
```

The unit and gateway information topics are read-only; they do not add MQTT control
commands or Home Assistant discovery entities. Existing climate and hot-water
commands and discovery remain unchanged.
