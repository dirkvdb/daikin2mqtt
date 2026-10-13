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

The three unit information topics are published only when the corresponding
management point is present in the cloud response. Their payloads expose the
available `name`, `model_info`, and `software_version` characteristic values using
snake_case keys, consistent with the existing status payloads. Missing or null
values are omitted; empty strings returned by the cloud are preserved.

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

The three unit information topics are read-only; they do not add MQTT control
commands or Home Assistant discovery entities. Existing climate and hot-water
commands and discovery remain unchanged.
