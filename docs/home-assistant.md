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

The bridge publishes retained climate and water-heater discovery configurations to:

```text
homeassistant/climate/{climate_unique_id}/config
homeassistant/water_heater/{water_heater_unique_id}/config
```

Unique IDs use `daikin_<hex encoding of the UTF-8 MQTT base topic>` followed by
`_climate_control` or `_domestic_hot_water`. Both entities share the same Daikin
heat-pump device in Home Assistant. For the default base topic `daikin`, the
discovery topics are:

```text
homeassistant/climate/daikin_6461696b696e_climate_control/config
homeassistant/water_heater/daikin_6461696b696e_domestic_hot_water/config
```

The entities use these topics, where `<base>` is configured with
`--mqtt-base-topic` or `DAIKIN_MQTT_BASE_TOPIC` (default: `daikin`):

| Purpose | Topic |
| --- | --- |
| Climate state | `<base>/climate_control` |
| Water-heater state | `<base>/domestic_hot_water` |
| Availability (both entities) | `<base>/state` |

The climate entity reports `off`, `heat`, or `auto`: the Daikin operation modes
`heating` and `auto` map to `heat` and `auto`, while an off power state takes
precedence. Unrecognized operation modes are reported as unknown.

Discovery configuration is published with retain enabled on every successful
status update. Home Assistant can discover the entity after connecting, and a
broker restart that loses retained messages is repaired by the next successful
status update.
