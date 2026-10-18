# MQTT TLS

MQTT uses plaintext TCP by default. Choose one of these mutually exclusive
options to enable TLS:

- **Certificate validation:** `--mqtt-ca-cert /path/to/ca.pem` (or
  `DAIKIN_MQTT_CA_CERT=/path/to/ca.pem`). Provide PEM CA certificates or the
  broker's self-signed certificate. The broker certificate must match
  `--mqtt-addr`. Unreadable files fail at startup; invalid certificates are
  reported when connecting.
- **Disable certificate validation:** `--mqtt-insecure` (or
  `DAIKIN_MQTT_INSECURE=true`). Traffic is encrypted, but the broker's identity
  is not verified. This is vulnerable to man-in-the-middle attacks; prefer
  providing a certificate file.

Set `--mqtt-port 8883` (or `DAIKIN_MQTT_PORT=8883`) if your broker uses the
standard TLS port. Enabling TLS does not change the default port of `1883`.
These options affect only MQTT, not the Daikin HTTPS API or the web UI.

For example:

```sh
daikin2mqtt --mqtt-addr mqtt.example.com --mqtt-port 8883 --mqtt-ca-cert /path/to/ca.pem
```

When using a container, mount the certificate file read-only and set
`DAIKIN_MQTT_CA_CERT` to its path inside the container.

At startup, the bridge logs the configured broker, port, client ID, username,
base topic, TLS mode (`disabled`, `verified`, or `insecure`), and CA certificate
path. Passwords and certificate contents are not logged. `MQTT client created;
broker connection pending` only confirms local client initialization. A successful
MQTT handshake logs `MQTT connection established`; connection errors include the
broker, port, and failure reason.
