use std::{path::PathBuf, sync::Arc, time::Duration};

use rumqttc::{
    tokio_rustls::rustls::{
        self,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
        pki_types::{CertificateDer, ServerName, UnixTime},
        ClientConfig, DigitallySignedStruct, SignatureScheme,
    },
    Transport,
};

use rumqttc::v5::{
    mqttbytes::{
        v5::{ConnectReturnCode, LastWill, Packet},
        QoS,
    },
    AsyncClient, Event, EventLoop, MqttOptions,
};

use crate::MqttBridgeError;

#[derive(Clone)]
pub struct MqttConfig {
    pub server: String,
    pub port: u16,
    pub ca_cert: Option<PathBuf>,
    pub insecure: bool,
    pub client_id: String,
    pub user: String,
    pub password: String,
    pub base_topic: String,
}

impl MqttConfig {
    fn transport(&self) -> Result<Transport, MqttBridgeError> {
        if self.ca_cert.is_some() && self.insecure {
            return Err(MqttBridgeError::RuntimeError(
                "MQTT CA certificate and insecure TLS options are mutually exclusive".into(),
            ));
        }

        if let Some(path) = &self.ca_cert {
            let ca =
                std::fs::read(path).map_err(|err| MqttBridgeError::RuntimeError(format!("Failed to read MQTT CA certificate {}: {err}", path.display())))?;
            Ok(Transport::tls(ca, None, None))
        } else if self.insecure {
            log::warn!("MQTT TLS certificate validation is disabled; the connection is vulnerable to man-in-the-middle attacks");
            let builder = ClientConfig::builder();
            let verifier = NoCertificateVerification(builder.crypto_provider().clone());
            let config = builder.dangerous().with_custom_certificate_verifier(Arc::new(verifier)).with_no_client_auth();
            Ok(Transport::tls_with_config(config.into()))
        } else {
            Ok(Transport::tcp())
        }
    }
}

#[derive(Debug)]
struct NoCertificateVerification(Arc<CryptoProvider>);

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    // Only certificate trust and hostname validation are skipped, not handshake signatures.
    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[derive(Debug)]
pub struct MqttData {
    pub topic: String,
    pub payload: String,
}

impl MqttData {
    pub fn new(topic: String, payload: String) -> MqttData {
        MqttData { topic, payload }
    }
}

pub struct MqttConnection {
    client: AsyncClient,
    eventloop: EventLoop,
    base_topic: String,
}

fn from_mqtt_string(stream: &bytes::Bytes) -> Result<String, MqttBridgeError> {
    match String::from_utf8(stream.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(MqttBridgeError::ParseError),
    }
}

const OFFLINE_PAYLOAD: &str = "offline";
const ONLINE_PAYLOAD: &str = "online";

fn state_topic(base_topic: &String) -> String {
    format!("{}/state", base_topic)
}

impl MqttConnection {
    pub fn new(cfg: MqttConfig) -> Result<MqttConnection, MqttBridgeError> {
        let transport = cfg.transport()?;
        let mut mqttoptions = MqttOptions::new(cfg.client_id, cfg.server, cfg.port);
        mqttoptions.set_transport(transport);
        mqttoptions.set_clean_start(true);
        mqttoptions.set_keep_alive(Duration::from_secs(180));
        mqttoptions.set_last_will(LastWill::new(state_topic(&cfg.base_topic), OFFLINE_PAYLOAD, QoS::AtLeastOnce, true, None));

        if !cfg.user.is_empty() {
            mqttoptions.set_credentials(cfg.user, cfg.password);
        }

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);

        log::info!("MQTT connection created");
        Ok(MqttConnection {
            client,
            eventloop,
            base_topic: cfg.base_topic,
        })
    }

    pub async fn poll(&mut self) -> Result<Option<MqttData>, MqttBridgeError> {
        match self.eventloop.poll().await {
            Ok(msg) => self.handle_mqtt_message(msg).await,
            Err(err) => {
                log::warn!("MQTT connection error: {err}");
                Err(err.into())
            }
        }
    }

    pub fn base_topic(&self) -> &String {
        &self.base_topic
    }

    pub async fn publish(&mut self, data: MqttData, retain: bool) -> Result<(), MqttBridgeError> {
        Ok(self.client.publish(data.topic, QoS::AtLeastOnce, retain, data.payload).await?)
    }

    async fn subscribe_to_commands(&mut self) -> Result<(), MqttBridgeError> {
        let cmd_subscription_topic = format!("{}/cmnd/+", self.base_topic);
        self.client.subscribe(cmd_subscription_topic, QoS::ExactlyOnce).await?;
        Ok(())
    }

    pub async fn publish_online(&mut self) -> Result<(), MqttBridgeError> {
        self.client
            .publish(state_topic(&self.base_topic), QoS::AtLeastOnce, true, ONLINE_PAYLOAD)
            .await?;

        Ok(())
    }

    pub async fn publish_offline(&mut self) -> Result<(), MqttBridgeError> {
        self.client
            .publish(state_topic(&self.base_topic), QoS::AtLeastOnce, true, OFFLINE_PAYLOAD)
            .await?;

        Ok(())
    }

    async fn handle_mqtt_message(&mut self, ev: Event) -> Result<Option<MqttData>, MqttBridgeError> {
        match ev {
            Event::Incoming(event) => match event {
                Packet::ConnAck(data) => {
                    if data.code == ConnectReturnCode::Success {
                        if !data.session_present {
                            log::info!("Subscribe to mqtt commands");
                            self.subscribe_to_commands().await?;
                        } else {
                            log::debug!("Session still active, no need to resubsribe");
                        }
                    } else {
                        log::error!("MQTT connection refused: {:?}", data.code);
                    }
                }
                Packet::Publish(publ) => {
                    if publ.retain {
                        let message = MqttData::new(
                            String::from_utf8_lossy(&publ.topic).into_owned(),
                            String::from_utf8_lossy(&publ.payload).into_owned(),
                        );
                        log::warn!("Ignored retained MQTT command: {}", crate::commands::details(&self.base_topic, &message));
                        return Ok(None);
                    }
                    return Ok(Some(MqttData {
                        topic: from_mqtt_string(&publ.topic)?,
                        payload: from_mqtt_string(&publ.payload)?,
                    }));
                }
                //Packet::Connect(_) => todo!(),
                // Packet::PubAck(_) => todo!(),
                // Packet::PubRec(_) => todo!(),
                // Packet::PubRel(_) => todo!(),
                // Packet::PubComp(_) => todo!(),
                // Packet::Subscribe(_) => todo!(),
                // Packet::SubAck(_) => todo!(),
                // Packet::Unsubscribe(_) => todo!(),
                // Packet::UnsubAck(_) => todo!(),
                // Packet::PingReq => todo!(),
                // Packet::PingResp => todo!(),
                // Packet::Disconnect => todo!(),
                _ => {}
            },
            _ => {}
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use rumqttc::TlsConfiguration;
    use std::io::Write;

    fn config() -> MqttConfig {
        MqttConfig {
            server: "localhost".into(),
            port: 1883,
            ca_cert: None,
            insecure: false,
            client_id: "tls-test".into(),
            user: String::new(),
            password: String::new(),
            base_topic: "daikin".into(),
        }
    }

    #[test]
    fn selects_plaintext_or_tls_transport() {
        assert!(matches!(config().transport().expect("default transport"), Transport::Tcp));
        let cfg = MqttConfig { insecure: true, ..config() };
        assert!(matches!(cfg.transport().expect("insecure TLS transport"), Transport::Tls(_)));

        let mut file = tempfile::NamedTempFile::new().expect("create CA file");
        // Certificate parsing is handled by rumqttc when connecting.
        file.write_all(b"test CA bytes").expect("write CA file");
        let cfg = MqttConfig {
            ca_cert: Some(file.path().to_owned()),
            ..config()
        };
        let mqtt = MqttConnection::new(cfg).expect("create certificate-based connection");
        let Transport::Tls(TlsConfiguration::Simple { ca, .. }) = mqtt.eventloop.options.transport() else {
            panic!("expected certificate-based TLS");
        };
        assert_eq!(ca, b"test CA bytes");
    }

    #[test]
    fn rejects_conflicting_tls_options_and_unreadable_certificates_at_startup() {
        let dir = tempfile::tempdir().expect("create test directory");
        let path = dir.path().join("missing-ca.pem");
        for insecure in [false, true] {
            let cfg = MqttConfig {
                ca_cert: Some(path.clone()),
                insecure,
                ..config()
            };
            let err = MqttConnection::new(cfg).err().expect("invalid configuration must fail at startup");
            let message = err.to_string();
            if insecure {
                assert!(message.contains("mutually exclusive"));
            } else {
                assert!(message.contains("Failed to read MQTT CA certificate"));
                assert!(message.contains(&path.display().to_string()));
            }
        }
    }

    #[tokio::test]
    async fn accepts_live_commands_but_ignores_retained_replays() {
        let mut mqtt = MqttConnection::new(MqttConfig {
            server: "localhost".into(),
            port: 1883,
            ca_cert: None,
            insecure: false,
            client_id: "command-test".into(),
            user: String::new(),
            password: String::new(),
            base_topic: "house/daikin".into(),
        })
        .expect("create MQTT connection");
        let topic = "house/daikin/cmnd/climate_mode";
        for retained in [false, true] {
            // MQTT 5 QoS 0 publish with an empty properties section.
            let mut packet = vec![0x30 | u8::from(retained), (topic.len() + 6) as u8];
            packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            packet.extend_from_slice(topic.as_bytes());
            packet.push(0);
            packet.extend_from_slice(b"off");
            let event = Event::Incoming(Packet::read(&mut BytesMut::from(packet.as_slice()), None).expect("publish packet"));
            let message = mqtt.handle_mqtt_message(event).await.expect("MQTT handling");
            if retained {
                assert!(message.is_none());
            } else {
                let message = message.expect("live command");
                assert_eq!(message.topic, topic);
                assert_eq!(message.payload, "off");
            }
        }
    }
}
