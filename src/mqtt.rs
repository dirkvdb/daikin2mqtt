use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use rumqttc::{
    tokio_rustls::rustls::{
        self,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
        pki_types::{CertificateDer, ServerName, UnixTime},
        ClientConfig, DigitallySignedStruct, SignatureScheme,
    },
    Outgoing, Transport,
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
    fn connection_details(&self) -> String {
        let tls = if self.ca_cert.is_some() {
            "verified"
        } else if self.insecure {
            "insecure"
        } else {
            "disabled"
        };
        format!(
            "broker={:?}, port={}, client_id={:?}, username={:?}, base_topic={:?}, tls={tls}, ca_cert={:?}",
            self.server, self.port, self.client_id, self.user, self.base_topic, self.ca_cert,
        )
    }

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
    connected: Arc<AtomicBool>,
    availability_payload: &'static str,
}

impl Drop for MqttConnection {
    fn drop(&mut self) {
        self.connected.store(false, Ordering::Relaxed);
    }
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
        log::info!("MQTT connection configuration: {}", cfg.connection_details());
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

        log::info!("MQTT client created; broker connection pending");
        Ok(MqttConnection {
            client,
            eventloop,
            base_topic: cfg.base_topic,
            connected: Arc::new(AtomicBool::new(false)),
            availability_payload: OFFLINE_PAYLOAD,
        })
    }

    pub async fn poll(&mut self) -> Result<Option<MqttData>, MqttBridgeError> {
        match self.eventloop.poll().await {
            Ok(msg) => self.handle_mqtt_message(msg).await,
            Err(err) => {
                self.connected.store(false, Ordering::Relaxed);
                let (broker, port) = self.eventloop.options.broker_address();
                log::warn!("MQTT connection error: broker={broker:?}, port={port}, error={err}");
                Err(err.into())
            }
        }
    }

    pub(crate) fn connection_status(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.connected)
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
        self.publish_availability(ONLINE_PAYLOAD).await
    }

    pub async fn publish_offline(&mut self) -> Result<(), MqttBridgeError> {
        self.publish_availability(OFFLINE_PAYLOAD).await
    }

    async fn publish_availability(&mut self, payload: &'static str) -> Result<(), MqttBridgeError> {
        self.client.publish(state_topic(&self.base_topic), QoS::AtLeastOnce, true, payload).await?;
        self.availability_payload = payload;
        Ok(())
    }

    async fn handle_mqtt_message(&mut self, ev: Event) -> Result<Option<MqttData>, MqttBridgeError> {
        match ev {
            Event::Incoming(event) => match event {
                Packet::ConnAck(data) => {
                    self.connected.store(data.code == ConnectReturnCode::Success, Ordering::Relaxed);
                    if data.code == ConnectReturnCode::Success {
                        let (broker, port) = self.eventloop.options.broker_address();
                        log::info!(
                            "MQTT connection established: broker={broker:?}, port={port}, session_present={}",
                            data.session_present
                        );
                        // The broker may retain our offline will after a disconnect.
                        // Restore cloud availability, not just MQTT connectivity.
                        self.publish_availability(self.availability_payload).await?;
                        if !data.session_present {
                            log::info!("Subscribe to mqtt commands");
                            self.subscribe_to_commands().await?;
                        } else {
                            log::debug!("Session still active, no need to resubsribe");
                        }
                    } else {
                        let (broker, port) = self.eventloop.options.broker_address();
                        log::error!("MQTT connection refused: broker={broker:?}, port={port}, reason={:?}", data.code);
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
                Packet::Disconnect(_) => {
                    self.connected.store(false, Ordering::Relaxed);
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
                _ => {}
            },
            Event::Outgoing(Outgoing::Disconnect) => {
                self.connected.store(false, Ordering::Relaxed);
            }
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

    fn incoming(packet: &[u8]) -> Event {
        Event::Incoming(Packet::read(&mut BytesMut::from(packet), None).expect("MQTT packet"))
    }

    #[test]
    fn connection_status_is_initially_false_and_shared() {
        let mqtt = MqttConnection::new(config()).expect("create MQTT connection");
        let status = mqtt.connection_status();
        assert!(!status.load(Ordering::Relaxed));
        assert!(Arc::ptr_eq(&status, &mqtt.connection_status()));
    }

    #[tokio::test]
    async fn connection_status_tracks_successful_and_refused_connacks() {
        let mut mqtt = MqttConnection::new(config()).expect("create MQTT connection");
        let status = mqtt.connection_status();
        for (session_present, code, connected) in [(0, 0, true), (0, 0x87, false), (1, 0, true)] {
            mqtt.handle_mqtt_message(incoming(&[0x20, 0x03, session_present, code, 0x00]))
                .await
                .expect("handle CONNACK");
            assert_eq!(status.load(Ordering::Relaxed), connected);
        }
    }

    #[tokio::test]
    async fn reconnect_republishes_retained_cloud_availability() {
        use rumqttc::v5::mqttbytes::Error as MqttError;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn read_packet(stream: &mut tokio::net::TcpStream, buffer: &mut BytesMut) -> Packet {
            loop {
                match Packet::read(buffer, None) {
                    Ok(packet) => return packet,
                    Err(MqttError::InsufficientBytes(_)) => assert!(stream.read_buf(buffer).await.expect("read MQTT packet") > 0),
                    Err(err) => panic!("invalid MQTT packet: {err}"),
                }
            }
        }

        for session_present in [0, 1] {
            for online in [None, Some(true), Some(false)] {
                let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.expect("bind test broker");
                let mut mqtt = MqttConnection::new(MqttConfig {
                    server: "127.0.0.1".into(),
                    port: listener.local_addr().expect("broker address").port(),
                    ..config()
                })
                .expect("create MQTT connection");
                let mut expected = Vec::new();
                if let Some(online) = online {
                    mqtt.publish_online().await.expect("queue cloud online status");
                    expected.push(ONLINE_PAYLOAD);
                    if !online {
                        mqtt.publish_offline().await.expect("queue cloud offline status");
                        expected.push(OFFLINE_PAYLOAD);
                    }
                }
                let payload = if online == Some(true) { ONLINE_PAYLOAD } else { OFFLINE_PAYLOAD };
                expected.push(payload);
                let (synced_tx, synced_rx) = tokio::sync::oneshot::channel();
                let broker = async {
                    let (mut stream, _) = listener.accept().await.expect("accept initial MQTT connection");
                    let mut buffer = BytesMut::new();
                    assert!(matches!(read_packet(&mut stream, &mut buffer).await, Packet::Connect(..)));
                    stream.write_all(&[0x20, 0x03, 0, 0, 0]).await.expect("send initial CONNACK");
                    for expected_payload in expected {
                        loop {
                            if let Packet::Publish(publish) = read_packet(&mut stream, &mut buffer).await {
                                assert_eq!(publish.topic.as_ref(), b"daikin/state");
                                assert_eq!(publish.payload.as_ref(), expected_payload.as_bytes());
                                assert!(publish.retain);
                                assert_eq!(publish.qos, QoS::AtLeastOnce);
                                let id = publish.pkid.to_be_bytes();
                                stream.write_all(&[0x40, 0x02, id[0], id[1]]).await.expect("send PUBACK");
                                break;
                            }
                        }
                    }
                    // Confirm all PUBACKs were processed before disconnecting, so an
                    // in-flight retransmission cannot masquerade as a fresh status.
                    stream.write_all(&[0x30, 7, 0, 4, b's', b'y', b'n', b'c', 0]).await.expect("send sync marker");
                    synced_rx.await.expect("client processed initial PUBACKs");
                    drop(stream);

                    let (mut stream, _) = listener.accept().await.expect("accept reconnected MQTT client");
                    let mut buffer = BytesMut::new();
                    assert!(matches!(read_packet(&mut stream, &mut buffer).await, Packet::Connect(..)));
                    stream.write_all(&[0x20, 0x03, session_present, 0, 0]).await.expect("send reconnect CONNACK");
                    loop {
                        if let Packet::Publish(publish) = read_packet(&mut stream, &mut buffer).await {
                            assert_eq!(publish.topic.as_ref(), b"daikin/state");
                            assert_eq!(publish.payload.as_ref(), payload.as_bytes());
                            assert!(publish.retain);
                            assert_eq!(publish.qos, QoS::AtLeastOnce);
                            break;
                        }
                    }
                };
                let drive_mqtt = async {
                    let mut synced_tx = Some(synced_tx);
                    loop {
                        if let Ok(Some(message)) = mqtt.poll().await {
                            assert_eq!(message.topic, "sync");
                            synced_tx.take().expect("single sync marker").send(()).expect("notify test broker");
                        }
                    }
                };
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = broker => {}
                        _ = drive_mqtt => panic!("MQTT event loop unexpectedly stopped"),
                    }
                })
                .await
                .expect("availability is republished promptly after reconnect");
            }
        }
    }

    #[tokio::test]
    async fn connection_status_clears_on_incoming_and_outgoing_disconnect() {
        let mut mqtt = MqttConnection::new(config()).expect("create MQTT connection");
        let status = mqtt.connection_status();
        for event in [incoming(&[0xe0, 0x02, 0x00, 0x00]), Event::Outgoing(Outgoing::Disconnect)] {
            mqtt.handle_mqtt_message(incoming(&[0x20, 0x03, 0x00, 0x00, 0x00]))
                .await
                .expect("handle CONNACK");
            assert!(status.load(Ordering::Relaxed));
            mqtt.handle_mqtt_message(event).await.expect("handle disconnect");
            assert!(!status.load(Ordering::Relaxed));
        }
    }

    #[tokio::test]
    async fn connection_status_clears_on_poll_error() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.expect("bind test broker");
        let mut mqtt = MqttConnection::new(MqttConfig {
            server: "127.0.0.1".into(),
            port: listener.local_addr().expect("broker address").port(),
            ..config()
        })
        .expect("create MQTT connection");
        let status = mqtt.connection_status();
        mqtt.handle_mqtt_message(incoming(&[0x20, 0x03, 0x00, 0x00, 0x00]))
            .await
            .expect("handle CONNACK");
        assert!(status.load(Ordering::Relaxed));

        let (_, result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                async {
                    let (stream, _) = listener.accept().await.expect("accept MQTT connection");
                    drop(stream);
                },
                mqtt.poll()
            )
        })
        .await
        .expect("poll must finish after broker closes connection");
        assert!(result.is_err());
        assert!(!status.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn connection_status_clears_when_connection_is_dropped() {
        let mut mqtt = MqttConnection::new(config()).expect("create MQTT connection");
        let status = mqtt.connection_status();
        mqtt.handle_mqtt_message(incoming(&[0x20, 0x03, 0x00, 0x00, 0x00]))
            .await
            .expect("handle CONNACK");
        assert!(status.load(Ordering::Relaxed));
        drop(mqtt);
        assert!(!status.load(Ordering::Relaxed));
    }

    #[test]
    fn connection_details_include_configuration_but_not_the_password() {
        let mut cfg = MqttConfig {
            server: "mqtt.lan".into(),
            port: 8883,
            client_id: "test-client".into(),
            user: "iot".into(),
            password: "must-not-appear-in-logs".into(),
            base_topic: "house/daikin".into(),
            ..config()
        };
        for (ca_cert, insecure, tls) in [
            (None, false, "disabled"),
            (None, true, "insecure"),
            (Some(PathBuf::from("/run/secrets/mqtt-ca.crt")), false, "verified"),
        ] {
            cfg.ca_cert = ca_cert;
            cfg.insecure = insecure;
            let details = cfg.connection_details();
            assert!(details.contains("broker=\"mqtt.lan\""));
            assert!(details.contains("port=8883"));
            assert!(details.contains("client_id=\"test-client\""));
            assert!(details.contains("username=\"iot\""));
            assert!(details.contains("base_topic=\"house/daikin\""));
            assert!(details.contains(&format!("tls={tls}")));
            if cfg.ca_cert.is_some() {
                assert!(details.contains("/run/secrets/mqtt-ca.crt"));
            }
            assert!(!details.contains(&cfg.password));
            assert!(!details.contains("password"));
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
